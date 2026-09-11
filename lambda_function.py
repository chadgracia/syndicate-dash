"""
syndicate-dash / lambda_function.py
────────────────────────────────────
Demand Board — read-only dashboard: one row per company with interested
buyers, tiered into QP / Accredited / Unknown.

Data sources (S3 full-pipeline-cache), read fresh on every request:
  - people.json           -> {"people": [ {..., "custom_fields": {...}}, ... ]}
                              (see chadgracia/portfolio-deploy/build_people_index.py
                              and chadgracia/deal-notifier/lambda_function.py)
  - interest_people.json  -> {"buy": {company_name: [person_id, ...]}, "last_updated": ...}
                              (see chadgracia/portfolio-deploy and chadgracia/web-bid
                              lambda_function.py, both read this exact shape)
  - deals.json             -> {"deals": [ {..., "custom_fields": {...}, "deal_stage": {...},
                              "company": {...}, "is_archived": ...}, ... ]}
                              (see chadgracia/daily-brief/lambda_function.py, which reads
                              this exact shape and enumerates the stage id/label constants
                              used below)

Excluded companies: EXCLUDED_COMPANIES (below) is filtered out of the board,
case-insensitive. Edited manually.

Sellers column: count of each company's live sell-side deals in deals.json —
custom_label_1958 (deal side) contains option 5011675 (Sell), and deal_stage
is one of LIVE_SELL_STAGE_IDS (verbatim from chadgracia/daily-brief's stage
constants: Firm, Matched, Inquiry, Hold, Confirm, LOI Signed, Transfer
Notice, SPA Signed), and the deal is not archived.

My Deals tab: one row per deals.json deal linked to the viewing tenant's
person_id (auto-enrolled — see _resolve_tenant). Linkage is read the same way
chadgracia/daily-brief's _deal_people does — a deal's "people" list of
dicts (each carrying "id"), falling back to the flat "person_ids" list when
"people" isn't present. No stage/archived filtering here (unlike Sellers):
every deal the person is linked to shows, whatever its stage.

portfolio-deploy/build_people_index.py's people_index.json only carries
email/first_name/name (no custom_fields), so it can't be used for tiering —
this Lambda parses the full people.json instead.

Tier logic per buyer, field IDs/options confirmed verbatim in
chadgracia/loi-sign (QP) and chadgracia/portfolio-deploy (IQF):
  - QP:         custom_label_3923758 (Investor Level) contains option 6950564
  - Accredited: not QP, and custom_label_3763008 (IQF Status) contains
                6496840 (Yes) or 6596073 (Unnecessary)
  - Unknown:    everyone else (including buyer IDs missing from people.json)

Caching: S3 is checked fresh on every request via a cheap head_object on all
three files. The (expensive) 113MB people.json parse only happens again when
any file's LastModified changes; only the small computed per-company table is
kept in the module-level cache between invocations, never the parsed people
list.

Access / identity: two doors, either grants access —
  - Admin: query param `key` equals os.environ["ADMIN_KEY"]. Full access;
    optional &view_as=<email> renders exactly what that tenant sees
    (including the not-enabled page) without needing their cookie.
  - Tenant SSO: same signed-handoff scheme as chadgracia/trades ->
    chadgracia/portfolio-deploy. A `?sso=<token>` verifies against
    IDENTITY_SECRET (see _verify_sso_handoff, ported from
    portfolio-deploy's _verify_sso_handoff) and, if valid, sets the same
    durable `gg_id` identity cookie trades itself mints (see
    _make_identity_cookie / _read_identity_email, ported from
    chadgracia/trades/lambda_function.py) before redirecting to a clean
    URL. Later requests read identity from that cookie. The verified email
    must auto-enroll as a tenant (see _resolve_tenant below) or the tenant
    sees a "not enabled" page instead of the board. Neither door open ->
    access-denied page, no data rendered/fetched.
GET only, except one route: POST ?action=update_intro, admin-only (ADMIN_KEY
in the POST body — the session cookie alone never authorizes a write), lets
an admin edit a deal's Intro Status / Next Steps / Buyer Notes. See
_handle_update_intro. No other route ever writes anything — no other S3
writes, no other CRM writes, no email.
"""

import base64
import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
import boto3
from boto3.dynamodb.conditions import Key

BUCKET = "full-pipeline-cache"
PEOPLE_KEY = "people.json"
INTEREST_KEY = "interest_people.json"
DEALS_KEY = "deals.json"
# Turn 24: the buyer page's "About the firm" card. New to this codebase --
# no other Lambda in this org has been confirmed to read companies.json, so
# its shape is assumed (on the same {"companies": [...]} + "id" convention
# every other snapshot in this bucket uses) rather than verified. See
# get_company_record, which fails soft (returns None, card omits itself)
# on any shape mismatch rather than erroring.
COMPANIES_KEY = "companies.json"

INVESTOR_LEVEL_FIELD = "custom_label_3923758"
QP_ID = 6950564
IQF_FIELD = "custom_label_3763008"
IQF_OK_IDS = {6496840, 6596073}

# "Substantive" (Investor LEVEL option, added 2026/02/26) — verified via a
# one-time person_custom_field_labels fetch against field 3923758. It is
# deliberately never QP_ID and never touches IQF_FIELD, so classify_person
# already falls through to "unknown" for it; SUBSTANTIVE_ID exists to make
# that exclusion explicit and testable rather than an accident of the two
# checks below never mentioning it.
SUBSTANTIVE_ID = 7162165

# Companies filtered out of the board entirely, case-insensitive. Edit this
# set by hand as needed — no other code changes required.
EXCLUDED_COMPANIES = {
    "Flexport", "Zipline", "Kraken", "Eat Just", "Thrasio", "Indigo", "Oyo", "Headspace",
}
_EXCLUDED_COMPANIES_LOWER = {c.lower() for c in EXCLUDED_COMPANIES}

# Deal side (custom_label_1958) option id for "Sell", and the deal_stage ids
# that represent live (pre-close) sell-side pipeline activity. Field id, the
# Sell option id, and every stage id/label below are verbatim from
# chadgracia/daily-brief/lambda_function.py, which reads deals.json in
# production. There may be additional stage ids present in deals.json (e.g.
# closed-won/closed-lost) that no Lambda in this org has needed to name yet;
# this list is a whitelist of only the known live stages, so an unrecognized
# stage is excluded by default rather than risking a won/lost/dead stage
# being counted as a live seller.
DEAL_SIDE_FIELD = "custom_label_1958"
DEAL_SIDE_SELL_ID = 5011675
DEAL_SIDE_BUY_ID = 5077819

STAGE_FIRM = 111800
STAGE_MATCHED = 2381534
STAGE_INQUIRY = 2109142
STAGE_HOLD = 2094373
STAGE_CONFIRM = 2388323
STAGE_LOI_SIGNED = 2517909
STAGE_TRANSFER_NOTICE = 2533998
STAGE_SPA_SIGNED = 2381535

LIVE_SELL_STAGE_IDS = {
    STAGE_FIRM, STAGE_MATCHED, STAGE_INQUIRY, STAGE_HOLD,
    STAGE_CONFIRM, STAGE_LOI_SIGNED, STAGE_TRANSFER_NOTICE, STAGE_SPA_SIGNED,
}

# Hold/Cancel/Reactivate + closed-stage detection (My Deals). Given
# directly as "verified from live deal_stages API" — taken on trust the
# way every other bare id supplied this way in this file has been, since
# this session has no live Pipeline access of its own to re-confirm them.
# HOLD_STAGE_ID is exactly STAGE_HOLD above (same id, same already-live
# stage — it stays in LIVE_SELL_STAGE_IDS so a Held deal is still fetched
# and shown, in My Deals' own "On Hold" section; My Deals' "Total in
# pipeline" sum excludes it explicitly since that total only counts the
# active section). OBSOLETE_STAGE_ID is verified verbatim in
# chadgracia/deal-update-form (OBSOLETE_STAGE_ID = 2348038).
# Neither WON_STAGE_IDS nor LOST_STAGE_IDS was ever in LIVE_SELL_STAGE_IDS
# to begin with, so excluding them from "live" needs no separate check —
# they were simply never in the live whitelist.
HOLD_STAGE_ID = STAGE_HOLD
OBSOLETE_STAGE_ID = 2348038
WON_STAGE_IDS = {111802, 2379321}
LOST_STAGE_IDS = {111801, 2379322}
# Trade Broken -- given directly (same trust basis as every other bare
# stage id in this file), a dead-buy-side stage distinct from Lost.
STAGE_TRADE_BROKEN = 2486672

# Labels for every stage id this org's code has ever named (verbatim from
# chadgracia/daily-brief's STAGE_LABELS). An id outside this map (e.g. a
# closed-won/closed-lost stage no Lambda has needed to name) falls back to
# its raw id string rather than guessing a label.
STAGE_LABELS = {
    STAGE_FIRM: "FIRM",
    STAGE_MATCHED: "MATCHED",
    STAGE_INQUIRY: "INQUIRY",
    STAGE_HOLD: "HOLD",
    STAGE_CONFIRM: "CONFIRM",
    STAGE_LOI_SIGNED: "LOI SIGNED",
    STAGE_TRANSFER_NOTICE: "TRANSFER NOTICE",
    STAGE_SPA_SIGNED: "SPA SIGNED",
}

# My Deals tab columns — every field id verified present on deal records in
# chadgracia/daily-brief/lambda_function.py (CF_TICKET_MIN/MAX, CF_GROSS,
# CF_STRUCTURE) and chadgracia/deal-notifier/lambda_function.py
# (LAYERS_FIELD/LAYERS_MAP). "Size" has no field id given, so it's mapped to
# the Ticket Size min/max fields — the only deal-level "size" fields any
# Lambda in this org reads. Deadline (custom_label_4006402) does not appear
# anywhere in portfolio-deploy, deal-notifier, loi-sign, web-bid, trades, or
# daily-brief, so it's omitted rather than guessing its shape.
TICKET_MIN_FIELD = "custom_label_3065488"
TICKET_MAX_FIELD = "custom_label_3064645"
GROSS_FIELD = "custom_label_3064339"
NET_FIELD = "custom_label_3064369"  # verified verbatim as CF_NET in chadgracia/daily-brief
STRUCTURE_FIELD = "custom_label_3064360"
LAYERS_FIELD = "custom_label_3938743"

STRUCTURE_LABELS = {6250090: "Direct", 5077906: "Fund"}
LAYERS_MAP = {7000228: "1-Layer", 7000229: "2-Layer", 7000230: "3-Layer"}

# Fund Exemption: no repo this org's code lives in reads
# custom_label_4006089, but the field id and all three option labels were
# given directly (same trust basis as the earlier bare "In Process" option
# ids) — implemented as instructed since a missing/differently-shaped
# field just omits the line rather than showing anything wrong.
EXEMPTION_FIELD = "custom_label_4006089"
EXEMPTION_LABELS = {
    7200027: "3(c)(1) — accredited investors",
    7200028: "3(c)(7) — qualified purchasers only",
    7201486: "Other",
}

# Person-level Ticket Size multi-select, for the company page's Buyer Demand
# tiles. Field id and every entry id -> (min, max) dollar tier verified
# verbatim in chadgracia/deal-notifier's TICKET_SIZE_MAP and
# chadgracia/portfolio-deploy's WL_TICKET_SIZE_MAP (identical maps in both).
TICKET_SIZE_FIELD = "custom_label_3052210"
TICKET_SIZE_MAP = {
    6870210: (100_000, 250_000),
    6631962: (100_000, 250_000),
    5014552: (251_000, 999_000),
    5014555: (1_000_000, 5_000_000),
    5014558: (5_000_000, 10_000_000),
    5014561: (10_000_000, 25_000_000),
    5014564: (25_000_000, 50_000_000),
    5014567: (50_000_000, 100_000_000),
    5014570: (100_000_000, None),
}

# Company page buyer tile anonymization: 32 chars, no I/L/O/0/1 (ambiguous
# on screen).
ANON_ALPHABET = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"

REF_LABELS = {"mydeals": "My Deals", "intros": "Active Intros", "demand": "Demand Board"}

FEATURE_REQUEST_EMAIL = "cgracia@rainmakersecurities.com"

# Deal-update-form ("Update / Cancel" button): chadgracia/deal-update-form,
# routed at this CloudFront URL. Token format (HMAC-SHA256 of str(deal_id),
# urlsafe base64, no padding) and query shape (?deal_id=<id>&token=<token>)
# copied verbatim from that repo's own make_token/verify_token, matching
# exactly how chadgracia/deal-nudge's form_url() already mints the same
# link. HMAC_SECRET must be the same secret value configured on
# deal-update-form's own Lambda — when it's unset here, _deal_update_form_url
# returns None and callers render without the button rather than a broken
# link.
DEAL_UPDATE_FORM_URL = "https://desk.graciagroup.com/update/"


def _deal_update_form_url(deal_id):
    secret = os.environ.get("HMAC_SECRET")
    if not secret:
        return None
    sig = hmac.new(secret.encode(), f"{deal_id}".encode(), hashlib.sha256).digest()
    token = base64.urlsafe_b64encode(sig).decode().rstrip("=")
    return f"{DEAL_UPDATE_FORM_URL}?deal_id={deal_id}&token={token}"


def _my_deal_action_chip_html(deal_id, company_name, is_overdue, stalled, visibility_state,
                               key=None, view_as=None):
    """Next Steps: ONE specific action chip per row, chosen by priority
    (highest first): a) deadline passed (red, Update link) > b) terms
    incomplete (red, Update link) > c) nudge buyers (amber: a stalled
    intro — turn 22 dropped the overdue-follow-up half of this
    condition, see _company_stats) > d) ID required (red, CEF form
    link) > e) sign agreement (amber, agreement template link). At most
    one of {b, d, e} ever applies — they're three of the "not live"
    visibility_state values and that state machine is first-match-wins
    (see _my_deal_visibility_state) — while a/c are independent
    conditions that can co-occur with any of them or each other. When
    more than one candidate applies, the top-priority chip carries a
    title/tooltip naming the rest."""
    candidates = []
    if is_overdue:
        update_url = _deal_update_form_url(deal_id)
        if update_url:
            candidates.append(("update deadline", "overdue", "Update deadline &rarr;", update_url, True))
    if visibility_state == "terms_incomplete":
        update_url = _deal_update_form_url(deal_id)
        if update_url:
            candidates.append(("provide deal terms", "terms", "Provide deal terms &rarr;", update_url, True))
    if stalled:
        href = _company_href(company_name, "mydeals", key, view_as)
        candidates.append(("nudge buyers", "nudge", "Nudge buyers &rarr;", href, False))
    if visibility_state == "id_required":
        candidates.append(("id required", "id-required", "ID required &rarr;", CEF_FORM_URL, True))
    if visibility_state == "agreement_unsigned":
        candidates.append(("sign agreement", "sign", "Sign agreement &rarr;", AGENT_AGREEMENT_DOC_URL, True))
    if not candidates:
        return ""
    phrase, css_class, label, href, new_tab = candidates[0]
    rest = [c[0] for c in candidates[1:]]
    title_attr = f' title="{_esc("Also: " + ", ".join(rest))}"' if rest else ""
    target_attr = ' target="_blank" rel="noopener noreferrer"' if new_tab else ""
    return f'<a class="action-chip {css_class}"{title_attr} href="{href}"{target_attr}>{label}</a>'


def _update_cancel_button_html(deal_id, label="Update / Cancel"):
    """label defaults to "Update / Cancel" everywhere except My Deals,
    which now has its own separate one-click Hold/Cancel REQUEST controls
    alongside this link and passes label="Update" so the three don't read
    as duplicates — same minted link, unchanged."""
    url = _deal_update_form_url(deal_id)
    if not url:
        return ""
    return (f'<a class="update-cancel-btn" href="{url}" target="_blank" rel="noopener noreferrer">'
            f'{_esc(label)}</a>')


# Public buyer-facing deal detail page. Verified verbatim in
# chadgracia/trades (the link the live marketplace grid itself puts on
# every deal id: f"https://trades.graciagroup.com/deal/{deal['id']}"),
# and confirmed as the route chadgracia/CRMDealDetails serves (it reads
# deal_id from either the query string or a path param, fetches the deal
# from Pipeline unconditionally, and renders — no auth/visibility gate,
# so every deal id resolves to a real page; there is no "no public page"
# case to fall back from). Note this is trades.graciagroup.com, not
# desk.graciagroup.com — the two are separate CloudFront routes serving
# different Lambdas (desk.graciagroup.com/update/ is deal-update-form,
# desk.graciagroup.com/bid/ is trades' bid submission; trades.graciagroup.com/deal/
# is CRMDealDetails).
def _deal_public_url(deal_id):
    return f"https://trades.graciagroup.com/deal/{deal_id}"


# Copy-icon markup (the double-rectangle SVG) verified verbatim in
# chadgracia/trades, which uses it for its own "copy deal ID" button —
# reused as-is here rather than inventing a different icon. No checkmark
# state (trades swaps in a check SVG + "copied" class on click; this one
# just copies, per instruction).
COPY_ICON_SVG = ('<svg viewBox="0 0 16 16" aria-hidden="true" focusable="false">'
                  '<rect x="5.5" y="5.5" width="8" height="8" rx="1.5"></rect>'
                  '<path d="M10.5 3.5v-1a1 1 0 0 0-1-1h-7a1 1 0 0 0-1 1v7a1 1 0 0 0 1 1h1"></path>'
                  '</svg>')


def _copy_id_button_html(value):
    return (f'<button type="button" class="copy-id" data-copy-url="{_esc(value)}" '
            f'title="Copy link" aria-label="Copy deal link">{COPY_ICON_SVG}</button>')


# Deal Card section. Agent Agreement field id, and the "Yes" option ids,
# verified verbatim in chadgracia/daily-brief (CF_AGENT_AGREEMENT,
# AGENT_YES_OPTS); storage shape (scalar OR list) confirmed in
# chadgracia/deal-notifier, which explicitly branches on isinstance(...,
# list) before comparing. The "In Process" option ids (6354283, 6354280)
# were given directly and don't appear in any repo's code, so they're taken
# on trust the way every other bare option id supplied this way has been.
AGENT_AGREEMENT_FIELD = "custom_label_3714334"
AGENT_ENGAGED_OPTS = {6354274, 6354277}       # Yes - Buyside, Yes - Sellside
AGENT_IN_PROCESS_OPTS = {6354283, 6354280}

# Fee fields verified verbatim in chadgracia/loi-sign and
# chadgracia/deal-notifier (both define the same three field ids under the
# same names). Partner Fee (custom_label_3940561) is deliberately never
# read — it's excluded from the card by omission, not filtered out.
MGMT_FEE_FIELD = "custom_label_3940558"
CARRY_FIELD = "custom_label_3940559"
SELLER_FEE_FIELD = "custom_label_3940560"

# Deadline: no code in any repo this org has read (portfolio-deploy,
# deal-notifier, loi-sign, web-bid, trades, daily-brief) ever touches
# custom_label_4006402 — unlike every other field id on this page, this one
# has no independent verification. Implemented per explicit instruction
# (date field, slash-format dates); it degrades safely (line omitted) if
# the field turns out to be absent or differently shaped.
DEADLINE_FIELD = "custom_label_4006402"

# Transactor Type: field id and the Natural Person option id verified
# verbatim in both chadgracia/loi-sign and chadgracia/portfolio-deploy.
# Neither repo (nor any other) has a label for any OTHER option on this
# field — loi-sign's INDIVIDUAL_TYPE_IDS additionally names 6716196
# (Employee Holder), 6892622 (Employee Holder - VIP), and 6484809
# (Ex-Employee Holder) as individual-type ids, but without confirming
# they should read as "Natural Person" on this page, so per instruction
# only 6484810 is labeled and everything else — including those three —
# is left blank.
TRANSACTOR_TYPE_FIELD = "custom_label_3759163"
NATURAL_PERSON_ID = 6484810

# Full Transactor Type option map, verified via a one-time
# person_custom_field_labels fetch against field 3759163 (superseding the
# single-option guess above, which is now just one entry in this map).
TRANSACTOR_TYPE_LABELS = {
    6484815: "Corporation",
    6716196: "Employee Holder",
    6892622: "Employee Holder - VIP",
    6484809: "Ex-Employee Holder",
    6484811: "Family Office",
    6484810: "Natural Person",
    6484812: "Institution",
    6577160: "Intermediary - Co-Broker",
    6888332: "Intermediary - Foreign Finder",
    6888333: "Intermediary - Other",
    6859893: "Syndicator",
    6484808: "VC or PE Fund",
    7037492: "Hedge Fund",
    6484813: "Wealth Advisor",
}

# Client Engagement Form (CEF - ID)? — person-level field, verified via the
# same one-time fetch. Each of the four options drives a distinct badge
# state (see _cef_badge_html): Yes -> on file, Pending -> pending,
# No/unset -> missing, N/A -> no badge at all.
CEF_FIELD = "custom_label_3796440"
CEF_NO_ID = 6600513
CEF_PENDING_ID = 6600514
CEF_YES_ID = 6600515
CEF_NA_ID = 6600516
CEF_FORM_URL = "https://www.rainmakersecurities.com/client-engagement-form-for-entity-persons"
CEF_LABELS = {
    CEF_NO_ID: "No",
    CEF_PENDING_ID: "Pending",
    CEF_YES_ID: "Yes",
    CEF_NA_ID: "N/A",
}

# My Deals visibility badge's amber state (item 3): the Agent Agreement
# document tenants review/sign, given directly.
AGENT_AGREEMENT_DOC_URL = "https://docs.google.com/document/d/1tWiM39QFYDWQSjZ-nWOS8yfbbVnwTjmMRPURmznHHQ0/edit?usp=sharing"

# Accepts (deal-structure preferences a buyer will take) — buyer page's
# Process Signals block (turn 23). Field id and every option id verified
# live by the account owner against real person records (unlike
# TRANSACTOR_TYPE/CEF above, no repo in this org reads this field, so
# there was no prior person_custom_field_labels fetch to cite).
ACCEPTS_FIELD = "custom_label_3998063"
ACCEPTS_LABELS = {
    7177773: "SPV",
    7177774: "2-Layer",
    7177775: "Fees",
    7177776: "Forwards",
    7177777: "Commons",
}

# Turn 24: PitchBook profile link, on the companies.json record's own
# custom_fields (given directly, same trust basis as every other bare
# option/field id supplied this way in this file).
COMPANY_PITCHBOOK_FIELD = "custom_label_3320818"

# Turn 25: three more stage ids, given directly as "verified from the live
# deal_stages API" (same trust basis as every other bare id supplied this
# way in this file — this sandbox has no live Pipeline access of its own to
# re-confirm them). WON_STAGE_IDS (111802 Won Deal, 2379321 Won) already
# existed above for My Deals' "Total closed" sum — reused here rather than
# re-declared.
STAGE_INVOICED = 2456153
STAGE_ROFR = 2426790
STAGE_BLOCKED = 2447751

# "Matched or later" buy-side stages, shared verbatim by the Matched Buyers
# section, the Active Intros tab, and the My Deals Intros counts (every
# consumer goes through get_my_matched_buy_deals / _is_matched_or_later_
# buy_deal, which key on this one set — see MATCHED_OR_LATER_STAGE_IDS's
# only two callers).
#
# Turn 25 replaced the old inferred set (every LIVE_SELL_STAGE_IDS entry
# except Inquiry/Hold — Matched, Firm, Confirm, LOI Signed, Transfer
# Notice, SPA Signed) with this explicit one, given directly as "verified
# from the live deal_stages API": Matched, LOI Signed, Transfer Notice, SPA
# Signed, Confirm, Invoiced, ROFR'd, Blocked, Won Deal, Won. Two changes
# from the old set: FIRM (111800) is now excluded outright (it was
# previously included solely as "everything in LIVE_SELL_STAGE_IDS except
# Inquiry/Hold" — an inference, never itself independently verified), and
# five later-pipeline/closed-won stages are newly included: Invoiced,
# ROFR'd, Blocked, Won Deal, Won — none of which existed in the old
# inferred set at all, since it only ever drew from LIVE_SELL_STAGE_IDS
# (itself a live-pre-close whitelist that was never going to contain a
# closed-won id). Deliberately excluded (unchanged reasoning from the old
# set): Inquiry (2109142), Hold (2094373), Trade Broken (2486672), Obsolete
# (2348038), Lost (111801/2379322).
#
# Deal-stage shape: deals.json's own documented shape (module docstring,
# above — "deal_stage": {...} — verified against chadgracia/daily-brief,
# which reads this exact structure) carries deal_stage as a NESTED OBJECT,
# not a bare name/id scalar; the numeric stage id lives at deal_stage.id.
# _deal_stage_id (below) already keys on that id — stage.get("id") first,
# falling back to a flat deal_stage_id scalar only as a defensive
# secondary path that's never been the primary shape. This file has no
# live S3 access in this sandbox to re-fetch a fresh deals.json snapshot
# and eyeball a raw row, so this is reported from the already-documented,
# already-verified shape every other stage predicate in this file (Live
# Sell, Hold, Obsolete, Won/Lost) already relies on — not a fresh spot
# check of live data.
#
# Deliberately NOT excluding is_archived here (unlike the Sellers column)
# — an archived deal can still carry a real Intro Status value (including
# an explicit "Closed", id 7207587) and needs to stay visible to show it.
# An archived deal with no Intro Status value set derives "Matched" like
# any other empty one (see _default_intro_status) and lands in Pending
# introductions, not "Closed" — is_archived is never treated as a status
# signal.
MATCHED_OR_LATER_STAGE_IDS = {
    STAGE_MATCHED, STAGE_LOI_SIGNED, STAGE_TRANSFER_NOTICE, STAGE_SPA_SIGNED,
    STAGE_CONFIRM, STAGE_INVOICED, STAGE_ROFR, STAGE_BLOCKED,
} | WON_STAGE_IDS

# Intro Status: a brand-new deal dropdown (custom_label_4008329), created
# the same day this was written, so no repo's code can possibly reference
# it yet — this is the first and only place it's read. Option ids given
# directly (same trust basis as every other bare option id in this file).
# If the deals.json snapshot hasn't picked up the field yet, or a given
# deal simply has no value set, _deal_intro_status_id returns None for it
# and the derived default applies — never raises, never crashes.
INTRO_STATUS_FIELD = "custom_label_4008329"

# The seven linear pipeline states share their names 1:1 with STATUS_STEPS
# (unchanged) so the existing strip/pill widgets need no changes beyond
# their data source. The three exit ids are rendered as chips instead —
# never as strip/pill segments — and never appear in STATUS_STEPS/
# STATUS_INDEX.
INTRO_STATUS_LABELS = {
    7207578: "Matched",
    7207579: "Introduced",
    7207580: "NDA Signed",
    7207581: "VDR Link Provided",
    7207582: "Signed Sub Docs",
    7207583: "Wired",
    7207587: "Closed",
    7207584: "Stalled",
    7207585: "Passed",
    7207586: "Withdrawn",
}
INTRO_STATUS_INTRODUCED_ID = 7207579
INTRO_STATUS_STALLED_ID = 7207584
INTRO_STATUS_PASSED_ID = 7207585
INTRO_STATUS_WITHDRAWN_ID = 7207586
INTRO_STATUS_CLOSED_ID = 7207587
EXIT_STATUS_IDS = {INTRO_STATUS_STALLED_ID, INTRO_STATUS_PASSED_ID, INTRO_STATUS_WITHDRAWN_ID}

# Active Intros tenant status editing: the six transitions a tenant may set
# themselves (NDA/VDR/Sub Docs/Wired/Stalled/Passed), and only on a row
# that's already Introduced-or-later (disclosed) -- never Matched,
# Introduced, Withdrawn, or Closed, which stay admin-only. Enforced
# server-side in _handle_update_intro, not just omitted from the tenant's
# dropdown.
TENANT_ALLOWED_STATUS_IDS = {7207580, 7207581, 7207582, 7207583, 7207584, 7207585}

# Milestone tracking (turn 16, item 1; checkbox UI added turn 17): a
# status write that sets one of these five ids also records
# {step: epoch} in the Dynamo intro item's "milestones" map (see
# _dynamo_write_intro_update) -- add-only for a raw status write (the
# still-shared Company page dropdown path), never overwritten once a
# step is first recorded. Stalled/Passed/Withdrawn are deliberately
# absent from this map: setting one of those three never touches
# milestones at all (see _handle_update_intro).
MILESTONE_STATUS_IDS = {
    7207580: "NDA",
    7207581: "VDR",
    7207582: "Sub Docs",
    7207583: "Wired",
    7207587: "Closed",
}
MILESTONE_STEPS = ["NDA", "VDR", "Sub Docs", "Wired", "Closed"]

# step name -> its Pipeline status id, the inverse of MILESTONE_STATUS_IDS.
MILESTONE_STEP_STATUS_ID = {step: sid for sid, step in MILESTONE_STATUS_IDS.items()}

# "Introduced-or-later" by RAW Pipeline Intro Status value (never
# override-aware) -- the six ids that represent genuine forward progress:
# Introduced plus the five MILESTONE_STATUS_IDS ids (NDA/VDR/Sub Docs/
# Wired/Closed). Deliberately excludes "Matched" (7207578, a real
# selectable option, not just the implicit empty default) and all three
# exit ids (Stalled/Passed/Withdrawn) -- those are exits, not progress.
# Used (turn 27) both to gate disclosure of stage-derived closed-out
# intros and, via the raw (pre-override) status id, as one of the two
# "status history" signals in _resolve_intro_status's own exit-status
# disclosure check below.
INTRODUCED_OR_LATER_STATUS_IDS = {INTRO_STATUS_INTRODUCED_ID} | set(MILESTONE_STATUS_IDS.keys())

# Item 2 (turn 20): "Sub Docs" -> "Docs Sent" is a DISPLAY-only rename —
# the internal step key stays "Sub Docs" everywhere else (Dynamo
# milestones map keys, MILESTONE_STATUS_IDS, derivation) so already-
# stored "Sub Docs" entries in live tenant data keep matching their
# checkbox on render; only the checkbox's own label text changes.
MILESTONE_STEP_LABELS = {"Sub Docs": "Docs Sent"}

# Turn 17, item 1: the four checkbox steps replacing the Active Intros
# status dropdown -- everything in MILESTONE_STEPS except "Closed",
# which stays a FLAG value (set via the flag control below), never a
# checkbox: checking off NDA/VDR/Sub Docs/Wired can move a deal forward
# or backward within the pipeline, but a deal is only ever marked Closed
# deliberately, admin-only.
CHECKBOX_MILESTONE_STEPS = [s for s in MILESTONE_STEPS if s != "Closed"]

# Turn 17, item 2: the flag control's values -> the status id each one
# writes directly (no milestones touched — see _handle_update_intro).
# "none" isn't here; it re-derives the status from whatever's currently
# checked instead of writing a fixed id. Stalled/Passed are tenant-
# settable (also in TENANT_ALLOWED_STATUS_IDS); Withdrawn/Closed stay
# admin-only, same restriction the old ten-option dropdown enforced.
FLAG_STATUS_IDS = {
    "stalled": INTRO_STATUS_STALLED_ID,
    "passed": INTRO_STATUS_PASSED_ID,
    "withdrawn": INTRO_STATUS_WITHDRAWN_ID,
    "closed": INTRO_STATUS_CLOSED_ID,
}
FLAG_ADMIN_ONLY_VALUES = {"withdrawn", "closed"}
FLAG_LABELS = {"none": "None", "stalled": "Stalled", "passed": "Passed",
               "withdrawn": "Withdrawn", "closed": "Closed"}

# Pipeline API v3 write (Intro Status only — see _pipeline_update_deal_status).
# Auth is Pipeline's query-string scheme — ?api_key=...&app_key=..., no
# Authorization header — confirmed working for reads against this account.
# Endpoint and payload shape (PUT deals/<id>.json,
# {"deal": {"custom_fields": {...}}}) mirror the verified
# companies/<id>.json write in pricing-updater and valuation-scanner
# exactly, swapping company for deal — those two repos authenticate with a
# per-session JWT Bearer token instead, fetched fresh from
# s3://pipeline-token, since that's the scheme their user-facing writes
# need; this admin write uses the query-string key pair instead, per
# instruction.
PIPELINE_API_KEY = os.environ.get("PIPELINE_API_KEY", "")
PIPELINE_APP_KEY = os.environ.get("PIPELINE_APP_KEY", "")
PIPELINE_API_BASE = "https://api.pipelinecrm.com/api/v3"

# Shared with chadgracia/trades and chadgracia/portfolio-deploy: signs both
# the trades gg_id identity cookie and the ?sso= handoff token. Never in
# repo code.
IDENTITY_SECRET = os.environ.get("IDENTITY_SECRET", "")

SIGNIN_URL = "https://trades.graciagroup.com"

# Auto-enrollment (see _resolve_tenant / _build_tenant_index below): a
# tenant is any authenticated identity whose email matches a person in
# people.json (case-insensitive, every email field the record carries —
# see _person_all_emails) AND who is linked to >=1 deal tagged Sell
# Order (DEAL_SIDE_FIELD contains DEAL_SIDE_SELL_ID), any stage —
# Hold/Obsolete/Won/Lost included, deliberately not filtered through
# _is_live_sell_deal's LIVE_SELL_STAGE_IDS whitelist, since losing
# someone's dashboard access because their one deal happens to be Held
# would defeat the point. person_id and a default display name both
# come from the matched people.json record.
#
# TENANT_OVERRIDES supplies a display-name override only — it plays no
# role in eligibility, which is entirely auto-enrollment now.
# TENANT_BLOCKLIST denies specific emails outright despite otherwise
# qualifying (renders the not-enabled page).
TENANT_OVERRIDES = {
    "michael@nonpublic.io": {"name": "Michael Ferkol (NonPublic)"},
    "natoli@mangustacap.com": {"name": "Natoli (Mangusta Capital)"},
}
TENANT_BLOCKLIST = set()

# Module-level cache: survives warm Lambda invocations, reset on cold start.
_cache = {"version": None, "table": None}


def cf_list(cf, key):
    """Normalize a Pipeline CRM custom_field value (scalar or list) to a list
    of ints. Matches the cf_list/cf_id_list helpers used across the other
    Lambdas that read these same custom_fields."""
    v = cf.get(key)
    if isinstance(v, list):
        out = []
        for item in v:
            try:
                out.append(int(item))
            except (TypeError, ValueError):
                pass
        return out
    if v in (None, ""):
        return []
    try:
        return [int(v)]
    except (TypeError, ValueError):
        return []


def classify_person(cf):
    level_ids = cf_list(cf, INVESTOR_LEVEL_FIELD)
    if QP_ID in level_ids:
        return "qp"
    if SUBSTANTIVE_ID in level_ids:
        # Explicit, even though it's already the default below: Substantive
        # is not a qualification tier, just a screening flag, and must
        # never be read as QP or Accredited.
        return "unknown"
    if set(cf_list(cf, IQF_FIELD)) & IQF_OK_IDS:
        return "accredited"
    return "unknown"


def _deal_cf_option_ids(deal, key):
    """Normalize a deals.json custom_field value to a set of option ids.
    Ported verbatim (algorithm only) from chadgracia/daily-brief's
    _cf_option_ids: deal custom_fields may hold a scalar, a dict
    ({"option_id"/"id"/"value": ...}), or a list mixing either — unlike
    people.json's custom_fields, which cf_list() above already handles as
    plain ints/lists of ints."""
    v = (deal.get("custom_fields") or {}).get(key)
    if v is None:
        return set()
    items = v if isinstance(v, list) else [v]
    out = set()
    for item in items:
        if isinstance(item, dict):
            raw = item.get("option_id") or item.get("id") or item.get("value")
        else:
            raw = item
        if raw is None:
            continue
        try:
            out.add(int(raw))
        except (TypeError, ValueError):
            continue
    return out


def _deal_stage_id(deal):
    stage = deal.get("deal_stage") or {}
    sid = stage.get("id") if isinstance(stage, dict) else None
    if sid is None:
        sid = deal.get("deal_stage_id")
    try:
        return int(sid) if sid is not None else None
    except (TypeError, ValueError):
        return None


def _deal_company_name(deal):
    company = deal.get("company") or {}
    if isinstance(company, dict):
        name = company.get("name")
        if name:
            return name
    return deal.get("company_name") or ""


def _is_live_sell_deal(deal):
    if deal.get("is_archived"):
        return False
    if _deal_stage_id(deal) not in LIVE_SELL_STAGE_IDS:
        return False
    return DEAL_SIDE_SELL_ID in _deal_cf_option_ids(deal, DEAL_SIDE_FIELD)


def _deal_linked_person_ids(deal):
    """IDs of every person attached to a deal. Mirrors the exact fallback
    chadgracia/daily-brief's _deal_people uses to read deals.json's person
    linkage: a "people" list of dicts (each carrying at least "id") when
    present, else the flat "person_ids" list of raw ids. daily-brief never
    merges the two — one or the other is populated — so neither do we."""
    out = set()
    raw = deal.get("people") or []
    if isinstance(raw, list) and raw:
        for p in raw:
            pid = p.get("id") if isinstance(p, dict) else p
            if pid is None:
                continue
            try:
                out.add(int(pid))
            except (TypeError, ValueError):
                continue
        return out
    for pid in (deal.get("person_ids") or []):
        if pid is None:
            continue
        try:
            out.add(int(pid))
        except (TypeError, ValueError):
            continue
    return out


def _deal_linked_person_ids_ordered(deal):
    """Same fallback shape as _deal_linked_person_ids (people list, else
    person_ids) but preserves deals.json's own listed order (deduped) --
    needed for "first listed" (item 2, turn 18): the plain SET
    _deal_linked_person_ids returns can't answer "which one comes
    first"."""
    out = []
    seen = set()
    raw = deal.get("people") or []
    source = raw if isinstance(raw, list) and raw else (deal.get("person_ids") or [])
    for p in source:
        pid = p.get("id") if isinstance(p, dict) else p
        if pid is None:
            continue
        try:
            pid = int(pid)
        except (TypeError, ValueError):
            continue
        if pid not in seen:
            seen.add(pid)
            out.append(pid)
    return out


def _select_primary_buyer(deal, buyer_recs):
    """Item 2 (turn 18): the deal's primary_contact_id when it names one
    of the already-linked buyer records, else the first-listed one
    (buyer_recs must already be in deals.json's own order — see
    _deal_linked_person_ids_ordered). Returns (primary_rec, more_count).

    primary_contact_id has never been confirmed present in a live
    deals.json snapshot -- no repo in this org reads such a field off a
    deal record -- so this is implemented on trust per instruction and
    quietly falls back to first-listed whenever it's absent or doesn't
    match any linked buyer; flagged back to the user rather than
    assumed correct."""
    if not buyer_recs:
        return None, 0
    primary = None
    raw_primary_id = deal.get("primary_contact_id")
    if raw_primary_id is not None:
        try:
            primary_id = int(raw_primary_id)
        except (TypeError, ValueError):
            primary_id = None
        if primary_id is not None:
            primary = next((r for r in buyer_recs if r.get("id") == primary_id), None)
    if primary is None:
        primary = buyer_recs[0]
    return primary, len(buyer_recs) - 1


def _deal_cf_number(deal, key):
    """A deal custom_field's numeric value. Mirrors daily-brief's
    _cf_number: the value may be a scalar, a one-item list, or a dict
    carrying it under "value"/"amount"/"number"."""
    v = (deal.get("custom_fields") or {}).get(key)
    if isinstance(v, list):
        v = v[0] if v else None
    if isinstance(v, dict):
        v = v.get("value") or v.get("amount") or v.get("number")
    if v in (None, ""):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _fmt_money(v):
    """$1.5M / $500K / $2,500 style formatting, matching
    chadgracia/deal-notifier's fmt_size."""
    if v is None:
        return "—"
    if v >= 1_000_000:
        n = v / 1_000_000
        return f"${n:.0f}M" if n == int(n) else f"${n:.1f}M"
    if v >= 1_000:
        n = v / 1_000
        return f"${n:.0f}K" if n == int(n) else f"${n:.1f}K"
    return f"${v:,.0f}"


def _fmt_short_date(date_str):
    """"Sep 15, '26" (item 2) from any _parse_dt-parseable date string, or
    None if it doesn't parse — callers fall back to the raw string rather
    than hiding an unparseable-but-present value."""
    dt = _parse_dt(date_str)
    if dt is None:
        return None
    return f"{dt.strftime('%b')} {dt.day}, '{dt.strftime('%y')}"


def _fmt_epoch_short_date(epoch):
    """"Sep 15, '26" from a Dynamo-stored epoch-seconds timestamp (e.g.
    notes_updated_at) — same output shape as _fmt_short_date, just from
    a number instead of a Pipeline date string (_parse_dt doesn't handle
    bare epoch seconds). None if unset/unparsable."""
    if epoch is None:
        return None
    try:
        dt = datetime.fromtimestamp(float(epoch), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None
    return f"{dt.strftime('%b')} {dt.day}, '{dt.strftime('%y')}"


def _deal_title(deal):
    return deal.get("name") or deal.get("title") or f"Deal {deal.get('id', '')}"


def get_person_ticket_range(cf):
    """(min, max) dollar ticket size for a person: min of all their tier
    lower-bounds, max of all upper-bounds (None if any selected tier is
    unbounded). Ported verbatim from chadgracia/deal-notifier's
    get_person_ticket_range. (None, None) if no ticket size is set."""
    entry_ids = cf_list(cf, TICKET_SIZE_FIELD)
    if not entry_ids:
        return None, None
    mins, maxs = [], []
    for eid in entry_ids:
        tier = TICKET_SIZE_MAP.get(eid)
        if tier:
            mins.append(tier[0])
            maxs.append(tier[1])
    if not mins:
        return None, None
    person_min = min(mins)
    person_max = None if any(m is None for m in maxs) else max(maxs)
    return person_min, person_max


def _fmt_ticket_range(min_v, max_v):
    if min_v is None:
        return None
    if max_v is None:
        return f"{_fmt_money(min_v)}+"
    return f"{_fmt_money(min_v)} – {_fmt_money(max_v)}"


def _person_has_won(rec):
    """won_deals_total > 0 — the raw signal behind both the closer chip
    and the closed-dot. Never raises on a missing/malformed field."""
    if not isinstance(rec, dict):
        return False
    won = rec.get("won_deals_total")
    try:
        return won is not None and float(won) > 0
    except (TypeError, ValueError):
        return False


def _build_firm_won_index():
    """Turn 23: one fresh people.json pass, returns
    {"by_company_id": {...won company_ids...}, "by_company_name":
    {...won lowercased company_names...}} — every company_id (and,
    separately, every lowercased company_name) that at least one person
    with won_deals_total > 0 belongs to. Callers look a given buyer's
    own company_id up in the first set when it's present, falling back
    to company_name in the second set when it's not (see _closer_kind)
    — per instruction: "verify company_id survives into the snapshot;
    if absent, match on company_name exact and report." This sandbox
    has no live Pipeline/S3 credentials to actually inspect a real
    people.json snapshot, so whether company_id is populated on every
    record (vs. present on some, null on others) is NOT verified here
    — flagging that explicitly rather than claiming a check that
    wasn't done. Both paths are implemented and exercised by
    _closer_kind per-record (company_id when present, company_name
    when it's None), so the fallback is live code either way, not
    dead speculative branching. Never counts, amounts, dates, or names
    are kept from
    this scan -- only the two membership sets, so nothing about who
    else won or how much can leak through _closer_kind's boolean
    result. Own fresh S3 fetch (same discard-after-use pattern as
    get_company_buyer_details) — not merged into get_people_by_ids
    since most of its callers have no use for this index and it would
    mean re-deriving it from scratch (it needs the FULL people list,
    not just the wanted ids) on every call anyway."""
    s3 = boto3.client("s3")
    people_obj = s3.get_object(Bucket=BUCKET, Key=PEOPLE_KEY)
    people_data = json.loads(people_obj["Body"].read())
    people_list = people_data.get("people", []) if isinstance(people_data, dict) else (people_data or [])
    by_company_id = set()
    by_company_name = set()
    for rec in people_list:
        if not _person_has_won(rec):
            continue
        cid = rec.get("company_id")
        if cid is not None:
            by_company_id.add(cid)
        cname = (rec.get("company_name") or "").strip().lower()
        if cname:
            by_company_name.add(cname)
    return {"by_company_id": by_company_id, "by_company_name": by_company_name}


def _closer_kind(rec, firm_won_index):
    """"person" (this buyer's own won_deals_total > 0), "firm" (a
    colleague's does, matched by company_id when this record carries
    one, else by exact lowercased company_name), or None. Drives both
    the closer chip and the closed-dot's tooltip -- the two must always
    agree, so both read this one function rather than duplicating the
    person-or-firm logic."""
    if not isinstance(rec, dict):
        return None
    if _person_has_won(rec):
        return "person"
    cid = rec.get("company_id")
    if cid is not None:
        if cid in firm_won_index["by_company_id"]:
            return "firm"
        return None
    cname = (rec.get("company_name") or "").strip().lower()
    if cname and cname in firm_won_index["by_company_name"]:
        return "firm"
    return None


CLOSER_CHIP_LABELS = {"person": "Proven closer with Rainmaker", "firm": "Firm has closed with Rainmaker"}


def _closer_chip_html(kind):
    if kind is None:
        return ""
    return f'<span class="closer-chip">{_esc(CLOSER_CHIP_LABELS[kind])}</span>'


def _parse_dt(s):
    """Best-effort parse of a Pipeline timestamp into an aware datetime, or
    None. Mirrors chadgracia/daily-brief's _parse_dt (ISO 8601, with a
    couple of common non-ISO fallbacks), plus a slash-date fallback for the
    "2026/08/21" Pipeline format noted elsewhere in this org's exports."""
    if not s:
        return None
    s = str(s)
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _anon_buyer_code(key_email, person_id):
    """Stable 4-char anonymous code for a Buyer Demand tile:
    HMAC-SHA256(IDENTITY_SECRET, f"{key_email}|{person_id}"), first 4
    digest bytes mapped onto ANON_ALPHABET. Same viewer + same person always
    produces the same code; a different viewer gets a different one. The
    code is a one-way function of the person id — it never appears in the
    output on its own."""
    digest = hmac.new(IDENTITY_SECRET.encode(), f"{key_email}|{person_id}".encode(),
                      hashlib.sha256).digest()
    return "".join(ANON_ALPHABET[b % len(ANON_ALPHABET)] for b in digest[:4])


def _company_href(company, ref, key=None, view_as=None):
    suffix = _tab_qs_suffix(key, view_as)
    return f"?company={urllib.parse.quote(company, safe='')}&ref={ref}{suffix}"


def _buyer_href(person_id, key=None, view_as=None):
    """Item 3 (turn 18): the buyer page's own URL — ?buyer=<person_id>,
    same key/view_as passthrough convention as _company_href."""
    suffix = _tab_qs_suffix(key, view_as)
    return f"?buyer={person_id}{suffix}"


# ── Identity: SSO handoff + durable cookie ───────────────────────────────
# Ported verbatim (formats and algorithms, not code layout) from the two
# sibling Lambdas that originate this scheme:
#   - chadgracia/trades/lambda_function.py: _make_identity_cookie,
#     _read_identity_email, _get_cookie (the gg_id cookie trades mints on
#     Cognito login and every other Gracia Group app reads).
#   - chadgracia/portfolio-deploy/lambda_function.py: _b64u, _b64u_decode,
#     _verify_sso_handoff (how the ?sso= handoff token is verified —
#     expiry check, hmac.compare_digest, base64 padding handling).

def _b64u(b):                       # bytes -> unpadded base64url str
    return base64.urlsafe_b64encode(b).decode().rstrip("=")


def _b64u_decode(s):                # unpadded base64url str -> bytes
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def _get_cookie(event, name):
    """Read a cookie value from a payload-v2 request, else None."""
    for c in (event.get("cookies") or []):
        if c.startswith(name + "="):
            return c.split("=", 1)[1]
    hdr = (event.get("headers") or {}).get("cookie", "")
    for c in hdr.split(";"):
        c = c.strip()
        if c.startswith(name + "="):
            return c.split("=", 1)[1]
    return None


def _make_identity_cookie(email):
    sig = hmac.new(IDENTITY_SECRET.encode(), email.encode(), hashlib.sha256).hexdigest()
    val = _b64u(f"{email}|{sig}".encode())
    return f"gg_id={val}; Max-Age=31536000; Path=/; Secure; SameSite=Lax"


def _read_identity_email(event):
    """Verified email from the gg_id cookie, or None. Reverses
    _make_identity_cookie. Never raises."""
    if not IDENTITY_SECRET:
        return None
    raw = _get_cookie(event, "gg_id")
    if not raw:
        return None
    try:
        decoded = _b64u_decode(raw).decode()
        email, sig = decoded.rsplit("|", 1)
        expected = hmac.new(IDENTITY_SECRET.encode(), email.encode(),
                            hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, sig):
            return None
        return email
    except Exception:
        return None


def _verify_sso_handoff(token):
    """Email if the trading site's signed, unexpired handoff verifies, else
    None. Token is base64url(f"{email}|{exp}|{sig}"), sig =
    HMAC-SHA256(IDENTITY_SECRET, f"{email}|{exp}").hexdigest(). Never
    raises."""
    if not (IDENTITY_SECRET and token):
        return None
    try:
        parts = _b64u_decode(token).decode().split("|")
        if len(parts) != 3:
            return None
        email, exp, sig = parts
        expected = hmac.new(IDENTITY_SECRET.encode(), f"{email}|{exp}".encode(),
                            hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, sig):
            return None
        if int(exp) < int(time.time()):
            return None
        return email
    except Exception:
        return None


def _object_version(s3, key):
    head = s3.head_object(Bucket=BUCKET, Key=key)
    return head["LastModified"].isoformat()


def _build_table(s3):
    """Fetch both S3 objects fresh and compute the per-company tier table.
    All large intermediates (parsed people list, id->tier index) are local
    variables and are dropped as soon as this function returns."""
    people_obj = s3.get_object(Bucket=BUCKET, Key=PEOPLE_KEY)
    people_data = json.loads(people_obj["Body"].read())
    people_list = people_data.get("people", []) if isinstance(people_data, dict) else (people_data or [])

    tier_by_id = {}
    for rec in people_list:
        pid = rec.get("id")
        if pid is None:
            continue
        cf = rec.get("custom_fields") or {}
        tier_by_id[str(pid)] = classify_person(cf)

    interest_obj = s3.get_object(Bucket=BUCKET, Key=INTEREST_KEY)
    interest_data = json.loads(interest_obj["Body"].read())
    buy = interest_data.get("buy") or {}

    deals_obj = s3.get_object(Bucket=BUCKET, Key=DEALS_KEY)
    deals_data = json.loads(deals_obj["Body"].read())
    deals_list = deals_data.get("deals", []) if isinstance(deals_data, dict) else (deals_data or [])

    sellers_by_company = {}
    for deal in deals_list:
        if not _is_live_sell_deal(deal):
            continue
        company = _deal_company_name(deal).strip()
        if not company:
            continue
        sellers_by_company[company] = sellers_by_company.get(company, 0) + 1

    table = []
    for company, ids in buy.items():
        if not isinstance(ids, list) or not ids:
            continue
        if company.strip().lower() in _EXCLUDED_COMPANIES_LOWER:
            continue
        counts = {"qp": 0, "accredited": 0, "unknown": 0}
        for pid in ids:
            counts[tier_by_id.get(str(pid), "unknown")] += 1
        table.append({
            "company": company,
            "total": len(ids),
            "qp": counts["qp"],
            "accredited": counts["accredited"],
            "unknown": counts["unknown"],
            "sellers": sellers_by_company.get(company, 0),
        })
    table.sort(key=lambda r: (-r["total"], r["company"].lower()))
    return table


def get_company_table():
    s3 = boto3.client("s3")
    version = (
        _object_version(s3, PEOPLE_KEY),
        _object_version(s3, INTEREST_KEY),
        _object_version(s3, DEALS_KEY),
    )
    if _cache["version"] == version and _cache["table"] is not None:
        return _cache["table"]
    table = _build_table(s3)
    _cache["version"] = version
    _cache["table"] = table
    return table


# Separate module-level cache for the raw deals.json list, used by My Deals.
# Kept independent of _cache/get_company_table above (which caches the
# Demand Board's precomputed per-company table, not a per-tenant view) so
# neither one touches the other's cache-invalidation behavior.
_deals_cache = {"version": None, "deals": None}


def get_deals_list():
    """Raw deals.json deals, fresh-checked via the same cheap
    head_object-version pattern as get_company_table, cached independently
    since My Deals needs the full per-deal records (for per-tenant
    filtering) rather than a precomputed aggregate."""
    s3 = boto3.client("s3")
    version = _object_version(s3, DEALS_KEY)
    if _deals_cache["version"] == version and _deals_cache["deals"] is not None:
        return _deals_cache["deals"]
    deals_obj = s3.get_object(Bucket=BUCKET, Key=DEALS_KEY)
    deals_data = json.loads(deals_obj["Body"].read())
    deals = deals_data.get("deals", []) if isinstance(deals_data, dict) else (deals_data or [])
    _deals_cache["version"] = version
    _deals_cache["deals"] = deals
    return deals


def get_my_deals(person_id):
    """Deals linked to person_id, newest-updated first."""
    deals = get_deals_list()
    mine = [d for d in deals if person_id in _deal_linked_person_ids(d)]
    mine.sort(key=lambda d: d.get("updated_at") or "", reverse=True)
    return mine


def get_company_buyer_details(company):
    """Tier/ticket-range/updated_at for every person with Buy Interest in
    one company, per interest_people.json + people.json. Fresh S3 fetch on
    every call, same as _build_table's own fetch of these two files — the
    parsed people list is a local variable here and is dropped when this
    function returns; per the module docstring, it is never added to any
    module-level cache."""
    s3 = boto3.client("s3")
    interest_obj = s3.get_object(Bucket=BUCKET, Key=INTEREST_KEY)
    interest_data = json.loads(interest_obj["Body"].read())
    buy = interest_data.get("buy") or {}

    target = company.strip().lower()
    person_ids = []
    for name, ids in buy.items():
        if isinstance(ids, list) and name.strip().lower() == target:
            person_ids = ids
            break
    if not person_ids:
        return []
    wanted = {str(pid) for pid in person_ids}

    people_obj = s3.get_object(Bucket=BUCKET, Key=PEOPLE_KEY)
    people_data = json.loads(people_obj["Body"].read())
    people_list = people_data.get("people", []) if isinstance(people_data, dict) else (people_data or [])

    out = []
    for rec in people_list:
        pid = rec.get("id")
        if pid is None or str(pid) not in wanted:
            continue
        cf = rec.get("custom_fields") or {}
        out.append({
            "person_id": pid,
            "tier": classify_person(cf),
            "ticket_range": get_person_ticket_range(cf),
            "updated_at": rec.get("updated_at"),
        })
    return out


def get_company_record(company_id, company_name):
    """Turn 24: companies.json lookup for the buyer page's "About the
    firm" card. Fresh S3 fetch, discarded after use (same pattern as
    get_company_buyer_details). Matched by company_id first (the same
    id _closer_kind already reads off a buyer's own record for the
    firm-won check) — falling back to an exact, case-insensitive
    company_name match when company_id is absent, the identical two-
    path strategy _closer_kind/_build_firm_won_index use and for the
    same stated reason (verify company_id survives into the snapshot;
    if absent, match on name). Returns None on ANY failure — missing
    file, unexpected shape, or no match — so the card simply omits
    itself rather than showing something wrong; this file's shape is
    unverified in this sandbox (see COMPANIES_KEY)."""
    try:
        s3 = boto3.client("s3")
        companies_obj = s3.get_object(Bucket=BUCKET, Key=COMPANIES_KEY)
        companies_data = json.loads(companies_obj["Body"].read())
        companies_list = (companies_data.get("companies", [])
                           if isinstance(companies_data, dict) else (companies_data or []))
    except Exception:
        return None
    if not isinstance(companies_list, list):
        return None
    if company_id is not None:
        for rec in companies_list:
            if isinstance(rec, dict) and rec.get("id") == company_id:
                return rec
        return None
    if company_name:
        target = company_name.strip().lower()
        for rec in companies_list:
            if isinstance(rec, dict) and (rec.get("name") or "").strip().lower() == target:
                return rec
    return None


def get_people_by_ids(person_ids):
    """{id: person record} for exactly the wanted ids, via a fresh
    people.json fetch. Same fetch-and-discard pattern as
    get_company_buyer_details — never cached."""
    wanted = {str(pid) for pid in person_ids if pid is not None}
    if not wanted:
        return {}
    s3 = boto3.client("s3")
    people_obj = s3.get_object(Bucket=BUCKET, Key=PEOPLE_KEY)
    people_data = json.loads(people_obj["Body"].read())
    people_list = people_data.get("people", []) if isinstance(people_data, dict) else (people_data or [])
    out = {}
    for rec in people_list:
        pid = rec.get("id")
        if pid is None or str(pid) not in wanted:
            continue
        out[pid] = rec
    return out


# Module-level cache for the auto-enrollment email->tenant index, keyed
# by the people.json snapshot's LastModified only (deliberately not
# deals.json's — per instruction) — same warm-invocation-survives,
# cold-start-resets pattern as _cache/_deals_cache above.
_tenant_cache = {"version": None, "by_email": None}


def _person_all_emails(rec):
    """Every email address a person record carries — the scalar "email"
    field plus each entry of the "emails" list (itself plain strings or
    {"address": ...} dicts — same two shapes _person_email_text already
    handles), lowercased and deduped, so auto-enrollment matches an
    identity regardless of which address on file they authenticate
    with. people.json carries both an "email" scalar and an "emails"
    list per the existing _person_email_text helper (ported from
    chadgracia/daily-brief) — this indexes every one of them rather
    than picking just the first, unlike that helper."""
    if not isinstance(rec, dict):
        return []
    out = []
    seen = set()

    def _add(addr):
        if isinstance(addr, str) and addr.strip():
            key = addr.strip().lower()
            if key not in seen:
                seen.add(key)
                out.append(key)

    _add(rec.get("email"))
    emails = rec.get("emails") or []
    if isinstance(emails, list):
        for item in emails:
            _add(item.get("address") if isinstance(item, dict) else item)
    return out


def _build_tenant_index(s3):
    """email (lowercased) -> {"name", "person_id"} for every auto-
    enrolled tenant: a people.json person linked to >=1 deal tagged Sell
    Order (DEAL_SIDE_FIELD contains DEAL_SIDE_SELL_ID), any stage —
    deliberately not run through _is_live_sell_deal's
    LIVE_SELL_STAGE_IDS/is_archived filtering, since a Held or Obsolete
    deal must not cost someone their access. TENANT_BLOCKLIST emails are
    dropped outright; TENANT_OVERRIDES only ever swaps in a display
    name, never affects eligibility. When a person has multiple emails
    on file, each maps to the same tenant record — first match wins
    ties are impossible since every email is this exact person's own.

    Matched via str(pid), not raw equality: _deal_linked_person_ids
    always coerces deal-side person ids to int, but people.json's own
    "id" field is not reliably int-typed across every record (seen
    live: natoli@mangustacap.com/person 1307332972's record carries a
    string id) — a bare `pid in seller_person_ids` against an int set
    silently drops any such record with no error. Every other
    people.json/deal-linkage cross-match in this file already goes
    through str() for exactly this reason (see get_people_by_ids,
    _build_table's tier_by_id) — this one is brought in line with that
    same established convention. person_id is still stored as an int
    when the raw id parses as one, since every downstream consumer
    (_tenant_email_for_deal, Hold/Cancel/Reactivate's deal-linkage
    checks) compares it against those same int-coerced deal-linkage
    sets."""
    people_obj = s3.get_object(Bucket=BUCKET, Key=PEOPLE_KEY)
    people_data = json.loads(people_obj["Body"].read())
    people_list = people_data.get("people", []) if isinstance(people_data, dict) else (people_data or [])

    seller_person_ids = set()
    for deal in get_deals_list():
        if DEAL_SIDE_SELL_ID in _deal_cf_option_ids(deal, DEAL_SIDE_FIELD):
            seller_person_ids.update(_deal_linked_person_ids(deal))
    seller_person_ids_str = {str(pid) for pid in seller_person_ids}

    by_email = {}
    for rec in people_list:
        pid = rec.get("id")
        if pid is None or str(pid) not in seller_person_ids_str:
            continue
        try:
            pid = int(pid)
        except (TypeError, ValueError):
            pass
        default_name = _person_display_name(rec)
        for email in _person_all_emails(rec):
            if email in TENANT_BLOCKLIST:
                continue
            override_name = (TENANT_OVERRIDES.get(email) or {}).get("name")
            by_email[email] = {"name": override_name or default_name or email, "person_id": pid}
    return by_email


def _tenant_index():
    s3 = boto3.client("s3")
    version = _object_version(s3, PEOPLE_KEY)
    if _tenant_cache["version"] == version and _tenant_cache["by_email"] is not None:
        return _tenant_cache["by_email"]
    by_email = _build_tenant_index(s3)
    _tenant_cache["version"] = version
    _tenant_cache["by_email"] = by_email
    return by_email


def _resolve_tenant(email):
    """{"name", "person_id"} for a qualifying (auto-enrolled,
    non-blocklisted) tenant email, case-insensitive, or None. The single
    source of truth every access-control check below calls instead of
    the old TENANTS lookup."""
    if not email:
        return None
    return _tenant_index().get(email.strip().lower())


def _tenant_cef_state(person_id):
    """The tenant's own Client Engagement Form option id (CEF_FIELD,
    verified via the one-time person_custom_field_labels fetch — one of
    CEF_NO_ID/CEF_PENDING_ID/CEF_YES_ID/CEF_NA_ID), or None when the
    field is unset, the tenant's own person record can't be found, or
    there's no person_id to look up at all (admin-without-view_as, which
    never gets a CEF badge in the first place — see the tenant is not
    None guard at the call site)."""
    if person_id is None:
        return None
    people = get_people_by_ids({person_id})
    rec = people.get(person_id)
    if rec is None:
        return None
    cf = rec.get("custom_fields") or {}
    ids = cf_list(cf, CEF_FIELD)
    return ids[0] if ids else None


def _cef_badge_html(cef_option_id, tenant_name):
    """Nav-bar badge for the tenant view (and admin &view_as preview),
    one state per CEF option: Yes -> green "ID verified"; Pending ->
    amber "ID pending"; No or unset (cef_option_id is None or any id
    outside the verified map) -> red "ID required — FINRA compliance"
    linking straight to the CEF form itself (CEF_FORM_URL, new tab) — no
    longer a mailto, since there's now a direct form to send people to;
    N/A -> no badge at all. "CEF" is internal-only from here on (field
    id, option ids, the form URL, Dynamo/variable names) — every
    client-facing string says "ID" instead. Only ever called when there
    IS a tenant to report on (see the call site's tenant is not None
    guard) — there's no separate "no tenant" case to handle here."""
    if cef_option_id == CEF_NA_ID:
        return ""
    if cef_option_id == CEF_YES_ID:
        return '<div class="gg-cef-badge cef-ok">&#10003; ID verified</div>'
    if cef_option_id == CEF_PENDING_ID:
        return '<div class="gg-cef-badge cef-pending">&#8226; ID pending</div>'
    return (f'<a class="gg-cef-badge cef-missing" href="{CEF_FORM_URL}" target="_blank" rel="noopener noreferrer">'
            '&#10007; ID required — FINRA compliance</a>')


def _person_display_name(rec):
    """Full "First Last" name. Mirrors chadgracia/daily-brief's
    _person_full_name (full_name/name, else first_name+last_name)."""
    if not isinstance(rec, dict):
        return ""
    full = (rec.get("full_name") or rec.get("name") or "").strip()
    if full:
        return full
    parts = [rec.get("first_name"), rec.get("last_name")]
    return " ".join(p for p in parts if p).strip()


def _person_email_text(rec):
    """Ported verbatim from chadgracia/daily-brief's _person_email: a
    scalar "email", falling back to the first entry of an "emails" list
    (which may itself be plain strings or {"address": ...} dicts)."""
    if not isinstance(rec, dict):
        return ""
    email = rec.get("email")
    if not email:
        emails = rec.get("emails") or []
        if isinstance(emails, list) and emails:
            first = emails[0]
            email = first.get("address") if isinstance(first, dict) else first
    if not isinstance(email, str):
        return ""
    return email.strip()


def _person_phone_text(rec):
    """Best-effort phone extraction, mirroring _person_email_text's own
    scalar/list fallback shape ("phone" / "phones" list) — no repo this
    org's code lives in ever reads a phone field off a person record, so
    this is implemented on trust and degrades to blank if the real shape
    differs."""
    if not isinstance(rec, dict):
        return ""
    phone = rec.get("phone")
    if not phone:
        phones = rec.get("phones") or []
        if isinstance(phones, list) and phones:
            first = phones[0]
            phone = first.get("number") if isinstance(first, dict) else first
    if not isinstance(phone, str):
        return ""
    return phone.strip()


def get_my_sell_deals(person_id, company):
    """The tenant's SELL deals for one company, for the Deal Card
    section."""
    target = company.strip().lower()
    out = []
    for d in get_my_deals(person_id):
        if (_deal_company_name(d) or "").strip().lower() != target:
            continue
        if DEAL_SIDE_SELL_ID in _deal_cf_option_ids(d, DEAL_SIDE_FIELD):
            out.append(d)
    return out


def _is_matched_or_later_buy_deal(deal):
    """The one shared predicate for "matched-or-later buy deal": used by
    both the company page's Matched Buyers section and the Active Intros
    tab, so the stage/side rules can never drift between them. is_archived
    is intentionally not checked — see MATCHED_OR_LATER_STAGE_IDS."""
    if _deal_stage_id(deal) not in MATCHED_OR_LATER_STAGE_IDS:
        return False
    return DEAL_SIDE_BUY_ID in _deal_cf_option_ids(deal, DEAL_SIDE_FIELD)


def get_my_matched_buy_deals(person_id, company=None):
    """The tenant's BUY deals at Matched-or-later stage — every company
    (Active Intros) when company is None, else just that one (the company
    page's Matched Buyers section)."""
    target = company.strip().lower() if company else None
    out = []
    for d in get_my_deals(person_id):
        if target is not None and (_deal_company_name(d) or "").strip().lower() != target:
            continue
        if _is_matched_or_later_buy_deal(d):
            out.append(d)
    return out


# Turn 27: stage-level exits. A dead-stage BUY deal never reaches
# MATCHED_OR_LATER_STAGE_IDS (Lost/Trade Broken/Obsolete are disjoint from
# it by construction), so it's invisible to get_my_matched_buy_deals today
# and simply vanishes instead of showing as a closed-out intro. This is a
# separate fetch path built the same way, keyed off the deal's own stage
# rather than the Intro Status field. The derived outcome name is
# DISPLAY-ONLY -- it is never written back to Pipeline or to any Dynamo
# intro item; only _handle_update_intro's own explicit status/flag writes
# ever touch status_override.
CLOSED_OUT_STAGE_OUTCOMES = {}
for _sid in LOST_STAGE_IDS | {STAGE_TRADE_BROKEN}:
    CLOSED_OUT_STAGE_OUTCOMES[_sid] = "Passed"
CLOSED_OUT_STAGE_OUTCOMES[OBSOLETE_STAGE_ID] = "Withdrawn"
del _sid


def _closed_out_outcome_name(stage_id):
    """Derived, display-only outcome for a dead-stage BUY deal: Lost or
    Trade Broken -> "Passed"; Obsolete -> "Withdrawn"; any other stage
    (including None) -> None, meaning "not a closed-out deal"."""
    return CLOSED_OUT_STAGE_OUTCOMES.get(stage_id)


def _is_closed_out_buy_deal(deal):
    """A Buy-tagged deal whose RAW Pipeline stage (never override-aware --
    stage overrides are a Sell-side-only concept) is Lost, Trade Broken,
    or Obsolete. These three stages are disjoint from
    MATCHED_OR_LATER_STAGE_IDS by construction, so no separate "not
    already matched-or-later" check is needed -- a deal can never satisfy
    both predicates at once."""
    if _closed_out_outcome_name(_deal_stage_id(deal)) is None:
        return False
    return DEAL_SIDE_BUY_ID in _deal_cf_option_ids(deal, DEAL_SIDE_FIELD)


def get_my_closed_out_buy_deals(person_id, company=None):
    """The tenant's BUY deals whose stage is a dead-stage exit (Lost/Trade
    Broken/Obsolete) — mirrors get_my_matched_buy_deals exactly, just
    keyed off _is_closed_out_buy_deal instead."""
    target = company.strip().lower() if company else None
    out = []
    for d in get_my_deals(person_id):
        if target is not None and (_deal_company_name(d) or "").strip().lower() != target:
            continue
        if _is_closed_out_buy_deal(d):
            out.append(d)
    return out


def _closed_out_disclosed(deal):
    """Item 3's first, standalone disclosure rule for closed-out rows: the
    RAW (never override-aware) Intro Status field must explicitly hold an
    Introduced-or-later value. No milestone fallback here (unlike
    _resolve_intro_status's own exit-status fix) -- the instruction states
    this rule on its own, without reference to milestones. An empty status
    on a dead deal renders anonymized exactly like a Pending row."""
    return _deal_intro_status_id(deal) in INTRODUCED_OR_LATER_STATUS_IDS


def _deal_loss_reason_text(deal):
    """Best-effort read of a brand-new, UNVERIFIED deals.json field
    (deal_loss_reason) -- this session has no live S3 access to confirm
    its actual shape survives the export, so this handles the two
    plausible shapes defensively and returns None for anything else,
    exactly like every other "might not be in the snapshot yet" field in
    this file (see INTRO_STATUS_FIELD's own docstring). Never raises."""
    raw = deal.get("deal_loss_reason")
    if raw is None:
        return None
    if isinstance(raw, dict):
        text = raw.get("name") or raw.get("value") or raw.get("label")
    elif isinstance(raw, list):
        first = raw[0] if raw else None
        text = first.get("name") if isinstance(first, dict) else first
    else:
        text = raw
    text = str(text).strip() if text is not None else ""
    return text or None


# ── Intro status pipeline ─────────────────────────────────────────────────
# Status source of truth is the Pipeline Intro Status deal field
# (custom_label_4008329, above) — see _resolve_intro_status. DynamoDB
# table "syndicate-dash" (us-east-1) is consulted only for the free-text
# next_steps/notes attributes now, via get_intro_details; it is never
# asked for "status" at all. Read-only this step — no write path yet.
INTRO_TABLE = "syndicate-dash"
INTRO_REGION = "us-east-1"

STATUS_STEPS = [
    "Matched", "Introduced", "NDA Signed", "VDR Link Provided",
    "Signed Sub Docs", "Wired", "Closed",
]
STATUS_INDEX = {name: i for i, name in enumerate(STATUS_STEPS)}


def _dynamo_table():
    return boto3.resource("dynamodb", region_name=INTRO_REGION).Table(INTRO_TABLE)


def get_intro_details(tenant_email):
    """{deal_id_str: {"next_steps", "notes", "follow_up", "status_override",
    "override_at", ..., "milestones"}} for every intro item under this
    tenant, via a single Query on the syndicate-dash table (never one
    GetItem per deal). Never
    raises: any failure (missing table, network, permissions) returns
    ({}, True) so the caller can fall back to Pipeline-only data and show
    a small note instead of a broken page. Returns (entries,
    dynamo_failed)."""
    try:
        table = _dynamo_table()
        resp = table.query(
            KeyConditionExpression=Key("tenant").eq(tenant_email) & Key("sk").begins_with("intro#"),
        )
        out = {}
        for item in resp.get("Items", []):
            sk = item.get("sk") or ""
            if not sk.startswith("intro#"):
                continue
            deal_id = sk[len("intro#"):]
            if not deal_id:
                continue
            out[deal_id] = {
                "next_steps": item.get("next_steps"),
                "notes": item.get("notes"),
                "notes_updated_at": item.get("notes_updated_at"),
                "follow_up": item.get("follow_up"),
                "status_override": item.get("status_override"),
                "override_at": item.get("override_at"),
                "deadline_override": item.get("deadline_override"),
                "deadline_override_at": item.get("deadline_override_at"),
                "stage_override": item.get("stage_override"),
                "stage_override_at": item.get("stage_override_at"),
                "milestones": item.get("milestones") or {},
            }
        return out, False
    except Exception:
        return {}, True


# ── Feature-request capture ──────────────────────────────────────────────
# Own Query, deliberately NOT merged into get_intro_details's — that query
# is called from several places that have nothing to do with feature
# requests (_handle_update_intro, both intro-status renderers) and its
# sk begins_with("intro#") filter would have to be dropped org-wide to
# pick up "feature#" items too, adding a second responsibility everywhere
# it's already used. One extra Query per normal page load here instead;
# the admin aggregate view (render_my_deals_page's tenant_picker branch)
# is the only place that issues more than one, by necessity — it has no
# single partition to query.
MAX_FEATURE_TEXT_LEN = 2000

# Item 5 (turn 20): each feature-request item now stores which page it
# was submitted from. A page's own box tags new items with its own page;
# a page's own list filters to just that page; an item with no "page"
# attribute at all (written before this turn) is treated as "my-deals",
# where the box lived exclusively before per-page requests existed. The
# admin aggregate view (no single tenant/page to scope to) is the one
# exception -- it shows every item, every page, labeled by both.
FEATURE_PAGES = {"my-deals", "intros", "demand", "company"}
FEATURE_PAGE_LABELS = {"my-deals": "My Deals", "intros": "Active Intros", "demand": "Demand Board",
                        "company": "Company"}


def get_feature_requests(tenant_partition, page=None):
    """{"open": [...], "done": [...]} feature-request items for one Dynamo
    partition (sk begins_with "feature#"), each item newest-first within
    its bucket (the sk's own epoch-ms suffix sorts correctly as a string).
    page=None returns every item regardless of page (the admin aggregate
    view only); any other value filters to items whose stored "page"
    matches it, with a missing "page" attribute treated as "my-deals"
    (see FEATURE_PAGES above). Never raises: any failure returns
    ({"open": [], "done": []}, True)."""
    try:
        table = _dynamo_table()
        resp = table.query(
            KeyConditionExpression=Key("tenant").eq(tenant_partition) & Key("sk").begins_with("feature#"),
        )
        items = []
        for item in resp.get("Items", []):
            sk = item.get("sk") or ""
            if not sk.startswith("feature#"):
                continue
            item_page = item.get("page") or "my-deals"
            if page is not None and item_page != page:
                continue
            items.append({
                "sk": sk,
                "tenant": tenant_partition,
                "text": item.get("text") or "",
                "submitted_by": item.get("submitted_by"),
                "created_at": item.get("created_at"),
                "done": bool(item.get("done")),
                "done_by": item.get("done_by"),
                "done_at": item.get("done_at"),
                "page": item_page,
            })
        items.sort(key=lambda it: it["sk"], reverse=True)
        return {"open": [it for it in items if not it["done"]],
                "done": [it for it in items if it["done"]]}, False
    except Exception:
        return {"open": [], "done": []}, True


def _dynamo_write_feature_request(tenant_partition, text, actor, page):
    try:
        table = _dynamo_table()
        table.put_item(Item={
            "tenant": tenant_partition,
            "sk": f"feature#{int(time.time() * 1000)}",
            "text": text,
            "submitted_by": actor,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "done": False,
            "page": page,
        })
        return True, None
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def _dynamo_toggle_feature_request(tenant_partition, feature_sk, done, actor):
    """Flips done and appends an audit item (sk=audit#<feature_sk>#<epoch_ms>,
    mirroring _dynamo_write_intro_update's own audit sk shape). Reopening
    (done=False) removes done_by/done_at rather than nulling them out.

    ConditionExpression="attribute_exists(sk)" is load-bearing: without it,
    DynamoDB's update_item silently upserts a brand-new item for any
    (tenant, feature_sk) pair that doesn't already exist — which would let
    a tenant "toggle" an sk copied or guessed from another partition and
    have it silently create garbage in their OWN partition instead of
    failing. With the condition, that case raises (caught below) and
    nothing is written."""
    now = time.time()
    try:
        table = _dynamo_table()
        if done:
            table.update_item(
                Key={"tenant": tenant_partition, "sk": feature_sk},
                UpdateExpression="SET done = :d, done_by = :db, done_at = :da",
                ConditionExpression="attribute_exists(sk)",
                ExpressionAttributeValues={
                    ":d": True, ":db": actor, ":da": datetime.now(timezone.utc).isoformat(),
                },
            )
        else:
            table.update_item(
                Key={"tenant": tenant_partition, "sk": feature_sk},
                UpdateExpression="SET done = :d REMOVE done_by, done_at",
                ConditionExpression="attribute_exists(sk)",
                ExpressionAttributeValues={":d": False},
            )
        table.put_item(Item={
            "tenant": tenant_partition,
            "sk": f"audit#{feature_sk}#{int(now * 1000)}",
            "actor": actor,
            "old": {"done": not done},
            "new": {"done": done},
        })
        return True, None
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def _handle_feature_request(event):
    """POST ?action=feature_request — create ({text}) or toggle
    ({feature_sk, done}) a feature-request item.

    Auth mirrors _handle_update_intro's tenant-write path: ADMIN_KEY in
    the body may act on any partition named by body["tenant"] ("admin" or
    an auto-enrolled tenant email) — that's how the admin aggregate view
    (many partitions on one page) targets a specific one. A tenant's own
    identity cookie may only ever act within their own partition; any
    body["tenant"] they send is ignored — the authenticated identity
    always wins. Never raises past this function."""
    body = _parse_json_body(event)

    admin_key = os.environ.get("ADMIN_KEY")
    is_admin = bool(admin_key) and body.get("key") == admin_key

    if is_admin:
        partition = str(body.get("tenant") or "").strip()
        if partition != "admin":
            partition = partition.lower()
            if _resolve_tenant(partition) is None:
                return _json_response({"error": "invalid tenant"}, 400)
        actor = "admin"
    else:
        identity_email = _read_identity_email(event)
        tenant_identity_email = identity_email.strip().lower() if identity_email else None
        if not tenant_identity_email or _resolve_tenant(tenant_identity_email) is None:
            return _json_response({"error": "forbidden"}, 403)
        partition = tenant_identity_email
        actor = tenant_identity_email

    feature_sk = body.get("feature_sk")
    if feature_sk is not None:
        feature_sk = str(feature_sk).strip()
        if not feature_sk.startswith("feature#"):
            return _json_response({"error": "invalid feature_sk"}, 400)
        if "done" not in body:
            return _json_response({"error": "done is required"}, 400)
        ok, err = _dynamo_toggle_feature_request(partition, feature_sk, bool(body.get("done")), actor)
        if not ok:
            return _json_response({"error": f"Save failed: {err}"}, 502)
        return _json_response({"ok": True})

    text = str(body.get("text") or "").strip()
    if not text:
        return _json_response({"error": "text is required"}, 400)
    if len(text) > MAX_FEATURE_TEXT_LEN:
        return _json_response({"error": "text too long"}, 400)

    # Item 5 (turn 20): tag the new item with the page it was submitted
    # from. An unrecognized/missing page quietly falls back to
    # "my-deals" rather than failing the whole request -- this is a
    # low-stakes feedback box, not worth a hard error over a bad tag.
    page = str(body.get("page") or "").strip()
    if page not in FEATURE_PAGES:
        page = "my-deals"

    ok, err = _dynamo_write_feature_request(partition, text, actor, page)
    if not ok:
        return _json_response({"error": f"Save failed: {err}"}, 502)
    return _json_response({"ok": True})


def _fmt_feature_date(iso_str):
    dt = _parse_dt(iso_str)
    return dt.strftime("%b %-d, %Y") if dt else None


def _feature_box_html(partition, key=None, page="my-deals"):
    """Submit box: "same auto-save fetch pattern" as the rest of the app
    (Saving…/Saved ✓/error via one shared inline .ei-msg), just triggered
    by a Submit button/Enter instead of blur, since this creates a new
    item rather than editing an existing field. Dismissable per page load
    only (box.hidden, no persistence) — reappears on the next load, by
    design. Reloads the page on success so the new item shows up in the
    list below without needing separate DOM-insertion logic.

    page (item 5, turn 20) tags the new item so this same page's own
    list (and no other page's) picks it up — see get_feature_requests."""
    key_json = json.dumps(key or "")
    tenant_json = json.dumps(partition)
    page_json = json.dumps(page)
    return f"""<div class="feature-box" id="feature-box">
  <button type="button" class="feature-box-dismiss" id="feature-box-dismiss" aria-label="Dismiss">&times;</button>
  <h2>Help shape this dashboard</h2>
  <p>Tell us one feature you'd like — submit as many as you want.</p>
  <div class="feature-box-form">
    <input type="text" id="feature-input" maxlength="{MAX_FEATURE_TEXT_LEN}" placeholder="I'd like…">
    <button type="button" id="feature-submit-btn">Submit</button>
  </div>
  <span class="ei-msg" id="feature-msg"></span>
</div>
<script>
(function() {{
  var KEY = {key_json};
  var TENANT = {tenant_json};
  var PAGE = {page_json};
  var box = document.getElementById('feature-box');
  var dismiss = document.getElementById('feature-box-dismiss');
  if (dismiss) dismiss.addEventListener('click', function() {{ box.hidden = true; }});

  var input = document.getElementById('feature-input');
  var btn = document.getElementById('feature-submit-btn');
  var msg = document.getElementById('feature-msg');

  function submit() {{
    var text = input.value.trim();
    if (!text) return;
    msg.className = 'ei-msg saving';
    msg.textContent = 'Saving…';
    fetch('?action=feature_request', {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: JSON.stringify({{ key: KEY, tenant: TENANT, text: text, page: PAGE }})
    }}).then(function(r) {{
      return r.json().then(function(data) {{ return {{ ok: r.ok, data: data }}; }});
    }}).then(function(res) {{
      if (res.ok) {{
        msg.className = 'ei-msg saved';
        msg.textContent = 'Saved ✓';
        input.value = '';
        setTimeout(function() {{ window.location.reload(); }}, 700);
      }} else {{
        msg.className = 'ei-msg error';
        msg.textContent = (res.data && res.data.error) || 'Error';
      }}
    }}).catch(function(err) {{
      msg.className = 'ei-msg error';
      msg.textContent = 'Error: ' + err;
    }});
  }}

  btn.addEventListener('click', submit);
  input.addEventListener('keydown', function(e) {{
    if (e.key === 'Enter') {{ e.preventDefault(); submit(); }}
  }});
}})();
</script>"""


def _feature_toggle_script_html(key):
    key_json = json.dumps(key or "")
    return f"""<script>
(function() {{
  var KEY = {key_json};
  document.querySelectorAll('.feature-toggle-btn').forEach(function(btn) {{
    btn.addEventListener('click', function() {{
      var sk = btn.getAttribute('data-sk');
      var tenant = btn.getAttribute('data-tenant');
      var done = btn.getAttribute('data-done') === 'true';
      var msgEl = btn.nextElementSibling;
      if (msgEl) {{ msgEl.className = 'ei-msg saving'; msgEl.textContent = 'Saving…'; }}
      fetch('?action=feature_request', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ key: KEY, tenant: tenant, feature_sk: sk, done: done }})
      }}).then(function(r) {{
        return r.json().then(function(data) {{ return {{ ok: r.ok, data: data }}; }});
      }}).then(function(res) {{
        if (res.ok) {{
          window.location.reload();
        }} else if (msgEl) {{
          msgEl.className = 'ei-msg error';
          msgEl.textContent = (res.data && res.data.error) || 'Error';
        }}
      }}).catch(function(err) {{
        if (msgEl) {{ msgEl.className = 'ei-msg error'; msgEl.textContent = 'Error: ' + err; }}
      }});
    }});
  }});
}})();
</script>"""


def _feature_request_row_html(item, show_tenant=False, tenant_name=None, show_page=False):
    text_html = _esc(item["text"])
    tenant_tag = (f'<span class="feature-tenant-tag">{_esc(tenant_name)}</span> '
                  if show_tenant and tenant_name else "")
    # Item 5 (turn 20): the admin aggregate view labels every item by
    # page too, since it shows every page's requests together.
    page_tag = (f'<span class="feature-page-tag">'
                f'{_esc(FEATURE_PAGE_LABELS.get(item.get("page"), item.get("page")))}</span> '
                if show_page and item.get("page") else "")
    sk_attr = _esc(item["sk"])
    tenant_attr = _esc(item["tenant"])
    if item["done"]:
        done_date = _fmt_feature_date(item.get("done_at"))
        meta = f"Done &#10003; {_esc(done_date)}" if done_date else "Done &#10003;"
        toggle_btn = (f'<button type="button" class="feature-toggle-btn" data-sk="{sk_attr}" '
                      f'data-tenant="{tenant_attr}" data-done="false">Reopen</button>')
        row_cls = "feature-row done"
    else:
        created = _fmt_feature_date(item.get("created_at"))
        meta = f"Requested {_esc(created)}" if created else "Requested"
        toggle_btn = (f'<button type="button" class="feature-toggle-btn" data-sk="{sk_attr}" '
                      f'data-tenant="{tenant_attr}" data-done="true">Mark done</button>')
        row_cls = "feature-row"

    return (f'<div class="{row_cls}">'
            f'<div class="feature-row-text">{tenant_tag}{page_tag}{text_html}</div>'
            f'<div class="feature-row-meta">{meta}&nbsp;&nbsp;{toggle_btn}<span class="ei-msg"></span></div>'
            f'</div>')


def _feature_requests_list_html(open_items, done_items, show_tenant=False, tenant_names=None, show_page=False):
    if not open_items and not done_items:
        return '<div class="gg-placeholder small">No feature requests yet.</div>'
    names = tenant_names or {}
    rows = [_feature_request_row_html(it, show_tenant=show_tenant, tenant_name=names.get(it["tenant"]),
                                        show_page=show_page)
            for it in open_items]
    rows += [_feature_request_row_html(it, show_tenant=show_tenant, tenant_name=names.get(it["tenant"]),
                                         show_page=show_page)
             for it in done_items]
    return "".join(rows)


def _feature_section_html(tenant_picker, anon_key_email, key=None, page="my-deals"):
    """Feature-request box + list, shared verbatim by My Deals, Active
    Intros, and the Demand Board (item 5, turn 20 — page-scoped):
    tenant_picker (admin, no view_as) is the one case with no single
    partition to scope to — it aggregates every
    auto-enrolled tenant partition plus "admin" instead; every other case
    (real tenant session, or admin under &view_as) is scoped to
    anon_key_email. Returns (feature_box_html, feature_list_html)."""
    if tenant_picker:
        # Admin aggregate: every tenant partition, every page, unfiltered
        # (get_feature_requests(partition) with no page= defaults to
        # None -- everything) -- each row labeled by both tenant and page.
        feature_box_html = _feature_box_html("admin", key=key, page=page)
        tenant_index = _tenant_index()
        tenant_names = {"admin": "Admin"}
        agg_open, agg_done = [], []
        for partition in list(tenant_index.keys()) + ["admin"]:
            items, _ = get_feature_requests(partition)
            agg_open += items["open"]
            agg_done += items["done"]
            tenant_names[partition] = tenant_index[partition]["name"] if partition in tenant_index else "Admin"
        agg_open.sort(key=lambda it: it["sk"], reverse=True)
        agg_done.sort(key=lambda it: it["sk"], reverse=True)
        feature_list_html = _feature_requests_list_html(agg_open, agg_done, show_tenant=True,
                                                          tenant_names=tenant_names, show_page=True)
    else:
        feature_box_html = _feature_box_html(anon_key_email, key=key, page=page)
        feature_items, _ = get_feature_requests(anon_key_email, page=page)
        feature_list_html = _feature_requests_list_html(feature_items["open"], feature_items["done"])
    return feature_box_html, feature_list_html


# ── Hold / Cancel: direct Pipeline stage writes (My Deals) ───────────────
# Replaces the old action=deal_request "ask admin" flow outright — Hold
# and Cancel are now one-click, PUT the deal's stage to Pipeline for
# real, notify Chad by email, and overlay the new stage in Dynamo for
# instant feedback. The old sk="request#<deal_id>" items are simply
# ignored from here on (never read, never migrated, never cleaned up).
#
# Dynamo storage: stage_override/stage_override_at on the SAME
# sk="intro#<deal_id>" item deadline_override already lives on (see
# _resolve_deal_deadline) — not a new sk. That item is already this
# deal's one-per-deal metadata record with the exact newer-wins-overlay
# machinery this needs, so reusing it avoids inventing a fourth sk
# pattern; stage_override_at is its own field (not the existing
# status_override's override_at) so the two overlays — Intro Status on
# buy deals, stage on sell deals — can never shadow each other.
def _resolve_deal_stage(deal, override_entry=None):
    """Newer-wins overlay for a deal's stage id, mirroring
    _resolve_deal_deadline's logic exactly. override_entry is this
    deal's Dynamo intro item (from get_intro_details), or None."""
    stage_id = _deal_stage_id(deal)
    if override_entry:
        override_val = override_entry.get("stage_override")
        override_at = override_entry.get("stage_override_at")
        if override_val and override_at:
            pipeline_dt = _parse_dt(deal.get("updated_at"))
            try:
                override_dt = datetime.fromtimestamp(float(override_at), tz=timezone.utc)
            except (TypeError, ValueError, OSError):
                override_dt = None
            if override_dt is not None and (pipeline_dt is None or override_dt > pipeline_dt):
                stage_id = override_val
    return stage_id


def _pipeline_update_deal_stage(deal_id, stage_id):
    """PUT the deal's stage to Pipeline. Field name (deal_stage_id, flat —
    not the nested deal_stage.id shape deals.json reads back) verified
    verbatim in chadgracia/deal-update-form's own deal-create payload
    ("deal_stage_id": INQUIRY_STAGE_ID). Auth is the same query-string
    scheme _pipeline_update_deal_status uses. Success requires BOTH a
    2xx AND the response body echoing the new stage id back (checked via
    _deal_stage_id against the raw response, and again against a
    response["deal"] wrapper in case the API nests it there — no live
    sample response to confirm which shape it actually returns) — on
    anything else this returns False and the caller must not touch
    Dynamo."""
    if not (PIPELINE_API_KEY and PIPELINE_APP_KEY):
        return False, "Pipeline API credentials not configured"
    body = json.dumps({"deal": {"deal_stage_id": stage_id}}).encode("utf-8")
    qs = urllib.parse.urlencode({"api_key": PIPELINE_API_KEY, "app_key": PIPELINE_APP_KEY})
    req = urllib.request.Request(
        f"{PIPELINE_API_BASE}/deals/{deal_id}.json?{qs}",
        data=body, method="PUT",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            if not (200 <= r.status < 300):
                return False, f"HTTP {r.status}"
            try:
                data = json.loads(r.read().decode("utf-8"))
            except Exception:
                return False, "Pipeline response was not valid JSON"
            echoed = _deal_stage_id(data) if isinstance(data, dict) else None
            if echoed != stage_id and isinstance(data, dict) and isinstance(data.get("deal"), dict):
                echoed = _deal_stage_id(data["deal"])
            if echoed != stage_id:
                return False, f"Pipeline did not confirm the new stage (got {echoed})"
            return True, None
    except urllib.error.HTTPError as e:
        return False, f"HTTP {e.code}: {e.read().decode('utf-8', errors='replace')[:300]}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


DEAL_STAGE_EMAIL_TO = "cgracia@rainmakersecurities.com"
DEAL_STAGE_EMAIL_FROM = "agent@agent.graciagroup.com"


def _send_deal_stage_email(deal, deal_id, tenant_name, target):
    """Best-effort SES notification — failure here must never roll back
    the Pipeline write or the Dynamo overlay (see _handle_deal_stage),
    only get flagged in the audit item. Returns True/False, never
    raises."""
    action_label = {"hold": "Hold", "cancel": "Cancel", "reactivate": "Reactivate"}[target]
    deal_name = _deal_title(deal)
    company = _deal_company_name(deal) or "—"
    subject = f"[Dashboard] {action_label}: {deal_name} — {tenant_name}"
    body = (
        f"Action: {action_label}\n"
        f"Deal: {deal_name} (ID {deal_id})\n"
        f"Company: {company}\n"
        f"Tenant: {tenant_name}\n"
        f"Timestamp: {datetime.now(timezone.utc).isoformat()}\n"
    )
    try:
        ses = boto3.client("ses", region_name="us-east-1")
        ses.send_email(
            Source=DEAL_STAGE_EMAIL_FROM,
            Destination={"ToAddresses": [DEAL_STAGE_EMAIL_TO]},
            Message={"Subject": {"Data": subject}, "Body": {"Text": {"Data": body}}},
        )
        return True
    except Exception:
        return False


def _dynamo_write_deal_stage_override(tenant_email, deal_id, stage_id, actor, old_stage_id, email_failed):
    now = time.time()
    try:
        table = _dynamo_table()
        table.update_item(
            Key={"tenant": tenant_email, "sk": f"intro#{deal_id}"},
            UpdateExpression="SET stage_override = :s, stage_override_at = :sa",
            ExpressionAttributeValues={":s": stage_id, ":sa": now},
        )
        table.put_item(Item={
            "tenant": tenant_email,
            "sk": f"audit#{deal_id}#{int(now * 1000)}",
            "actor": actor,
            "old": {"stage": old_stage_id},
            "new": {"stage": stage_id},
            "email_failed": email_failed,
        })
        return True, None
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def _handle_deal_stage(event):
    """POST ?action=deal_stage — Hold, Cancel, or Reactivate a deal via a DIRECT
    Pipeline stage write (never a request/ask — see the replacement note
    above). Auth mirrors _handle_update_intro exactly: the deal's owning
    partition is always derived server-side via _tenant_email_for_deal,
    never taken from the client, so a tenant post can only ever land on
    their own deal; ADMIN_KEY may act on any deal.

    Order, per instruction: (a) PUT the new stage to Pipeline and abort
    everything — nothing written to Dynamo, no email sent — on any
    failure, including a mismatched echoed stage id; (b) send the
    Chad-facing SES notification, whose failure does NOT roll back
    anything, just gets flagged in the audit item; (c) write the
    stage_override + audit item. Never raises past this function."""
    body = _parse_json_body(event)

    admin_key = os.environ.get("ADMIN_KEY")
    is_admin = bool(admin_key) and body.get("key") == admin_key

    deal_id = str(body.get("deal_id") or "").strip()
    if not deal_id:
        return _json_response({"error": "deal_id is required"}, 400)

    target = body.get("target")
    if target not in ("hold", "cancel", "reactivate"):
        return _json_response({"error": "invalid target"}, 400)
    target_stage_id = {"hold": HOLD_STAGE_ID, "cancel": OBSOLETE_STAGE_ID, "reactivate": STAGE_INQUIRY}[target]

    deals = get_deals_list()
    deal = next((d for d in deals if str(d.get("id")) == deal_id), None)
    if deal is None:
        return _json_response({"error": "deal not found"}, 404)

    tenant_email = _tenant_email_for_deal(deal)
    if tenant_email is None:
        return _json_response({"error": "deal has no linked tenant"}, 400)

    if is_admin:
        actor = "admin"
    else:
        identity_email = _read_identity_email(event)
        tenant_identity_email = identity_email.strip().lower() if identity_email else None
        if not tenant_identity_email or _resolve_tenant(tenant_identity_email) is None:
            return _json_response({"error": "forbidden"}, 403)
        if tenant_identity_email != tenant_email:
            return _json_response({"error": "forbidden"}, 403)
        actor = tenant_identity_email

    old_stage_id = _deal_stage_id(deal)

    ok, err = _pipeline_update_deal_stage(deal_id, target_stage_id)
    if not ok:
        return _json_response({"error": f"Pipeline update failed: {err}"}, 502)

    tenant_name = (_resolve_tenant(tenant_email) or {}).get("name", tenant_email)
    email_ok = _send_deal_stage_email(deal, deal_id, tenant_name, target)

    ok, err = _dynamo_write_deal_stage_override(tenant_email, deal_id, target_stage_id, actor, old_stage_id,
                                                 not email_ok)
    if not ok:
        return _json_response({"error": f"Pipeline updated but save failed: {err}"}, 502)

    return _json_response({"ok": True})


def _deal_stage_script_html():
    return """<script>
(function() {
  var CONFIRM_TEXT = {
    hold: function(name) {
      return 'Put ' + name + ' on hold? Buyers will no longer be shown this deal until you reactivate.';
    },
    cancel: function(name) {
      return 'Cancel ' + name + '? This marks the deal obsolete and removes it from circulation.';
    },
    reactivate: function(name) {
      return 'Reactivate ' + name + '? This sets the deal back to Inquiry and returns it to your active pipeline.';
    }
  };
  document.querySelectorAll('.deal-stage-btn').forEach(function(btn) {
    btn.addEventListener('click', function() {
      var target = btn.getAttribute('data-target');
      if (!window.confirm(CONFIRM_TEXT[target](btn.getAttribute('data-deal-name')))) return;
      fetch('?action=deal_stage', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          key: btn.getAttribute('data-key'),
          deal_id: btn.getAttribute('data-deal-id'),
          target: target
        })
      }).then(function(r) {
        return r.json().then(function(data) { return { ok: r.ok, data: data }; });
      }).then(function(res) {
        if (res.ok) { window.location.reload(); }
        else { alert((res.data && res.data.error) || 'Error'); }
      }).catch(function(err) { alert('Error: ' + err); });
    });
  });
})();
</script>"""


def _deal_actions_html(deal_id, deal_name, update_btn, section, key=None):
    """update_btn is the existing minted Update link, unchanged. section is
    which of My Deals' stacked tables this row belongs to: "active" rows
    get Hold + Cancel; "hold" rows get Reactivate (writes stage back
    to Inquiry — same direct-write path, available to tenant and admin
    alike, exactly like Hold/Cancel: the data-key value is only ever a
    real ADMIN_KEY for an admin session, empty for a tenant session, and
    the server derives the tenant identity from the auth cookie either
    way, never from this attribute); "cancelled" and "closed" (turn 26 —
    a won deal has nothing left to Hold/Cancel/Reactivate either) rows
    are both terminal, no stage-change actions, Update link only."""
    key_attr = _esc(key or "")
    id_attr = _esc(deal_id)
    name_attr = _esc(deal_name)
    if section == "hold":
        reactivate_btn = (f'<button type="button" class="deal-stage-btn" data-key="{key_attr}" '
                           f'data-deal-id="{id_attr}" data-deal-name="{name_attr}" '
                           f'data-target="reactivate">Reactivate</button>')
        return f'<div class="actions-stack">{update_btn}{reactivate_btn}</div>'
    if section in ("cancelled", "closed"):
        return f'<div class="actions-stack">{update_btn}</div>'
    hold_btn = (f'<button type="button" class="deal-stage-btn" data-key="{key_attr}" data-deal-id="{id_attr}" '
                f'data-deal-name="{name_attr}" data-target="hold">Hold</button>')
    cancel_btn = (f'<button type="button" class="deal-stage-btn" data-key="{key_attr}" data-deal-id="{id_attr}" '
                  f'data-deal-name="{name_attr}" data-target="cancel">Cancel</button>')
    return f'<div class="actions-stack">{update_btn}{hold_btn}{cancel_btn}</div>'


def _default_intro_status(deal):
    """Derived status when the Intro Status field is empty/absent for a
    deal (including when the deals.json snapshot hasn't picked up the
    brand-new field yet): always "Matched" — per instruction, empty/
    derived-Matched is the ONLY default and belongs in Pending
    introductions regardless of any other deal attribute.

    Previously this returned "Closed" when deal.get("is_archived") was
    true, on the theory that is_archived was the closest available proxy
    for "won" before this field existed. That was the actual bug behind
    rows with a genuinely empty Intro Status showing up under
    "Introduced": is_archived also covers deals that are archived because
    they're dead/lost/inactive, not just won ones, so any archived deal
    that was never actually introduced was mislabeled "Closed" and
    misclassified as disclosed. "Closed" is now only ever reached through
    a real Pipeline value (id 7207587) via _deal_intro_status_id — never
    guessed."""
    return "Matched"


def _deal_intro_status_id(deal):
    """The raw Intro Status option id from custom_label_4008329, or None
    if empty/absent/unrecognized — including every deal, always, until the
    field has actually been backfilled into a deals.json snapshot. Never
    raises."""
    status_id = next(iter(_deal_cf_option_ids(deal, INTRO_STATUS_FIELD)), None)
    return status_id if status_id in INTRO_STATUS_LABELS else None


def _resolve_intro_status(deal, override_entry=None):
    """The single place every page reads a deal's intro status from.
    Returns {"id", "name", "is_exit", "disclosed"}.

    override_entry is this deal's Dynamo intro item (from
    get_intro_details), or None. When it carries a valid status_override
    and an override_at strictly newer than the deal's own updated_at in
    deals.json, the override wins outright — including for the disclosure
    gate below. An older or missing override, or one on a deal with no
    updated_at to compare against unless the override itself is present,
    never shadows the Pipeline field.

    "disclosed" is the hard privacy gate: True for every status except an
    explicit or derived Matched, WITH ONE CARVE-OUT (turn 27, closing the
    hole this docstring used to flag for confirmation): for the three
    exit ids (Stalled/Passed/Withdrawn) specifically, "not Matched" is no
    longer enough on its own. A deal can reach an exit status straight
    from empty/Matched without ever having been Introduced (an admin
    flagging it Passed, or Pipeline's own field jumping there directly),
    and naming a row for that case would leak identity for a deal the
    buyer was never actually shown to have been introduced on. So an exit
    status now discloses only when there is real evidence of prior
    progress: either the Dynamo intro item has ever recorded a milestone
    (the "milestones" map is add-only, never erased -- see
    MILESTONE_STATUS_IDS), or the deal's RAW (pre-override)
    Intro Status field itself is independently Introduced-or-later (see
    INTRODUCED_OR_LATER_STATUS_IDS) -- covering the case where an admin's
    status_override holds the exit value but Pipeline's own field still
    shows genuine progress. Neither signal present (status jumped from
    empty straight to an exit) -> stays anonymized, exactly like a
    Pending row. The six named Introduced-through-Closed ids are
    unaffected: those still disclose simply for not being Matched, and an
    empty/absent field always derives "Matched" (see
    _default_intro_status) -- there is no other derived value, so
    disclosed is False whenever the field is genuinely unset, regardless
    of any other deal attribute (is_archived included)."""
    status_id = _deal_intro_status_id(deal)
    raw_status_id = status_id

    if override_entry:
        override_id = override_entry.get("status_override")
        override_at = override_entry.get("override_at")
        if override_id in INTRO_STATUS_LABELS and override_at:
            pipeline_dt = _parse_dt(deal.get("updated_at"))
            try:
                override_dt = datetime.fromtimestamp(float(override_at), tz=timezone.utc)
            except (TypeError, ValueError, OSError):
                override_dt = None
            if override_dt is not None and (pipeline_dt is None or override_dt > pipeline_dt):
                status_id = override_id

    name = INTRO_STATUS_LABELS[status_id] if status_id is not None else _default_intro_status(deal)
    if status_id in EXIT_STATUS_IDS:
        has_milestone = bool((override_entry or {}).get("milestones"))
        disclosed = has_milestone or (raw_status_id in INTRODUCED_OR_LATER_STATUS_IDS)
    else:
        disclosed = name != "Matched"
    return {
        "id": status_id,
        "name": name,
        "is_exit": status_id in EXIT_STATUS_IDS,
        "disclosed": disclosed,
    }


def _tenant_has_disclosed_deal_with(person_id, tenant_email, buyer_id):
    """The buyer page's disclosure gate (item 3, turn 18): True only when
    this tenant has at least one Introduced-or-later deal linked to
    buyer_id, scanned across EVERY company the tenant has matched buy
    deals in — not just the one deal the viewer happened to click in
    from — using the exact same _resolve_intro_status disclosure rule
    Active Intros itself already enforces per row. person_id is the
    tenant's own person_id (never the buyer's).

    Turn 27: also true when the ONLY disclosed deal linking this buyer to
    the tenant is a stage-derived closed-out one (Lost/Trade Broken/
    Obsolete) — otherwise a buyer the tenant was genuinely introduced to,
    on a deal that later died, would be gated behind the anonymized
    fallback and _buyer_track_with_you_html's own closed-out row would
    never be reachable at all."""
    if person_id is None or buyer_id is None:
        return False
    intro_details = None
    deals = get_my_matched_buy_deals(person_id)
    if deals:
        intro_details, _ = get_intro_details(tenant_email)
        for d in deals:
            if buyer_id not in _deal_linked_person_ids(d):
                continue
            resolved = _resolve_intro_status(d, intro_details.get(str(d.get("id"))))
            if resolved["disclosed"]:
                return True
    for d in get_my_closed_out_buy_deals(person_id):
        if buyer_id not in _deal_linked_person_ids(d):
            continue
        if _closed_out_disclosed(d):
            return True
    return False


def _derive_status_from_checked(checked_steps):
    """Turn 17, item 1: the Pipeline status implied by a set of checked
    milestone checkboxes -- the furthest one reached
    (CHECKBOX_MILESTONE_STEPS order), or Introduced when nothing's
    checked. checked_steps is never expected to contain "Closed" --
    checkboxes only ever cover NDA/VDR/Sub Docs/Wired -- so this can
    never derive anything past Wired; Closed only ever comes from the
    flag control."""
    for step in reversed(CHECKBOX_MILESTONE_STEPS):
        if step in checked_steps:
            return MILESTONE_STEP_STATUS_ID[step]
    return INTRO_STATUS_INTRODUCED_ID


# ── Admin write path: Intro Status / Next Steps / Buyer Notes ───────────────

def _pipeline_update_deal_status(deal_id, status_id):
    """PUT the Intro Status field to Pipeline. Returns (ok, error_message).
    Auth is Pipeline's query-string scheme (?api_key=...&app_key=...,
    both URL-encoded, no Authorization header) — verified working for
    reads against this account; no Authorization-header alternative was
    ever confirmed, so this replaces the earlier unverified Basic-auth
    guess outright. On any non-2xx response (or any other failure) this
    returns False and writes nothing — the caller must not touch Dynamo
    when this fails."""
    if not (PIPELINE_API_KEY and PIPELINE_APP_KEY):
        return False, "Pipeline API credentials not configured"
    body = json.dumps({"deal": {"custom_fields": {INTRO_STATUS_FIELD: status_id}}}).encode("utf-8")
    qs = urllib.parse.urlencode({"api_key": PIPELINE_API_KEY, "app_key": PIPELINE_APP_KEY})
    req = urllib.request.Request(
        f"{PIPELINE_API_BASE}/deals/{deal_id}.json?{qs}",
        data=body, method="PUT",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            if 200 <= r.status < 300:
                return True, None
            return False, f"HTTP {r.status}"
    except urllib.error.HTTPError as e:
        return False, f"HTTP {e.code}: {e.read().decode('utf-8', errors='replace')[:300]}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def _pipeline_update_deal_deadline(deal_id, deadline_iso):
    """PUT the Deadline field to Pipeline, converting the input's ISO
    yyyy-mm-dd to the "YYYY/MM/DD" slash format _deal_deadline_text (and
    _parse_dt) already assume deals.json uses for this field — see
    DEADLINE_FIELD's own comment: that assumption was never independently
    verified against a live snapshot, and this mirrors it symmetrically on
    the write side rather than introducing a second, different guess.
    Returns (ok, error_message); otherwise identical to
    _pipeline_update_deal_status, including "abort on any non-2xx"."""
    try:
        dt = datetime.strptime(deadline_iso, "%Y-%m-%d")
    except ValueError:
        return False, "invalid deadline date"
    if not (PIPELINE_API_KEY and PIPELINE_APP_KEY):
        return False, "Pipeline API credentials not configured"
    body = json.dumps({"deal": {"custom_fields": {DEADLINE_FIELD: dt.strftime("%Y/%m/%d")}}}).encode("utf-8")
    qs = urllib.parse.urlencode({"api_key": PIPELINE_API_KEY, "app_key": PIPELINE_APP_KEY})
    req = urllib.request.Request(
        f"{PIPELINE_API_BASE}/deals/{deal_id}.json?{qs}",
        data=body, method="PUT",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            if 200 <= r.status < 300:
                return True, None
            return False, f"HTTP {r.status}"
    except urllib.error.HTTPError as e:
        return False, f"HTTP {e.code}: {e.read().decode('utf-8', errors='replace')[:300]}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def _tenant_email_for_deal(deal):
    """The auto-enrolled tenant email whose person_id is linked to this
    deal, or None if no tenant maps to it — writes are rejected outright
    in that case."""
    linked = _deal_linked_person_ids(deal)
    for email, info in _tenant_index().items():
        if info.get("person_id") in linked:
            return email
    return None


def _dynamo_write_intro_update(tenant_email, deal_id, status_id, next_steps, notes, follow_up, deadline,
                                old_values, actor, milestones=None):
    """Update the intro item's Dynamo-owned attributes (status_override/
    override_at when status_id is not None, next_steps/notes/follow_up
    when they are not None, deadline_override/deadline_override_at when
    deadline is not None, milestones when milestones is not None -- the
    caller (_handle_update_intro) has already merged it add-only against
    the item's prior milestones map, so this just SETs the whole map
    wholesale rather than touching a single nested key) and append an
    audit item (sk=audit#<deal_id>#<epoch_ms>, actor "admin" or the
    tenant's own email, old and new values). Returns (ok, error_message).
    Never called when a requested Pipeline write failed — see
    _handle_update_intro."""
    update_parts = []
    expr_names = {}
    expr_values = {}
    new_values = {}
    now = time.time()

    if status_id is not None:
        update_parts.append("#so = :so")
        expr_names["#so"] = "status_override"
        expr_values[":so"] = status_id
        update_parts.append("override_at = :oa")
        expr_values[":oa"] = int(now)
        new_values["status_override"] = status_id
    if milestones is not None:
        update_parts.append("milestones = :ms")
        expr_values[":ms"] = milestones
        new_values["milestones"] = milestones
    if next_steps is not None:
        update_parts.append("next_steps = :ns")
        expr_values[":ns"] = next_steps
        new_values["next_steps"] = next_steps
    if notes is not None:
        update_parts.append("notes = :no")
        expr_values[":no"] = notes
        new_values["notes"] = notes
        # Turn 23: the Notes Ledger block on the buyer page shows a
        # last-edited date per entry -- stamped here (epoch seconds,
        # matching override_at's own shape) rather than trusting Dynamo's
        # own item metadata, which this table never otherwise reads.
        update_parts.append("notes_updated_at = :nua")
        expr_values[":nua"] = int(now)
        new_values["notes_updated_at"] = int(now)
    if follow_up is not None:
        update_parts.append("follow_up = :fu")
        expr_values[":fu"] = follow_up
        new_values["follow_up"] = follow_up
    if deadline is not None:
        update_parts.append("deadline_override = :do")
        expr_values[":do"] = deadline
        update_parts.append("deadline_override_at = :doa")
        expr_values[":doa"] = int(now)
        new_values["deadline"] = deadline

    if not update_parts:
        return True, None

    try:
        table = _dynamo_table()
        kwargs = {
            "Key": {"tenant": tenant_email, "sk": f"intro#{deal_id}"},
            "UpdateExpression": "SET " + ", ".join(update_parts),
            "ExpressionAttributeValues": expr_values,
        }
        if expr_names:
            kwargs["ExpressionAttributeNames"] = expr_names
        table.update_item(**kwargs)
        table.put_item(Item={
            "tenant": tenant_email,
            "sk": f"audit#{deal_id}#{int(now * 1000)}",
            "actor": actor,
            "old": old_values,
            "new": new_values,
        })
        return True, None
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def _status_strip_html(status):
    idx = STATUS_INDEX.get(status, 0)
    segments = []
    for i, step_name in enumerate(STATUS_STEPS):
        cls = "status-step"
        if i < idx:
            cls += " done"
        elif i == idx:
            cls += " current"
        segments.append(f'<span class="{cls}" title="{_esc(step_name)}"></span>')
    return (f'<div class="status-strip">{"".join(segments)}'
            f'<span class="status-label">{_esc(status)}</span></div>')


def _status_pill_html(status):
    return f'<span class="status-pill">{_esc(status)}</span>'


def _status_chip_html(status_id, name):
    """Stalled/Passed/Withdrawn — never a strip/pill segment."""
    if status_id == INTRO_STATUS_STALLED_ID:
        return '<span class="status-chip stalled">Stalled — needs a nudge</span>'
    return f'<span class="status-chip exit">{_esc(name)}</span>'


def _status_display_html(resolved, compact):
    """compact=True -> pill (Matched Buyers/Buyers table); False -> the
    7-segment strip (Active Intros). Exit states always render as a chip
    regardless of compact."""
    if resolved["is_exit"]:
        return _status_chip_html(resolved["id"], resolved["name"])
    if compact:
        return _status_pill_html(resolved["name"])
    return _status_strip_html(resolved["name"])


def _intro_sort_rank(resolved):
    """Furthest-along-first sort key. Stalled has no real notion of "how
    far" a deal got before stalling, so it's ranked alongside Introduced —
    a documented judgment call, not a verified fact."""
    if resolved["is_exit"]:
        return STATUS_INDEX.get("Introduced", 0)
    return STATUS_INDEX.get(resolved["name"], 0)


def _fmt_pct(v):
    if v is None:
        return None
    return f"{v:.0f}%" if v == int(v) else f"{v:.1f}%"


def _fmt_fees(deal):
    """'2% mgmt · 20% carry · 4% seller fee', omitting empty parts; None if
    all three are empty (the line is omitted entirely). Partner Fee
    (custom_label_3940561) is never read here."""
    mgmt = _fmt_pct(_deal_cf_number(deal, MGMT_FEE_FIELD))
    carry = _fmt_pct(_deal_cf_number(deal, CARRY_FIELD))
    seller = _fmt_pct(_deal_cf_number(deal, SELLER_FEE_FIELD))
    parts = []
    if mgmt:
        parts.append(f"{mgmt} mgmt")
    if carry:
        parts.append(f"{carry} carry")
    if seller:
        parts.append(f"{seller} seller fee")
    return " · ".join(parts) if parts else None


def _deal_deadline_text(deal):
    """Normalized deadline date, or None (line omitted) if empty/unparsable.
    _parse_dt already handles the "YYYY/MM/DD" slash format."""
    raw = (deal.get("custom_fields") or {}).get(DEADLINE_FIELD)
    if isinstance(raw, list):
        raw = raw[0] if raw else None
    if not raw:
        return None
    dt = _parse_dt(raw)
    return dt.strftime("%Y-%m-%d") if dt else str(raw)


def _resolve_deal_deadline(deal, override_entry=None):
    """Newer-wins overlay for Deadline, mirroring _resolve_intro_status's
    override logic exactly but keyed on deadline_override/
    deadline_override_at — kept separate from status_override/override_at
    so the two overlays never shadow each other. override_entry is this
    deal's Dynamo intro item (from get_intro_details), or None. Returns
    the resolved ISO yyyy-mm-dd date string, or None."""
    deadline = _deal_deadline_text(deal)
    if override_entry:
        override_val = override_entry.get("deadline_override")
        override_at = override_entry.get("deadline_override_at")
        if override_val and override_at:
            pipeline_dt = _parse_dt(deal.get("updated_at"))
            try:
                override_dt = datetime.fromtimestamp(float(override_at), tz=timezone.utc)
            except (TypeError, ValueError, OSError):
                override_dt = None
            if override_dt is not None and (pipeline_dt is None or override_dt > pipeline_dt):
                deadline = override_val
    return deadline


def _deal_size_text(deal):
    """Ticket min/max, falling back to the deal's own 'value' field (a
    built-in Pipeline deal attribute, not a custom_field) when both are
    empty."""
    ticket_max = _deal_cf_number(deal, TICKET_MAX_FIELD)
    ticket_min = _deal_cf_number(deal, TICKET_MIN_FIELD)
    size_val = ticket_max if ticket_max is not None else ticket_min
    if size_val is None:
        try:
            size_val = float(deal.get("value"))
        except (TypeError, ValueError):
            size_val = None
    return _fmt_money(size_val)


def _engagement_badge_html(deal, company):
    opts = _deal_cf_option_ids(deal, AGENT_AGREEMENT_FIELD)
    if opts & AGENT_ENGAGED_OPTS:
        return '<span class="engagement-badge engaged">&#10003; Engaged with RMS</span>'
    if opts & AGENT_IN_PROCESS_OPTS:
        return '<span class="engagement-badge in-process">&#8226; Engagement in process</span>'
    subject = urllib.parse.quote(f"Engage RMS re {company}", safe="")
    href = f"mailto:{FEATURE_REQUEST_EMAIL}?subject={subject}"
    return (f'<a class="engagement-badge not-engaged" href="{href}">'
            '&#10007; Not engaged — contact us to activate this deal</a>')


def _deal_pipeline_size(deal):
    """Numeric size for the summary strip's dollar totals: the larger of
    min/max ticket size, else the deal's own "value" field. None if
    neither is present — callers must skip rather than treat as 0."""
    min_val = _deal_cf_number(deal, TICKET_MIN_FIELD)
    max_val = _deal_cf_number(deal, TICKET_MAX_FIELD)
    candidates = [v for v in (min_val, max_val) if v is not None]
    if candidates:
        return max(candidates)
    try:
        return float(deal.get("value"))
    except (TypeError, ValueError):
        return None


# Stage-based won predicate: routes a Sell deal into My Deals' own
# "Closed" section (turn 26 — see render_my_deals_page) and, by the same
# stroke, is what "Total closed" sums (every row in that section is won
# by construction). Superseded the earlier deal.get("status")-based guess
# entirely — stage id is the shape deals.json actually carries (see
# _deal_stage_id) and WON_STAGE_IDS is given as verified. Callers pass
# the deal's *resolved* stage (_resolve_deal_stage, override-aware)
# rather than calling _deal_stage_id directly, so a same-session
# Hold/Cancel/Reactivate is reflected in the totals immediately without
# waiting for deals.json to resync.
def _is_won_stage(stage_id):
    return stage_id in WON_STAGE_IDS


def _deal_terms_complete(deal):
    """State 3's "terms incomplete" gate: a ticket-size bound (min OR max)
    present, AND each of Mgmt Fee/Carry/Seller Fee individually carries a
    defined numeric value. 0.0 counts as defined — _deal_cf_number
    already returns 0.0 (not None) for a scalar zero, and only returns
    None for a genuinely null/empty/absent field, which is exactly the
    distinction this gate needs."""
    has_size = (_deal_cf_number(deal, TICKET_MIN_FIELD) is not None
                or _deal_cf_number(deal, TICKET_MAX_FIELD) is not None)
    if not has_size:
        return False
    return all(_deal_cf_number(deal, field) is not None
               for field in (MGMT_FEE_FIELD, CARRY_FIELD, SELLER_FEE_FIELD))


def _my_deal_visibility_state(deal, cef_state, is_held, is_won=False):
    """Strict state machine, first match wins: (0, turn 26) is_won (the
    row's resolved stage is in WON_STAGE_IDS — the caller derives this
    from the same resolved-stage/section logic My Deals' sectioning
    already uses) -> "sold", short-circuiting every other check outright
    — a won deal is never "held" and never "live" regardless of its
    CEF/Agreement/Terms state, none of which mean anything once the deal
    has actually closed; (1) ID/CEF not Yes -> "id_required"; (2) Agent
    Agreement not Yes — In-Process counts as unsigned — ->
    "agreement_unsigned"; (3) deal terms incomplete (see
    _deal_terms_complete) -> "terms_incomplete"; (4) stage is Hold
    (is_held, override-aware) -> "held"; (5) otherwise -> "live". Single
    source of truth shared by the Visibility badge, the Next Steps chip,
    and the summary-strip counts. Obsolete/Lost ("cancelled" section)
    rows run through this exact same ladder unchanged — their muted
    treatment is the Cancelled section's own card styling, not a
    distinct visibility state; only "sold" gets one."""
    if is_won:
        return "sold"
    if cef_state != CEF_YES_ID:
        return "id_required"
    opts = _deal_cf_option_ids(deal, AGENT_AGREEMENT_FIELD)
    if not (opts & AGENT_ENGAGED_OPTS):
        return "agreement_unsigned"
    if not _deal_terms_complete(deal):
        return "terms_incomplete"
    if is_held:
        return "held"
    return "live"


def _my_deal_visibility_badge_html(deal, cef_state, is_held, is_won=False):
    """State-only — no links (item 1); Next Steps carries the actionable
    links for these same states now (item 2). Turn 26: "sold" is the
    Closed section's own muted-but-positive green badge."""
    state = _my_deal_visibility_state(deal, cef_state, is_held, is_won=is_won)
    if state == "sold":
        return '<span class="visibility-badge sold">Sold &#10003;</span>'
    if state == "id_required":
        return '<span class="visibility-badge id-required">Not live · ID required</span>'
    if state == "agreement_unsigned":
        return '<span class="visibility-badge agreement-unsigned">Not live · unsigned agreement</span>'
    if state == "terms_incomplete":
        return '<span class="visibility-badge terms-incomplete">Not live · awaiting deal terms</span>'
    if state == "held":
        return '<span class="visibility-badge held">Held · not shown to buyers</span>'
    return '<span class="visibility-badge live">Live · shown to buyers</span>'


def _deal_card_html(deal, company, override_entry=None, edit_mode=False):
    """override_entry is this deal's Dynamo intro item (from
    get_intro_details, keyed by the deal's own linked tenant — see
    render_company_page), used to resolve any deadline_override. edit_mode
    (admin-only, never tenant_edit_mode) swaps the read-only Deadline line
    for an auto-saving date input. Overdue emphasis (red border + chip)
    always applies, admin and tenant alike, whenever the resolved deadline
    is in the past — no date, no warning."""
    name = _esc(_deal_title(deal))
    sid = _deal_stage_id(deal)
    stage = _esc(STAGE_LABELS.get(sid, str(sid) if sid is not None else "—"))
    size_text = _esc(_deal_size_text(deal))
    net_text = _esc(_fmt_money(_deal_cf_number(deal, NET_FIELD)))

    struct_label = STRUCTURE_LABELS.get(next(iter(_deal_cf_option_ids(deal, STRUCTURE_FIELD)), None))
    layer_label = LAYERS_MAP.get(next(iter(_deal_cf_option_ids(deal, LAYERS_FIELD)), None))
    struct_parts = [p for p in (struct_label, layer_label) if p]
    structure_text = _esc(" · ".join(struct_parts)) if struct_parts else "—"

    deal_id = str(deal.get("id"))
    deadline = _resolve_deal_deadline(deal, override_entry)
    if edit_mode:
        deadline_input = _ei_date_field_html(deal_id, deadline or "", css_class="ei-deadline", field="deadline")
        deadline_html = f'<div class="dc-line">Deadline: {deadline_input}</div>'
    else:
        deadline_html = (f'<div class="dc-line">Deadline: {_esc(deadline)}</div>'
                          if deadline else "")

    is_overdue = False
    if deadline:
        deadline_dt = _parse_dt(deadline)
        is_overdue = bool(deadline_dt and deadline_dt.date() < datetime.now(timezone.utc).date())

    card_cls = "deal-card overdue" if is_overdue else "deal-card"
    overdue_html = ""
    if is_overdue:
        if edit_mode:
            overdue_html = '<div class="overdue-chip">Deadline passed — update or cancel this deal</div>'
        else:
            # The update-form link replaces the mailto for tenants (see
            # _deal_update_form_url) — falls back to the mailto only when
            # HMAC_SECRET isn't configured, so this chip is never dead.
            update_url = _deal_update_form_url(deal_id)
            if update_url:
                href = update_url
            else:
                subject = urllib.parse.quote(f"Deadline passed: {_deal_title(deal)}", safe="")
                href = f"mailto:{FEATURE_REQUEST_EMAIL}?subject={subject}"
            overdue_html = (f'<a class="overdue-chip" href="{href}" target="_blank" rel="noopener noreferrer">'
                             'Deadline passed — update or cancel this deal</a>')

    exemption_id = next(iter(_deal_cf_option_ids(deal, EXEMPTION_FIELD)), None)
    exemption_label = EXEMPTION_LABELS.get(exemption_id)
    exemption_html = (f'<div class="dc-line">Exemption: {_esc(exemption_label)}</div>'
                       if exemption_label else "")

    fees = _fmt_fees(deal)
    fees_html = f'<div class="dc-line">{_esc(fees)}</div>' if fees else ""

    badge_html = _engagement_badge_html(deal, company) + _update_cancel_button_html(deal_id)

    return f"""<div class="{card_cls}">
      {overdue_html}
      <div class="deal-card-head">
        <div class="deal-card-title">{name}</div>
        <div class="deal-card-stage">{stage}</div>
      </div>
      <div class="deal-card-metrics">
        <div><span class="dc-label">Size</span><span class="dc-value">{size_text}</span></div>
        <div><span class="dc-label">Net</span><span class="dc-value">{net_text}</span></div>
        <div><span class="dc-label">Structure</span><span class="dc-value">{structure_text}</span></div>
      </div>
      {deadline_html}
      {exemption_html}
      {fees_html}
      {badge_html}
    </div>"""


def _buyer_name_cell_html(buyer_recs, show_contact, link=False, key=None, view_as=None):
    """Buyer name(s) for the Company page's Matched Buyers / Buyers
    table, plus a contact second line (email/phone) when show_contact is
    True. When False, no contact info is emitted anywhere in the HTML —
    not hidden via CSS, simply never written. link=True (item 3, turn
    18 — Introduced-or-later rows only, the caller gates this on
    resolved["disclosed"]) wraps each name in a link to that buyer's own
    page (_buyer_href).

    won_deals_total (for the green "has closed with Rainmaker" dot) does
    not appear anywhere in portfolio-deploy, deal-notifier, loi-sign,
    web-bid, trades, or daily-brief — no code anywhere reads such a field
    off a person record — so the dot is never rendered here."""
    if not buyer_recs:
        return "—"
    if link:
        # Item 6 (turn 20): the same clear link affordance as Active
        # Intros -- accent color, hover underline, muted "profile →"
        # suffix (see .buyer-link/.buyer-link-suffix CSS below).
        names = ", ".join(
            f'<a class="buyer-link" href="{_buyer_href(r.get("id"), key, view_as)}">'
            f'{_esc(_person_display_name(r) or "—")}'
            f'<span class="buyer-link-suffix"> profile &rarr;</span></a>'
            for r in buyer_recs
        )
    else:
        names = ", ".join(_esc(_person_display_name(r) or "—") for r in buyer_recs)
    if not show_contact:
        return f'<div>{names}</div>'
    contact_lines = []
    for r in buyer_recs:
        bits = [b for b in (_person_email_text(r), _person_phone_text(r)) if b]
        if bits:
            contact_lines.append(" · ".join(bits))
    contact_html = (f'<div class="buyer-contact">{_esc(", ".join(contact_lines))}</div>'
                     if contact_lines else "")
    return f'<div>{names}</div>{contact_html}'


def _buyer_contact_detail_html(buyer_recs):
    """Compact extra line under the disclosed buyer's name/contact block:
    country (work_country, falling back to home_country) · website
    (linkified) · LinkedIn (linkified). Unlike email/phone above, no
    sibling repo in this org has ever read these three native person
    fields, so — same trust basis as _person_phone_text's own "phone"
    field — they're read via plain .get() and simply omitted (the whole
    line included) if absent or differently shaped; flag for confirmation
    against a real people.json snapshot. Only ever called for a disclosed
    buyer — the caller gates this on resolved["disclosed"], never a
    pending/anonymous row."""
    if not buyer_recs:
        return ""
    rec = buyer_recs[0]
    country = rec.get("work_country") or rec.get("home_country") or ""
    country = country.strip() if isinstance(country, str) else ""
    website = rec.get("website") or ""
    website = website.strip() if isinstance(website, str) else ""
    linked_in = rec.get("linked_in_url") or ""
    linked_in = linked_in.strip() if isinstance(linked_in, str) else ""

    parts = []
    if country:
        parts.append(_esc(country))
    if website:
        href = website if "://" in website else f"https://{website}"
        parts.append(f'<a href="{_esc(href)}" target="_blank" rel="noopener noreferrer">{_esc(website)}</a>')
    if linked_in:
        href = linked_in if "://" in linked_in else f"https://{linked_in}"
        parts.append(f'<a href="{_esc(href)}" target="_blank" rel="noopener noreferrer">LinkedIn</a>')
    if not parts:
        return ""
    return f'<div class="buyer-detail">{" · ".join(parts)}</div>'


def _investor_type_and_company(buyer_recs, disclosed):
    """(investor_type_text, company_text) for the first linked buyer — the
    same "first buyer" convention already used for the Entity/Natural
    Person determination. Investor Type renders the full Transactor Type
    label (TRANSACTOR_TYPE_LABELS) — blank only when the field is truly
    unset or holds an id outside the verified map — and isn't
    identity-revealing on its own (entity type, not who), so it's shown
    regardless of disclosure. company_name IS one of the four
    disclosure-gated fields, so it's forced to "—" whenever disclosed is
    False — never the real value, gated or not."""
    if not buyer_recs:
        return "", "—"
    first_cf = buyer_recs[0].get("custom_fields") or {}
    transactor_ids = cf_list(first_cf, TRANSACTOR_TYPE_FIELD)
    transactor_id = transactor_ids[0] if transactor_ids else None
    is_natural = transactor_id == NATURAL_PERSON_ID
    investor_type = TRANSACTOR_TYPE_LABELS.get(transactor_id, "")
    if not disclosed:
        return investor_type, "—"
    company = "—" if is_natural else (buyer_recs[0].get("company_name") or "—")
    return investor_type, company


def _pending_buyer_cell_html(buyer_recs, anon_key_email):
    """The anonymized "Pending introduction" replacement for the buyer
    name cell: each linked buyer's existing 4-char anon code, tier badge,
    and ticket range — reusing the exact same helpers and CSS classes
    (_anon_buyer_code, classify_person/TIER_LABELS, get_person_ticket_range/
    _fmt_ticket_range, .buyer-code/.tier-badge/.buyer-range) as the Buyer
    Demand tiles. No name, company, email, or phone is ever looked up or
    written here."""
    if not buyer_recs:
        return "—"
    blocks = []
    for rec in buyer_recs:
        pid = rec.get("id")
        cf = rec.get("custom_fields") or {}
        code = _anon_buyer_code(anon_key_email, pid)
        tier = classify_person(cf)
        tier_html = _tier_badge_html(tier)
        min_v, max_v = get_person_ticket_range(cf)
        range_text = _fmt_ticket_range(min_v, max_v)
        range_html = f'<div class="buyer-range">{_esc(range_text)}</div>' if range_text else ""
        blocks.append(
            f'<div class="pending-buyer">'
            f'<span class="buyer-code">Buyer {_esc(code)}</span> '
            f'{tier_html}'
            f'{range_html}</div>'
        )
    return "".join(blocks)


def _intro_buyer_cell_html(primary, more_count, firm_won_index, key=None, view_as=None):
    """Active Intros' disclosed buyer cell (item 2/3, turn 18), three
    lines: (1) the PRIMARY buyer's name (deal.primary_contact_id when
    present, else first-listed — see _select_primary_buyer), linked to
    their buyer page (item 3), plus a muted "+N more" when the deal
    links additional buyers, plus a green dot when _closer_kind finds a
    closer (person or firm — turn 23, same logic and tooltip text as
    the buyer page's own closer chip, see CLOSER_CHIP_LABELS); (2)
    muted entity company (natural-person buyers never show one) and
    country; (3) small muted email as a mailto link, website, and
    LinkedIn. Lines 2/3 truncate with an ellipsis via CSS — the buyer
    page is where the untruncated detail for OTHER linked buyers lives
    now (the old inline expand-panel is gone, replaced by that page
    entirely). Only ever called with a resolved primary buyer from a
    disclosed (Introduced-or-later) row; pending rows use
    _pending_buyer_cell_html instead and never link."""
    if primary is None:
        return "—"

    name = _esc(_person_display_name(primary) or "—")
    href = _buyer_href(primary.get("id"), key, view_as)
    closer_kind = _closer_kind(primary, firm_won_index)
    dot_html = (f' <span class="closed-dot" title="{_esc(CLOSER_CHIP_LABELS[closer_kind])}"></span>'
                if closer_kind else "")
    more_html = f' <span class="buyer-cell-more">+{more_count} more</span>' if more_count > 0 else ""
    # Item 6 (turn 20): a clear link affordance -- accent color, hover
    # underline, a small muted "profile →" suffix inside the link (so
    # the whole thing, name and suffix alike, is one click target).
    line1 = (f'<div class="buyer-cell-name"><a class="buyer-link" href="{href}">{name}'
             f'<span class="buyer-link-suffix"> profile &rarr;</span></a>{dot_html}{more_html}</div>')

    cf = primary.get("custom_fields") or {}
    transactor_ids = cf_list(cf, TRANSACTOR_TYPE_FIELD)
    is_natural = (transactor_ids[0] if transactor_ids else None) == NATURAL_PERSON_ID
    company = primary.get("company_name") or ""
    company = company.strip() if isinstance(company, str) and not is_natural else ""
    country = primary.get("work_country") or primary.get("home_country") or ""
    country = country.strip() if isinstance(country, str) else ""
    line2_parts = [_esc(p) for p in (company, country) if p]
    line2 = f'<div class="buyer-cell-sub">{" · ".join(line2_parts)}</div>' if line2_parts else ""

    line3_parts = []
    email = _person_email_text(primary)
    if email:
        line3_parts.append(f'<a href="mailto:{_esc(email)}">{_esc(email)}</a>')
    website = primary.get("website") or ""
    website = website.strip() if isinstance(website, str) else ""
    if website:
        href_w = website if "://" in website else f"https://{website}"
        line3_parts.append(f'<a href="{_esc(href_w)}" target="_blank" rel="noopener noreferrer">{_esc(website)}</a>')
    linked_in = primary.get("linked_in_url") or ""
    linked_in = linked_in.strip() if isinstance(linked_in, str) else ""
    if linked_in:
        href_l = linked_in if "://" in linked_in else f"https://{linked_in}"
        line3_parts.append(f'<a href="{_esc(href_l)}" target="_blank" rel="noopener noreferrer">LinkedIn</a>')
    line3 = f'<div class="buyer-cell-links">{" · ".join(line3_parts)}</div>' if line3_parts else ""

    return f'{line1}{line2}{line3}'


def _matched_buyer_row_html(deal, tenant_person_id, people_by_id, intro_details, anon_key_email, editable=False,
                             key=None, view_as=None):
    """editable=True (tenant_edit_mode — a real tenant session, or an admin
    &view_as preview without &edit=1) swaps the plain-text Next Steps /
    Follow-up cells for the same auto-saving inputs admin edit mode uses,
    while Status and Buyer Notes stay read-only text — tenants never get
    those two fields. Pending (not-yet-disclosed) rows never get inputs
    either way, only a blank cell added to keep the column count matching
    the 8-column editable header."""
    deal_id = str(deal.get("id"))
    entry = intro_details.get(deal_id) or {}
    linked = _deal_linked_person_ids(deal) - {tenant_person_id}
    buyer_recs = [people_by_id[pid] for pid in linked if pid in people_by_id]
    resolved = _resolve_intro_status(deal, entry)
    size_text = _esc(_deal_size_text(deal))

    if not resolved["disclosed"]:
        buyer_cell = _pending_buyer_cell_html(buyer_recs, anon_key_email)
        status_html = _status_pill_html("Matched")
        extra_td = "<td></td>" if editable else ""
        return (
            f'<tr class="pending-row"><td>{buyer_cell}</td>'
            f'<td>—</td><td></td>'
            f'<td class="num">{size_text}</td>'
            f'<td>{status_html}</td>'
            f'<td></td>{extra_td}<td></td></tr>'
        )

    name_cell = _buyer_name_cell_html(buyer_recs, show_contact=True, link=True, key=key, view_as=view_as)
    name_cell += _buyer_contact_detail_html(buyer_recs)
    investor_type, company_text = _investor_type_and_company(buyer_recs, disclosed=True)
    status_html = _status_display_html(resolved, compact=True)
    notes_html = _esc(entry.get("notes") or "")

    if editable:
        next_steps_html = _ei_field_html("ei-next-steps", deal_id, "next_steps", _esc(entry.get("next_steps") or ""))
        follow_up_td = f'<td>{_ei_date_field_html(deal_id, _esc(entry.get("follow_up") or ""))}</td>'
    else:
        next_steps_html = _esc(entry.get("next_steps") or "")
        follow_up_text = _fmt_follow_up_short(entry.get("follow_up"))
        if follow_up_text:
            next_steps_html += f'<div class="follow-up-note">Follow-up: {_esc(follow_up_text)}</div>'
        follow_up_td = ""

    return (
        f'<tr><td>{name_cell}</td>'
        f'<td>{_esc(company_text)}</td>'
        f'<td>{_esc(investor_type)}</td>'
        f'<td class="num">{size_text}</td>'
        f'<td>{status_html}</td>'
        f'<td>{next_steps_html}</td>'
        f'{follow_up_td}'
        f'<td>{notes_html}</td></tr>'
    )


def _intro_status_select_html(deal_id, current_id, allowed_ids=None):
    """allowed_ids=None -> admin, all ten options (unchanged default for
    every existing caller). A set -> tenant editing (item 4): only those
    options are selectable, but the row's current status is always
    included so the dropdown always shows the truth even when it's a
    status the tenant themselves couldn't have set (e.g. Introduced) —
    selecting it back is a no-op, not a privilege escalation."""
    options = []
    for oid, label in INTRO_STATUS_LABELS.items():
        if allowed_ids is not None and oid not in allowed_ids and oid != current_id:
            continue
        selected = " selected" if oid == current_id else ""
        options.append(f'<option value="{oid}"{selected}>{_esc(label)}</option>')
    return (f'<select class="ei-status" data-deal-id="{_esc(deal_id)}">'
            f'{"".join(options)}</select><span class="ei-msg"></span>')


def _milestone_checkboxes_html(deal_id, milestones, disabled=False):
    """Turn 17, item 1: the four NDA/VDR/Docs Sent/Wired checkboxes
    replacing the old status dropdown/strip on Active Intros. Checked
    purely from what's actually recorded in the Dynamo milestones map —
    deliberately no backfill/implied-reached inference here (unlike the
    previous pass's now-removed milestone line): once a box is
    unchecked it STAYS unchecked on every future render regardless of
    the deal's current Pipeline status, which a backfilled display could
    not guarantee (the current status alone can't distinguish "never
    recorded" from "explicitly un-recorded"). "Closed" is never a
    checkbox — see _flag_select_html. The checkbox's value= attribute
    (and data-deal-id) always carries the internal step key, never the
    display label (item 2, turn 20) — see MILESTONE_STEP_LABELS."""
    stored = milestones or {}
    disabled_attr = " disabled" if disabled else ""
    boxes = []
    for step in CHECKBOX_MILESTONE_STEPS:
        checked_attr = " checked" if step in stored else ""
        label = MILESTONE_STEP_LABELS.get(step, step)
        boxes.append(
            f'<label class="ei-milestone-label"><input type="checkbox" class="ei-milestone" '
            f'data-deal-id="{_esc(deal_id)}" value="{_esc(step)}"{checked_attr}{disabled_attr}>'
            f'{_esc(label)}</label>'
        )
    return f'<div class="ei-milestones">{"".join(boxes)}</div>'


def _current_flag_value(resolved):
    if resolved["id"] == INTRO_STATUS_STALLED_ID:
        return "stalled"
    if resolved["id"] == INTRO_STATUS_PASSED_ID:
        return "passed"
    if resolved["id"] == INTRO_STATUS_WITHDRAWN_ID:
        return "withdrawn"
    if resolved["name"] == "Closed":
        return "closed"
    return "none"


def _flag_select_html(deal_id, resolved, admin_controls, disabled=False):
    """Turn 17, item 2: the compact flag select beside the checkboxes.
    Tenant-facing rows (admin_controls=False): None/Stalled/Passed.
    Admin edit rows (admin_controls=True, _intro_row_edit_html only):
    also Withdrawn/Closed. The row's CURRENT flag is always included
    even when it's admin-only — same "always show the truth even when
    it's not a value this viewer could have set" convention
    _intro_status_select_html already uses (e.g. a tenant viewing a
    Withdrawn row, which they can't set but also can't be lied to
    about).

    Dedup fix (turn 19): the select itself carries an amber (Stalled) or
    gray (Passed/Withdrawn) border+tint so the state stays glanceable
    without a separate chip repeating it — see
    _status_milestones_column_html, which drops the chip entirely
    whenever this select is actually editable."""
    current = _current_flag_value(resolved)
    values = ["none", "stalled", "passed"]
    if admin_controls:
        values += ["withdrawn", "closed"]
    elif current not in values:
        values.append(current)
    options = "".join(
        f'<option value="{v}"{" selected" if v == current else ""}>{_esc(FLAG_LABELS[v])}</option>'
        for v in values
    )
    disabled_attr = " disabled" if disabled else ""
    flag_cls = ""
    if current == "stalled":
        flag_cls = " flag-stalled"
    elif current in ("passed", "withdrawn"):
        flag_cls = " flag-exit"
    return (f'<select class="ei-flag{flag_cls}" data-deal-id="{_esc(deal_id)}"{disabled_attr}>'
            f'{options}</select>')


def _status_milestones_column_html(resolved, deal_id, milestones, admin_controls, disabled=False):
    """Turn 17: the full Active Intros status cell -- the four milestone
    checkboxes (item 1), the flag chip (Stalled amber, Passed/Withdrawn
    gray, Closed its own color), a muted "Awaiting our confirmation"
    note on a Wired-not-yet-Closed row, and the flag select (item 2) —
    one shared .ei-msg at the end covers every control in the cell (see
    _edit_script_html's saveMilestone).

    Dedup fix (turn 19): the chip and the select never both show the
    same state. disabled=False means the select is actually editable
    (a tenant on an Introduced-or-later, non-Closed row; admin in edit
    mode, always) — the select alone carries the state (styled
    amber/gray by _flag_select_html), so the chip is dropped entirely.
    disabled=True is a read-only context (a tenant viewing a Closed-
    locked row today — the only case that currently reaches this
    function disabled, since admin_controls=True is never disabled) —
    there the chip renders alongside the (disabled) select. Pending
    rows keep their own separate "Matched" pill, untouched by any of
    this."""
    checkboxes_html = _milestone_checkboxes_html(deal_id, milestones, disabled=disabled)

    flag_html = ""
    if disabled:
        if resolved["is_exit"]:
            flag_cls = "stalled" if resolved["id"] == INTRO_STATUS_STALLED_ID else "exit"
            flag_html = f'<span class="status-chip {flag_cls}">{_esc(resolved["name"])}</span>'
        elif resolved["name"] == "Closed":
            flag_html = '<span class="status-chip closed">Closed</span>'

    awaiting_html = ('<div class="status-awaiting">Awaiting our confirmation</div>'
                      if resolved["name"] == "Wired" else "")

    select_html = _flag_select_html(deal_id, resolved, admin_controls, disabled=disabled)

    return (f'<div class="status-column">{checkboxes_html}{flag_html}{awaiting_html}'
            f'{select_html}<span class="ei-msg"></span></div>')


def _ei_field_html(css_class, deal_id, field, value, placeholder=""):
    """An auto-saving text input: saved on blur or Enter (see
    _edit_script_html), with its own inline .ei-msg indicator right
    beside it — no Save button anywhere."""
    placeholder_attr = f' placeholder="{_esc(placeholder)}"' if placeholder else ""
    return (f'<input type="text" class="{css_class}" data-deal-id="{_esc(deal_id)}" '
            f'data-field="{field}" maxlength="2000" value="{value}"{placeholder_attr}>'
            f'<span class="ei-msg"></span>')


def _ei_date_field_html(deal_id, value, css_class="ei-follow-up", field="follow_up"):
    """Auto-saving native date input for Follow-up (default) or, via
    css_class/field, Deadline — same save path as _ei_field_html (blur/
    Enter via _edit_script_html), just a type="date" input (browser-native
    picker, ISO yyyy-mm-dd value) with no maxlength."""
    return (f'<input type="date" class="{css_class}" data-deal-id="{_esc(deal_id)}" '
            f'data-field="{field}" value="{_esc(value)}">'
            f'<span class="ei-msg"></span>')


def _fmt_follow_up_short(follow_up):
    """"Sep 20" style, no year — for the Company page's read-only
    tenant-facing follow-up note (its Buyers table's own Follow-up
    column, unaffected by turn 22's removal of follow-up dates from
    Active Intros/My Deals). None if unset/unparsable."""
    dt = _parse_dt(follow_up)
    return dt.strftime("%b %-d") if dt else None


def _edit_script_html(key):
    """Plain HTML+fetch(), no frameworks, no Save button: the Status/Flag
    dropdown posts on change, a milestone checkbox posts on change too
    (turn 17), Next Steps / Follow-up / Buyer Notes post on blur or
    Enter (Enter just blurs, so there's one save path, not two). Each
    control's own .ei-msg shows "Saving…", then either "Saved ✓" (fades
    after 2s) or the returned error text in red (stays) — every control
    except the milestone checkboxes finds its .ei-msg as its very next
    sibling; the four checkboxes share ONE .ei-msg at the end of their
    .status-column (see _status_milestones_column_html), found via
    closest(), since there's no sensible single "next sibling" for four
    inputs. One shared script, included on both Active Intros and the
    Buyers table when edit_mode is on."""
    admin_key_json = json.dumps(key or "")
    return f"""<script>
(function() {{
  var ADMIN_KEY = {admin_key_json};

  function postUpdate(payload, msgEl) {{
    if (msgEl) {{ msgEl.className = 'ei-msg saving'; msgEl.textContent = 'Saving…'; }}
    fetch('?action=update_intro', {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: JSON.stringify(payload)
    }}).then(function(r) {{
      return r.json().then(function(data) {{ return {{ ok: r.ok, data: data }}; }});
    }}).then(function(res) {{
      if (!msgEl) return;
      if (res.ok) {{
        msgEl.className = 'ei-msg saved';
        msgEl.textContent = 'Saved ✓';
        setTimeout(function() {{
          msgEl.className = 'ei-msg';
          msgEl.textContent = '';
        }}, 2000);
      }} else {{
        msgEl.className = 'ei-msg error';
        msgEl.textContent = (res.data && res.data.error) || 'Error';
      }}
    }}).catch(function(err) {{
      if (msgEl) {{
        msgEl.className = 'ei-msg error';
        msgEl.textContent = 'Error: ' + err;
      }}
    }});
  }}

  function saveField(el, field) {{
    var dealId = el.getAttribute('data-deal-id');
    var msgEl = el.nextElementSibling;
    var payload = {{ key: ADMIN_KEY, deal_id: dealId }};
    payload[field] = el.value;
    postUpdate(payload, msgEl);
  }}

  function saveMilestone(el) {{
    var dealId = el.getAttribute('data-deal-id');
    var col = el.closest('.status-column');
    var msgEl = col ? col.querySelector('.ei-msg') : null;
    postUpdate({{ key: ADMIN_KEY, deal_id: dealId, milestone_step: el.value,
                  milestone_checked: el.checked }}, msgEl);
  }}

  document.querySelectorAll('.ei-status').forEach(function(el) {{
    el.addEventListener('change', function() {{ saveField(el, 'status'); }});
  }});
  document.querySelectorAll('.ei-flag').forEach(function(el) {{
    el.addEventListener('change', function() {{ saveField(el, 'flag'); }});
  }});
  document.querySelectorAll('.ei-milestone').forEach(function(el) {{
    el.addEventListener('change', function() {{ saveMilestone(el); }});
  }});
  document.querySelectorAll('.ei-next-steps, .ei-notes, .ei-follow-up, .ei-deadline').forEach(function(el) {{
    var field = el.getAttribute('data-field');
    el.addEventListener('blur', function() {{ saveField(el, field); }});
    el.addEventListener('keydown', function(e) {{
      if (e.key === 'Enter') {{ e.preventDefault(); el.blur(); }}
    }});
  }});
}})();
</script>"""


def _matched_buyer_row_edit_html(deal, tenant_person_id, people_by_id, intro_details, key=None, view_as=None):
    """Admin edit-mode row: always the real buyer(s), regardless of
    disclosure — the disclosure gate is a tenant-facing privacy rule, not
    something that should blind the admin managing the pipeline. Status
    always reflects the TRUE current resolved status (Matched included),
    editable via a dropdown of all ten options. The buyer name only
    LINKS to the buyer page when the row is Introduced-or-later (item 3,
    turn 18) — a still-pending/Matched row has no buyer page to link to
    yet (that page's own disclosure gate would just anonymize it right
    back)."""
    deal_id = str(deal.get("id"))
    entry = intro_details.get(deal_id) or {}
    resolved = _resolve_intro_status(deal, entry)

    linked = _deal_linked_person_ids(deal) - {tenant_person_id}
    buyer_recs = [people_by_id[pid] for pid in linked if pid in people_by_id]
    name_cell = _buyer_name_cell_html(buyer_recs, show_contact=True, link=resolved["disclosed"],
                                       key=key, view_as=view_as)
    if resolved["disclosed"]:
        name_cell += _buyer_contact_detail_html(buyer_recs)
    investor_type, company_text = _investor_type_and_company(buyer_recs, disclosed=True)
    size_text = _esc(_deal_size_text(deal))
    select_html = _intro_status_select_html(deal_id, resolved["id"])
    next_steps_html = _ei_field_html("ei-next-steps", deal_id, "next_steps", _esc(entry.get("next_steps") or ""))
    follow_up_html = _ei_date_field_html(deal_id, _esc(entry.get("follow_up") or ""))
    notes_html = _ei_field_html("ei-notes", deal_id, "notes", _esc(entry.get("notes") or ""))

    return (
        f'<tr><td>{name_cell}</td>'
        f'<td>{_esc(company_text)}</td>'
        f'<td>{_esc(investor_type)}</td>'
        f'<td class="num">{size_text}</td>'
        f'<td>{select_html}</td>'
        f'<td>{next_steps_html}</td>'
        f'<td>{follow_up_html}</td>'
        f'<td>{notes_html}</td></tr>'
    )


def _closed_out_buyer_row_html(deal, tenant_person_id, people_by_id, anon_key_email, editable=False,
                                admin_reveals=False, key=None, view_as=None):
    """One row in the company page's Buyers "Closed out" section (turn
    27, item 2) — same column shape as _matched_buyer_row_html (7 cols
    tenant-facing, 8 with the extra blank when editable, matching that
    header exactly) but Status is always the derived, muted outcome chip
    and Next Steps/Follow-up/Buyer Notes stay blank — nothing left to
    plan or note on a dead deal. Admin edit mode and tenant view share
    this one renderer (unlike the live rows' edit/non-edit split) since
    there is nothing here to make editable either way. admin_reveals=True
    (full admin edit_mode) never anonymizes — same "the admin sees and
    edits the real buyer regardless of pending/disclosed" convention
    _matched_buyer_row_edit_html already follows."""
    linked = _deal_linked_person_ids(deal) - {tenant_person_id}
    buyer_recs = [people_by_id[pid] for pid in linked if pid in people_by_id]
    disclosed = admin_reveals or _closed_out_disclosed(deal)
    size_text = _esc(_deal_size_text(deal))
    status_html = _closed_out_status_chip_html(deal)
    extra_td = "<td></td>" if editable else ""

    if not disclosed:
        buyer_cell = _pending_buyer_cell_html(buyer_recs, anon_key_email)
        return (
            f'<tr class="closed-out-row"><td>{buyer_cell}</td>'
            f'<td>—</td><td></td>'
            f'<td class="num">{size_text}</td>'
            f'<td>{status_html}</td>'
            f'<td></td>{extra_td}<td></td></tr>'
        )

    name_cell = _buyer_name_cell_html(buyer_recs, show_contact=True, link=True, key=key, view_as=view_as)
    name_cell += _buyer_contact_detail_html(buyer_recs)
    investor_type, company_text = _investor_type_and_company(buyer_recs, disclosed=True)
    return (
        f'<tr class="closed-out-row"><td>{name_cell}</td>'
        f'<td>{_esc(company_text)}</td>'
        f'<td>{_esc(investor_type)}</td>'
        f'<td class="num">{size_text}</td>'
        f'<td>{status_html}</td>'
        f'<td></td>{extra_td}<td></td></tr>'
    )


# ── Nav shell: header bar + tabs, shared by every authenticated page ────────
# Palette lifted verbatim from the shared trades/portfolio-deploy design
# (portfolio-deploy/lambda_function.py :root — --ink #16181d, --bg #f4f2ee,
# --muted #6b7280, --pos #1f7a4d) so the bar reads as part of the same
# family, but repurposed as a dark bar (--ink as background, --bg as text)
# rather than that palette's own light page/card backgrounds — the
# "distinct background tint" the nav needs to read as its own band above
# the (unchanged, separately dark-themed) Demand Board.
NAV_CSS = """
  .gg-nav {
    background: transparent;
    border-bottom: 1px solid var(--line);
  }
  .gg-nav-inner {
    max-width: 1000px;
    margin: 0 auto;
    padding: 14px 24px;
    display: flex;
    align-items: center;
    gap: 24px;
  }
  .gg-brand {
    color: var(--accent);
    font-weight: 700;
    font-size: 14px;
    letter-spacing: 0.02em;
    white-space: nowrap;
  }
  .gg-tabs {
    display: flex;
    gap: 4px;
    flex: 1;
  }
  .gg-tab {
    color: var(--muted);
    text-decoration: none;
    font-size: 13px;
    font-weight: 600;
    padding: 8px 14px;
    border-radius: 6px;
  }
  .gg-tab:hover { color: var(--ink); }
  .gg-tab.active {
    color: var(--accent);
    background: rgba(61,90,115,0.10);
    box-shadow: inset 0 -2px 0 var(--accent);
  }
  .gg-viewer {
    color: var(--muted);
    font-size: 13px;
    white-space: nowrap;
  }
  .gg-admin-badge {
    background: #7a1f1f;
    color: #ffffff;
    font-weight: 800;
    font-size: 12px;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    padding: 6px 12px;
    border-radius: 5px;
    border: 1px solid #ff6b6b;
    white-space: nowrap;
  }
  .gg-cef-badge {
    font-size: 12px;
    font-weight: 600;
    padding: 5px 10px;
    border-radius: 999px;
    white-space: nowrap;
    text-decoration: none;
  }
  .gg-cef-badge.cef-ok { background: rgba(31,122,77,0.15); color: var(--qp); }
  .gg-cef-badge.cef-pending { background: rgba(201,162,39,0.15); color: var(--accredited); }
  .gg-cef-badge.cef-missing { background: rgba(178,59,59,0.12); color: #b23b3b; }
  .gg-cef-badge.cef-missing:hover { text-decoration: underline; }
"""

# Feature-request submit box + list, shared verbatim by render_my_deals_page
# and render_company_page (both embed this via {FEATURE_CSS} the same way
# every page already embeds {NAV_CSS}) so the two pages' boxes/lists never
# drift from each other.
FEATURE_CSS = """
  .feature-box {
    position: relative;
    background: rgba(61,90,115,0.10);
    border: 1px solid rgba(61,90,115,0.45);
    border-radius: 10px;
    padding: 18px 44px 18px 20px;
    margin: 0 0 24px;
  }
  .feature-box h2 { font-size: 15px; font-weight: 700; margin: 0 0 4px; }
  .feature-box p { font-size: 13px; color: var(--muted); margin: 0 0 12px; }
  .feature-box-dismiss {
    position: absolute;
    top: 10px;
    right: 12px;
    background: none;
    border: none;
    color: var(--muted);
    font-size: 18px;
    line-height: 1;
    cursor: pointer;
    padding: 4px;
  }
  .feature-box-dismiss:hover { color: var(--ink); }
  .feature-box-form { display: flex; gap: 10px; flex-wrap: wrap; }
  #feature-input {
    flex: 1;
    min-width: 200px;
    background: var(--bg);
    border: 1px solid var(--line);
    color: var(--ink);
    border-radius: 6px;
    padding: 9px 12px;
    font-size: 14px;
  }
  #feature-submit-btn {
    background: var(--accent);
    color: #fff;
    border: none;
    padding: 9px 18px;
    border-radius: 6px;
    font-size: 14px;
    font-weight: 600;
    cursor: pointer;
  }
  #feature-submit-btn:hover { opacity: 0.9; }
  .feature-box .ei-msg { display: block; margin-top: 8px; }
  .feature-row {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    gap: 16px;
    padding: 12px 16px;
    border-bottom: 1px solid var(--line);
    font-size: 14px;
    flex-wrap: wrap;
  }
  .feature-row:last-child { border-bottom: none; }
  .feature-row.done { opacity: 0.6; }
  .feature-row.done .feature-row-text { text-decoration: line-through; }
  .feature-row-text { flex: 1 1 200px; }
  .feature-row-meta { flex: 0 0 auto; font-size: 12px; color: var(--muted); white-space: nowrap; }
  .feature-toggle-btn {
    background: none;
    border: 1px solid var(--line);
    color: var(--ink);
    border-radius: 6px;
    padding: 3px 10px;
    font-size: 12px;
    cursor: pointer;
  }
  .feature-toggle-btn:hover { border-color: var(--accent); }
  .feature-tenant-tag {
    display: inline-block;
    font-size: 11px;
    font-weight: 600;
    color: var(--accent);
    margin-right: 6px;
  }
  .feature-page-tag {
    display: inline-block;
    font-size: 11px;
    font-weight: 600;
    color: var(--muted);
    background: rgba(22,24,29,0.06);
    border-radius: 999px;
    padding: 1px 8px;
    margin-right: 6px;
  }
"""


def _tab_qs_suffix(key=None, view_as=None):
    """&key=...&view_as=... to append to tab links, so admin key / preview
    access carries through tab clicks. Empty for cookie-authenticated
    tenants, since the cookie already carries automatically."""
    suffix = ""
    if key:
        suffix += f"&key={urllib.parse.quote(key, safe='')}"
    if view_as:
        suffix += f"&view_as={urllib.parse.quote(view_as, safe='')}"
    return suffix


def _nav_html(active_tab, viewer_name, key=None, view_as=None, show_viewer=True, edit_flag=False, cef_html=""):
    suffix = _tab_qs_suffix(key, view_as)
    mydeals_href = f"?tab=mydeals{suffix}"
    intros_href = f"?tab=intros{suffix}"
    demand_href = f"?tab=demand{suffix}"
    mydeals_cls = "gg-tab active" if active_tab == "mydeals" else "gg-tab"
    intros_cls = "gg-tab active" if active_tab == "intros" else "gg-tab"
    demand_cls = "gg-tab active" if active_tab == "demand" else "gg-tab"
    # show_viewer=False (the company detail page) omits the viewer's own
    # name from the nav — that page's "no identities anywhere" rule is
    # unqualified except for section (a)'s deal names, so even the
    # viewer's own identity stays off it. Every other page keeps showing it.
    viewer_html = _esc(viewer_name) if show_viewer else ""

    # key is only ever non-None for a valid ADMIN_KEY session (see
    # lambda_handler's nav_key) — never for a real tenant, cookie or not —
    # so it's the exact same signal used to gate edit_mode and is safe to
    # reuse here as "is admin" without threading a separate flag through
    # every render_* call.
    admin_badge_html = ""
    if key is not None:
        badge_text = "ADMIN"
        if edit_flag:
            badge_text += " · editing"
        if view_as:
            badge_text += f" · viewing as {view_as}"
        admin_badge_html = f'<div class="gg-admin-badge">{_esc(badge_text)}</div>'

    return f"""<header class="gg-nav">
  <div class="gg-nav-inner">
    <div class="gg-brand">Gracia Group</div>
    <nav class="gg-tabs">
      <a class="{mydeals_cls}" href="{mydeals_href}">My Deals</a>
      <a class="{intros_cls}" href="{intros_href}">Active Intros</a>
      <a class="{demand_cls}" href="{demand_href}">Demand Board</a>
    </nav>
    {admin_badge_html}
    {cef_html}
    <div class="gg-viewer">{viewer_html}</div>
  </div>
</header>"""


def _group_header_row_html(label, colspan):
    return f'<tr class="group-divider"><td colspan="{colspan}">{_esc(label)}</td></tr>'


def _group_empty_row_html(message, colspan):
    return f'<tr><td colspan="{colspan}" class="group-empty">{_esc(message)}</td></tr>'


def _company_update_link_html(person_id, company_name):
    """Item 6 (turn 16): a small muted "Update deal ->" link under the
    company name on each Active Intros row, to the tenant's own Sell
    Order deal for that company (get_my_sell_deals — never the buy-side
    intro deal this row itself represents) via the same minted deal-
    update-form URL as My Deals' Update/Cancel button. Most-recently-
    updated Sell deal wins when a tenant somehow has more than one on
    file for the same company (get_my_deals already sorts that way).
    Omitted outright when HMAC_SECRET isn't configured (see
    _deal_update_form_url, which returns None) or the tenant has no Sell
    deal on file for this company."""
    sell_deals = get_my_sell_deals(person_id, company_name)
    if not sell_deals:
        return ""
    update_url = _deal_update_form_url(str(sell_deals[0].get("id")))
    if not update_url:
        return ""
    return (f'<div class="company-update-link"><a href="{update_url}" target="_blank" '
            f'rel="noopener noreferrer">Update deal &rarr;</a></div>')


def _investor_type_cell_html(buyer_recs):
    """Item 4 (turn 16): the Investor Type cell's text plus, on its own
    line beneath it, the same tier badge (item 4, turn 18: Unknown is no
    badge at all — see _tier_badge_html) the Pending Introductions /
    Buyer Demand tiles already use — computed from the first linked
    buyer, the same "first buyer" convention _investor_type_and_company
    already uses for type/company."""
    investor_type, _company_text = _investor_type_and_company(buyer_recs, disclosed=True)
    tier_html = ""
    if buyer_recs:
        tier = classify_person(buyer_recs[0].get("custom_fields") or {})
        badge = _tier_badge_html(tier)
        tier_html = f'<div class="tier-badge-line">{badge}</div>' if badge else ""
    return f'{_esc(investor_type)}{tier_html}'


def _ei_notes_textarea_html(deal_id, value, placeholder=""):
    """Turn 21: Notes' auto-saving field on Active Intros is a 2-line
    textarea rather than a single-line input — same .ei-notes class, same
    blur/Enter save path (_edit_script_html), same next-sibling .ei-msg
    convention, just a taller control since notes can run longer than one
    line. A <textarea>'s initial text is its content, not a value=
    attribute, unlike _ei_field_html's <input> — value must already be
    escaped by the caller, exactly like _ei_field_html expects. The
    Company page's own admin-only Buyer Notes column keeps its single-
    line .ei-notes <input> (_ei_field_html) untouched — the two never
    collide since each page embeds its own <style>/<script>, so a
    textarea and an input can safely share one class name across pages."""
    placeholder_attr = f' placeholder="{_esc(placeholder)}"' if placeholder else ""
    return (f'<textarea class="ei-notes" data-deal-id="{_esc(deal_id)}" data-field="notes" '
            f'maxlength="2000" rows="2"{placeholder_attr}>{value}</textarea>'
            f'<span class="ei-msg"></span>')


def _notes_cell_html(deal_id, notes_value, editable):
    """The Notes cell — a 2-line auto-saving textarea when editable, plain
    text otherwise (Passed/Withdrawn rows, non-editable tenant views).
    Turn 21 folded the Follow-up column in here as a compact sub-row;
    turn 22 removed follow-up dates from the UI entirely (column, date
    input, Due chip, and the sort/summary/chip logic keyed off it — see
    _handle_update_intro's follow_up parsing for the still-live but now
    UI-dormant backend field), so this cell is Notes alone again."""
    if editable:
        return _ei_notes_textarea_html(deal_id, _esc(notes_value), placeholder="Add a note…")
    return _esc(notes_value or "—")


def _intro_row_html(deal, resolved, people_by_id, tenant_person_id, entry, firm_won_index, key=None, view_as=None,
                     editable=False, company_repeated=False):
    """Tenant-facing Introduced-or-later row: Company | Buyer | Investor
    Type | Size | Status | Notes. editable=True (tenant_edit_mode — see
    render_intros_page) leaves the Status cell's milestone checkboxes
    and flag select (turn 17 — see _status_milestones_column_html)
    interactive, and makes Notes an auto-saving textarea, except on a
    Passed/Withdrawn row, which renders Notes as read-only text instead
    — nothing left to plan for a dead intro. A Closed row locks the
    status controls read-only for tenants regardless of editable
    (item 2) — notes is unaffected by that lock, only is_dead is.

    Notes replaces Next Steps entirely (item 3, turn 20): free text, no
    suggested placeholder, stored in the same Dynamo "notes" attribute
    the Company page's admin-only Buyer Notes column already writes —
    tenant and admin now share that one field. A row with legacy
    next_steps but no notes yet shows the next_steps text as the
    initial value (read once, never written back to next_steps) so old
    data stays visible; the next_steps input itself is retired from
    this page. company_repeated (item 7) blanks the company cell (and
    its Update-deal link, turn 16 item 6) and adds a subtle left-accent
    instead of repeating the same company name row after row.

    Turn 21 made Notes a 2-line textarea and folded the Follow-up column
    into a compact row underneath it; turn 22 removed follow-up dates
    from the UI entirely (see _notes_cell_html), so Notes now simply
    fills the whole cell — the underlying "follow_up" Dynamo attribute
    and its write path are untouched, just dormant."""
    company_name = _deal_company_name(deal)
    if company_repeated:
        company_cell = ""
        row_cls = ' class="grouped-row"'
    elif company_name:
        company_cell = (f'<a href="{_company_href(company_name, "intros", key, view_as)}">'
                        f'{_esc(company_name)}</a>')
        company_cell += _company_update_link_html(tenant_person_id, company_name)
        row_cls = ""
    else:
        company_cell = "—"
        row_cls = ""

    linked_ordered = [pid for pid in _deal_linked_person_ids_ordered(deal) if pid != tenant_person_id]
    buyer_recs = [people_by_id[pid] for pid in linked_ordered if pid in people_by_id]
    primary, more_count = _select_primary_buyer(deal, buyer_recs)
    buyer_cell = _intro_buyer_cell_html(primary, more_count, firm_won_index, key=key, view_as=view_as)
    investor_type_cell = _investor_type_cell_html(buyer_recs)

    size_text = _esc(_deal_size_text(deal))
    deal_id = str(deal.get("id"))
    milestones = entry.get("milestones")

    is_closed_locked = resolved["name"] == "Closed"
    status_html = _status_milestones_column_html(resolved, deal_id, milestones, admin_controls=False,
                                                   disabled=(not editable) or is_closed_locked)

    is_dead = resolved["name"] in ("Passed", "Withdrawn")
    notes_value = entry.get("notes")
    if notes_value is None:
        notes_value = entry.get("next_steps") or ""
    notes_cell_html = _notes_cell_html(deal_id, notes_value, editable and not is_dead)

    return (
        f'<tr{row_cls}><td class="company">{company_cell}</td>'
        f'<td>{buyer_cell}</td>'
        f'<td>{investor_type_cell}</td>'
        f'<td class="num">{size_text}</td>'
        f'<td>{status_html}</td>'
        f'<td class="notes-cell">{notes_cell_html}</td></tr>'
    )


def _pending_intro_row_html(deal, buyer_recs, anon_key_email, tenant_person_id, key=None,
                             view_as=None, company_repeated=False):
    """Pending rows never get inputs — only Introduced-or-later rows are
    editable, disclosed or not — but they still get the Update-deal link
    (turn 16, item 6), which is about the tenant's own Sell deal, not
    buyer disclosure. company_repeated: see _intro_row_html."""
    company_name = _deal_company_name(deal)
    if company_repeated:
        company_cell = ""
        row_cls = ' class="pending-row grouped-row"'
    elif company_name:
        company_cell = (f'<a href="{_company_href(company_name, "intros", key, view_as)}">'
                        f'{_esc(company_name)}</a>')
        company_cell += _company_update_link_html(tenant_person_id, company_name)
        row_cls = ' class="pending-row"'
    else:
        company_cell = "—"
        row_cls = ' class="pending-row"'
    buyer_cell = _pending_buyer_cell_html(buyer_recs, anon_key_email)
    size_text = _esc(_deal_size_text(deal))
    status_html = _status_pill_html("Matched")

    return (
        f'<tr{row_cls}><td class="company">{company_cell}</td>'
        f'<td>{buyer_cell}</td>'
        f'<td></td>'
        f'<td class="num">{size_text}</td>'
        f'<td>{status_html}</td>'
        f'<td class="notes-cell"></td></tr>'
    )


def _closed_out_status_chip_html(deal, loss_reason=None):
    """The gray Passed/Withdrawn chip for a stage-derived closed-out row
    (turn 27) — reuses _status_chip_html's existing "exit" styling
    (status_id=None never matches INTRO_STATUS_STALLED_ID, so this always
    renders the plain gray chip, never the amber Stalled one) plus, when a
    loss reason is on record, a small muted "— <reason>" suffix."""
    outcome = _closed_out_outcome_name(_deal_stage_id(deal))
    chip = _status_chip_html(None, outcome)
    if loss_reason is None:
        loss_reason = _deal_loss_reason_text(deal)
    if loss_reason:
        chip += f' <span class="closed-out-reason">— {_esc(loss_reason)}</span>'
    return chip


def _closed_out_intro_row_html(deal, buyer_recs, disclosed, anon_key_email, tenant_person_id, firm_won_index,
                                key=None, view_as=None, company_repeated=False):
    """One row in Active Intros' "Closed out" section (turn 27, item 2):
    same Company/Buyer/Investor Type/Size columns as a live row, but
    Status always shows the derived, muted outcome chip (never the
    milestone checkboxes or flag select — there's nothing left to edit on
    a dead deal) and Notes stays empty. disclosed comes from
    _closed_out_disclosed (item 3's simple, milestone-free rule) — when
    False the buyer cell renders exactly like a Pending row's anonymized
    code, never a name."""
    company_name = _deal_company_name(deal)
    if company_repeated:
        company_cell = ""
        row_cls = ' class="closed-out-row grouped-row"'
    elif company_name:
        company_cell = (f'<a href="{_company_href(company_name, "intros", key, view_as)}">'
                        f'{_esc(company_name)}</a>')
        company_cell += _company_update_link_html(tenant_person_id, company_name)
        row_cls = ' class="closed-out-row"'
    else:
        company_cell = "—"
        row_cls = ' class="closed-out-row"'

    if disclosed:
        primary, more_count = _select_primary_buyer(deal, buyer_recs)
        buyer_cell = _intro_buyer_cell_html(primary, more_count, firm_won_index, key=key, view_as=view_as)
        investor_type_cell = _investor_type_cell_html(buyer_recs)
    else:
        buyer_cell = _pending_buyer_cell_html(buyer_recs, anon_key_email)
        investor_type_cell = ""

    size_text = _esc(_deal_size_text(deal))
    status_html = _closed_out_status_chip_html(deal)

    return (
        f'<tr{row_cls}><td class="company">{company_cell}</td>'
        f'<td>{buyer_cell}</td>'
        f'<td>{investor_type_cell}</td>'
        f'<td class="num">{size_text}</td>'
        f'<td>{status_html}</td>'
        f'<td class="notes-cell"></td></tr>'
    )


def _intro_row_edit_html(deal, people_by_id, tenant_person_id, intro_details, firm_won_index, key=None, view_as=None,
                          company_repeated=False):
    """Admin edit-mode row for Active Intros: always the real buyer(s),
    the same milestone checkboxes + flag select as the tenant-facing row
    (turn 17 — see _status_milestones_column_html) but never disabled —
    admin has full rights everywhere, Closed rows included — plus an
    auto-saving Notes textarea (item 3, turn 20 — see _intro_row_html
    for the Notes/next_steps fallback; turn 22 removed Follow-up from
    the UI entirely — see _notes_cell_html)."""
    deal_id = str(deal.get("id"))
    entry = intro_details.get(deal_id) or {}
    resolved = _resolve_intro_status(deal, entry)

    company_name = _deal_company_name(deal)
    if company_repeated:
        company_cell = ""
        row_cls = ' class="grouped-row"'
    elif company_name:
        company_cell = (f'<a href="{_company_href(company_name, "intros", key, view_as)}">'
                        f'{_esc(company_name)}</a>')
        company_cell += _company_update_link_html(tenant_person_id, company_name)
        row_cls = ""
    else:
        company_cell = "—"
        row_cls = ""

    linked_ordered = [pid for pid in _deal_linked_person_ids_ordered(deal) if pid != tenant_person_id]
    buyer_recs = [people_by_id[pid] for pid in linked_ordered if pid in people_by_id]
    primary, more_count = _select_primary_buyer(deal, buyer_recs)
    buyer_cell = _intro_buyer_cell_html(primary, more_count, firm_won_index, key=key, view_as=view_as)
    investor_type_cell = _investor_type_cell_html(buyer_recs)

    size_text = _esc(_deal_size_text(deal))
    milestones = entry.get("milestones")
    status_html = _status_milestones_column_html(resolved, deal_id, milestones, admin_controls=True)
    notes_value = entry.get("notes")
    if notes_value is None:
        notes_value = entry.get("next_steps") or ""
    notes_cell_html = _notes_cell_html(deal_id, notes_value, True)

    return (
        f'<tr{row_cls}><td class="company">{company_cell}</td>'
        f'<td>{buyer_cell}</td>'
        f'<td>{investor_type_cell}</td>'
        f'<td class="num">{size_text}</td>'
        f'<td>{status_html}</td>'
        f'<td class="notes-cell">{notes_cell_html}</td></tr>'
    )


def render_intros_page(viewer_name, tenant=None, tenant_email=None, key=None, view_as=None, edit_mode=False,
                        cef_html=""):
    """tenant is None only for admin-without-view_as — the same
    tenant-picker signal render_my_deals_page uses. tenant_email is always
    a real email otherwise (the logged-in tenant's own, or the previewed
    tenant's under &view_as), used for the pending rows' anon buyer codes,
    as the Dynamo partition key for status overrides / edit data, and
    (item 3) as the feature-request partition — "admin" in the
    tenant-picker case, aggregating every partition, exactly like My
    Deals.

    edit_mode is only ever True for a valid ADMIN_KEY (see lambda_handler)
    — never for a real tenant."""
    nav = _nav_html("intros", viewer_name, key=key, view_as=view_as, edit_flag=edit_mode, cef_html=cef_html)
    tenant_picker = tenant is None

    feature_box_html, feature_list_html = _feature_section_html(tenant_picker, tenant_email or "admin", key=key,
                                                                   page="intros")

    if tenant_picker:
        body_html = (
            '<div class="gg-placeholder">Pick a tenant to preview — '
            'add &amp;view_as=&lt;email&gt; to the URL.</div>'
        )
        summary_html = ""
        subtle_html = ""
    else:
        person_id = tenant.get("person_id")
        deals = get_my_matched_buy_deals(person_id) if person_id is not None else []
        # Turn 27, item 1: stage-level exits — a dead-stage (Lost/Trade
        # Broken/Obsolete) BUY deal never reaches get_my_matched_buy_deals,
        # so it simply vanished instead of showing as a closed-out intro.
        # Fetched separately, keyed off the deal's own Pipeline stage
        # rather than the Intro Status field — see get_my_closed_out_buy_deals.
        closed_out_deals = get_my_closed_out_buy_deals(person_id) if person_id is not None else []

        intro_details, dynamo_failed = get_intro_details(tenant_email) if (deals or closed_out_deals) else ({}, False)

        resolved_by_deal_id = {}
        kept_deals = []
        for d in deals:
            resolved = _resolve_intro_status(d, intro_details.get(str(d.get("id"))))
            if resolved["name"] in ("Passed", "Withdrawn"):
                continue
            resolved_by_deal_id[str(d.get("id"))] = resolved
            kept_deals.append(d)

        # Item 3: closed-out rows use their own, simpler disclosure rule
        # (_closed_out_disclosed — raw Intro Status explicitly Introduced-
        # or-later, no milestone fallback), computed once per deal here.
        # Edit mode never anonymizes (same convention as every other row
        # on this page) — the admin sees the real buyer regardless.
        closed_out_disclosed_by_id = {str(d.get("id")): (edit_mode or _closed_out_disclosed(d))
                                       for d in closed_out_deals}

        wanted_ids = set()
        for d in kept_deals:
            wanted_ids |= _deal_linked_person_ids(d) - {person_id}
        for d in closed_out_deals:
            wanted_ids |= _deal_linked_person_ids(d) - {person_id}
        people_by_id = get_people_by_ids(wanted_ids) if wanted_ids else {}
        # Turn 23: the closed-dot needs the same person-or-firm closer
        # signal as the buyer page's chip. Admin edit mode shows the
        # real buyer (and so the real dot) on every row, pending
        # included; a tenant only ever sees it on a disclosed row — skip
        # the extra people.json pass entirely when neither applies.
        needs_firm_won_index = (edit_mode or any(r["disclosed"] for r in resolved_by_deal_id.values())
                                 or any(closed_out_disclosed_by_id.values()))
        firm_won_index = (_build_firm_won_index() if needs_firm_won_index
                           else {"by_company_id": set(), "by_company_name": set()})

        note_html = ('<p class="gg-note">Status overrides unavailable — showing Pipeline values.</p>'
                     if dynamo_failed else "")

        main_rows, pending_rows = [], []
        for d in kept_deals:
            resolved = resolved_by_deal_id[str(d.get("id"))]
            (main_rows if resolved["disclosed"] else pending_rows).append((d, resolved))

        def _entry_for(d):
            return intro_details.get(str(d.get("id"))) or {}

        def _stalled_key(resolved):
            return 0 if resolved["id"] == INTRO_STATUS_STALLED_ID else 1

        # Item 7 (turn 22 dropped the due/overdue-follow-up tier that used
        # to sort first — follow-up dates are gone from the UI entirely):
        # Stalled first, then furthest-progressed first, then company A-Z.
        main_rows.sort(key=lambda dr: (_stalled_key(dr[1]), -_intro_sort_rank(dr[1]),
                                        (_deal_company_name(dr[0]) or "").lower()))
        pending_rows.sort(key=lambda dr: (_deal_company_name(dr[0]) or "").lower())

        # Item 7: a company repeated in consecutive rows (within the same
        # section) is shown once, with a subtle left-accent on the
        # follow-on rows instead of repeating the name.
        def _mark_repeats(rows):
            prev = None
            flags = []
            for d, resolved in rows:
                name = (_deal_company_name(d) or "").strip().lower()
                flags.append(bool(name and name == prev))
                prev = name
            return flags

        main_repeats = _mark_repeats(main_rows)
        pending_repeats = _mark_repeats(pending_rows)

        # Closed out: company A-Z, same as Pending — there's no "how far
        # did it get" ranking that means anything for a dead deal.
        closed_out_rows = sorted(closed_out_deals, key=lambda d: (_deal_company_name(d) or "").lower())
        closed_out_repeats = _mark_repeats([(d, None) for d in closed_out_rows])

        # tenant_edit_mode: the tenant (real session, or admin &view_as
        # preview without &edit=1) can auto-save Notes on every
        # Introduced-or-later row, and now (item 4) Status too, restricted
        # to TENANT_ALLOWED_STATUS_IDS.
        tenant_edit_mode = tenant is not None and not edit_mode

        # Item 2: summary strip counts. "In motion" is every disclosed
        # intro that isn't Stalled or Closed (both terminal in different
        # ways). Turn 22 dropped the "follow-ups due" segment along with
        # every other follow-up-date UI surface.
        stalled_count = sum(1 for _, r in main_rows if r["id"] == INTRO_STATUS_STALLED_ID)
        closed_count = sum(1 for _, r in main_rows if r["name"] == "Closed")
        in_motion_count = len(main_rows) - stalled_count - closed_count
        pending_count = len(pending_rows)

        summary_parts = []
        if in_motion_count:
            summary_parts.append(f"{in_motion_count} in motion")
        if stalled_count:
            summary_parts.append(f"{stalled_count} stalled")
        if pending_count:
            summary_parts.append(f"{pending_count} pending introduction{'s' if pending_count != 1 else ''}")
        summary_html = (f'<p class="mydeals-summary">{_esc(" · ".join(summary_parts))}</p>'
                         if summary_parts else "")
        subtle_html = ('<p class="mydeals-subtle">Update statuses as buyers progress — '
                        'we see your changes instantly.</p>')

        head_row = (
            '<th>Company</th><th>Buyer</th><th>Investor Type</th>'
            '<th class="num">Size</th>'
            '<th title="Where this introduction stands">Status</th>'
            '<th>Notes</th>'
        )

        # Item 2: "Closed out" — a collapsed, expandable section below
        # Pending, built regardless of whether kept_deals is empty (a
        # tenant can have zero live intros and still have a dead one on
        # record).
        closed_out_html = ""
        if closed_out_rows:
            co_parts = []
            for i, d in enumerate(closed_out_rows):
                linked = _deal_linked_person_ids(d) - {person_id}
                buyer_recs = [people_by_id[pid] for pid in linked if pid in people_by_id]
                co_parts.append(_closed_out_intro_row_html(
                    d, buyer_recs, closed_out_disclosed_by_id[str(d.get("id"))], tenant_email, person_id,
                    firm_won_index, key=key, view_as=view_as, company_repeated=closed_out_repeats[i]))
            co_rows_html = "".join(co_parts)
            closed_out_html = f"""<details class="closed-out-section">
      <summary>Closed out <span class="count">({len(closed_out_rows)})</span></summary>
      <div class="card closed-out-card">
        <table>
          <colgroup>
            <col style="width:13%">
            <col style="width:24%">
            <col style="width:11%">
            <col style="width:7%">
            <col style="width:18%">
            <col style="width:27%">
          </colgroup>
          <thead>
            <tr>
              {head_row}
            </tr>
          </thead>
          <tbody>{co_rows_html}</tbody>
        </table>
      </div>
    </details>"""

        if not kept_deals:
            # Item 9: no intros anywhere for this tenant — a friendlier,
            # page-level empty state instead of a near-empty table.
            my_deals_href = f"?tab=mydeals{_tab_qs_suffix(key, view_as)}"
            if closed_out_rows:
                empty_note = '<p>No live introductions right now — see Closed out below for past ones.</p>'
            else:
                empty_note = ('<p>No introductions yet. Your live deals are being shown to buyers — '
                               'introductions appear here as matches firm up.</p>')
            body_html = (
                f'{note_html}<div class="gg-empty-state">'
                f'{empty_note}'
                f'<p><a class="gg-link" href="{my_deals_href}">View My Deals &rarr;</a></p>'
                f'</div>{closed_out_html}'
            )
        else:
            # "Introduced" always renders — even with zero rows — per
            # instruction; "Pending introductions" only when there's
            # something pending.
            parts = [_group_header_row_html("Introduced", 6)]
            if main_rows:
                if edit_mode:
                    parts += [_intro_row_edit_html(d, people_by_id, person_id, intro_details, firm_won_index,
                                                    key=key, view_as=view_as, company_repeated=main_repeats[i])
                              for i, (d, _) in enumerate(main_rows)]
                else:
                    parts += [_intro_row_html(d, resolved, people_by_id, person_id, _entry_for(d), firm_won_index,
                                               key=key, view_as=view_as, editable=tenant_edit_mode,
                                               company_repeated=main_repeats[i])
                              for i, (d, resolved) in enumerate(main_rows)]
            else:
                parts.append(_group_empty_row_html("No introductions yet on this deal.", 6))

            if pending_rows:
                parts.append(_group_header_row_html("Pending introductions", 6))
                parts.append(
                    f'<tr><td colspan="6" class="group-note">'
                    f"We're preparing these introductions — buyer identities appear here "
                    f'the moment we connect you.</td></tr>'
                )
                if edit_mode:
                    # Edit mode never anonymizes — the admin sees and edits
                    # the real buyer regardless of pending/disclosed.
                    parts += [_intro_row_edit_html(d, people_by_id, person_id, intro_details, firm_won_index,
                                                    key=key, view_as=view_as, company_repeated=pending_repeats[i])
                              for i, (d, _) in enumerate(pending_rows)]
                else:
                    # Pending rows never get inputs even under
                    # tenant_edit_mode — only Introduced-or-later rows are
                    # editable.
                    for i, (d, resolved) in enumerate(pending_rows):
                        linked = _deal_linked_person_ids(d) - {person_id}
                        buyer_recs = [people_by_id[pid] for pid in linked if pid in people_by_id]
                        parts.append(_pending_intro_row_html(d, buyer_recs, tenant_email, person_id,
                                                              key=key, view_as=view_as,
                                                              company_repeated=pending_repeats[i]))

            rows_html = "".join(parts)
            table_html = f"""<div class="card">
      <table>
        <colgroup>
          <col style="width:13%">
          <col style="width:24%">
          <col style="width:11%">
          <col style="width:7%">
          <col style="width:18%">
          <col style="width:27%">
        </colgroup>
        <thead>
          <tr>
            {head_row}
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>"""

            edit_script = _edit_script_html(key) if (edit_mode or tenant_edit_mode) else ""
            body_html = f"{note_html}{table_html}{closed_out_html}{edit_script}"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Active Intros</title>
<style>
  * {{ box-sizing: border-box; }}
  :root {{
    --bg: #f4f2ee;
    --card: #ffffff;
    --line: #e7e5e0;
    --ink: #16181d;
    --muted: #6b7280;
    --accent: #3d5a73;
    --qp: #1f7a4d;
  }}
  body {{
    margin: 0;
    background: var(--bg);
    color: var(--ink);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
    padding: 32px 24px 64px;
  }}
{NAV_CSS}
{FEATURE_CSS}
  .wrap {{ max-width: 1000px; margin: 28px auto 0; }}
  h1 {{ font-size: 22px; font-weight: 600; margin: 0 0 4px; }}
  .mydeals-summary {{ color: var(--ink); font-size: 14px; margin: 0 0 4px; }}
  .mydeals-subtle {{ color: var(--muted); font-size: 13px; margin: 0 0 20px; }}
  .feature-section-heading {{ font-size: 16px; font-weight: 600; margin: 32px 0 12px; }}
  .card {{
    background: var(--card);
    border: 1px solid var(--line);
    border-radius: 10px;
    overflow: hidden;
  }}
  table {{ width: 100%; table-layout: fixed; border-collapse: collapse; }}
  thead th {{
    text-align: left;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--muted);
    padding: 12px 16px;
    border-bottom: 1px solid var(--line);
  }}
  thead th.num, td.num {{ text-align: center; }}
  tbody td {{
    padding: 11px 16px;
    border-bottom: 1px solid var(--line);
    font-size: 14px;
    vertical-align: middle;
  }}
  tbody tr:last-child td {{ border-bottom: none; }}
  td.company {{ font-weight: 500; }}
  td.company a {{ color: inherit; text-decoration: none; border-bottom: 1px solid var(--line); }}
  td.company a:hover {{ border-bottom-color: var(--muted); }}
  .gg-placeholder {{
    max-width: 1000px;
    margin: 96px auto;
    padding: 0 24px;
    text-align: center;
    color: var(--muted);
    font-size: 15px;
  }}
  .gg-empty-state {{
    background: var(--card);
    border: 1px solid var(--line);
    border-radius: 10px;
    padding: 40px 32px;
    text-align: center;
    color: var(--muted);
    font-size: 14px;
    line-height: 1.5;
  }}
  .gg-empty-state p {{ margin: 0 0 10px; }}
  .gg-empty-state p:last-child {{ margin-bottom: 0; }}
  .gg-link {{ color: var(--accent); font-weight: 600; text-decoration: none; }}
  .gg-link:hover {{ text-decoration: underline; }}
  .status-column {{ display: flex; flex-direction: column; gap: 6px; align-items: flex-start; }}
  .status-awaiting {{ font-size: 12px; color: var(--muted); font-style: italic; }}
  .ei-milestones {{ display: flex; flex-wrap: wrap; gap: 4px 10px; }}
  .ei-milestone-label {{
    display: inline-flex;
    align-items: center;
    gap: 4px;
    font-size: 12px;
    color: var(--ink);
    white-space: nowrap;
  }}
  .ei-milestone {{ margin: 0; }}
  .status-pill {{
    display: inline-block;
    font-size: 11px;
    font-weight: 600;
    color: var(--muted);
    background: rgba(22,24,29,0.06);
    border-radius: 999px;
    padding: 2px 8px;
    margin-left: 6px;
  }}
  .status-chip {{
    display: inline-block;
    font-size: 11px;
    font-weight: 600;
    border-radius: 999px;
    padding: 3px 9px;
  }}
  .status-chip.stalled {{ background: rgba(201,162,39,0.15); color: #8a6d1f; }}
  .status-chip.exit {{ background: rgba(22,24,29,0.06); color: var(--muted); }}
  .status-chip.closed {{ background: rgba(31,122,77,0.15); color: var(--qp); }}
  .buyer-code {{ font-size: 12px; font-weight: 600; color: var(--ink); }}
  .buyer-range {{ font-size: 11px; color: var(--muted); margin-top: 2px; }}
  .tier-badge {{
    display: inline-block;
    font-size: 10px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    padding: 2px 7px;
    border-radius: 999px;
    color: #16181d;
  }}
  .tier-badge.tier-qp {{ background: var(--qp); }}
  .tier-badge.tier-accredited {{ background: #c9a227; }}
  .tier-badge.tier-unknown {{ background: var(--muted); }}
  tr.pending-row {{ opacity: 0.85; }}
  tr.grouped-row {{ box-shadow: inset 3px 0 0 var(--line); }}
  tr.closed-out-row {{ opacity: 0.7; }}
  .closed-out-reason {{ color: var(--muted); font-size: 11px; }}
  details.closed-out-section {{ margin-top: 20px; }}
  details.closed-out-section summary {{
    cursor: pointer;
    font-size: 15px;
    font-weight: 600;
    color: var(--ink);
    list-style: none;
    padding: 4px 0;
  }}
  details.closed-out-section summary::-webkit-details-marker {{ display: none; }}
  details.closed-out-section summary::before {{ content: "\\25B8 "; color: var(--muted); font-size: 12px; }}
  details.closed-out-section[open] summary::before {{ content: "\\25BE "; }}
  details.closed-out-section summary .count {{ color: var(--muted); font-weight: 500; font-size: 12px; }}
  .closed-out-card {{ margin-top: 6px; }}
  tr.group-divider td {{
    padding: 8px 16px;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--muted);
    background: rgba(22,24,29,0.02);
    border-bottom: 1px solid var(--line);
  }}
  td.group-empty {{
    padding: 16px;
    color: var(--muted);
    font-size: 13px;
    font-style: italic;
  }}
  td.group-note {{
    padding: 8px 16px 14px;
    color: var(--muted);
    font-size: 12px;
    font-style: italic;
    border-bottom: 1px solid var(--line);
  }}
  .gg-note {{
    color: var(--accredited, #8a6d1f);
    font-size: 13px;
    margin: 0 0 16px;
  }}
  .ei-status, .ei-notes, .ei-flag {{
    background: var(--bg);
    border: 1px solid var(--line);
    color: var(--ink);
    border-radius: 6px;
    padding: 5px 8px;
    font-size: 13px;
    width: 100%;
    box-sizing: border-box;
  }}
  /* Dedup fix (turn 19): the flag select carries the Stalled/Passed/
     Withdrawn state itself (glanceable border+tint) whenever it's the
     only place that state shows — see _status_milestones_column_html. */
  select.ei-flag.flag-stalled {{ border-color: #c9a227; background: rgba(201,162,39,0.15); color: #8a6d1f; }}
  select.ei-flag.flag-exit {{ border-color: var(--muted); background: rgba(22,24,29,0.06); }}
  .ei-msg {{ display: inline-block; font-size: 11px; margin-left: 6px; color: var(--muted); }}
  .ei-msg.saving {{ color: var(--muted); }}
  .ei-msg.saved {{ color: var(--qp); }}
  .ei-msg.error {{ color: #b23b3b; }}
  .buyer-cell-name {{ font-weight: 600; }}
  .buyer-cell-name a.buyer-link {{ color: var(--accent); text-decoration: none; }}
  .buyer-cell-name a.buyer-link:hover {{ text-decoration: underline; }}
  .buyer-link-suffix {{ font-weight: 400; font-size: 11px; color: var(--muted); }}
  .buyer-cell-more {{ font-weight: 400; font-size: 12px; color: var(--muted); }}
  .buyer-cell-sub, .buyer-cell-links {{
    font-size: 12px;
    color: var(--muted);
    margin-top: 2px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }}
  .buyer-cell-links a {{ color: var(--accent); text-decoration: none; }}
  .buyer-cell-links a:hover {{ text-decoration: underline; }}
  /* Item 1 (turn 18): clear gap between the company name and the
     Update-deal link beneath it, matching My Deals' own deal-id-sub
     spacing fix. */
  .company-update-link {{ margin-top: 7px; }}
  .company-update-link a {{ font-size: 12px; color: var(--muted); text-decoration: none; }}
  .company-update-link a:hover {{ color: var(--accent); text-decoration: underline; }}
  .closed-dot {{
    display: inline-block;
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: var(--qp);
    vertical-align: middle;
  }}
  /* Turn 21 folded the Follow-up column into the Notes cell; turn 22
     removed follow-up dates from the UI entirely, so Notes is just a
     2-line textarea filling the whole (still widened) cell. */
  textarea.ei-notes {{
    resize: vertical;
    min-height: 44px;
    font-family: inherit;
    line-height: 1.35;
  }}
</style>
</head>
<body>
{nav}
<div class="wrap">
  <h1>Active Intros</h1>
  {feature_box_html}
  {summary_html}
  {subtle_html}
  {body_html}
  <h2 class="feature-section-heading">Feature requests</h2>
  <div class="card">
    {feature_list_html}
  </div>
</div>
{_feature_toggle_script_html(key)}
</body>
</html>"""


def _my_deal_row_html(deal, company_name, deadline, stats, buyer_count, cef_state, section,
                       key=None, view_as=None, edit_mode=False):
    deal_id = str(deal.get("id"))
    if company_name:
        company_link = (f'<a href="{_company_href(company_name, "mydeals", key, view_as)}">'
                         f'{_esc(company_name)}</a>')
    else:
        company_link = "—"

    # Deal ID now renders under the company name (its own no-longer-a-
    # column) — same copy icon, smaller muted type via .deal-id-sub.
    public_url = _deal_public_url(deal_id)
    deal_id_sub = (
        f'<div class="deal-id-sub">'
        f'<a href="{public_url}" target="_blank" rel="noopener noreferrer">#{deal_id}</a>'
        f'{_copy_id_button_html(public_url)}</div>'
    )

    is_held = section == "hold"
    # Turn 26: a Closed row's badge/state short-circuits to "sold" —
    # see _my_deal_visibility_state.
    is_won = section == "closed"
    badge_html = _my_deal_visibility_badge_html(deal, cef_state, is_held, is_won=is_won)
    visibility_state = _my_deal_visibility_state(deal, cef_state, is_held, is_won=is_won)

    buyer_text = str(buyer_count)
    intro_text = str(stats["intro_count"]) if stats["intro_count"] else "—"

    if is_won:
        # Item 1 (turn 26): no deadline warnings on the trophy shelf — a
        # deal that's already won has nothing left to be overdue about.
        # Always plain read-only text (never the edit_mode date input
        # either — nothing left to maintain on a closed deal).
        is_overdue = False
        deadline_html = f'<span>{_esc(_fmt_short_date(deadline) or deadline)}</span>' if deadline else "—"
    else:
        is_overdue = False
        if deadline:
            deadline_dt = _parse_dt(deadline)
            is_overdue = bool(deadline_dt and deadline_dt.date() < datetime.now(timezone.utc).date())

        if edit_mode:
            deadline_css = "ei-deadline overdue-input" if is_overdue else "ei-deadline"
            deadline_html = _ei_date_field_html(deal_id, deadline or "", css_class=deadline_css, field="deadline")
        elif deadline:
            deadline_cls = ' class="deadline-overdue"' if is_overdue else ""
            deadline_html = f'<span{deadline_cls}>{_esc(_fmt_short_date(deadline) or deadline)}</span>'
        else:
            deadline_html = "—"

    # Item 1: no Next Steps chip either on a Closed row — there's
    # nothing left to nudge/update/sign on a deal that's already sold.
    action_chip_html = ("" if is_won else
                         _my_deal_action_chip_html(deal_id, company_name, is_overdue, stats["stalled"],
                                                    visibility_state, key=key, view_as=view_as))

    update_btn = _update_cancel_button_html(deal_id, label="Update")
    actions_html = _deal_actions_html(deal_id, _deal_title(deal), update_btn, section, key=key)

    return (
        f'<tr><td class="company">{company_link}{deal_id_sub}</td>'
        f'<td>{badge_html}</td>'
        f'<td class="num">{buyer_text}</td>'
        f'<td class="num">{intro_text}</td>'
        f'<td>{deadline_html}</td>'
        f'<td>{action_chip_html}</td>'
        f'<td class="actions">{actions_html}</td></tr>'
    )


def render_my_deals_page(viewer_name, deals=None, tenant_picker=False, key=None, view_as=None, cef_html="",
                          edit_mode=False, person_id=None, anon_key_email=None):
    """deals is always Sell-order-tagged only (see lambda_handler's mydeals
    branch). person_id/anon_key_email are needed here (not just deals)
    because the Buyers/Intros/Needs-attention columns are company-level
    buy-side signals, not attributes of the Sell deal itself."""
    nav = _nav_html("mydeals", viewer_name, key=key, view_as=view_as, edit_flag=edit_mode, cef_html=cef_html)

    feature_box_html, feature_list_html = _feature_section_html(tenant_picker, anon_key_email, key=key,
                                                                   page="my-deals")

    summary_html = ""
    subtle_html = ""
    edit_script = ""
    if tenant_picker:
        body_html = (
            '<div class="gg-placeholder">Pick a tenant to preview — '
            'add &amp;view_as=&lt;email&gt; to the URL.</div>'
        )
    elif not deals:
        body_html = '<div class="gg-placeholder">No deals yet.</div>'
    else:
        company_table = get_company_table()
        buyer_counts = {row["company"].strip().lower(): row["total"] for row in company_table}

        intro_details, _ = get_intro_details(anon_key_email) if anon_key_email else ({}, True)
        cef_state = _tenant_cef_state(person_id)

        # Per-company buy-side aggregation (Intros count, Stalled flag),
        # memoized per distinct company so two Sell deals for the same
        # company don't redo the same get_my_matched_buy_deals scan.
        # Turn 22: follow_up is no longer read here — see _my_deal_action_chip_html.
        company_stats_cache = {}

        def _company_stats(company_name):
            cache_key = (company_name or "").strip().lower()
            if cache_key in company_stats_cache:
                return company_stats_cache[cache_key]
            matched = (get_my_matched_buy_deals(person_id, company_name)
                       if person_id is not None and company_name else [])
            intro_count = 0
            stalled = False
            for d in matched:
                entry = intro_details.get(str(d.get("id"))) or {}
                resolved = _resolve_intro_status(d, entry)
                if resolved["disclosed"]:
                    intro_count += 1
                if resolved["id"] == INTRO_STATUS_STALLED_ID:
                    stalled = True
            stats = {"intro_count": intro_count, "stalled": stalled}
            company_stats_cache[cache_key] = stats
            return stats

        rows = []
        for d in deals:
            company_name = _deal_company_name(d)
            override_entry = intro_details.get(str(d.get("id"))) or {}
            deadline = _resolve_deal_deadline(d, override_entry)
            resolved_stage = _resolve_deal_stage(d, override_entry)
            if resolved_stage == HOLD_STAGE_ID:
                section = "hold"
            elif _is_won_stage(resolved_stage):
                # Item 1 (turn 26): a won deal gets its own "Closed"
                # section — the trophy shelf — and is excluded from
                # "active" outright, same footing as Hold/Cancelled.
                section = "closed"
            elif resolved_stage == OBSOLETE_STAGE_ID or resolved_stage in LOST_STAGE_IDS:
                # Item 4: Lost (111801/2379322) sits alongside Obsolete
                # in Cancelled -- previously Lost wasn't checked here at
                # all, so a Lost-stage sell deal fell into the "else"
                # branch below and rendered in the ACTIVE table exactly
                # like a normal live deal: full visibility-state ladder
                # (frequently landing on the "Live" badge), deadline
                # overdue warnings, a Next Steps chip, and Hold/Cancel
                # buttons -- all wrong for a deal that's already dead.
                section = "cancelled"
            else:
                section = "active"
            rows.append({
                "deal": d,
                "company_name": company_name,
                "deadline": deadline,
                "stats": _company_stats(company_name),
                "buyer_count": buyer_counts.get((company_name or "").strip().lower(), 0),
                "resolved_stage": resolved_stage,
                "section": section,
            })

        # Deadline ascending (ISO yyyy-mm-dd sorts correctly as a string),
        # no-deadline rows last, then company A-Z.
        rows.sort(key=lambda r: ((0, r["deadline"]) if r["deadline"] else (1, ""),
                                  (r["company_name"] or "").lower()))

        live_count = 0
        not_engaged_count = 0
        terms_incomplete_count = 0
        intros_total = 0
        attention_count = 0
        deadlines = []
        section_row_htmls = {"active": [], "hold": [], "cancelled": [], "closed": []}
        for r in rows:
            d = r["deal"]
            deadline = r["deadline"]
            stats = r["stats"]
            section = r["section"]

            # Summary-strip counts and totals cover the active section
            # only — Held, Cancelled, and (turn 26) Closed deals get their
            # own sections below and no longer contribute here (see item
            # 2). This is also what fixes "live"/"Total in pipeline"
            # actually excluding won deals: before turn 26 a won-stage
            # deal fell into this same "active" branch (there was no
            # separate Closed section to route it to), so it silently
            # counted toward live_count/pipeline_total same as any other
            # live deal -- now it never reaches this branch at all.
            if section == "active":
                is_overdue = False
                if deadline:
                    deadline_dt = _parse_dt(deadline)
                    is_overdue = bool(deadline_dt and deadline_dt.date() < datetime.now(timezone.utc).date())
                    deadlines.append(deadline)

                if is_overdue or stats["stalled"]:
                    attention_count += 1
                intros_total += stats["intro_count"]

                # is_held=False: this loop is already gated to the active
                # section, which by construction never holds a Held deal.
                state = _my_deal_visibility_state(d, cef_state, False)
                if state == "live":
                    live_count += 1
                elif state == "id_required":
                    not_engaged_count += 1
                elif state == "terms_incomplete":
                    terms_incomplete_count += 1

            section_row_htmls[section].append(
                _my_deal_row_html(d, r["company_name"], deadline, stats, r["buyer_count"], cef_state,
                                   section, key=key, view_as=view_as, edit_mode=edit_mode))

        today_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        future_deadlines = [dl for dl in deadlines if dl >= today_iso]

        # Resolved (override-aware) stage, not the deal's own raw
        # deal_stage — so a same-session Hold/Cancel/Reactivate moves the
        # deal in or out of "Total in pipeline" immediately, without
        # waiting for deals.json to resync. Scoped to the active section
        # only (see above) — Held/Cancelled/Closed deals no longer count
        # here.
        active_rows = [r for r in rows if r["section"] == "active"]
        pipeline_total = sum(v for v in (_deal_pipeline_size(r["deal"]) for r in active_rows
                                          if not r["deal"].get("is_archived"))
                              if v is not None)
        # Turn 26: sourced from the new "closed" section's own rows, not
        # active_rows -- before this turn, closed_total summed won-stage
        # deals out of active_rows because that was the only place a won
        # deal could ever land (there was no separate Closed section).
        # Once won-stage deals got their own section they stopped
        # appearing in active_rows at all, so this sum would have gone
        # silently to zero (or, worse, "Total closed" would simply never
        # render, since it's only shown `if closed_total`) had it not
        # been repointed here. The old _is_won_stage filter is now
        # redundant and dropped -- every row already in "closed" is won
        # by construction (see the section-assignment loop above) — and
        # the size math itself was never the problem: _deal_pipeline_size
        # already falls back from ticket min/max to the deal's own
        # "value" field when neither custom field is set, exactly as
        # asked to verify.
        closed_rows = [r for r in rows if r["section"] == "closed"]
        closed_total = sum(v for v in (_deal_pipeline_size(r["deal"]) for r in closed_rows) if v is not None)

        # Each part is escaped individually rather than the joined string as
        # a whole, so the dollar totals can carry their own <span> for
        # medium-weight emphasis (item 4) without that markup being
        # escaped away.
        summary_parts = []
        if live_count:
            summary_parts.append(f"{live_count} live")
        if not_engaged_count:
            summary_parts.append(f"{not_engaged_count} not engaged")
        if terms_incomplete_count:
            summary_parts.append(f"{terms_incomplete_count} awaiting terms")
        if intros_total:
            summary_parts.append(f"{intros_total} intros in motion")
        if attention_count:
            summary_parts.append(f"{attention_count} need attention")
        if pipeline_total:
            summary_parts.append(f'Total in pipeline: <span class="mydeals-total">'
                                  f'{_esc(_fmt_money(pipeline_total))}</span>')
        if closed_total:
            summary_parts.append(f'Total closed: <span class="mydeals-total">'
                                  f'{_esc(_fmt_money(closed_total))}</span>')
        if future_deadlines:
            next_deadline = min(future_deadlines)
            summary_parts.append(f"next deadline {_esc(_fmt_short_date(next_deadline) or next_deadline)}")
        summary_html = (f'<p class="mydeals-summary">{" · ".join(summary_parts)}</p>'
                         if summary_parts else "")
        subtle_html = '<p class="mydeals-subtle">Click a company name for deal details, buyers, and live demand.</p>'

        def _section_table_html(rows_html, extra_card_class=""):
            card_cls = f"card {extra_card_class}".strip()
            # table-layout:fixed + an explicit colgroup (item 1's actual
            # fix) — with the default table-layout:auto this table had,
            # nowrap badge/chip text was free to force each column wider
            # than its share of the card, so the table's real width (measured
            # in a real browser, not eyeballed) exceeded the card's and the
            # actions column bled out past its right edge. Fixed layout
            # makes these percentages the real column widths regardless of
            # content, and the badges/chips below wrap (max ~2 lines for
            # their actual text) instead of forcing more.
            return f"""<div class="{card_cls}">
    <table>
      <colgroup>
        <col style="width:17%">
        <col style="width:21%">
        <col style="width:8%">
        <col style="width:8%">
        <col style="width:12%">
        <col style="width:22%">
        <col style="width:12%">
      </colgroup>
      <thead>
        <tr>
          <th>Company</th>
          <th>Visibility</th>
          <th class="num">Buyers</th>
          <th class="num">Intros</th>
          <th>Deadline</th>
          <th>Next Steps</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        {rows_html}
      </tbody>
    </table>
  </div>"""

        # Four stacked sections; a section with no rows is omitted
        # entirely (item 2) rather than rendered empty. Closed (turn 26)
        # is last, below Cancelled — the trophy shelf comes after the
        # graveyard, not before it.
        sections_html = []
        if section_row_htmls["active"]:
            sections_html.append(_section_table_html("".join(section_row_htmls["active"])))
        if section_row_htmls["hold"]:
            sections_html.append(
                f'<h2 class="mydeals-section-heading hold">On Hold '
                f'<span class="count">({len(section_row_htmls["hold"])})</span></h2>'
                + _section_table_html("".join(section_row_htmls["hold"]), "section-hold"))
        if section_row_htmls["cancelled"]:
            sections_html.append(
                f'<h2 class="mydeals-section-heading cancelled">Cancelled '
                f'<span class="count">({len(section_row_htmls["cancelled"])})</span></h2>'
                + _section_table_html("".join(section_row_htmls["cancelled"]), "section-cancelled"))
        if section_row_htmls["closed"]:
            sections_html.append(
                f'<h2 class="mydeals-section-heading closed">Closed '
                f'<span class="count">({len(section_row_htmls["closed"])})</span></h2>'
                + _section_table_html("".join(section_row_htmls["closed"]), "section-closed"))
        body_html = "".join(sections_html)
        edit_script = _edit_script_html(key) if edit_mode else ""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>My Deals</title>
<style>
{NAV_CSS}
{FEATURE_CSS}
  :root {{
    --bg: #f4f2ee;
    --card: #ffffff;
    --line: #e7e5e0;
    --ink: #16181d;
    --muted: #6b7280;
    --accent: #3d5a73;
    --qp: #1f7a4d;
    --accredited: #8a6d1f;
  }}
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0;
    background: var(--bg);
    color: var(--ink);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
    padding: 32px 24px 64px;
  }}
  /* 1000px, matching the other tabs (was 1100px) — Deal ID and Size no
     longer have their own columns, so this table needs less room than
     it used to, not more. */
  .wrap {{ max-width: 1000px; margin: 28px auto 0; }}
  h1 {{ font-size: 22px; font-weight: 600; margin: 0 0 4px; }}
  .mydeals-summary {{ color: var(--ink); font-size: 14px; margin: 0 0 4px; }}
  .mydeals-total {{ font-weight: 600; }}
  .mydeals-subtle {{ color: var(--muted); font-size: 13px; margin: 0 0 20px; }}
  .feature-section-heading {{ font-size: 16px; font-weight: 600; margin: 32px 0 12px; }}
  .card {{
    background: var(--card);
    border: 1px solid var(--line);
    border-radius: 10px;
  }}
  table {{ width: 100%; table-layout: fixed; border-collapse: collapse; }}
  /* Sticky header needs the card unclipped (overflow:hidden on an
     ancestor defeats position:sticky), so the rounded top corners are
     applied directly to the header cells instead of via .card overflow
     clipping. Also why this table has no .table-scroll wrapper (unlike
     Active Intros/the company page) — overflow-x:auto on a wrapper
     forces overflow-y to auto too (per the CSS overflow spec, whenever
     one axis goes non-visible the other stops being visible), which
     would make that wrapper — not the page — the sticky header's
     scrolling ancestor and break "pin to viewport while the page
     scrolls". Item 5's fit is handled by trimming columns and width
     instead (Deal ID/Size removed from the row entirely, .wrap back
     down to 1000px). */
  thead th:first-child {{ border-top-left-radius: 10px; }}
  thead th:last-child {{ border-top-right-radius: 10px; }}
  tbody tr:last-child td:first-child {{ border-bottom-left-radius: 10px; }}
  tbody tr:last-child td:last-child {{ border-bottom-right-radius: 10px; }}
  thead th {{
    position: sticky;
    top: 0;
    z-index: 1;
    background: var(--card);
    text-align: left;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--muted);
    padding: 12px 16px;
    border-bottom: 1px solid var(--line);
    white-space: nowrap;
  }}
  thead th.num, td.num {{ text-align: center; }}
  tbody td {{
    padding: 11px 16px;
    border-bottom: 1px solid var(--line);
    font-size: 14px;
  }}
  tbody tr:last-child td {{ border-bottom: none; }}
  tbody tr:hover {{ background: rgba(22,24,29,0.03); }}
  td.company {{ font-weight: 500; }}
  td.company a {{ color: inherit; text-decoration: none; border-bottom: 1px solid var(--line); }}
  td.company a:hover {{ border-bottom-color: var(--muted); }}
  .deal-id-sub {{
    margin-top: 7px;
    font-size: 12px;
    font-weight: 400;
  }}
  .deal-id-sub a {{ color: var(--muted); text-decoration: none; }}
  .deal-id-sub a:hover {{ color: var(--accent); text-decoration: underline; }}
  .copy-id {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 22px;
    height: 22px;
    margin-left: 4px;
    padding: 0;
    background: transparent;
    border: 1px solid var(--line);
    border-radius: 4px;
    color: var(--muted);
    cursor: pointer;
    line-height: 0;
    vertical-align: middle;
  }}
  .copy-id svg {{
    width: 13px;
    height: 13px;
    fill: none;
    stroke: currentColor;
    stroke-width: 1.5;
    stroke-linecap: round;
    stroke-linejoin: round;
  }}
  .copy-id:hover {{ border-color: var(--accent); color: var(--accent); }}
  td.actions {{ white-space: nowrap; }}
  .actions-stack {{
    display: flex;
    flex-direction: column;
    align-items: stretch;
    gap: 4px;
  }}
  .update-cancel-btn, .deal-stage-btn {{
    display: block;
    width: 100%;
    box-sizing: border-box;
    text-align: center;
    font-size: 12px;
    font-weight: 600;
    padding: 4px 8px;
    border-radius: 6px;
    border: none;
    background: rgba(22,24,29,0.06);
    color: var(--ink);
    text-decoration: none;
    cursor: pointer;
    margin: 0;
    font-family: inherit;
  }}
  .update-cancel-btn:hover, .deal-stage-btn:hover {{
    text-decoration: underline;
  }}
  .update-cancel-btn {{ background: rgba(31,122,77,0.15); color: var(--qp); }}
  .deal-stage-btn[data-target="hold"] {{ background: rgba(201,162,39,0.15); color: var(--accredited); }}
  .deal-stage-btn[data-target="cancel"] {{ background: rgba(178,59,59,0.12); color: #b23b3b; }}
  .mydeals-section-heading {{
    font-size: 14px;
    font-weight: 600;
    margin: 28px 0 10px;
    display: flex;
    align-items: center;
    gap: 8px;
  }}
  .mydeals-section-heading .count {{ color: var(--muted); font-weight: 500; font-size: 12px; }}
  .card.section-hold {{ border-color: rgba(201,162,39,0.45); }}
  .card.section-hold thead th {{ background: rgba(201,162,39,0.08); }}
  .mydeals-section-heading.hold {{ color: var(--accredited); }}
  .card.section-cancelled {{ opacity: 0.7; }}
  .mydeals-section-heading.cancelled {{ color: var(--muted); }}
  /* Turn 26: the Closed section is the trophy shelf, not a graveyard —
     muted-but-positive (a soft green tint, no opacity fade unlike
     Cancelled's washed-out look above). */
  .card.section-closed {{ border-color: rgba(31,122,77,0.3); }}
  .card.section-closed thead th {{ background: rgba(31,122,77,0.06); }}
  .mydeals-section-heading.closed {{ color: var(--qp); }}
  .visibility-badge {{
    display: inline-block;
    font-size: 12px;
    font-weight: 600;
    line-height: 1.3;
    padding: 4px 10px;
    border-radius: 14px;
    text-decoration: none;
    white-space: normal;
  }}
  .visibility-badge.live, .visibility-badge.sold {{ background: rgba(31,122,77,0.15); color: var(--qp); }}
  .visibility-badge.held {{ background: rgba(201,162,39,0.15); color: var(--accredited); }}
  .visibility-badge.id-required, .visibility-badge.agreement-unsigned, .visibility-badge.terms-incomplete {{
    background: rgba(178,59,59,0.12); color: #b23b3b;
  }}
  .deadline-overdue {{ color: #b23b3b; font-weight: 600; }}
  .ei-deadline.overdue-input {{ border-color: #b23b3b; }}
  .action-chip {{
    display: inline-block;
    font-size: 11px;
    font-weight: 700;
    line-height: 1.3;
    padding: 3px 9px;
    border-radius: 12px;
    text-decoration: none;
    white-space: normal;
    cursor: pointer;
  }}
  .action-chip:hover {{ text-decoration: underline; }}
  .action-chip.overdue, .action-chip.terms, .action-chip.id-required {{
    background: rgba(178,59,59,0.12); color: #b23b3b;
  }}
  .action-chip.nudge, .action-chip.sign {{
    background: rgba(201,162,39,0.15); color: var(--accredited);
  }}
  .ei-deadline {{
    background: var(--bg);
    border: 1px solid var(--line);
    color: var(--ink);
    border-radius: 6px;
    padding: 5px 8px;
    font-size: 13px;
    width: 150px;
  }}
  .ei-msg {{ display: inline-block; font-size: 11px; margin-left: 6px; color: var(--muted); }}
  .ei-msg.saving {{ color: var(--muted); }}
  .ei-msg.saved {{ color: var(--qp); }}
  .ei-msg.error {{ color: #b23b3b; }}
  .gg-placeholder {{
    max-width: 1000px;
    margin: 96px auto;
    padding: 0 24px;
    text-align: center;
    color: var(--muted);
    font-size: 15px;
  }}
  .gg-placeholder.small {{ margin: 0; padding: 24px; }}
</style>
</head>
<body>
{nav}
<div class="wrap">
  <h1>My Deals</h1>
  {feature_box_html}
  {summary_html}
  {subtle_html}
  {body_html}
  <h2 class="feature-section-heading">Feature requests</h2>
  <div class="card">
    {feature_list_html}
  </div>
</div>
{edit_script}
{_feature_toggle_script_html(key)}
{_deal_stage_script_html()}
<script>
(function() {{
  function copyTextToClipboard(text) {{
    if (navigator.clipboard && window.isSecureContext) {{
      return navigator.clipboard.writeText(text);
    }}
    return new Promise(function(resolve, reject) {{
      var ta = document.createElement('textarea');
      ta.value = text;
      ta.setAttribute('readonly', '');
      ta.style.position = 'fixed';
      ta.style.top = '-1000px';
      document.body.appendChild(ta);
      ta.select();
      var ok = false;
      try {{ ok = document.execCommand('copy'); }} catch (e) {{ ok = false; }}
      document.body.removeChild(ta);
      ok ? resolve() : reject(new Error('Copy failed'));
    }});
  }}
  document.addEventListener('click', function(event) {{
    var btn = event.target.closest ? event.target.closest('.copy-id') : null;
    if (!btn) return;
    event.preventDefault();
    copyTextToClipboard(btn.getAttribute('data-copy-url')).catch(function() {{}});
  }});
}})();
</script>
</body>
</html>"""


TIER_LABELS = {"qp": "QP", "accredited": "Accredited", "unknown": "Unknown"}
TIER_ORDER = {"qp": 0, "accredited": 1, "unknown": 2}


def _tier_badge_html(tier):
    """Item 4 (turn 18): the QP/Accredited tier badge pill, wherever tier
    renders as a per-buyer badge (buyer rows, tiles, the buyer page) —
    Unknown renders as no badge at all, since "everyone we couldn't
    classify" isn't a meaningful label to show next to one buyer. This
    does NOT apply to the Demand Board's per-company QP/Accredited/
    Unknown COLUMN counts (_build_table) — that aggregate is meaningful
    and stays exactly as-is; those are plain numbers, never this badge
    markup."""
    if tier == "unknown":
        return ""
    tier_label = TIER_LABELS.get(tier, "Unknown")
    return f'<span class="tier-badge tier-{tier}">{_esc(tier_label)}</span>'


def _buyer_tile_html(buyer, anon_key_email, now):
    code = _anon_buyer_code(anon_key_email, buyer["person_id"])
    tier = buyer["tier"]
    tier_html = _tier_badge_html(tier)

    min_v, max_v = buyer["ticket_range"]
    range_text = _fmt_ticket_range(min_v, max_v)
    range_html = f'<div class="buyer-range">{_esc(range_text)}</div>' if range_text else ""

    dt = _parse_dt(buyer["updated_at"])
    recent = bool(dt and (now - dt).days <= 365)
    dot_cls = "buyer-dot filled" if recent else "buyer-dot"
    dot_title = "Active within 12 months" if recent else "No recent activity"

    return f"""<div class="buyer-tile">
      <span class="{dot_cls}" title="{dot_title}"></span>
      <div class="buyer-code">Buyer {_esc(code)}</div>
      {tier_html}
      {range_html}
    </div>"""


def render_company_page(company, viewer_name, tenant, anon_key_email, ref, key=None, view_as=None, edit_mode=False,
                         cef_html=""):
    """No tab is highlighted (active_tab=None never matches mydeals/intros/
    demand in _nav_html). Section (a) is included only when tenant is not
    None — the same "admin with no view_as" signal render_my_deals_page's
    tenant_picker branch uses, since there is no tenant to scope deals to.
    Section (b) never receives — and so can never render — a buyer's name,
    email, or raw person id; the tile only ever sees the anonymized code,
    tier, ticket range, and a boolean recency flag from _buyer_tile_html.

    edit_mode is only ever True for a valid ADMIN_KEY (see lambda_handler)
    — never for a real tenant — and switches the Buyers table to editable
    rows with no pending/disclosed split (see _matched_buyer_row_edit_html)."""
    nav = _nav_html(None, viewer_name, key=key, view_as=view_as, show_viewer=False, edit_flag=edit_mode,
                     cef_html=cef_html)
    suffix = _tab_qs_suffix(key, view_as)
    back_href = f"?tab={ref}{suffix}"
    back_label = REF_LABELS.get(ref, "My Deals")

    # Feature-request box + list: always scoped to anon_key_email's own
    # partition (the viewing tenant's own email, the previewed tenant's
    # under &view_as, or the literal "admin" when neither applies) — no
    # cross-tenant aggregation here, that's render_my_deals_page's
    # tenant_picker branch only (see get_feature_requests).
    feature_items, _ = get_feature_requests(anon_key_email, page="company")
    feature_box_html = _feature_box_html(anon_key_email, key=key, page="company")
    feature_list_html = _feature_requests_list_html(feature_items["open"], feature_items["done"])

    # tenant_edit_mode: the tenant (real session, or admin &view_as preview
    # without &edit=1) can auto-save Next Steps / Follow-up on their own
    # Introduced-or-later rows. Mutually exclusive with edit_mode (full
    # admin edit) — never both, since edit_mode already implies tenant is
    # not None whenever it renders anything on this page (see lambda_handler:
    # tenant stays None only for admin-without-view_as, and edit_mode is
    # only True there or under &view_as&edit=1).
    tenant_edit_mode = tenant is not None and not edit_mode

    your_deals_html = ""
    matched_buyers_html = ""
    if tenant is not None:
        person_id = tenant.get("person_id")

        # Fetched once, unconditionally, and shared by both sections below:
        # get_intro_details returns every intro#<deal_id> item under this
        # tenant's own Dynamo partition, which covers deadline_override
        # entries on their SELL deals (Deal Details) just as much as
        # status/next_steps/follow_up on their BUY-side matches (Buyers).
        intro_details, dynamo_failed = get_intro_details(anon_key_email)

        sell_deals = get_my_sell_deals(person_id, company) if person_id is not None else []
        if sell_deals:
            deals_body = "".join(
                _deal_card_html(d, company, intro_details.get(str(d.get("id"))), edit_mode=edit_mode)
                for d in sell_deals
            )
        else:
            deals_body = '<div class="gg-placeholder small">No deals with this company yet.</div>'
        your_deals_html = f"""<section class="cd-section">
    <h2>Deal Details</h2>
    {deals_body}
  </section>"""

        matched_deals = get_my_matched_buy_deals(person_id, company) if person_id is not None else []
        wanted_ids = set()
        for d in matched_deals:
            wanted_ids |= _deal_linked_person_ids(d) - {person_id}
        people_by_id = get_people_by_ids(wanted_ids) if wanted_ids else {}
        resolved_by_deal_id = {str(d.get("id")): _resolve_intro_status(d, intro_details.get(str(d.get("id"))))
                                for d in matched_deals}

        note_html = ('<p class="gg-note">Next steps and notes unavailable right now.</p>'
                     if dynamo_failed else "")

        main_deals, pending_deals = [], []
        for d in matched_deals:
            resolved = resolved_by_deal_id[str(d.get("id"))]
            (main_deals if resolved["disclosed"] else pending_deals).append(d)
        main_deals.sort(key=lambda d: (-_intro_sort_rank(resolved_by_deal_id[str(d.get("id"))]),
                                        (_deal_title(d) or "").lower()))
        pending_deals.sort(key=lambda d: (_deal_title(d) or "").lower())

        if edit_mode or tenant_edit_mode:
            colspan = 8
            head_row = ('<th>Buyer name</th><th>Company</th><th>Investor Type</th>'
                        '<th class="num">Size</th><th>Status</th>'
                        '<th>Next Steps</th><th>Follow-up</th><th>Buyer Notes</th>')
        else:
            colspan = 7
            head_row = ('<th>Buyer name</th><th>Company</th><th>Investor Type</th>'
                        '<th class="num">Size</th><th>Status</th>'
                        '<th>Next Steps</th><th>Buyer Notes</th>')

        # "Introduced" always renders — even with zero rows — per instruction;
        # "Pending introductions" only when there's something pending.
        rows_parts = [_group_header_row_html("Introduced", colspan)]
        if main_deals:
            if edit_mode:
                rows_parts += [_matched_buyer_row_edit_html(d, person_id, people_by_id, intro_details,
                                                              key=key, view_as=view_as)
                               for d in main_deals]
            else:
                rows_parts += [_matched_buyer_row_html(d, person_id, people_by_id, intro_details, anon_key_email,
                                                         editable=tenant_edit_mode, key=key, view_as=view_as)
                               for d in main_deals]
        else:
            rows_parts.append(_group_empty_row_html("No introductions yet on this deal.", colspan))

        if pending_deals:
            rows_parts.append(_group_header_row_html("Pending introductions", colspan))
            if edit_mode:
                # Edit mode never anonymizes — the admin sees and edits the
                # real buyer regardless of pending/disclosed.
                rows_parts += [_matched_buyer_row_edit_html(d, person_id, people_by_id, intro_details,
                                                              key=key, view_as=view_as)
                               for d in pending_deals]
            else:
                # Pending rows never get inputs even under tenant_edit_mode —
                # only Introduced-or-later rows are editable.
                rows_parts += [_matched_buyer_row_html(d, person_id, people_by_id, intro_details, anon_key_email,
                                                         editable=tenant_edit_mode, key=key, view_as=view_as)
                               for d in pending_deals]

        matched_rows_html = "".join(rows_parts)
        matched_body = f"""{note_html}<div class="card">
      <div class="table-scroll">
      <table>
        <thead>
          <tr>
            {head_row}
          </tr>
        </thead>
        <tbody>{matched_rows_html}</tbody>
      </table>
      </div>
    </div>"""

        # Turn 27, item 2: the same "Closed out" collapsed treatment as
        # Active Intros, at the bottom of Buyers — a dead-stage (Lost/
        # Trade Broken/Obsolete) BUY deal for this company, keyed off
        # get_my_closed_out_buy_deals rather than the matched-or-later
        # fetch above.
        closed_out_deals = get_my_closed_out_buy_deals(person_id, company) if person_id is not None else []
        closed_out_html = ""
        if closed_out_deals:
            for d in closed_out_deals:
                wanted_ids |= _deal_linked_person_ids(d) - {person_id}
            people_by_id = get_people_by_ids(wanted_ids) if wanted_ids else {}
            closed_out_deals.sort(key=lambda d: (_deal_title(d) or "").lower())
            if edit_mode or tenant_edit_mode:
                co_rows_html = "".join(
                    _closed_out_buyer_row_html(d, person_id, people_by_id, anon_key_email,
                                                 editable=True, admin_reveals=edit_mode, key=key, view_as=view_as)
                    for d in closed_out_deals)
            else:
                co_rows_html = "".join(
                    _closed_out_buyer_row_html(d, person_id, people_by_id, anon_key_email,
                                                 editable=False, key=key, view_as=view_as)
                    for d in closed_out_deals)
            closed_out_html = f"""<details class="closed-out-section">
      <summary>Closed out <span class="count">({len(closed_out_deals)})</span></summary>
      <div class="card closed-out-card">
        <div class="table-scroll">
        <table>
          <thead>
            <tr>
              {head_row}
            </tr>
          </thead>
          <tbody>{co_rows_html}</tbody>
        </table>
        </div>
      </div>
    </details>"""

        edit_script = _edit_script_html(key) if (edit_mode or tenant_edit_mode) else ""
        matched_buyers_html = f"""<section class="cd-section">
    <h2>Buyers</h2>
    {matched_body}
    {closed_out_html}
    {edit_script}
  </section>"""

    buyers = get_company_buyer_details(company)
    buyers.sort(key=lambda b: b["updated_at"] or "", reverse=True)
    buyers.sort(key=lambda b: TIER_ORDER.get(b["tier"], 3))
    now = datetime.now(timezone.utc)
    if buyers:
        tiles_html = "".join(_buyer_tile_html(b, anon_key_email, now) for b in buyers)
        buyer_demand_body = f'<div class="buyer-grid">{tiles_html}</div>'
    else:
        buyer_demand_body = '<div class="gg-placeholder small">No buy interest recorded yet.</div>'

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{_esc(company)}</title>
<style>
{NAV_CSS}
{FEATURE_CSS}
  :root {{
    --bg: #f4f2ee;
    --card: #ffffff;
    --line: #e7e5e0;
    --ink: #16181d;
    --muted: #6b7280;
    --accent: #3d5a73;
    --qp: #1f7a4d;
    --accredited: #8a6d1f;
    --unknown: #6b7280;
  }}
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0;
    background: var(--bg);
    color: var(--ink);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
    padding: 32px 24px 64px;
  }}
  .wrap {{ max-width: 1000px; margin: 0 auto; }}
  .cd-back {{
    display: inline-block;
    color: var(--muted);
    text-decoration: none;
    font-size: 13px;
    margin-bottom: 12px;
  }}
  .cd-back:hover {{ color: var(--ink); }}
  h1 {{ font-size: 22px; font-weight: 600; margin: 0 0 28px; }}
  .cd-section {{ margin-bottom: 32px; }}
  .cd-section h2 {{
    font-size: 13px;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--muted);
    font-weight: 600;
    margin: 0 0 12px;
  }}
  .card {{
    background: var(--card);
    border: 1px solid var(--line);
    border-radius: 10px;
    overflow: hidden;
  }}
  table {{ width: 100%; border-collapse: collapse; }}
  thead th {{
    text-align: left;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--muted);
    padding: 12px 16px;
    border-bottom: 1px solid var(--line);
    white-space: nowrap;
  }}
  thead th.num, td.num {{ text-align: right; }}
  tbody td {{
    padding: 11px 16px;
    border-bottom: 1px solid var(--line);
    font-size: 14px;
  }}
  tbody tr:last-child td {{ border-bottom: none; }}
  .gg-placeholder {{
    padding: 40px 24px;
    text-align: center;
    color: var(--muted);
    font-size: 15px;
    background: var(--card);
    border: 1px solid var(--line);
    border-radius: 10px;
  }}
  .gg-placeholder.small {{ padding: 24px; font-size: 14px; }}
  .buyer-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
    gap: 12px;
  }}
  .buyer-tile {{
    background: var(--card);
    border: 1px solid var(--line);
    border-radius: 10px;
    padding: 14px;
    position: relative;
  }}
  .buyer-dot {{
    position: absolute;
    top: 14px;
    right: 14px;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    border: 1px solid var(--muted);
    background: transparent;
  }}
  .buyer-dot.filled {{ background: var(--qp); border-color: var(--qp); }}
  .buyer-code {{
    font-size: 13px;
    font-weight: 600;
    color: var(--ink);
    margin-bottom: 8px;
    letter-spacing: 0.02em;
  }}
  .tier-badge {{
    display: inline-block;
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    padding: 3px 8px;
    border-radius: 999px;
    color: #16181d;
    margin-bottom: 8px;
  }}
  .tier-badge.tier-qp {{ background: var(--qp); }}
  .tier-badge.tier-accredited {{ background: #c9a227; }}
  .tier-badge.tier-unknown {{ background: var(--unknown); color: var(--ink); }}
  .buyer-range {{ font-size: 12px; color: var(--muted); }}
  .deal-card {{
    background: var(--card);
    border: 1px solid var(--line);
    border-radius: 12px;
    padding: 22px 24px;
    margin-bottom: 16px;
  }}
  .deal-card:last-child {{ margin-bottom: 0; }}
  .deal-card.overdue {{ border-color: #b23b3b; }}
  .overdue-chip {{
    display: inline-block;
    font-size: 12px;
    font-weight: 700;
    padding: 4px 10px;
    border-radius: 999px;
    margin-bottom: 12px;
    background: rgba(178,59,59,0.12);
    color: #b23b3b;
    text-decoration: none;
  }}
  .overdue-chip:hover {{ text-decoration: underline; }}
  .deal-card-head {{
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 12px;
    margin-bottom: 16px;
  }}
  .deal-card-title {{ font-size: 20px; font-weight: 700; }}
  .deal-card-stage {{
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    color: var(--muted);
    white-space: nowrap;
  }}
  .deal-card-metrics {{
    display: flex;
    flex-wrap: wrap;
    gap: 32px;
    margin-bottom: 14px;
  }}
  .dc-label {{
    display: block;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--muted);
    margin-bottom: 4px;
  }}
  .dc-value {{ font-size: 16px; font-weight: 600; }}
  .dc-line {{ font-size: 13px; color: var(--muted); margin-bottom: 4px; }}
  .buyer-contact {{ font-size: 12px; color: var(--muted); margin-top: 2px; }}
  a.buyer-link {{ color: var(--accent); text-decoration: none; }}
  a.buyer-link:hover {{ text-decoration: underline; }}
  .buyer-link-suffix {{ font-weight: 400; font-size: 11px; color: var(--muted); }}
  .engagement-badge {{
    display: inline-block;
    font-size: 12px;
    font-weight: 600;
    padding: 4px 10px;
    border-radius: 999px;
    margin-top: 6px;
    text-decoration: none;
  }}
  .engagement-badge.engaged {{ background: rgba(31,122,77,0.15); color: var(--qp); }}
  .engagement-badge.in-process {{ background: rgba(201,162,39,0.15); color: var(--accredited); }}
  .engagement-badge.not-engaged {{ background: rgba(178,59,59,0.12); color: #b23b3b; }}
  .engagement-badge.not-engaged:hover {{ text-decoration: underline; }}
  .update-cancel-btn {{
    display: inline-block;
    font-size: 12px;
    font-weight: 600;
    padding: 4px 10px;
    border-radius: 999px;
    margin-top: 6px;
    margin-left: 8px;
    background: rgba(22,24,29,0.06);
    color: var(--ink);
    text-decoration: none;
  }}
  .update-cancel-btn:hover {{ text-decoration: underline; }}
  .status-pill {{
    display: inline-block;
    font-size: 11px;
    font-weight: 600;
    color: var(--muted);
    background: rgba(22,24,29,0.06);
    border-radius: 999px;
    padding: 2px 8px;
  }}
  .status-chip {{
    display: inline-block;
    font-size: 11px;
    font-weight: 600;
    border-radius: 999px;
    padding: 3px 9px;
  }}
  .status-chip.stalled {{ background: rgba(201,162,39,0.15); color: var(--accredited); }}
  .status-chip.exit {{ background: rgba(22,24,29,0.06); color: var(--muted); }}
  .gg-note {{
    color: var(--accredited);
    font-size: 13px;
    margin: 0 0 10px;
  }}
  tr.pending-row {{ opacity: 0.85; }}
  tr.closed-out-row {{ opacity: 0.7; }}
  .closed-out-reason {{ color: var(--muted); font-size: 11px; }}
  details.closed-out-section {{ margin-top: 14px; }}
  details.closed-out-section summary {{
    cursor: pointer;
    font-size: 14px;
    font-weight: 600;
    color: var(--ink);
    list-style: none;
    padding: 4px 0;
  }}
  details.closed-out-section summary::-webkit-details-marker {{ display: none; }}
  details.closed-out-section summary::before {{ content: "\\25B8 "; color: var(--muted); font-size: 12px; }}
  details.closed-out-section[open] summary::before {{ content: "\\25BE "; }}
  details.closed-out-section summary .count {{ color: var(--muted); font-weight: 500; font-size: 12px; }}
  .closed-out-card {{ margin-top: 6px; }}
  tr.group-divider td {{
    padding: 8px 16px;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--muted);
    background: rgba(22,24,29,0.02);
    border-bottom: 1px solid var(--line);
  }}
  td.group-empty {{
    padding: 16px;
    color: var(--muted);
    font-size: 13px;
    font-style: italic;
  }}
  .table-scroll {{ overflow-x: auto; }}
  .ei-status, .ei-next-steps, .ei-notes, .ei-follow-up, .ei-deadline {{
    background: var(--bg);
    border: 1px solid var(--line);
    color: var(--ink);
    border-radius: 6px;
    padding: 5px 8px;
    font-size: 13px;
  }}
  .ei-next-steps, .ei-notes {{ width: 140px; }}
  .ei-follow-up, .ei-deadline {{ width: 150px; }}
  .ei-msg {{ display: inline-block; font-size: 11px; margin-left: 6px; color: var(--muted); }}
  .ei-msg.saving {{ color: var(--muted); }}
  .ei-msg.saved {{ color: var(--qp); }}
  .ei-msg.error {{ color: #b23b3b; }}
  .buyer-detail {{ font-size: 12px; color: var(--muted); margin-top: 2px; }}
  .buyer-detail a {{ color: var(--accent); text-decoration: none; }}
  .buyer-detail a:hover {{ text-decoration: underline; }}
  .follow-up-note {{ font-size: 11px; color: var(--muted); margin-top: 2px; }}
  .due-chip {{
    display: inline-block;
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    padding: 2px 7px;
    border-radius: 999px;
    margin-left: 6px;
    background: rgba(178,59,59,0.12);
    color: #b23b3b;
  }}
</style>
</head>
<body>
{nav}
<div class="wrap">
  <a class="cd-back" href="{back_href}">&larr; Back to {_esc(back_label)}</a>
  <h1>{_esc(company)}</h1>
  {feature_box_html}
  {your_deals_html}
  {matched_buyers_html}
  <section class="cd-section">
    <h2>Buyer Demand</h2>
    {buyer_demand_body}
  </section>
  <section class="cd-section">
    <h2>Feature requests</h2>
    <div class="card">
      {feature_list_html}
    </div>
    {_feature_toggle_script_html(key)}
  </section>
</div>
</body>
</html>"""


def _buyer_header_html(rec, closer_kind):
    """Turn 24: the buyer page's single header card, merging what used
    to be two separate blocks (IDENTITY + CAPACITY, turn 23) into one —
    name large; one line beneath it joining title (if present) · firm
    name · city/country (work_* falling back to home_*); a second line
    of mailto/LinkedIn/website links; the closer chip and ID-verified
    chip (custom_label_3796440, the Client Engagement Form field, Yes
    only -- every other state omitted, no red/amber states on a page
    the TENANT reads about someone else) right-aligned; and a trailing
    chip row for tier badge / ticket range (custom_label_3052210 map) /
    transactor type label (custom_label_3759163 map) -- the old
    separate near-empty "Capacity" card is gone. Every field renders
    only when present. title/city are implemented on trust the same
    way phone was (_person_phone_text): no repo in this org reads a
    person record's "title"/"work_city"/"home_city", but they follow
    the exact naming convention work_country/home_country already
    verified working in production. Person-record fields only; nothing
    about any company, deal, or interest is read or shown here."""
    name = _esc(_person_display_name(rec) or "—")
    title = (rec.get("title") or "").strip()
    cf = rec.get("custom_fields") or {}
    transactor_ids = cf_list(cf, TRANSACTOR_TYPE_FIELD)
    transactor_id = transactor_ids[0] if transactor_ids else None
    is_natural = transactor_id == NATURAL_PERSON_ID
    firm_name = (rec.get("company_name") or "").strip() if not is_natural else ""
    city = (rec.get("work_city") or rec.get("home_city") or "").strip()
    country = (rec.get("work_country") or rec.get("home_country") or "").strip()
    location = ", ".join(p for p in (city, country) if p)

    line1_parts = [p for p in (title, firm_name, location) if p]
    line1_html = (f'<div class="buyer-header-line">{" &middot; ".join(_esc(p) for p in line1_parts)}</div>'
                  if line1_parts else "")

    email = _person_email_text(rec)
    website = (rec.get("website") or "").strip()
    linked_in = (rec.get("linked_in_url") or "").strip()
    line2_links = []
    if email:
        line2_links.append(f'<a href="mailto:{_esc(email)}">{_esc(email)}</a>')
    if linked_in:
        href_l = linked_in if "://" in linked_in else f"https://{linked_in}"
        line2_links.append(f'<a href="{_esc(href_l)}" target="_blank" rel="noopener noreferrer">LinkedIn</a>')
    if website:
        href_w = website if "://" in website else f"https://{website}"
        line2_links.append(f'<a href="{_esc(href_w)}" target="_blank" rel="noopener noreferrer">{_esc(website)}</a>')
    line2_html = (f'<div class="buyer-header-line buyer-header-links">{" &middot; ".join(line2_links)}</div>'
                  if line2_links else "")

    id_verified = CEF_YES_ID in cf_list(cf, CEF_FIELD)
    badge_parts = [h for h in (_closer_chip_html(closer_kind),
                                '<span class="id-verified-chip">ID verified</span>' if id_verified else "") if h]
    badges_html = f'<div class="buyer-header-badges">{"".join(badge_parts)}</div>' if badge_parts else ""

    tier_html = _tier_badge_html(classify_person(cf))
    min_v, max_v = get_person_ticket_range(cf)
    range_text = _fmt_ticket_range(min_v, max_v)
    range_html = f'<span class="capacity-chip">{_esc(range_text)}</span>' if range_text else ""
    transactor_label = TRANSACTOR_TYPE_LABELS.get(transactor_id, "")
    transactor_html = f'<span class="capacity-chip">{_esc(transactor_label)}</span>' if transactor_label else ""
    chips = "".join(h for h in (tier_html, range_html, transactor_html) if h)
    chip_row_html = f'<div class="buyer-header-chips">{chips}</div>' if chips else ""

    return (
        f'<div class="card buyer-header"><div class="buyer-header-top">'
        f'<div><div class="buyer-page-name">{name}</div>{line1_html}{line2_html}</div>'
        f'{badges_html}</div>{chip_row_html}</div>'
    )


def _truncate_words(text, limit):
    """(visible_prefix, remainder) split at `limit` words. remainder is
    "" when text is already short enough to show whole — callers treat
    a non-empty remainder as "needs a more-expander"."""
    words = text.split()
    if len(words) <= limit:
        return text, ""
    return " ".join(words[:limit]), " " + " ".join(words[limit:])


def _buyer_about_firm_html(firm_name, company_rec, edit_mode):
    """Turn 24, LEFT column: "About <firm>" — companies.json fields
    only (get_company_record), each rendered only when present (this
    snapshot's shape is unverified in this sandbox, see COMPANIES_KEY —
    every field here degrades to "omit the line" rather than erroring
    on a mismatch). description: rendered as-is (no rewriting), trimmed
    to ~60 words with a zero-JS <details>/<summary> "more" expander;
    ADMIN-ONLY for now, with an "(internal — review before publishing)"
    marker — tenants see the rest of the card (location, founded year,
    PitchBook link) without it until a real publish flow exists.
    founded year is read verbatim from a "founded_year" field ONLY —
    never derived or computed from anything else (a deal date, a
    person's updated_at, etc.) — so a company with no real value there
    simply has no "Founded" line, rather than a guessed one. PitchBook
    link (custom_label_3320818) as "PitchBook profile →" when present.
    Returns "" (card omitted) when there's nothing this viewer can see —
    natural-person buyers (no firm at all) never even reach this."""
    if company_rec is None or not firm_name:
        return ""
    description = (company_rec.get("description") or "").strip()
    city = (company_rec.get("city") or "").strip()
    country = (company_rec.get("country") or "").strip()
    location = ", ".join(p for p in (city, country) if p)
    founded = company_rec.get("founded_year")
    founded_text = str(founded).strip() if founded else ""
    cf = company_rec.get("custom_fields") or {}
    pb_raw = cf.get(COMPANY_PITCHBOOK_FIELD)
    if isinstance(pb_raw, list):
        pb_raw = pb_raw[0] if pb_raw else None
    pitchbook_url = str(pb_raw).strip() if pb_raw else ""

    rows = []
    if edit_mode and description:
        visible, rest = _truncate_words(description, 60)
        if rest:
            desc_html = (f'<p class="firm-description">{_esc(visible)}'
                         f'<details class="firm-more"><summary>… more</summary>'
                         f'<span>{_esc(rest)}</span></details></p>')
        else:
            desc_html = f'<p class="firm-description">{_esc(description)}</p>'
        rows.append(desc_html)
        rows.append('<div class="firm-internal-note">(internal — review before publishing)</div>')
    if location:
        rows.append(f'<div class="buyer-page-row">{_esc(location)}</div>')
    if founded_text:
        rows.append(f'<div class="buyer-page-row">Founded {_esc(founded_text)}</div>')
    if pitchbook_url:
        href = pitchbook_url if "://" in pitchbook_url else f"https://{pitchbook_url}"
        rows.append(f'<div class="buyer-page-row"><a href="{_esc(href)}" target="_blank" '
                    f'rel="noopener noreferrer">PitchBook profile &rarr;</a></div>')

    if not rows:
        return ""
    return f'<div class="card"><h2 class="buyer-section-heading">About {_esc(firm_name)}</h2>{"".join(rows)}</div>'


def _buyer_process_signals_html(rec):
    """Turn 23, block 5 (PROCESS SIGNALS): Accepts (custom_label_3998063,
    ACCEPTS_LABELS) as one small chip listing every accepted structure
    ("Accepts: SPV · Fees"); IQF status as a green "Qualification on
    file" chip when the SAME IQF_FIELD/IQF_OK_IDS this file's tier
    classifier already uses (6496840 Yes / 6596073 Unnecessary) is set,
    otherwise omitted outright -- no partial/negative state shown."""
    cf = rec.get("custom_fields") or {}
    accepted_ids = cf_list(cf, ACCEPTS_FIELD)
    accepted_labels = [ACCEPTS_LABELS[i] for i in accepted_ids if i in ACCEPTS_LABELS]
    accepts_html = ""
    if accepted_labels:
        accepts_html = (f'<span class="accepts-chip">Accepts: '
                         f'{" &middot; ".join(_esc(label) for label in accepted_labels)}</span>')
    iqf_html = ""
    if set(cf_list(cf, IQF_FIELD)) & IQF_OK_IDS:
        iqf_html = '<span class="iqf-chip">Qualification on file</span>'

    if not (accepts_html or iqf_html):
        return ""
    chips = "".join(h for h in (accepts_html, iqf_html) if h)
    return f'<div class="card"><h2 class="buyer-section-heading">Process signals</h2><div class="buyer-page-row">{chips}</div></div>'


def _track_deal_row_html(deal, entry, resolved, tenant_label=None, loss_reason=None):
    """One deal's row in TRACK WITH YOU: company · size · the resolved
    status as a flag chip (_status_display_html, compact — a pill for
    an in-progress status, a colored chip for Stalled/Passed/Withdrawn/
    Closed), then the same read-only milestone checkboxes Active
    Intros itself uses (_milestone_checkboxes_html, disabled=True --
    never editable here; turn 24 dropped the disabled flag SELECT
    _status_milestones_column_html also drew, since the flag chip
    alone already carries that state — one signal, not two). tenant_
    label is set only in admin edit mode (grouped-by-tenant heading
    above each of that tenant's rows). loss_reason (turn 27): a small
    muted "— <reason>" suffix appended after the chip, for a stage-
    derived closed-out deal that has one on record — see
    _deal_loss_reason_text; None for every ordinary row, unchanged."""
    deal_id = str(deal.get("id"))
    company_name = _deal_company_name(deal) or "—"
    size_text = _esc(_deal_size_text(deal))
    milestones = entry.get("milestones")
    checkboxes_html = _milestone_checkboxes_html(deal_id, milestones, disabled=True)
    flag_chip_html = _status_display_html(resolved, compact=True)
    if loss_reason:
        flag_chip_html += f' <span class="closed-out-reason">— {_esc(loss_reason)}</span>'
    label_html = f'<div class="track-row-tenant">{tenant_label}</div>' if tenant_label else ""
    return (
        f'<div class="track-row">{label_html}'
        f'<div class="track-row-head"><span class="track-row-company">{_esc(company_name)}</span>'
        f'<span class="track-row-size">{size_text}</span>{flag_chip_html}</div>'
        f'{checkboxes_html}</div>'
    )


def _closed_out_resolved_dict(deal):
    """A _resolve_intro_status-shaped dict for a stage-derived closed-out
    deal (turn 27) — lets _track_deal_row_html/_status_display_html
    render it exactly like a real Passed/Withdrawn row (is_exit=True,
    status_id=None so the gray "exit" chip renders, never the amber
    Stalled one) without those two deals having gone through Pipeline's
    Intro Status field at all. "disclosed" is _closed_out_disclosed's own
    rule, not the general exit-status one _resolve_intro_status uses."""
    return {
        "id": None,
        "name": _closed_out_outcome_name(_deal_stage_id(deal)),
        "is_exit": True,
        "disclosed": _closed_out_disclosed(deal),
    }


def _buyer_track_with_you_html(buyer_id, tenant, anon_key_email, edit_mode):
    """Turn 23, block 3 (TRACK WITH YOU): every deal linking this buyer
    to the viewing tenant, read-only (company/size/milestones/flag/
    outcome — never an input; only the Notes Ledger below is
    editable). Tenant view: strictly this tenant's own deals, and only
    the ones already disclosed to them (mirrors the page's own
    disclosure gate and the Notes Ledger's own scoping — never a
    pending/Matched-only deal's milestone detail about a buyer they
    haven't actually been introduced on for THAT deal). Admin edit
    mode: every tenant's matched-or-later deals with this buyer,
    disclosed or not, grouped by tenant with a label above each
    tenant's block of rows.

    Turn 27, item 2: stage-derived closed-out deals (Lost/Trade Broken/
    Obsolete) are folded into this same list — a tenant's history with a
    buyer includes the ones that died, not just the live ones — using
    _closed_out_resolved_dict so they render through the exact same row
    renderer and disclosure-gated chip as everything else here."""
    rows = []
    if edit_mode:
        tenant_index = _tenant_index()
        intro_cache = {}
        by_tenant = {}
        for d in get_deals_list():
            if not (_is_matched_or_later_buy_deal(d) or _is_closed_out_buy_deal(d)):
                continue
            if buyer_id not in _deal_linked_person_ids(d):
                continue
            owner_email = _tenant_email_for_deal(d)
            if owner_email is None:
                continue
            if owner_email not in intro_cache:
                intro_cache[owner_email], _ = get_intro_details(owner_email)
            entry = intro_cache[owner_email].get(str(d.get("id"))) or {}
            if _is_closed_out_buy_deal(d):
                resolved = _closed_out_resolved_dict(d)
                loss_reason = _deal_loss_reason_text(d)
            else:
                resolved = _resolve_intro_status(d, entry)
                loss_reason = None
            by_tenant.setdefault(owner_email, []).append((d, entry, resolved, loss_reason))
        for owner_email in sorted(by_tenant, key=lambda e: (tenant_index.get(e) or {}).get("name", e)):
            tenant_name = (tenant_index.get(owner_email) or {}).get("name", owner_email)
            deals_for_tenant = by_tenant[owner_email]
            for i, (d, entry, resolved, loss_reason) in enumerate(deals_for_tenant):
                label = _esc(tenant_name) if i == 0 else None
                rows.append(_track_deal_row_html(d, entry, resolved, tenant_label=label, loss_reason=loss_reason))
    else:
        person_id = tenant.get("person_id")
        intro_details, _ = get_intro_details(anon_key_email)
        for d in get_my_matched_buy_deals(person_id):
            if buyer_id not in _deal_linked_person_ids(d):
                continue
            entry = intro_details.get(str(d.get("id"))) or {}
            resolved = _resolve_intro_status(d, entry)
            if not resolved["disclosed"]:
                continue
            rows.append(_track_deal_row_html(d, entry, resolved))
        for d in get_my_closed_out_buy_deals(person_id):
            if buyer_id not in _deal_linked_person_ids(d):
                continue
            resolved = _closed_out_resolved_dict(d)
            if not resolved["disclosed"]:
                continue
            entry = intro_details.get(str(d.get("id"))) or {}
            rows.append(_track_deal_row_html(d, entry, resolved, loss_reason=_deal_loss_reason_text(d)))

    body = "".join(rows) if rows else '<div class="gg-placeholder small">No deals with this buyer yet.</div>'
    return f'<div class="card buyer-track-card"><h2 class="buyer-section-heading">Track with you</h2>{body}</div>'


def _notes_ledger_entry_html(deal_id, label_prefix, notes_value, notes_updated_at):
    """One NOTES LEDGER entry: company name, the note itself (same
    2-line auto-saving textarea Active Intros' own Notes column uses —
    _ei_notes_textarea_html, same .ei-notes save path), and a
    last-edited date when notes_updated_at is on record (stamped by
    _dynamo_write_intro_update whenever notes is written — absent for
    any note that predates turn 23, which simply omits the date rather
    than showing something wrong)."""
    field_html = _ei_notes_textarea_html(deal_id, _esc(notes_value), placeholder="Add a note…")
    edited_text = _fmt_epoch_short_date(notes_updated_at)
    edited_html = f'<span class="ledger-entry-edited">Last edited {_esc(edited_text)}</span>' if edited_text else ""
    return (f'<div class="ledger-entry">'
            f'<div class="ledger-entry-head"><span class="ledger-entry-company">{label_prefix}</span>'
            f'{edited_html}</div>{field_html}</div>')


def _buyer_notes_ledger_html(buyer_id, tenant, anon_key_email, edit_mode):
    """Turn 23, block 4 (NOTES LEDGER, the page's centerpiece): every
    Dynamo intro note for this buyer across the viewing tenant's deals,
    one entry per company — company name, the note, last-edited date,
    editable inline via the same auto-save path Active Intros' own
    Notes column uses (.ei-notes, keyed by THAT deal's own deal_id --
    notes are stored per intro item, not per buyer, so each entry saves
    to a different Dynamo record).

    Tenant view (edit_mode=False, whether a real tenant session or an
    admin &view_as preview without &edit=1 -- the same tenant_edit_mode
    signal every other page uses): only the viewing tenant's own notes,
    scoped to their Introduced-or-later deals with this buyer (mirrors
    the page's own disclosure gate -- never a pending/anonymized deal,
    which is exactly what makes every listed entry here safe to render
    as an editable field unconditionally). Admin edit mode: every
    tenant's notes on every matched-or-later buy deal linking this
    buyer, disclosed or not, each entry labeled by tenant then company.
    Only ever called after the page's own full_access check already
    passed -- there is nothing to show otherwise."""
    entries = []
    if edit_mode:
        tenant_index = _tenant_index()
        intro_cache = {}
        for d in get_deals_list():
            if not _is_matched_or_later_buy_deal(d):
                continue
            if buyer_id not in _deal_linked_person_ids(d):
                continue
            owner_email = _tenant_email_for_deal(d)
            if owner_email is None:
                continue
            if owner_email not in intro_cache:
                intro_cache[owner_email], _ = get_intro_details(owner_email)
            entry = intro_cache[owner_email].get(str(d.get("id"))) or {}
            notes_value = entry.get("notes")
            if notes_value is None:
                notes_value = entry.get("next_steps") or ""
            company_name = _deal_company_name(d) or "—"
            tenant_name = (tenant_index.get(owner_email) or {}).get("name", owner_email)
            label = f'{_esc(tenant_name)} &middot; {_esc(company_name)}'
            entries.append(_notes_ledger_entry_html(str(d.get("id")), label, notes_value,
                                                      entry.get("notes_updated_at")))
    else:
        person_id = tenant.get("person_id")
        intro_details, _ = get_intro_details(anon_key_email)
        for d in get_my_matched_buy_deals(person_id):
            if buyer_id not in _deal_linked_person_ids(d):
                continue
            entry = intro_details.get(str(d.get("id"))) or {}
            if not _resolve_intro_status(d, entry)["disclosed"]:
                continue
            notes_value = entry.get("notes")
            if notes_value is None:
                notes_value = entry.get("next_steps") or ""
            company_name = _deal_company_name(d) or "—"
            entries.append(_notes_ledger_entry_html(str(d.get("id")), _esc(company_name), notes_value,
                                                      entry.get("notes_updated_at")))

    body = ("".join(entries) if entries
            else '<div class="gg-placeholder small">No notes yet — add one from Active Intros or right here.</div>')
    return f'<div class="card buyer-notes-card"><h2 class="buyer-section-heading">Notes</h2>{body}</div>'


def _buyer_page_anonymized_html(rec, anon_key_email, buyer_id):
    """The not-yet-disclosed fallback (item 3, turn 18) — the same code/
    tier/ticket-range shape as _pending_buyer_cell_html and the Buyer
    Demand tiles, never a name, company, email, or phone."""
    cf = rec.get("custom_fields") or {}
    code = _anon_buyer_code(anon_key_email, buyer_id)
    tier_html = _tier_badge_html(classify_person(cf))
    min_v, max_v = get_person_ticket_range(cf)
    range_text = _fmt_ticket_range(min_v, max_v)
    range_html = f'<div class="buyer-page-row">{_esc(range_text)}</div>' if range_text else ""
    tier_row = f'<div class="buyer-page-row">{tier_html}</div>' if tier_html else ""
    return (f'<div class="card"><div class="buyer-page-code">Buyer {_esc(code)}</div>'
            f'{tier_row}{range_html}'
            f'<div class="buyer-page-note">Identity available after introduction.</div></div>')


def render_buyer_page(buyer_id_raw, viewer_name, tenant, anon_key_email, key=None, view_as=None, edit_mode=False,
                       cef_html=""):
    """Item 3 (turn 18): ?buyer=<person_id> — replaces the old inline
    <details> expansion on Active Intros with a real page. DISCLOSURE
    GATE: the full profile (turn 24 layout — one header card, then a
    two-column body: About-the-firm + Process Signals on the left,
    Track With You + Notes on the right) renders only when the viewing
    tenant has at least one Introduced-or-later deal linked to this
    specific person (_tenant_has_disclosed_deal_with, checked across
    EVERY company the tenant has matched buy deals in, not just
    wherever the viewer clicked in from) — otherwise the anonymized
    card (_buyer_page_anonymized_html: code/tier/ticket range only,
    UNCHANGED by this redesign) with "Identity available after
    introduction." edit_mode=True (admin) always gets the full profile,
    gate skipped entirely. Nothing about this buyer's OTHER companies,
    other tenants' demand, or their Pipeline interests is ever looked
    up or shown here.

    Full access also unlocks the edit script (checkbox/text-field
    auto-save), so it's only ever included when there's actually
    something on the page it needs to wire up.

    tenant stays None only for admin-without-view_as (the same
    tenant-picker signal every other page uses) — there's no tenant
    context to gate disclosure against, so that case gets the same
    picker placeholder as Active Intros/My Deals."""
    nav = _nav_html(None, viewer_name, key=key, view_as=view_as, show_viewer=False, edit_flag=edit_mode,
                     cef_html=cef_html)

    try:
        buyer_id = int(buyer_id_raw)
    except (TypeError, ValueError):
        buyer_id = None

    edit_script_html = ""
    if tenant is None:
        body_html = (
            '<div class="gg-placeholder">Pick a tenant to preview — '
            'add &amp;view_as=&lt;email&gt; to the URL.</div>'
        )
    elif buyer_id is None:
        body_html = '<div class="gg-placeholder">Buyer not found.</div>'
    else:
        rec = get_people_by_ids({buyer_id}).get(buyer_id)
        if rec is None:
            body_html = '<div class="gg-placeholder">Buyer not found.</div>'
        else:
            person_id = tenant.get("person_id")
            full_access = edit_mode or _tenant_has_disclosed_deal_with(person_id, anon_key_email, buyer_id)
            if full_access:
                closer_kind = _closer_kind(rec, _build_firm_won_index())
                header_html = _buyer_header_html(rec, closer_kind)

                # About-the-firm: only ever looked up for an entity buyer
                # (a natural person has no firm to look up) and only when
                # there's a company_id or company_name to key the lookup
                # on at all -- same company_id-first, company_name-
                # fallback strategy as _closer_kind (see get_company_record).
                cf = rec.get("custom_fields") or {}
                transactor_ids = cf_list(cf, TRANSACTOR_TYPE_FIELD)
                is_natural = (transactor_ids[0] if transactor_ids else None) == NATURAL_PERSON_ID
                firm_name = (rec.get("company_name") or "").strip() if not is_natural else ""
                company_id = rec.get("company_id")
                company_rec = get_company_record(company_id, firm_name) if firm_name else None

                left_html = (
                    _buyer_about_firm_html(firm_name, company_rec, edit_mode)
                    + _buyer_process_signals_html(rec)
                )
                right_html = (
                    _buyer_track_with_you_html(buyer_id, tenant, anon_key_email, edit_mode)
                    + _buyer_notes_ledger_html(buyer_id, tenant, anon_key_email, edit_mode)
                )
                body_html = (
                    header_html
                    + f'<div class="buyer-columns"><div class="buyer-col-left">{left_html}</div>'
                    + f'<div class="buyer-col-right">{right_html}</div></div>'
                )
                edit_script_html = _edit_script_html(key)
            else:
                body_html = _buyer_page_anonymized_html(rec, anon_key_email, buyer_id)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Buyer</title>
<style>
  * {{ box-sizing: border-box; }}
  :root {{
    --bg: #f4f2ee;
    --card: #ffffff;
    --line: #e7e5e0;
    --ink: #16181d;
    --muted: #6b7280;
    --accent: #3d5a73;
    --qp: #1f7a4d;
  }}
{NAV_CSS}
  body {{
    margin: 0;
    background: var(--bg);
    color: var(--ink);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
    padding: 32px 24px 64px;
  }}
  .wrap {{ max-width: 900px; margin: 28px auto 0; }}
  .card {{
    background: var(--card);
    border: 1px solid var(--line);
    border-radius: 10px;
    padding: 20px;
  }}
  .buyer-page-name {{ font-size: 21px; font-weight: 600; margin: 0 0 2px; }}
  .buyer-page-row {{ font-size: 14px; color: var(--ink); margin-top: 6px; }}
  .buyer-page-row a {{ color: var(--accent); text-decoration: none; }}
  .buyer-page-row a:hover {{ text-decoration: underline; }}
  .buyer-page-code {{ font-size: 18px; font-weight: 600; }}
  .buyer-page-note {{ color: var(--muted); font-size: 13px; margin-top: 14px; }}
  .buyer-section-heading {{ font-size: 15px; font-weight: 600; margin: 0 0 10px; }}
  /* Turn 24: single header card -- name, two info lines, badges
     right-aligned, then a trailing chip row for tier/ticket/transactor
     (see _buyer_header_html). */
  .buyer-header-top {{ display: flex; justify-content: space-between; align-items: flex-start; gap: 16px; flex-wrap: wrap; }}
  .buyer-header-line {{ font-size: 13px; color: var(--muted); margin-top: 4px; }}
  .buyer-header-line a {{ color: var(--accent); text-decoration: none; }}
  .buyer-header-line a:hover {{ text-decoration: underline; }}
  .buyer-header-badges {{ display: flex; gap: 6px; flex-wrap: wrap; flex-shrink: 0; }}
  .buyer-header-chips {{ display: flex; gap: 6px; flex-wrap: wrap; margin-top: 12px; }}
  .capacity-chip {{
    display: inline-block;
    font-size: 11px;
    font-weight: 600;
    padding: 3px 9px;
    border-radius: 999px;
    background: rgba(22,24,29,0.06);
    color: var(--ink);
  }}
  .tier-badge {{
    display: inline-block;
    font-size: 10px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    padding: 2px 7px;
    border-radius: 999px;
    color: #16181d;
  }}
  .tier-badge.tier-qp {{ background: var(--qp); }}
  .tier-badge.tier-accredited {{ background: #c9a227; }}
  .tier-badge.tier-unknown {{ background: var(--muted); }}
  /* Closer chip and small green signal chips (ID verified, IQF) -- all
     the same pill shape, green fill, white text, used only where the
     underlying boolean is True (see _closer_chip_html /
     _buyer_header_html / _buyer_process_signals_html — the false/unset
     case is always just omitted, never a red/amber chip). */
  .closer-chip, .id-verified-chip, .iqf-chip {{
    display: inline-block;
    font-size: 11px;
    font-weight: 600;
    padding: 3px 9px;
    border-radius: 999px;
    background: var(--qp);
    color: #ffffff;
  }}
  .accepts-chip {{
    display: inline-block;
    font-size: 11px;
    font-weight: 600;
    padding: 3px 9px;
    border-radius: 999px;
    background: rgba(22,24,29,0.06);
    color: var(--muted);
  }}
  .gg-placeholder {{
    max-width: 640px;
    margin: 96px auto;
    padding: 0 24px;
    text-align: center;
    color: var(--muted);
    font-size: 15px;
  }}
  .gg-placeholder.small {{ margin: 0; padding: 6px 0; text-align: left; font-size: 13px; }}
  /* Turn 24: header card, then a two-column body (single column under
     680px) -- About the firm + Process signals on the left, Track
     with you + Notes on the right (see render_buyer_page). Cards
     within a column stack with a tight gap; an empty column (nothing
     to show in either of its cards) just collapses to nothing, never
     a visible empty box. */
  .buyer-header {{ margin-bottom: 14px; }}
  .buyer-columns {{ display: grid; grid-template-columns: 1fr 1fr; gap: 14px; align-items: start; }}
  .buyer-col-left, .buyer-col-right {{ display: flex; flex-direction: column; gap: 14px; min-width: 0; }}
  @media (max-width: 680px) {{
    .buyer-columns {{ grid-template-columns: 1fr; }}
  }}
  .track-row {{ margin-top: 12px; padding-top: 12px; border-top: 1px solid var(--line); }}
  .track-row:first-of-type {{ margin-top: 0; padding-top: 0; border-top: none; }}
  .track-row-tenant {{
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    color: var(--muted);
    margin-bottom: 6px;
  }}
  .track-row-head {{ display: flex; align-items: center; gap: 8px; flex-wrap: wrap; margin-bottom: 6px; }}
  .track-row-company {{ font-size: 14px; font-weight: 600; }}
  .track-row-size {{ font-size: 13px; color: var(--muted); }}
  .closed-out-reason {{ color: var(--muted); font-size: 11px; }}
  .ledger-entry {{ margin-top: 12px; }}
  .ledger-entry:first-of-type {{ margin-top: 0; }}
  .ledger-entry-head {{
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 8px;
    margin-bottom: 4px;
  }}
  .ledger-entry-company {{ font-size: 13px; font-weight: 600; }}
  .ledger-entry-edited {{ font-size: 11px; color: var(--muted); white-space: nowrap; }}
  .ei-milestones {{ display: flex; flex-wrap: wrap; gap: 4px 10px; }}
  .ei-milestone-label {{
    display: inline-flex;
    align-items: center;
    gap: 4px;
    font-size: 12px;
    color: var(--ink);
    white-space: nowrap;
  }}
  .ei-milestone {{ margin: 0; }}
  .status-pill {{
    display: inline-block;
    font-size: 11px;
    font-weight: 600;
    color: var(--muted);
    background: rgba(22,24,29,0.06);
    border-radius: 999px;
    padding: 2px 8px;
  }}
  .status-chip {{
    display: inline-block;
    font-size: 11px;
    font-weight: 600;
    border-radius: 999px;
    padding: 3px 9px;
  }}
  .status-chip.stalled {{ background: rgba(201,162,39,0.15); color: #8a6d1f; }}
  .status-chip.exit {{ background: rgba(22,24,29,0.06); color: var(--muted); }}
  .status-chip.closed {{ background: rgba(31,122,77,0.15); color: var(--qp); }}
  /* "About the firm" card: description is admin-only for now (a
     zero-JS <details>/<summary> "more" expander), everyone else sees
     just location/founded/PitchBook -- see _buyer_about_firm_html. */
  .firm-description {{ font-size: 14px; line-height: 1.5; margin: 0 0 2px; color: var(--ink); }}
  .firm-more {{ display: inline; }}
  .firm-more summary {{ display: inline; cursor: pointer; color: var(--accent); list-style: none; }}
  .firm-more summary::-webkit-details-marker {{ display: none; }}
  .firm-more[open] summary {{ display: none; }}
  .firm-internal-note {{ font-size: 11px; color: #b23b3b; font-style: italic; margin-top: 6px; }}
  .ei-notes {{
    background: var(--bg);
    border: 1px solid var(--line);
    color: var(--ink);
    border-radius: 6px;
    padding: 5px 8px;
    font-size: 13px;
    width: 100%;
    box-sizing: border-box;
  }}
  textarea.ei-notes {{ resize: vertical; min-height: 44px; font-family: inherit; line-height: 1.35; }}
  .ei-msg {{ display: inline-block; font-size: 11px; margin-left: 6px; color: var(--muted); }}
  .ei-msg.saving {{ color: var(--muted); }}
  .ei-msg.saved {{ color: var(--qp); }}
  .ei-msg.error {{ color: #b23b3b; }}
</style>
</head>
<body>
{nav}
<div class="wrap">
  {body_html}
</div>
{edit_script_html}
</body>
</html>"""


SELL_ORDER_MAILTO_URL = (
    "mailto:cgracia@rainmakersecurities.com"
    "?subject=" + urllib.parse.quote("I want to sell", safe="")
    + "&body=" + urllib.parse.quote("Company, approximate size, and structure:", safe="")
)


def _message_page(title, message, show_signin=False, show_sell_cta=False):
    """Standalone pre-auth / not-enabled page. Reuses the board's own dark
    palette (not the nav's) since there's no tab shell to sit under here.

    show_sell_cta (item 2): only for the authenticated-but-no-sell-order
    "Not enabled" case — gives that person a path to become a seller.
    Neither chadgracia/trades nor chadgracia/web-bid has a general-
    purpose public sell-order intake: web-bid's own ?side=sell form
    (trades routes "deal-less" companies to it) requires a company_id
    already resolved against directory_companies.json and carries no
    company picker of its own, so it cannot be linked bare from here —
    this page has no company context to hand it, and an unresolvable
    company_id makes that form's own POST handler reject the submission
    outright ("Missing company."). Falls back to the mailto this task
    specifies instead."""
    signin_html = ""
    if show_signin:
        signin_html = (
            f'<p><a class="gg-link" href="{SIGNIN_URL}">'
            "Sign in via trades.graciagroup.com</a></p>"
        )
    sell_cta_html = ""
    if show_sell_cta:
        sell_cta_html = (
            f'<p>Have a block or shares to sell? '
            f'<a class="gg-link" href="{SELL_ORDER_MAILTO_URL}">Submit a sell order &rarr;</a></p>'
        )
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{_esc(title)}</title>
<style>
  body {{
    margin: 0;
    min-height: 100vh;
    display: flex;
    align-items: center;
    justify-content: center;
    background: #f4f2ee;
    color: #16181d;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
    padding: 24px;
  }}
  .gg-card {{
    max-width: 420px;
    text-align: center;
    background: #ffffff;
    border: 1px solid #e7e5e0;
    border-radius: 10px;
    padding: 32px 28px;
  }}
  .gg-card h1 {{ font-size: 18px; margin: 0 0 12px; }}
  .gg-card p {{ color: #6b7280; font-size: 14px; line-height: 1.5; margin: 0 0 8px; }}
  .gg-link {{ color: #3d5a73; text-decoration: none; font-weight: 600; }}
</style>
</head>
<body>
  <div class="gg-card">
    <h1>{_esc(title)}</h1>
    <p>{_esc(message)}</p>
    {sell_cta_html}
    {signin_html}
  </div>
</body>
</html>"""


def render_page(table, viewer_name, key=None, view_as=None, cef_html="", anon_key_email="admin",
                 tenant_picker=False):
    feature_box_html, feature_list_html = _feature_section_html(tenant_picker, anon_key_email, key=key,
                                                                   page="demand")
    rows_html = "".join(
        f'<tr><td class="company"><a href="{_company_href(r["company"], "demand", key, view_as)}">'
        f'{_esc(r["company"])}</a></td>'
        f'<td class="num">{r["total"]}</td>'
        f'<td class="num">{r["qp"]}</td>'
        f'<td class="num">{r["accredited"]}</td>'
        f'<td class="num">{r["unknown"]}</td>'
        f'<td class="num">{r["sellers"]}</td></tr>'
        for r in table
    )
    nav = _nav_html("demand", viewer_name, key=key, view_as=view_as, cef_html=cef_html)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Demand Board</title>
<style>
{NAV_CSS}
{FEATURE_CSS}
  :root {{
    --bg: #f4f2ee;
    --card: #ffffff;
    --line: #e7e5e0;
    --ink: #16181d;
    --muted: #6b7280;
    --accent: #3d5a73;
    --qp: #1f7a4d;
    --accredited: #8a6d1f;
    --unknown: #6b7280;
  }}
  .feature-section-heading {{ font-size: 16px; font-weight: 600; margin: 32px 0 12px; }}
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0;
    background: var(--bg);
    color: var(--ink);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
    padding: 32px 24px 64px;
  }}
  .wrap {{ max-width: 1000px; margin: 28px auto 0; }}
  .header-row {{
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 16px;
  }}
  h1 {{ font-size: 22px; font-weight: 600; margin: 0 0 4px; }}
  .sub {{ color: var(--muted); font-size: 13px; margin: 0 0 24px; }}
  .toolbar {{ display: flex; gap: 12px; margin-bottom: 16px; }}
  #search {{
    flex: 1;
    background: var(--card);
    border: 1px solid var(--line);
    color: var(--ink);
    padding: 10px 14px;
    border-radius: 8px;
    font-size: 14px;
    outline: none;
  }}
  #search:focus {{ border-color: var(--accent); }}
  .card {{
    background: var(--card);
    border: 1px solid var(--line);
    border-radius: 10px;
  }}
  table {{ width: 100%; border-collapse: collapse; }}
  /* Sticky header needs the card unclipped (overflow:hidden on an
     ancestor defeats position:sticky), so the rounded top corners are
     applied directly to the header cells instead of via .card overflow
     clipping. */
  thead th:first-child {{ border-top-left-radius: 10px; }}
  thead th:last-child {{ border-top-right-radius: 10px; }}
  tbody tr:last-child td:first-child {{ border-bottom-left-radius: 10px; }}
  tbody tr:last-child td:last-child {{ border-bottom-right-radius: 10px; }}
  thead th {{
    position: sticky;
    top: 0;
    z-index: 1;
    background: var(--card);
    text-align: left;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--muted);
    padding: 12px 16px;
    border-bottom: 1px solid var(--line);
    cursor: pointer;
    user-select: none;
    white-space: nowrap;
  }}
  thead th:hover {{ color: var(--ink); }}
  thead th.num, td.num {{ text-align: right; }}
  thead th .arrow {{ font-size: 10px; margin-left: 4px; color: var(--accent); }}
  tbody td {{
    padding: 11px 16px;
    border-bottom: 1px solid var(--line);
    font-size: 14px;
  }}
  tbody tr:last-child td {{ border-bottom: none; }}
  tbody tr:hover {{ background: rgba(22,24,29,0.03); }}
  td.company {{ font-weight: 500; }}
  td.company a {{ color: inherit; text-decoration: none; border-bottom: 1px solid var(--line); }}
  td.company a:hover {{ border-bottom-color: var(--muted); }}
  .legend {{ display: flex; gap: 16px; margin-top: 14px; font-size: 12px; color: var(--muted); }}
  .legend span {{ display: inline-flex; align-items: center; gap: 6px; }}
  .dot {{ width: 8px; height: 8px; border-radius: 50%; display: inline-block; }}
  .dot.qp {{ background: var(--qp); }}
  .dot.accredited {{ background: var(--accredited); }}
  .dot.unknown {{ background: var(--unknown); }}
  .empty {{ padding: 40px; text-align: center; color: var(--muted); }}
  .ei-msg {{ display: inline-block; font-size: 11px; margin-left: 6px; color: var(--muted); }}
  .ei-msg.saving {{ color: var(--muted); }}
  .ei-msg.saved {{ color: var(--qp); }}
  .ei-msg.error {{ color: #b23b3b; }}
  .gg-placeholder {{
    max-width: 1000px;
    margin: 96px auto;
    padding: 0 24px;
    text-align: center;
    color: var(--muted);
    font-size: 15px;
  }}
  .gg-placeholder.small {{ margin: 0; padding: 24px; }}
</style>
</head>
<body>
{nav}
<div class="wrap">
  <div class="header-row">
    <div>
      <h1>Demand Board</h1>
      <p class="sub">{len(table)} companies with interested buyers</p>
    </div>
  </div>
  {feature_box_html}
  <div class="toolbar">
    <input id="search" type="text" placeholder="Search companies...">
  </div>
  <div class="card">
    <table id="board">
      <thead>
        <tr>
          <th data-key="company" data-type="string">Company<span class="arrow"></span></th>
          <th class="num" data-key="total" data-type="number">Total buyers<span class="arrow"></span></th>
          <th class="num" data-key="qp" data-type="number">QP<span class="arrow"></span></th>
          <th class="num" data-key="accredited" data-type="number">Accredited<span class="arrow"></span></th>
          <th class="num" data-key="unknown" data-type="number">Unknown<span class="arrow"></span></th>
          <th class="num" data-key="sellers" data-type="number">Sellers<span class="arrow"></span></th>
        </tr>
      </thead>
      <tbody id="board-body">
        {rows_html}
      </tbody>
    </table>
    <div class="empty" id="empty-state" hidden>No matching companies.</div>
  </div>
  <div class="legend">
    <span><span class="dot qp"></span>QP &mdash; Investor Level = Qualified Purchaser</span>
    <span><span class="dot accredited"></span>Accredited &mdash; IQF Status Yes / Unnecessary</span>
    <span><span class="dot unknown"></span>Unknown &mdash; unclassified</span>
  </div>
  <h2 class="feature-section-heading">Feature requests</h2>
  <div class="card">
    {feature_list_html}
  </div>
</div>
<script>
(function() {{
  var tbody = document.getElementById('board-body');
  var rows = Array.prototype.slice.call(tbody.querySelectorAll('tr'));
  var search = document.getElementById('search');
  var emptyState = document.getElementById('empty-state');
  var headers = document.querySelectorAll('#board thead th');
  var sortState = {{ key: null, dir: 1 }};

  function cellText(row, colIndex) {{
    return row.children[colIndex].textContent.trim();
  }}

  function applyFilter() {{
    var q = search.value.trim().toLowerCase();
    var visibleCount = 0;
    rows.forEach(function(row) {{
      var name = cellText(row, 0).toLowerCase();
      var match = name.indexOf(q) !== -1;
      row.hidden = !match;
      if (match) visibleCount++;
    }});
    emptyState.hidden = visibleCount !== 0;
  }}

  function applySort(colIndex, key, type) {{
    var dir = (sortState.key === key) ? -sortState.dir : 1;
    sortState = {{ key: key, dir: dir }};
    headers.forEach(function(h) {{
      var arrow = h.querySelector('.arrow');
      if (!arrow) return;
      arrow.textContent = '';
    }});
    var activeHeader = headers[colIndex];
    var arrow = activeHeader.querySelector('.arrow');
    if (arrow) arrow.textContent = dir === 1 ? '\\u25B2' : '\\u25BC';

    rows.sort(function(a, b) {{
      var av = cellText(a, colIndex);
      var bv = cellText(b, colIndex);
      if (type === 'number') {{
        return (parseFloat(av) - parseFloat(bv)) * dir;
      }}
      return av.localeCompare(bv) * dir;
    }});
    rows.forEach(function(row) {{ tbody.appendChild(row); }});
  }}

  search.addEventListener('input', applyFilter);
  headers.forEach(function(h, idx) {{
    h.addEventListener('click', function() {{
      applySort(idx, h.getAttribute('data-key'), h.getAttribute('data-type'));
    }});
  }});
}})();
</script>
{_feature_toggle_script_html(key)}
</body>
</html>"""


def _esc(s):
    return (str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;"))


def _forbidden():
    return {
        "statusCode": 403,
        "headers": {"Content-Type": "text/plain"},
        "body": "403 Forbidden",
    }


def _html_response(body, status=200):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "text/html; charset=utf-8"},
        "body": body,
    }


def _json_response(data, status=200):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(data),
    }


def _parse_json_body(event):
    body = event.get("body") or ""
    if event.get("isBase64Encoded"):
        try:
            body = base64.b64decode(body).decode("utf-8", errors="replace")
        except Exception:
            return {}
    try:
        data = json.loads(body)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


MAX_INTRO_TEXT_LEN = 2000


def _handle_update_intro(event):
    """POST ?action=update_intro — the only write path in this Lambda.

    Two auth modes:
    - Admin: ADMIN_KEY present IN THE BODY (the session cookie alone never
      authorizes a write, even an admin's own) — full rights over status
      (all ten options via any of the three mechanisms below), next_steps,
      notes, follow_up, and deadline, on any deal, Closed rows included.
    - Tenant: no ADMIN_KEY, but a valid gg_id identity cookie naming an
      auto-enrolled tenant email (see _resolve_tenant) — rights to
      next_steps and follow_up always, PLUS status on a row that's
      already Introduced-or-later (disclosed) and NOT Closed (item 2 —
      a Closed row locks read-only for tenants across every status
      mechanism), PLUS notes (item 3, turn 20 — no longer admin-only)
      on any disclosed row, Closed included. deadline is still rejected
      outright with 403 for a tenant. Every tenant write is also scoped
      to a deal_id whose linked tenant (via person linkage,
      _tenant_email_for_deal) is that same authenticated tenant.

    Turn 17: status can be set three mutually exclusive ways (at most one
    per request):
    - milestone_step (one of CHECKBOX_MILESTONE_STEPS) + milestone_checked
      (bool) — a checkbox toggle. Adds/removes that one step in the
      Dynamo milestones map (add-only per step: checking an
      already-checked step or unchecking an already-unchecked one is a
      no-op on the map), then DERIVES the Pipeline status from the
      resulting checked set (_derive_status_from_checked — furthest
      checked, or Introduced if none). Tenant-allowed on any
      Introduced-or-later, non-Closed row; admin everywhere. On a Closed
      row an admin's checkbox edit still updates the milestones map (for
      retroactive record-keeping) but never re-derives/overwrites the
      Closed status itself — checkboxes cap out at Wired and must never
      demote a Closed deal by accident.
    - flag (none/stalled/passed/withdrawn/closed) — the compact flag
      select. Tenant: none/stalled/passed. Admin also: withdrawn/closed.
      NEVER touches the milestones map. "none" re-derives status from
      whatever's currently checked (same formula as a checkbox toggle,
      just without changing the map); the other four write their fixed
      status id directly, exactly like Setting Closed "writes status
      7207587 as today."
    - status (a literal status id) — unchanged from before this turn:
      the Company page's own admin Buyers-table dropdown
      (_intro_status_select_html) still posts this directly, and it
      still records a milestone via the OLD MILESTONE_STATUS_IDS path
      (including for Closed) since that page's write behavior is out of
      this turn's scope.

    Order: validate -> look up the deal and its owning tenant -> resolve
    the row's CURRENT status (needed for the tenant transition/Closed-
    lock check AND to derive from milestone_step/flag="none") -> if
    status or deadline is part of the write, PUT it to Pipeline first
    and abort the whole request (writing nothing to Dynamo) on any
    non-2xx -> write the Dynamo intro-item update (status_override/
    override_at, next_steps, notes, follow_up, deadline_override/
    deadline_override_at, milestones) -> append an audit item (actor =
    tenant email or "admin"). Never raises past this function; every
    failure mode returns a JSON error the UI can show."""
    body = _parse_json_body(event)

    admin_key = os.environ.get("ADMIN_KEY")
    is_admin = bool(admin_key) and body.get("key") == admin_key

    tenant_identity_email = None
    if not is_admin:
        identity_email = _read_identity_email(event)
        tenant_identity_email = identity_email.strip().lower() if identity_email else None
        if not tenant_identity_email or _resolve_tenant(tenant_identity_email) is None:
            return _json_response({"error": "forbidden"}, 403)
        if body.get("deadline") not in (None, ""):
            return _json_response({"error": "forbidden"}, 403)

    deal_id = str(body.get("deal_id") or "").strip()
    if not deal_id:
        return _json_response({"error": "deal_id is required"}, 400)

    milestone_step = body.get("milestone_step") or None
    flag = body.get("flag")
    if flag == "":
        flag = None
    raw_status = body.get("status")

    provided = sum([milestone_step is not None, flag is not None, raw_status not in (None, "")])
    if provided > 1:
        return _json_response({"error": "only one of status/milestone_step/flag per request"}, 400)

    if milestone_step is not None and milestone_step not in CHECKBOX_MILESTONE_STEPS:
        return _json_response({"error": "invalid milestone_step"}, 400)

    if flag is not None:
        if flag != "none" and flag not in FLAG_STATUS_IDS:
            return _json_response({"error": "invalid flag"}, 400)
        if flag in FLAG_ADMIN_ONLY_VALUES and not is_admin:
            return _json_response({"error": "forbidden"}, 403)

    # status_id is resolved here only for the literal "status" mechanism;
    # milestone_step and flag=="none" can only be derived once the deal's
    # current milestones are loaded further down (see "status derivation").
    status_id = None
    if raw_status not in (None, ""):
        try:
            status_id = int(raw_status)
        except (TypeError, ValueError):
            return _json_response({"error": "invalid status"}, 400)
        if status_id not in INTRO_STATUS_LABELS:
            return _json_response({"error": "invalid status"}, 400)
        if not is_admin and status_id not in TENANT_ALLOWED_STATUS_IDS:
            return _json_response({"error": "forbidden"}, 403)
    elif flag not in (None, "none"):
        status_id = FLAG_STATUS_IDS[flag]

    next_steps = body.get("next_steps")
    if next_steps is not None:
        next_steps = str(next_steps)
        if len(next_steps) > MAX_INTRO_TEXT_LEN:
            return _json_response({"error": "next_steps too long"}, 400)

    # Item 3 (turn 20): Notes is no longer admin-only -- it's the same
    # Dynamo "notes" attribute Active Intros' tenant-facing Notes column
    # now writes to directly (replacing next_steps there), shared with
    # the Company page's admin-only Buyer Notes column. Tenant rights
    # are still gated to a disclosed row -- see the disclosure check
    # below, once old_resolved is available.
    notes = body.get("notes")
    if notes is not None:
        notes = str(notes)
        if len(notes) > MAX_INTRO_TEXT_LEN:
            return _json_response({"error": "notes too long"}, 400)

    # Turn 22: Active Intros/My Deals no longer read or write follow_up
    # anywhere (column, date input, Due chip, overdue-first sort tier,
    # "follow-ups due" summary segment, and the Nudge-buyers chip
    # condition are all gone) -- the attribute and this write path stay
    # intact but dormant there, reserved for a future digest feature.
    # The Company page's Buyers table has its own separate Follow-up
    # column and is unaffected -- it still reads/writes this same field.
    follow_up = body.get("follow_up")
    if follow_up is not None:
        follow_up = str(follow_up).strip()
        if follow_up:
            try:
                datetime.strptime(follow_up, "%Y-%m-%d")
            except ValueError:
                return _json_response({"error": "invalid follow_up date"}, 400)

    deadline = body.get("deadline") if is_admin else None
    if deadline is not None:
        deadline = str(deadline).strip()
        if deadline:
            try:
                datetime.strptime(deadline, "%Y-%m-%d")
            except ValueError:
                return _json_response({"error": "invalid deadline date"}, 400)

    has_status_intent = status_id is not None or milestone_step is not None or flag is not None
    if not has_status_intent and next_steps is None and notes is None and follow_up is None and deadline is None:
        return _json_response({"error": "nothing to update"}, 400)

    deals = get_deals_list()
    deal = next((d for d in deals if str(d.get("id")) == deal_id), None)
    if deal is None:
        return _json_response({"error": "deal not found"}, 404)

    tenant_email = _tenant_email_for_deal(deal)
    if tenant_email is None:
        return _json_response({"error": "deal has no linked tenant"}, 400)

    if not is_admin and tenant_email != tenant_identity_email:
        return _json_response({"error": "forbidden"}, 403)

    intro_details, _ = get_intro_details(tenant_email)
    old_entry = intro_details.get(deal_id) or {}
    old_resolved = _resolve_intro_status(deal, old_entry)

    if has_status_intent and not is_admin:
        if not old_resolved["disclosed"]:
            return _json_response({"error": "forbidden"}, 403)
        if old_resolved["name"] == "Closed":
            # Item 2: a Closed row locks read-only for tenants across
            # every status mechanism -- checkbox, flag, and (defensively)
            # a literal status id too.
            return _json_response({"error": "forbidden"}, 403)

    if notes is not None and not is_admin and not old_resolved["disclosed"]:
        # Item 3 (turn 20): a tenant may only write Notes on a row
        # that's already Introduced-or-later -- same disclosure gate as
        # status, but NOT the Closed-lock above: Notes stays editable
        # after a deal closes (there's still reason to annotate it),
        # unlike the status controls.
        return _json_response({"error": "forbidden"}, 403)

    old_values = {
        "status": old_resolved["id"] if old_resolved["id"] is not None else old_resolved["name"],
        "next_steps": old_entry.get("next_steps"),
        "notes": old_entry.get("notes"),
        "follow_up": old_entry.get("follow_up"),
        "deadline": _resolve_deal_deadline(deal, old_entry),
    }

    milestones_update = None

    if milestone_step is not None:
        # Item 1 (turn 17): the checkbox toggle. Add/remove exactly the
        # one step requested -- never touch any other key already in the
        # map -- then derive the Pipeline status from the resulting
        # checked set. On a Closed row (admin only -- tenants were
        # already rejected above) the milestones map still updates, for
        # retroactive record-keeping, but status_id is forced back to
        # None so the derivation never demotes a Closed deal.
        stored = dict(old_entry.get("milestones") or {})
        milestone_checked = bool(body.get("milestone_checked"))
        if milestone_checked:
            stored.setdefault(milestone_step, int(time.time()))
        else:
            stored.pop(milestone_step, None)
        milestones_update = stored
        status_id = None if old_resolved["name"] == "Closed" else _derive_status_from_checked(set(stored.keys()))
    elif flag == "none":
        # Item 2: clearing the flag re-derives from whatever's currently
        # checked -- same formula as a checkbox toggle, milestones map
        # untouched (flags never touch it).
        stored_keys = set((old_entry.get("milestones") or {}).keys())
        status_id = _derive_status_from_checked(stored_keys)
    elif flag is None and status_id is not None and status_id in MILESTONE_STATUS_IDS:
        # Unchanged from before this turn: the Company page's own raw
        # status dropdown still records a milestone this way (add-only,
        # once) -- including for Closed, which the new flag control
        # deliberately does NOT do (see item 2's docstring above).
        # "flag is None" is what distinguishes this from a flag write:
        # status_id can be FLAG_STATUS_IDS[flag] too (e.g. "closed" ->
        # 7207587, which IS in MILESTONE_STATUS_IDS), and that path must
        # never fall through to here.
        step = MILESTONE_STATUS_IDS[status_id]
        old_milestones = old_entry.get("milestones") or {}
        if step not in old_milestones:
            milestones_update = dict(old_milestones)
            milestones_update[step] = int(time.time())

    if status_id is not None:
        ok, err = _pipeline_update_deal_status(deal_id, status_id)
        if not ok:
            return _json_response({"error": f"Pipeline update failed: {err}"}, 502)

    if deadline:
        ok, err = _pipeline_update_deal_deadline(deal_id, deadline)
        if not ok:
            return _json_response({"error": f"Pipeline update failed: {err}"}, 502)

    actor = "admin" if is_admin else tenant_identity_email
    ok, err = _dynamo_write_intro_update(tenant_email, deal_id, status_id, next_steps, notes, follow_up, deadline,
                                          old_values, actor, milestones=milestones_update)
    if not ok:
        return _json_response({"error": f"Save failed: {err}"}, 502)

    return _json_response({"ok": True})


NOT_ENABLED_MESSAGE = "This dashboard is for sellers."


def lambda_handler(event, context):
    method = (event.get("requestContext", {}).get("http", {}).get("method")
              or event.get("httpMethod") or "GET")
    query = event.get("queryStringParameters") or {}

    # The one write route: POST only. A GET here renders nothing and
    # changes nothing.
    if query.get("action") == "update_intro":
        if method != "POST":
            return _json_response({"error": "POST only"}, 405)
        return _handle_update_intro(event)

    if query.get("action") == "feature_request":
        if method != "POST":
            return _json_response({"error": "POST only"}, 405)
        return _handle_feature_request(event)

    if query.get("action") == "deal_stage":
        if method != "POST":
            return _json_response({"error": "POST only"}, 405)
        return _handle_deal_stage(event)

    if method != "GET":
        return _forbidden()

    admin_key = os.environ.get("ADMIN_KEY")
    is_admin_key = bool(admin_key) and query.get("key") == admin_key

    # SSO handoff: verify, set the durable identity cookie, redirect to a
    # clean URL. An invalid/expired token just falls through to normal
    # identity resolution (e.g. an existing cookie) rather than erroring.
    sso_token = query.get("sso")
    if sso_token:
        email = _verify_sso_handoff(sso_token)
        if email:
            location = event.get("rawPath") or "/"
            tab = query.get("tab")
            if tab:
                location += f"?tab={urllib.parse.quote(tab, safe='')}"
            return {
                "statusCode": 302,
                "headers": {"Location": location},
                "cookies": [_make_identity_cookie(email)],
                "body": "",
            }

    tab = query.get("tab") or "mydeals"
    if tab not in ("mydeals", "intros", "demand"):
        tab = "mydeals"

    # view_as is admin-only: never let a non-admin request steer whose view
    # they get.
    view_as = (query.get("view_as") or "").strip() if is_admin_key else ""

    tenant = None  # stays None for admin-without-view_as: the tenant-picker case
    # Company page Buyer Demand anonymization key: the viewing tenant's own
    # email (or the previewed tenant's, under &view_as — same identity
    # view_as already renders everything else as), falling back to the
    # literal "admin" only when there is no tenant context at all.
    anon_key_email = "admin"
    if is_admin_key:
        if view_as:
            tenant = _resolve_tenant(view_as)
            if not tenant:
                return _html_response(_message_page("Not enabled", NOT_ENABLED_MESSAGE, show_sell_cta=True))
            viewer_name = tenant["name"]
            anon_key_email = view_as.strip().lower()
        else:
            viewer_name = "Admin"
    else:
        identity_email = _read_identity_email(event)
        if not identity_email:
            return _html_response(_message_page(
                "Access denied",
                "Sign in to view the Demand Board.",
                show_signin=True,
            ), 403)
        tenant = _resolve_tenant(identity_email)
        if not tenant:
            return _html_response(_message_page("Not enabled", NOT_ENABLED_MESSAGE, show_sell_cta=True))
        viewer_name = tenant["name"]
        anon_key_email = identity_email.strip().lower()

    nav_key = query.get("key") if is_admin_key else None
    nav_view_as = view_as or None

    # Edit UI (Intro Status / Next Steps / Buyer Notes) is admin-only and,
    # when previewing a tenant via &view_as, opt-in via &edit=1 — without
    # it, &view_as previews exactly the tenant's read-only page. Never
    # True for a real tenant: is_admin_key is never True on their session.
    edit_mode = is_admin_key and (not view_as or query.get("edit") == "1")

    # CEF badge: the tenant's own Client Engagement Form state, shown on
    # every page's nav for the tenant view and the admin &view_as preview
    # alike (tenant is not None in both cases) — computed once here since
    # the nav is shared across every page below. Stays "" (admin with no
    # view_as) exactly when there's no tenant context to report on.
    cef_html = ""
    if tenant is not None:
        cef_html = _cef_badge_html(_tenant_cef_state(tenant.get("person_id")), tenant["name"])

    # Company detail page: same auth resolution as the tabs above, just a
    # different route param.
    company = query.get("company")
    if company:
        ref = query.get("ref") or "mydeals"
        if ref not in REF_LABELS:
            ref = "mydeals"
        body = render_company_page(company, viewer_name, tenant, anon_key_email, ref,
                                    key=nav_key, view_as=nav_view_as, edit_mode=edit_mode,
                                    cef_html=cef_html)
        return _html_response(body)

    # Buyer detail page (item 3, turn 18): same auth resolution as every
    # other route here, just a different route param.
    buyer_param = query.get("buyer")
    if buyer_param:
        body = render_buyer_page(buyer_param, viewer_name, tenant, anon_key_email,
                                  key=nav_key, view_as=nav_view_as, edit_mode=edit_mode, cef_html=cef_html)
        return _html_response(body)

    if tab == "demand":
        table = get_company_table()
        body = render_page(table, viewer_name, key=nav_key, view_as=nav_view_as, cef_html=cef_html,
                            anon_key_email=anon_key_email, tenant_picker=(tenant is None))
    elif tab == "mydeals":
        if tenant is None:
            body = render_my_deals_page(viewer_name, tenant_picker=True,
                                         key=nav_key, view_as=nav_view_as, cef_html=cef_html)
        else:
            person_id = tenant.get("person_id")
            # My Deals shows only Sell Order-tagged deals — the buy side
            # (Matched Buyers / Active Intros) and untouched ?company= pages
            # keep seeing exactly what they saw before this filter.
            deals = ([d for d in get_my_deals(person_id) if DEAL_SIDE_SELL_ID in _deal_cf_option_ids(d, DEAL_SIDE_FIELD)]
                     if person_id is not None else [])
            body = render_my_deals_page(viewer_name, deals=deals,
                                         key=nav_key, view_as=nav_view_as, cef_html=cef_html,
                                         edit_mode=edit_mode, person_id=person_id, anon_key_email=anon_key_email)
    else:
        body = render_intros_page(viewer_name, tenant=tenant, tenant_email=anon_key_email,
                                   key=nav_key, view_as=nav_view_as, edit_mode=edit_mode,
                                   cef_html=cef_html)
    return _html_response(body)
