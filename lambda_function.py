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
person_id (TENANTS[email]["person_id"]). Linkage is read the same way
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
    must be in TENANTS (below) or the tenant sees a "not enabled yet" page
    instead of the board. Neither door open -> access-denied page, no data
    rendered/fetched.
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


def _update_cancel_button_html(deal_id):
    url = _deal_update_form_url(deal_id)
    if not url:
        return ""
    return (f'<a class="update-cancel-btn" href="{url}" target="_blank" rel="noopener noreferrer">'
            'Update / Cancel</a>')


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
CEF_LABELS = {
    CEF_NO_ID: "No",
    CEF_PENDING_ID: "Pending",
    CEF_YES_ID: "Yes",
    CEF_NA_ID: "N/A",
}

# "Matched or later" buy-side stages, shared verbatim by the Matched Buyers
# section and the Active Intros tab: every known stage id (STAGE_LABELS,
# above) except Inquiry and Hold, per instruction to include
# matched/firm/transfer-notice/SPA-style stages and exclude
# inquiry/hold/lost/dead. Confirm/LOI Signed sit later than Matched in this
# org's own stage progression (see chadgracia/daily-brief's
# TO_CLOSE_ALL_STAGES / TO_CLOSE_AGED_STAGE ordering), so they're included
# too. Deliberately NOT excluding is_archived here (unlike the Sellers
# column) — an archived deal can still carry a real Intro Status value
# (including an explicit "Closed", id 7207587) and needs to stay visible
# to show it. An archived deal with no Intro Status value set derives
# "Matched" like any other empty one (see _default_intro_status) and
# lands in Pending introductions, not "Closed" — is_archived is never
# treated as a status signal.
MATCHED_OR_LATER_STAGE_IDS = {
    STAGE_MATCHED, STAGE_FIRM, STAGE_CONFIRM,
    STAGE_LOI_SIGNED, STAGE_TRANSFER_NOTICE, STAGE_SPA_SIGNED,
}

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
INTRO_STATUS_STALLED_ID = 7207584
INTRO_STATUS_PASSED_ID = 7207585
INTRO_STATUS_WITHDRAWN_ID = 7207586
EXIT_STATUS_IDS = {INTRO_STATUS_STALLED_ID, INTRO_STATUS_PASSED_ID, INTRO_STATUS_WITHDRAWN_ID}

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

# Tenants enabled for this dashboard, keyed by lowercase email. Edited by
# hand — no other code changes required. person_id is that tenant's
# Pipeline CRM person id, used to match them against deals.json's people
# linkage for the My Deals tab — carry it for every entry going forward.
TENANTS = {
    "michael@nonpublic.io": {"name": "Michael Ferkol (NonPublic)", "person_id": 1307955474},
}

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
    one state per CEF option: Yes -> green "CEF on file"; Pending ->
    amber "CEF pending"; No or unset (cef_option_id is None or any id
    outside the verified map) -> red "CEF missing — contact us" (mailto,
    subject "CEF for <tenant name>"); N/A -> no badge at all. Only ever
    called when there IS a tenant to report on (see the call site's
    tenant is not None guard) — there's no separate "no tenant" case to
    handle here."""
    if cef_option_id == CEF_NA_ID:
        return ""
    if cef_option_id == CEF_YES_ID:
        return '<div class="gg-cef-badge cef-ok">&#10003; CEF on file</div>'
    if cef_option_id == CEF_PENDING_ID:
        return '<div class="gg-cef-badge cef-pending">&#8226; CEF pending</div>'
    subject = urllib.parse.quote(f"CEF for {tenant_name}", safe="")
    href = f"mailto:{FEATURE_REQUEST_EMAIL}?subject={subject}"
    return f'<a class="gg-cef-badge cef-missing" href="{href}">&#10007; CEF missing — contact us</a>'


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
    "override_at"}} for every intro item under this tenant, via a single
    Query on the syndicate-dash table (never one GetItem per deal). Never
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
                "follow_up": item.get("follow_up"),
                "status_override": item.get("status_override"),
                "override_at": item.get("override_at"),
                "deadline_override": item.get("deadline_override"),
                "deadline_override_at": item.get("deadline_override_at"),
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


def get_feature_requests(tenant_partition):
    """{"open": [...], "done": [...]} feature-request items for one Dynamo
    partition (sk begins_with "feature#"), each item newest-first within
    its bucket (the sk's own epoch-ms suffix sorts correctly as a string).
    Never raises: any failure returns ({"open": [], "done": []}, True)."""
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
            items.append({
                "sk": sk,
                "tenant": tenant_partition,
                "text": item.get("text") or "",
                "submitted_by": item.get("submitted_by"),
                "created_at": item.get("created_at"),
                "done": bool(item.get("done")),
                "done_by": item.get("done_by"),
                "done_at": item.get("done_at"),
            })
        items.sort(key=lambda it: it["sk"], reverse=True)
        return {"open": [it for it in items if not it["done"]],
                "done": [it for it in items if it["done"]]}, False
    except Exception:
        return {"open": [], "done": []}, True


def _dynamo_write_feature_request(tenant_partition, text, actor):
    try:
        table = _dynamo_table()
        table.put_item(Item={
            "tenant": tenant_partition,
            "sk": f"feature#{int(time.time() * 1000)}",
            "text": text,
            "submitted_by": actor,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "done": False,
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
    a TENANTS email) — that's how the admin aggregate view (many
    partitions on one page) targets a specific one. A tenant's own
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
            if partition not in TENANTS:
                return _json_response({"error": "invalid tenant"}, 400)
        actor = "admin"
    else:
        identity_email = _read_identity_email(event)
        tenant_identity_email = identity_email.strip().lower() if identity_email else None
        if not tenant_identity_email or tenant_identity_email not in TENANTS:
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

    ok, err = _dynamo_write_feature_request(partition, text, actor)
    if not ok:
        return _json_response({"error": f"Save failed: {err}"}, 502)
    return _json_response({"ok": True})


def _fmt_feature_date(iso_str):
    dt = _parse_dt(iso_str)
    return dt.strftime("%b %-d, %Y") if dt else None


def _feature_box_html(partition, key=None):
    """Submit box: "same auto-save fetch pattern" as the rest of the app
    (Saving…/Saved ✓/error via one shared inline .ei-msg), just triggered
    by a Submit button/Enter instead of blur, since this creates a new
    item rather than editing an existing field. Dismissable per page load
    only (box.hidden, no persistence) — reappears on the next load, by
    design. Reloads the page on success so the new item shows up in the
    list below without needing separate DOM-insertion logic."""
    key_json = json.dumps(key or "")
    tenant_json = json.dumps(partition)
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
      body: JSON.stringify({{ key: KEY, tenant: TENANT, text: text }})
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


def _feature_request_row_html(item, show_tenant=False, tenant_name=None):
    text_html = _esc(item["text"])
    tenant_tag = (f'<span class="feature-tenant-tag">{_esc(tenant_name)}</span> '
                  if show_tenant and tenant_name else "")
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
            f'<div class="feature-row-text">{tenant_tag}{text_html}</div>'
            f'<div class="feature-row-meta">{meta}&nbsp;&nbsp;{toggle_btn}<span class="ei-msg"></span></div>'
            f'</div>')


def _feature_requests_list_html(open_items, done_items, show_tenant=False, tenant_names=None):
    if not open_items and not done_items:
        return '<div class="gg-placeholder small">No feature requests yet.</div>'
    names = tenant_names or {}
    rows = [_feature_request_row_html(it, show_tenant=show_tenant, tenant_name=names.get(it["tenant"]))
            for it in open_items]
    rows += [_feature_request_row_html(it, show_tenant=show_tenant, tenant_name=names.get(it["tenant"]))
             for it in done_items]
    return "".join(rows)


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
    explicit or derived Matched. That covers the six named
    Introduced-through-Closed ids and the three exit ids (Stalled/Passed/
    Withdrawn) — none of those are "Matched" either. An empty/absent
    field always derives "Matched" (see _default_intro_status) — there is
    no other derived value, so disclosed is False whenever the field is
    genuinely unset, regardless of any other deal attribute (is_archived
    included). The instruction enumerated the six Introduced-through-
    Closed ids explicitly and didn't say either way for the exit ids;
    "not Matched" is what makes "Stalled stays, flagged" (a NAMED row)
    consistent with a pending row's fixed "status: Matched" without the
    two contradicting each other. Flag for confirmation if a deal marked
    Stalled/Passed/Withdrawn before ever being Introduced should actually
    still be anonymized."""
    status_id = _deal_intro_status_id(deal)

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
    return {
        "id": status_id,
        "name": name,
        "is_exit": status_id in EXIT_STATUS_IDS,
        "disclosed": name != "Matched",
    }


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
    """The TENANTS email whose person_id is linked to this deal, or None
    if no tenant maps to it — writes are rejected outright in that case."""
    linked = _deal_linked_person_ids(deal)
    for email, info in TENANTS.items():
        if info.get("person_id") in linked:
            return email
    return None


def _dynamo_write_intro_update(tenant_email, deal_id, status_id, next_steps, notes, follow_up, deadline,
                                old_values, actor):
    """Update the intro item's Dynamo-owned attributes (status_override/
    override_at when status_id is not None, next_steps/notes/follow_up
    when they are not None, deadline_override/deadline_override_at when
    deadline is not None) and append an audit item
    (sk=audit#<deal_id>#<epoch_ms>, actor "admin" or the tenant's own
    email, old and new values). Returns (ok, error_message). Never called
    when a requested Pipeline write failed — see _handle_update_intro."""
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
    if next_steps is not None:
        update_parts.append("next_steps = :ns")
        expr_values[":ns"] = next_steps
        new_values["next_steps"] = next_steps
    if notes is not None:
        update_parts.append("notes = :no")
        expr_values[":no"] = notes
        new_values["notes"] = notes
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


def _my_deal_size_text(deal):
    """"$1M – $5M" from min/max ticket size (TICKET_MIN_FIELD/
    TICKET_MAX_FIELD); a single value when only one of the two is set;
    falls back to the deal's own "value" field, same as _deal_size_text,
    when neither is set."""
    min_val = _deal_cf_number(deal, TICKET_MIN_FIELD)
    max_val = _deal_cf_number(deal, TICKET_MAX_FIELD)
    if min_val is not None and max_val is not None:
        return f"{_fmt_money(min_val)} – {_fmt_money(max_val)}"
    if max_val is not None:
        return _fmt_money(max_val)
    if min_val is not None:
        return _fmt_money(min_val)
    try:
        return _fmt_money(float(deal.get("value")))
    except (TypeError, ValueError):
        return _fmt_money(None)


def _my_deal_visibility_state(deal, cef_state):
    """"live" (Agent Agreement Yes), "setup" (In Process, or the tenant's
    own CEF is Yes), or "not_live" — single source of truth shared by the
    My Deals visibility badge and its summary-strip counts."""
    opts = _deal_cf_option_ids(deal, AGENT_AGREEMENT_FIELD)
    if opts & AGENT_ENGAGED_OPTS:
        return "live"
    if (opts & AGENT_IN_PROCESS_OPTS) or cef_state == CEF_YES_ID:
        return "setup"
    return "not_live"


def _my_deal_visibility_badge_html(deal, company, cef_state):
    state = _my_deal_visibility_state(deal, cef_state)
    if state == "live":
        return '<span class="visibility-badge live">Live — shown to buyers</span>'
    if state == "setup":
        return '<span class="visibility-badge setup">Setup in progress</span>'
    subject = urllib.parse.quote(f"Engage RMS re {company}", safe="")
    href = f"mailto:{FEATURE_REQUEST_EMAIL}?subject={subject}"
    return (f'<a class="visibility-badge not-live" href="{href}">'
            'Not live — engage to activate</a>')


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


def _buyer_name_cell_html(buyer_recs, show_contact):
    """Buyer name(s), plus a contact second line (email/phone) when
    show_contact is True. When False, no contact info is emitted anywhere
    in the HTML — not hidden via CSS, simply never written. Shared by the
    Matched Buyers table and Active Intros rows so the gating rule can't
    drift between them.

    won_deals_total (for the green "has closed with Rainmaker" dot) does
    not appear anywhere in portfolio-deploy, deal-notifier, loi-sign,
    web-bid, trades, or daily-brief — no code anywhere reads such a field
    off a person record — so the dot is never rendered here."""
    if not buyer_recs:
        return "—"
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
        tier_label = TIER_LABELS.get(tier, "Unknown")
        min_v, max_v = get_person_ticket_range(cf)
        range_text = _fmt_ticket_range(min_v, max_v)
        range_html = f'<div class="buyer-range">{_esc(range_text)}</div>' if range_text else ""
        blocks.append(
            f'<div class="pending-buyer">'
            f'<span class="buyer-code">Buyer {_esc(code)}</span> '
            f'<span class="tier-badge tier-{tier}">{_esc(tier_label)}</span>'
            f'{range_html}</div>'
        )
    return "".join(blocks)


def _matched_buyer_row_html(deal, tenant_person_id, people_by_id, intro_details, anon_key_email, editable=False):
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

    name_cell = _buyer_name_cell_html(buyer_recs, show_contact=True)
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


def _intro_status_select_html(deal_id, current_id):
    options = []
    for oid, label in INTRO_STATUS_LABELS.items():
        selected = " selected" if oid == current_id else ""
        options.append(f'<option value="{oid}"{selected}>{_esc(label)}</option>')
    return (f'<select class="ei-status" data-deal-id="{_esc(deal_id)}">'
            f'{"".join(options)}</select><span class="ei-msg"></span>')


def _ei_field_html(css_class, deal_id, field, value):
    """An auto-saving text input: saved on blur or Enter (see
    _edit_script_html), with its own inline .ei-msg indicator right
    beside it — no Save button anywhere."""
    return (f'<input type="text" class="{css_class}" data-deal-id="{_esc(deal_id)}" '
            f'data-field="{field}" maxlength="2000" value="{value}">'
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
    """"Sep 20" style, no year — for the read-only tenant-facing note and
    the edit-mode Due chip's tooltip. None if unset/unparsable."""
    dt = _parse_dt(follow_up)
    return dt.strftime("%b %-d") if dt else None


def _follow_up_is_due(follow_up):
    """True when follow_up is a real date that is today or earlier (UTC
    calendar date, no time-of-day component since follow_up is stored as
    a bare ISO date string)."""
    dt = _parse_dt(follow_up)
    if not dt:
        return False
    return dt.date() <= datetime.now(timezone.utc).date()


def _due_chip_html():
    return '<span class="due-chip" title="Follow-up due">Due</span>'


def _edit_script_html(key):
    """Plain HTML+fetch(), no frameworks, no Save button: the Status
    dropdown posts on change; Next Steps / Follow-up / Buyer Notes post
    on blur or Enter (Enter just blurs, so there's one save path, not
    two). Each
    control's own .ei-msg (its very next sibling) shows "Saving…", then
    either "Saved ✓" (fades after 2s) or the returned error text in red
    (stays). One shared script, included on both Active Intros and the
    Buyers table when edit_mode is on."""
    admin_key_json = json.dumps(key or "")
    return f"""<script>
(function() {{
  var ADMIN_KEY = {admin_key_json};

  function saveField(el, field) {{
    var dealId = el.getAttribute('data-deal-id');
    var msgEl = el.nextElementSibling;
    var payload = {{ key: ADMIN_KEY, deal_id: dealId }};
    payload[field] = el.value;
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

  document.querySelectorAll('.ei-status').forEach(function(el) {{
    el.addEventListener('change', function() {{ saveField(el, 'status'); }});
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


def _matched_buyer_row_edit_html(deal, tenant_person_id, people_by_id, intro_details):
    """Admin edit-mode row: always the real buyer(s), regardless of
    disclosure — the disclosure gate is a tenant-facing privacy rule, not
    something that should blind the admin managing the pipeline. Status
    always reflects the TRUE current resolved status (Matched included),
    editable via a dropdown of all ten options."""
    deal_id = str(deal.get("id"))
    entry = intro_details.get(deal_id) or {}
    resolved = _resolve_intro_status(deal, entry)

    linked = _deal_linked_person_ids(deal) - {tenant_person_id}
    buyer_recs = [people_by_id[pid] for pid in linked if pid in people_by_id]
    name_cell = _buyer_name_cell_html(buyer_recs, show_contact=True)
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
    background: #16181d;
    border-bottom: 1px solid rgba(244,242,238,0.12);
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
    color: #f4f2ee;
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
    color: #9aa0ac;
    text-decoration: none;
    font-size: 13px;
    font-weight: 600;
    padding: 8px 14px;
    border-radius: 6px;
  }
  .gg-tab:hover { color: #f4f2ee; }
  .gg-tab.active {
    color: #f4f2ee;
    background: rgba(244,242,238,0.08);
    box-shadow: inset 0 -2px 0 #1f7a4d;
  }
  .gg-viewer {
    color: #9aa0ac;
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
  .gg-cef-badge.cef-ok { background: rgba(46,157,106,0.15); color: #2e9d6a; }
  .gg-cef-badge.cef-pending { background: rgba(201,162,39,0.15); color: #c9a227; }
  .gg-cef-badge.cef-missing { background: rgba(220,80,80,0.15); color: #e06666; }
  .gg-cef-badge.cef-missing:hover { text-decoration: underline; }
"""

# Feature-request submit box + list, shared verbatim by render_my_deals_page
# and render_company_page (both embed this via {FEATURE_CSS} the same way
# every page already embeds {NAV_CSS}) so the two pages' boxes/lists never
# drift from each other.
FEATURE_CSS = """
  .feature-box {
    position: relative;
    background: rgba(79,140,255,0.12);
    border: 1px solid rgba(79,140,255,0.35);
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


def _intro_row_html(deal, resolved, people_by_id, tenant_person_id, follow_up=None, key=None, view_as=None,
                     editable=False, next_steps=None, notes=None):
    """editable=True (tenant_edit_mode — see render_intros_page) adds
    Next Steps / Follow-up / Buyer Notes columns matching the admin edit
    row's layout: Next Steps and Follow-up as the same auto-saving inputs
    admin edit mode uses, Buyer Notes as read-only text — tenants never
    get to edit Status or Buyer Notes."""
    company_name = _deal_company_name(deal)
    if company_name:
        company_cell = (f'<a href="{_company_href(company_name, "intros", key, view_as)}">'
                        f'{_esc(company_name)}</a>')
    else:
        company_cell = "—"
    name = _esc(_deal_title(deal))

    linked = _deal_linked_person_ids(deal) - {tenant_person_id}
    buyer_recs = [people_by_id[pid] for pid in linked if pid in people_by_id]

    name_cell = _buyer_name_cell_html(buyer_recs, show_contact=True)
    name_cell += _buyer_contact_detail_html(buyer_recs)
    investor_type, _company_text = _investor_type_and_company(buyer_recs, disclosed=True)

    size_text = _esc(_deal_size_text(deal))
    status_html = _status_display_html(resolved, compact=False)
    if _follow_up_is_due(follow_up):
        status_html += _due_chip_html()

    extra_html = ""
    if editable:
        deal_id = str(deal.get("id"))
        next_steps_html = _ei_field_html("ei-next-steps", deal_id, "next_steps", _esc(next_steps or ""))
        follow_up_html = _ei_date_field_html(deal_id, _esc(follow_up or ""))
        extra_html = f'<td>{next_steps_html}</td><td>{follow_up_html}</td><td>{_esc(notes or "")}</td>'

    return (
        f'<tr><td class="company">{company_cell}</td>'
        f'<td>{name}</td>'
        f'<td>{name_cell}</td>'
        f'<td>{_esc(investor_type)}</td>'
        f'<td class="num">{size_text}</td>'
        f'<td>{status_html}</td>{extra_html}</tr>'
    )


def _pending_intro_row_html(deal, buyer_recs, anon_key_email, follow_up=None, key=None, view_as=None,
                             editable=False):
    """editable pads the row with three blank cells (Next Steps /
    Follow-up / Buyer Notes) to match the 9-column editable header —
    pending rows never get inputs, disclosed or not."""
    company_name = _deal_company_name(deal)
    company_cell = (f'<a href="{_company_href(company_name, "intros", key, view_as)}">'
                    f'{_esc(company_name)}</a>') if company_name else "—"
    name = _esc(_deal_title(deal))
    buyer_cell = _pending_buyer_cell_html(buyer_recs, anon_key_email)
    size_text = _esc(_deal_size_text(deal))
    status_html = _status_pill_html("Matched")
    if _follow_up_is_due(follow_up):
        status_html += _due_chip_html()

    extra_html = "<td></td><td></td><td></td>" if editable else ""

    return (
        f'<tr class="pending-row"><td class="company">{company_cell}</td>'
        f'<td>{name}</td>'
        f'<td>{buyer_cell}</td>'
        f'<td></td>'
        f'<td class="num">{size_text}</td>'
        f'<td>{status_html}</td>{extra_html}</tr>'
    )


def _intro_row_edit_html(deal, people_by_id, tenant_person_id, intro_details, key=None, view_as=None):
    """Admin edit-mode row for Active Intros: always the real buyer(s),
    always the true current status (Matched included) via a dropdown of
    all ten options, plus auto-saving Next Steps / Buyer Notes inputs —
    mirrors _matched_buyer_row_edit_html's rationale exactly."""
    deal_id = str(deal.get("id"))
    entry = intro_details.get(deal_id) or {}
    resolved = _resolve_intro_status(deal, entry)

    company_name = _deal_company_name(deal)
    company_cell = (f'<a href="{_company_href(company_name, "intros", key, view_as)}">'
                    f'{_esc(company_name)}</a>') if company_name else "—"
    name = _esc(_deal_title(deal))

    linked = _deal_linked_person_ids(deal) - {tenant_person_id}
    buyer_recs = [people_by_id[pid] for pid in linked if pid in people_by_id]
    name_cell = _buyer_name_cell_html(buyer_recs, show_contact=True)
    if resolved["disclosed"]:
        name_cell += _buyer_contact_detail_html(buyer_recs)
    investor_type, _company_text = _investor_type_and_company(buyer_recs, disclosed=True)

    size_text = _esc(_deal_size_text(deal))
    select_html = _intro_status_select_html(deal_id, resolved["id"])
    if _follow_up_is_due(entry.get("follow_up")):
        select_html += _due_chip_html()
    next_steps_html = _ei_field_html("ei-next-steps", deal_id, "next_steps", _esc(entry.get("next_steps") or ""))
    follow_up_html = _ei_date_field_html(deal_id, _esc(entry.get("follow_up") or ""))
    notes_html = _ei_field_html("ei-notes", deal_id, "notes", _esc(entry.get("notes") or ""))

    return (
        f'<tr><td class="company">{company_cell}</td>'
        f'<td>{name}</td>'
        f'<td>{name_cell}</td>'
        f'<td>{_esc(investor_type)}</td>'
        f'<td class="num">{size_text}</td>'
        f'<td>{select_html}</td>'
        f'<td>{next_steps_html}</td>'
        f'<td>{follow_up_html}</td>'
        f'<td>{notes_html}</td></tr>'
    )


def render_intros_page(viewer_name, tenant=None, tenant_email=None, key=None, view_as=None, edit_mode=False,
                        cef_html=""):
    """tenant is None only for admin-without-view_as — the same
    tenant-picker signal render_my_deals_page uses. tenant_email is always
    a real email otherwise (the logged-in tenant's own, or the previewed
    tenant's under &view_as), used for the pending rows' anon buyer codes
    and as the Dynamo partition key for status overrides / edit data.

    edit_mode is only ever True for a valid ADMIN_KEY (see lambda_handler)
    — never for a real tenant."""
    nav = _nav_html("intros", viewer_name, key=key, view_as=view_as, edit_flag=edit_mode, cef_html=cef_html)

    if tenant is None:
        body_html = (
            '<div class="gg-placeholder">Pick a tenant to preview — '
            'add &amp;view_as=&lt;email&gt; to the URL.</div>'
        )
    else:
        person_id = tenant.get("person_id")
        deals = get_my_matched_buy_deals(person_id) if person_id is not None else []

        intro_details, dynamo_failed = get_intro_details(tenant_email) if deals else ({}, False)

        resolved_by_deal_id = {}
        kept_deals = []
        for d in deals:
            resolved = _resolve_intro_status(d, intro_details.get(str(d.get("id"))))
            if resolved["name"] in ("Passed", "Withdrawn"):
                continue
            resolved_by_deal_id[str(d.get("id"))] = resolved
            kept_deals.append(d)

        wanted_ids = set()
        for d in kept_deals:
            wanted_ids |= _deal_linked_person_ids(d) - {person_id}
        people_by_id = get_people_by_ids(wanted_ids) if wanted_ids else {}

        note_html = ('<p class="gg-note">Status overrides unavailable — showing Pipeline values.</p>'
                     if dynamo_failed else "")

        main_rows, pending_rows = [], []
        for d in kept_deals:
            resolved = resolved_by_deal_id[str(d.get("id"))]
            (main_rows if resolved["disclosed"] else pending_rows).append((d, resolved))

        def _due_key(d):
            follow_up = (intro_details.get(str(d.get("id"))) or {}).get("follow_up")
            return 0 if _follow_up_is_due(follow_up) else 1

        main_rows.sort(key=lambda dr: (_due_key(dr[0]), -_intro_sort_rank(dr[1]),
                                        (_deal_company_name(dr[0]) or "").lower()))
        pending_rows.sort(key=lambda dr: (_due_key(dr[0]), (_deal_company_name(dr[0]) or "").lower()))

        # tenant_edit_mode: mirrors render_company_page's — the tenant (real
        # session, or admin &view_as preview without &edit=1) can auto-save
        # Next Steps / Follow-up on their own Introduced-or-later rows here
        # too, per the task header's "(and Active Intros where noted)".
        tenant_edit_mode = tenant is not None and not edit_mode

        if edit_mode or tenant_edit_mode:
            colspan = 9
            head_row = ('<th>Company</th><th>Deal</th><th>Buyer name(s)</th><th>Investor Type</th>'
                        '<th class="num">Size</th><th>Status</th>'
                        '<th>Next Steps</th><th>Follow-up</th><th>Buyer Notes</th>')
        else:
            colspan = 6
            head_row = ('<th>Company</th><th>Deal</th><th>Buyer name(s)</th><th>Investor Type</th>'
                        '<th class="num">Size</th><th>Status</th>')

        # "Introduced" always renders — even with zero rows — per instruction;
        # "Pending introductions" only when there's something pending.
        parts = [_group_header_row_html("Introduced", colspan)]
        if main_rows:
            if edit_mode:
                parts += [_intro_row_edit_html(d, people_by_id, person_id, intro_details, key=key, view_as=view_as)
                          for d, _ in main_rows]
            else:
                parts += [_intro_row_html(d, resolved, people_by_id, person_id,
                                           follow_up=(intro_details.get(str(d.get("id"))) or {}).get("follow_up"),
                                           key=key, view_as=view_as, editable=tenant_edit_mode,
                                           next_steps=(intro_details.get(str(d.get("id"))) or {}).get("next_steps"),
                                           notes=(intro_details.get(str(d.get("id"))) or {}).get("notes"))
                          for d, resolved in main_rows]
        else:
            parts.append(_group_empty_row_html("No introductions yet on this deal.", colspan))

        if pending_rows:
            parts.append(_group_header_row_html("Pending introductions", colspan))
            if edit_mode:
                # Edit mode never anonymizes — the admin sees and edits the
                # real buyer regardless of pending/disclosed.
                parts += [_intro_row_edit_html(d, people_by_id, person_id, intro_details, key=key, view_as=view_as)
                          for d, _ in pending_rows]
            else:
                # Pending rows never get inputs even under tenant_edit_mode —
                # only Introduced-or-later rows are editable.
                for d, resolved in pending_rows:
                    linked = _deal_linked_person_ids(d) - {person_id}
                    buyer_recs = [people_by_id[pid] for pid in linked if pid in people_by_id]
                    follow_up = (intro_details.get(str(d.get("id"))) or {}).get("follow_up")
                    parts.append(_pending_intro_row_html(d, buyer_recs, tenant_email, follow_up=follow_up,
                                                          key=key, view_as=view_as, editable=tenant_edit_mode))

        rows_html = "".join(parts)
        table_html = f"""<div class="card">
      <div class="table-scroll">
      <table>
        <thead>
          <tr>
            {head_row}
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
      </div>
    </div>"""

        edit_script = _edit_script_html(key) if (edit_mode or tenant_edit_mode) else ""
        body_html = f"{note_html}{table_html}{edit_script}"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Active Intros</title>
<style>
  * {{ box-sizing: border-box; }}
  :root {{
    --bg: #14161a;
    --card: #1c1f26;
    --line: #2a2e37;
    --ink: #e8eaed;
    --muted: #9aa0ac;
    --accent: #4f8cff;
    --qp: #2e9d6a;
  }}
  body {{
    margin: 0;
    background: var(--bg);
    color: var(--ink);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
    padding: 32px 24px 64px;
  }}
{NAV_CSS}
  .wrap {{ max-width: 1000px; margin: 0 auto; }}
  h1 {{ font-size: 22px; font-weight: 600; margin: 0 0 24px; }}
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
  .status-strip {{ display: flex; align-items: center; gap: 3px; white-space: nowrap; }}
  .status-step {{
    width: 14px;
    height: 6px;
    border-radius: 3px;
    background: var(--line);
    flex: 0 0 auto;
  }}
  .status-step.done {{ background: var(--qp); }}
  .status-step.current {{ background: var(--accent); }}
  .status-label {{ font-size: 12px; color: var(--muted); margin-left: 6px; }}
  .status-pill {{
    display: inline-block;
    font-size: 11px;
    font-weight: 600;
    color: var(--muted);
    background: rgba(255,255,255,0.06);
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
  .status-chip.stalled {{ background: rgba(201,162,39,0.15); color: #c9a227; }}
  .status-chip.exit {{ background: rgba(255,255,255,0.06); color: var(--muted); }}
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
    color: #14161a;
  }}
  .tier-badge.tier-qp {{ background: var(--qp); }}
  .tier-badge.tier-accredited {{ background: #c9a227; }}
  .tier-badge.tier-unknown {{ background: var(--muted); }}
  tr.pending-row {{ opacity: 0.85; }}
  tr.group-divider td {{
    padding: 8px 16px;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--muted);
    background: rgba(255,255,255,0.02);
    border-bottom: 1px solid var(--line);
  }}
  td.group-empty {{
    padding: 16px;
    color: var(--muted);
    font-size: 13px;
    font-style: italic;
  }}
  .table-scroll {{ overflow-x: auto; }}
  .gg-note {{
    color: var(--accredited, #c9a227);
    font-size: 13px;
    margin: 0 0 16px;
  }}
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
  .ei-msg.error {{ color: #e06666; }}
  .buyer-detail {{ font-size: 12px; color: var(--muted); margin-top: 2px; }}
  .buyer-detail a {{ color: var(--accent); text-decoration: none; }}
  .buyer-detail a:hover {{ text-decoration: underline; }}
  .due-chip {{
    display: inline-block;
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    padding: 2px 7px;
    border-radius: 999px;
    margin-left: 6px;
    background: rgba(220,80,80,0.15);
    color: #e06666;
  }}
</style>
</head>
<body>
{nav}
<div class="wrap">
  <h1>Active Intros</h1>
  {body_html}
</div>
</body>
</html>"""


def _my_deal_row_html(deal, company_name, deadline, stats, buyer_count, cef_state, key=None, view_as=None,
                       edit_mode=False):
    deal_id = str(deal.get("id"))
    if company_name:
        company_cell = (f'<a href="{_company_href(company_name, "mydeals", key, view_as)}">'
                         f'{_esc(company_name)}</a>')
    else:
        company_cell = "—"

    public_url = _deal_public_url(deal_id)
    deal_id_cell = (
        f'<a href="{public_url}" target="_blank" rel="noopener noreferrer">#{deal_id}</a>'
        f'<button type="button" class="copy-link-btn" data-copy-url="{public_url}" '
        f'title="Copy link" aria-label="Copy deal link">&#128203;</button>'
    )

    size_text = _esc(_my_deal_size_text(deal))
    badge_html = _my_deal_visibility_badge_html(deal, company_name or "", cef_state)

    buyer_text = str(buyer_count)
    intro_text = str(stats["intro_count"]) if stats["intro_count"] else "—"

    is_overdue = False
    if deadline:
        deadline_dt = _parse_dt(deadline)
        is_overdue = bool(deadline_dt and deadline_dt.date() < datetime.now(timezone.utc).date())

    if edit_mode:
        deadline_css = "ei-deadline overdue-input" if is_overdue else "ei-deadline"
        deadline_html = _ei_date_field_html(deal_id, deadline or "", css_class=deadline_css, field="deadline")
    elif deadline:
        deadline_cls = ' class="deadline-overdue"' if is_overdue else ""
        deadline_html = f'<span{deadline_cls}>{_esc(deadline)}</span>'
    else:
        deadline_html = "—"

    reasons = []
    if is_overdue:
        reasons.append("Deadline passed")
    if stats["stalled"]:
        reasons.append("A buyer is Stalled")
    if stats["follow_up_due"]:
        reasons.append("Follow-up due")
    attention_html = ""
    if reasons:
        tooltip = _esc(" · ".join(reasons))
        attention_html = f'<span class="attention-chip" title="{tooltip}">Needs attention</span>'

    update_btn = _update_cancel_button_html(deal_id)

    return (
        f'<tr><td class="company">{company_cell}</td>'
        f'<td class="deal-id">{deal_id_cell}</td>'
        f'<td>{size_text}</td>'
        f'<td>{badge_html}</td>'
        f'<td class="num">{buyer_text}</td>'
        f'<td class="num">{intro_text}</td>'
        f'<td>{deadline_html}</td>'
        f'<td>{attention_html}</td>'
        f'<td class="actions">{update_btn}</td></tr>'
    )


def render_my_deals_page(viewer_name, deals=None, tenant_picker=False, key=None, view_as=None, cef_html="",
                          edit_mode=False, person_id=None, anon_key_email=None):
    """deals is always Sell-order-tagged only (see lambda_handler's mydeals
    branch). person_id/anon_key_email are needed here (not just deals)
    because the Buyers/Intros/Needs-attention columns are company-level
    buy-side signals, not attributes of the Sell deal itself."""
    nav = _nav_html("mydeals", viewer_name, key=key, view_as=view_as, edit_flag=edit_mode, cef_html=cef_html)

    # Feature-request box + list. tenant_picker (admin, no view_as) is the
    # one case with no single partition to scope to — it aggregates every
    # TENANTS partition plus "admin" instead (see the task's admin-
    # aggregate requirement); every other case (real tenant session, or
    # admin under &view_as) is scoped to anon_key_email like everywhere
    # else on this page.
    if tenant_picker:
        feature_box_html = _feature_box_html("admin", key=key)
        tenant_names = {"admin": "Admin"}
        agg_open, agg_done = [], []
        for partition in list(TENANTS.keys()) + ["admin"]:
            items, _ = get_feature_requests(partition)
            agg_open += items["open"]
            agg_done += items["done"]
            tenant_names[partition] = TENANTS[partition]["name"] if partition in TENANTS else "Admin"
        agg_open.sort(key=lambda it: it["sk"], reverse=True)
        agg_done.sort(key=lambda it: it["sk"], reverse=True)
        feature_list_html = _feature_requests_list_html(agg_open, agg_done, show_tenant=True,
                                                          tenant_names=tenant_names)
    else:
        feature_box_html = _feature_box_html(anon_key_email, key=key)
        feature_items, _ = get_feature_requests(anon_key_email)
        feature_list_html = _feature_requests_list_html(feature_items["open"], feature_items["done"])

    summary_html = ""
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

        # Per-company buy-side aggregation (Intros count, Stalled/follow-up
        # flags), memoized per distinct company so two Sell deals for the
        # same company don't redo the same get_my_matched_buy_deals scan.
        company_stats_cache = {}

        def _company_stats(company_name):
            cache_key = (company_name or "").strip().lower()
            if cache_key in company_stats_cache:
                return company_stats_cache[cache_key]
            matched = (get_my_matched_buy_deals(person_id, company_name)
                       if person_id is not None and company_name else [])
            intro_count = 0
            stalled = False
            follow_up_due = False
            for d in matched:
                entry = intro_details.get(str(d.get("id"))) or {}
                resolved = _resolve_intro_status(d, entry)
                if resolved["disclosed"]:
                    intro_count += 1
                if resolved["id"] == INTRO_STATUS_STALLED_ID:
                    stalled = True
                if _follow_up_is_due(entry.get("follow_up")):
                    follow_up_due = True
            stats = {"intro_count": intro_count, "stalled": stalled, "follow_up_due": follow_up_due}
            company_stats_cache[cache_key] = stats
            return stats

        rows = []
        for d in deals:
            company_name = _deal_company_name(d)
            override_entry = intro_details.get(str(d.get("id"))) or {}
            deadline = _resolve_deal_deadline(d, override_entry)
            rows.append({
                "deal": d,
                "company_name": company_name,
                "deadline": deadline,
                "stats": _company_stats(company_name),
                "buyer_count": buyer_counts.get((company_name or "").strip().lower(), 0),
            })

        # Deadline ascending (ISO yyyy-mm-dd sorts correctly as a string),
        # no-deadline rows last, then company A-Z.
        rows.sort(key=lambda r: ((0, r["deadline"]) if r["deadline"] else (1, ""),
                                  (r["company_name"] or "").lower()))

        live_count = 0
        not_engaged_count = 0
        intros_total = 0
        attention_count = 0
        deadlines = []
        row_htmls = []
        for r in rows:
            d = r["deal"]
            deadline = r["deadline"]
            stats = r["stats"]
            is_overdue = False
            if deadline:
                deadline_dt = _parse_dt(deadline)
                is_overdue = bool(deadline_dt and deadline_dt.date() < datetime.now(timezone.utc).date())
                deadlines.append(deadline)

            if is_overdue or stats["stalled"] or stats["follow_up_due"]:
                attention_count += 1
            intros_total += stats["intro_count"]

            state = _my_deal_visibility_state(d, cef_state)
            if state == "live":
                live_count += 1
            elif state == "not_live":
                not_engaged_count += 1

            row_htmls.append(_my_deal_row_html(d, r["company_name"], deadline, stats, r["buyer_count"], cef_state,
                                                key=key, view_as=view_as, edit_mode=edit_mode))

        summary_parts = []
        if live_count:
            summary_parts.append(f"{live_count} live")
        if not_engaged_count:
            summary_parts.append(f"{not_engaged_count} not engaged")
        if intros_total:
            summary_parts.append(f"{intros_total} intros in motion")
        if attention_count:
            summary_parts.append(f"{attention_count} need attention")
        if deadlines:
            summary_parts.append(f"next deadline {deadlines[0]}")
        summary_html = (f'<p class="mydeals-summary">{_esc(" · ".join(summary_parts))}</p>'
                         if summary_parts else "")

        rows_html = "".join(row_htmls)
        body_html = f"""<div class="card">
    <table>
      <thead>
        <tr>
          <th>Company</th>
          <th>Deal ID</th>
          <th>Size</th>
          <th>Visibility</th>
          <th class="num">Buyers</th>
          <th class="num">Intros</th>
          <th>Deadline</th>
          <th></th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        {rows_html}
      </tbody>
    </table>
  </div>"""
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
    --bg: #14161a;
    --card: #1c1f26;
    --line: #2a2e37;
    --ink: #e8eaed;
    --muted: #9aa0ac;
    --accent: #4f8cff;
    --qp: #2e9d6a;
    --accredited: #c9a227;
  }}
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0;
    background: var(--bg);
    color: var(--ink);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
    padding: 32px 24px 64px;
  }}
  .wrap {{ max-width: 1100px; margin: 0 auto; }}
  h1 {{ font-size: 22px; font-weight: 600; margin: 0 0 4px; }}
  .mydeals-summary {{ color: var(--muted); font-size: 13px; margin: 0 0 20px; }}
  .feature-section-heading {{ font-size: 16px; font-weight: 600; margin: 32px 0 12px; }}
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
    white-space: nowrap;
  }}
  thead th.num, td.num {{ text-align: right; }}
  tbody td {{
    padding: 11px 16px;
    border-bottom: 1px solid var(--line);
    font-size: 14px;
  }}
  tbody tr:last-child td {{ border-bottom: none; }}
  tbody tr:hover {{ background: rgba(255,255,255,0.03); }}
  td.company {{ font-weight: 500; }}
  td.company a {{ color: inherit; text-decoration: none; border-bottom: 1px solid var(--line); }}
  td.company a:hover {{ border-bottom-color: var(--muted); }}
  td.deal-id {{ white-space: nowrap; }}
  td.deal-id a {{ color: var(--accent); text-decoration: none; }}
  td.deal-id a:hover {{ text-decoration: underline; }}
  .copy-link-btn {{
    background: none;
    border: none;
    color: var(--muted);
    cursor: pointer;
    font-size: 12px;
    margin-left: 4px;
    padding: 2px;
    vertical-align: middle;
  }}
  .copy-link-btn:hover {{ color: var(--ink); }}
  td.actions {{ white-space: nowrap; }}
  .update-cancel-btn {{
    display: inline-block;
    font-size: 12px;
    font-weight: 600;
    padding: 4px 10px;
    border-radius: 999px;
    background: rgba(255,255,255,0.06);
    color: var(--ink);
    text-decoration: none;
  }}
  .update-cancel-btn:hover {{ text-decoration: underline; }}
  .visibility-badge {{
    display: inline-block;
    font-size: 12px;
    font-weight: 600;
    padding: 4px 10px;
    border-radius: 999px;
    text-decoration: none;
    white-space: nowrap;
  }}
  .visibility-badge.live {{ background: rgba(46,157,106,0.15); color: var(--qp); }}
  .visibility-badge.setup {{ background: rgba(201,162,39,0.15); color: var(--accredited); }}
  .visibility-badge.not-live {{ background: rgba(220,80,80,0.15); color: #e06666; }}
  .visibility-badge.not-live:hover {{ text-decoration: underline; }}
  .deadline-overdue {{ color: #e06666; font-weight: 600; }}
  .ei-deadline.overdue-input {{ border-color: #e06666; }}
  .attention-chip {{
    display: inline-block;
    font-size: 11px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    padding: 3px 9px;
    border-radius: 999px;
    background: rgba(220,80,80,0.15);
    color: #e06666;
    white-space: nowrap;
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
  .ei-msg.error {{ color: #e06666; }}
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
  {body_html}
  <h2 class="feature-section-heading">Feature requests</h2>
  <div class="card">
    {feature_list_html}
  </div>
</div>
{edit_script}
{_feature_toggle_script_html(key)}
<script>
(function() {{
  document.querySelectorAll('.copy-link-btn').forEach(function(btn) {{
    btn.addEventListener('click', function() {{
      var url = btn.getAttribute('data-copy-url');
      var orig = btn.textContent;
      navigator.clipboard.writeText(url).then(function() {{
        btn.textContent = '\\u2713';
        setTimeout(function() {{ btn.textContent = orig; }}, 1500);
      }}).catch(function() {{}});
    }});
  }});
}})();
</script>
</body>
</html>"""


TIER_LABELS = {"qp": "QP", "accredited": "Accredited", "unknown": "Unknown"}
TIER_ORDER = {"qp": 0, "accredited": 1, "unknown": 2}


def _buyer_tile_html(buyer, anon_key_email, now):
    code = _anon_buyer_code(anon_key_email, buyer["person_id"])
    tier = buyer["tier"]
    tier_label = TIER_LABELS.get(tier, "Unknown")

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
      <span class="tier-badge tier-{tier}">{_esc(tier_label)}</span>
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
    feature_items, _ = get_feature_requests(anon_key_email)
    feature_box_html = _feature_box_html(anon_key_email, key=key)
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
                rows_parts += [_matched_buyer_row_edit_html(d, person_id, people_by_id, intro_details)
                               for d in main_deals]
            else:
                rows_parts += [_matched_buyer_row_html(d, person_id, people_by_id, intro_details, anon_key_email,
                                                         editable=tenant_edit_mode)
                               for d in main_deals]
        else:
            rows_parts.append(_group_empty_row_html("No introductions yet on this deal.", colspan))

        if pending_deals:
            rows_parts.append(_group_header_row_html("Pending introductions", colspan))
            if edit_mode:
                # Edit mode never anonymizes — the admin sees and edits the
                # real buyer regardless of pending/disclosed.
                rows_parts += [_matched_buyer_row_edit_html(d, person_id, people_by_id, intro_details)
                               for d in pending_deals]
            else:
                # Pending rows never get inputs even under tenant_edit_mode —
                # only Introduced-or-later rows are editable.
                rows_parts += [_matched_buyer_row_html(d, person_id, people_by_id, intro_details, anon_key_email,
                                                         editable=tenant_edit_mode)
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
        edit_script = _edit_script_html(key) if (edit_mode or tenant_edit_mode) else ""
        matched_buyers_html = f"""<section class="cd-section">
    <h2>Buyers</h2>
    {matched_body}
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
    --bg: #14161a;
    --card: #1c1f26;
    --line: #2a2e37;
    --ink: #e8eaed;
    --muted: #9aa0ac;
    --accent: #4f8cff;
    --qp: #2e9d6a;
    --accredited: #c9a227;
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
    color: #14161a;
    margin-bottom: 8px;
  }}
  .tier-badge.tier-qp {{ background: var(--qp); }}
  .tier-badge.tier-accredited {{ background: var(--accredited); }}
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
  .deal-card.overdue {{ border-color: #e06666; }}
  .overdue-chip {{
    display: inline-block;
    font-size: 12px;
    font-weight: 700;
    padding: 4px 10px;
    border-radius: 999px;
    margin-bottom: 12px;
    background: rgba(220,80,80,0.15);
    color: #e06666;
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
  .engagement-badge {{
    display: inline-block;
    font-size: 12px;
    font-weight: 600;
    padding: 4px 10px;
    border-radius: 999px;
    margin-top: 6px;
    text-decoration: none;
  }}
  .engagement-badge.engaged {{ background: rgba(46,157,106,0.15); color: var(--qp); }}
  .engagement-badge.in-process {{ background: rgba(201,162,39,0.15); color: var(--accredited); }}
  .engagement-badge.not-engaged {{ background: rgba(220,80,80,0.15); color: #e06666; }}
  .engagement-badge.not-engaged:hover {{ text-decoration: underline; }}
  .update-cancel-btn {{
    display: inline-block;
    font-size: 12px;
    font-weight: 600;
    padding: 4px 10px;
    border-radius: 999px;
    margin-top: 6px;
    margin-left: 8px;
    background: rgba(255,255,255,0.06);
    color: var(--ink);
    text-decoration: none;
  }}
  .update-cancel-btn:hover {{ text-decoration: underline; }}
  .status-pill {{
    display: inline-block;
    font-size: 11px;
    font-weight: 600;
    color: var(--muted);
    background: rgba(255,255,255,0.06);
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
  .status-chip.exit {{ background: rgba(255,255,255,0.06); color: var(--muted); }}
  .gg-note {{
    color: var(--accredited);
    font-size: 13px;
    margin: 0 0 10px;
  }}
  tr.pending-row {{ opacity: 0.85; }}
  tr.group-divider td {{
    padding: 8px 16px;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--muted);
    background: rgba(255,255,255,0.02);
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
  .ei-msg.error {{ color: #e06666; }}
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
    background: rgba(220,80,80,0.15);
    color: #e06666;
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


def _message_page(title, message, show_signin=False):
    """Standalone pre-auth / not-enabled page. Reuses the board's own dark
    palette (not the nav's) since there's no tab shell to sit under here."""
    signin_html = ""
    if show_signin:
        signin_html = (
            f'<p><a class="gg-link" href="{SIGNIN_URL}">'
            "Sign in via trades.graciagroup.com</a></p>"
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
    background: #14161a;
    color: #e8eaed;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
    padding: 24px;
  }}
  .gg-card {{
    max-width: 420px;
    text-align: center;
    background: #1c1f26;
    border: 1px solid #2a2e37;
    border-radius: 10px;
    padding: 32px 28px;
  }}
  .gg-card h1 {{ font-size: 18px; margin: 0 0 12px; }}
  .gg-card p {{ color: #9aa0ac; font-size: 14px; line-height: 1.5; margin: 0 0 8px; }}
  .gg-link {{ color: #4f8cff; text-decoration: none; font-weight: 600; }}
</style>
</head>
<body>
  <div class="gg-card">
    <h1>{_esc(title)}</h1>
    <p>{_esc(message)}</p>
    {signin_html}
  </div>
</body>
</html>"""


def render_page(table, viewer_name, key=None, view_as=None, cef_html=""):
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
  :root {{
    --bg: #14161a;
    --card: #1c1f26;
    --line: #2a2e37;
    --ink: #e8eaed;
    --muted: #9aa0ac;
    --accent: #4f8cff;
    --qp: #2e9d6a;
    --accredited: #c9a227;
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
  .header-row {{
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 16px;
  }}
  h1 {{ font-size: 22px; font-weight: 600; margin: 0 0 4px; }}
  .sub {{ color: var(--muted); font-size: 13px; margin: 0 0 24px; }}
  .feature-btn {{
    flex: 0 0 auto;
    background: var(--accent);
    color: #fff;
    font-size: 13px;
    font-weight: 600;
    text-decoration: none;
    padding: 10px 16px;
    border-radius: 8px;
    white-space: nowrap;
  }}
  .feature-btn:hover {{ opacity: 0.9; }}
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
  tbody tr:hover {{ background: rgba(255,255,255,0.03); }}
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
    <a class="feature-btn"
       href="mailto:cgracia@rainmakersecurities.com?subject=Syndicator%20Dashboard%20feature%20request">Request a feature</a>
  </div>
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
      authorizes a write, even an admin's own) — full rights over status,
      next_steps, notes, follow_up, and deadline, on any deal.
    - Tenant: no ADMIN_KEY, but a valid gg_id identity cookie naming an
      email in TENANTS — rights restricted to next_steps and follow_up
      ONLY (status/notes/deadline are rejected outright with 403), and
      only on a deal_id whose linked tenant (via person linkage,
      _tenant_email_for_deal) is that same authenticated tenant. No
      Pipeline call ever happens on a tenant post, since status and
      deadline (the only two fields that ever reach Pipeline) are never
      part of one.

    Order: validate -> look up the deal and its owning tenant -> if status
    or deadline is part of the write, PUT it to Pipeline first and abort
    the whole request (writing nothing to Dynamo) on any non-2xx -> write
    the Dynamo intro-item update (status_override/override_at, next_steps,
    notes, follow_up, deadline_override/deadline_override_at) -> append an
    audit item (actor = tenant email or "admin"). Never raises past this
    function; every failure mode returns a JSON error the UI can show."""
    body = _parse_json_body(event)

    admin_key = os.environ.get("ADMIN_KEY")
    is_admin = bool(admin_key) and body.get("key") == admin_key

    tenant_identity_email = None
    if not is_admin:
        identity_email = _read_identity_email(event)
        tenant_identity_email = identity_email.strip().lower() if identity_email else None
        if not tenant_identity_email or tenant_identity_email not in TENANTS:
            return _json_response({"error": "forbidden"}, 403)
        if (body.get("status") not in (None, "") or body.get("notes") not in (None, "")
                or body.get("deadline") not in (None, "")):
            return _json_response({"error": "forbidden"}, 403)

    deal_id = str(body.get("deal_id") or "").strip()
    if not deal_id:
        return _json_response({"error": "deal_id is required"}, 400)

    status_id = None
    if is_admin and body.get("status") not in (None, ""):
        try:
            status_id = int(body.get("status"))
        except (TypeError, ValueError):
            return _json_response({"error": "invalid status"}, 400)
        if status_id not in INTRO_STATUS_LABELS:
            return _json_response({"error": "invalid status"}, 400)

    next_steps = body.get("next_steps")
    if next_steps is not None:
        next_steps = str(next_steps)
        if len(next_steps) > MAX_INTRO_TEXT_LEN:
            return _json_response({"error": "next_steps too long"}, 400)

    notes = body.get("notes") if is_admin else None
    if notes is not None:
        notes = str(notes)
        if len(notes) > MAX_INTRO_TEXT_LEN:
            return _json_response({"error": "notes too long"}, 400)

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

    if status_id is None and next_steps is None and notes is None and follow_up is None and deadline is None:
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
    old_values = {
        "status": old_resolved["id"] if old_resolved["id"] is not None else old_resolved["name"],
        "next_steps": old_entry.get("next_steps"),
        "notes": old_entry.get("notes"),
        "follow_up": old_entry.get("follow_up"),
        "deadline": _resolve_deal_deadline(deal, old_entry),
    }

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
                                          old_values, actor)
    if not ok:
        return _json_response({"error": f"Save failed: {err}"}, 502)

    return _json_response({"ok": True})


NOT_ENABLED_MESSAGE = (
    "This dashboard isn't enabled for your account yet — "
    "contact cgracia@rainmakersecurities.com."
)


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
            tenant = TENANTS.get(view_as.lower())
            if not tenant:
                return _html_response(_message_page("Not enabled", NOT_ENABLED_MESSAGE))
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
        tenant = TENANTS.get(identity_email.strip().lower())
        if not tenant:
            return _html_response(_message_page("Not enabled", NOT_ENABLED_MESSAGE))
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

    if tab == "demand":
        table = get_company_table()
        body = render_page(table, viewer_name, key=nav_key, view_as=nav_view_as, cef_html=cef_html)
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
