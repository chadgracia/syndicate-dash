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
GET only. Read-only: no S3 writes, no CRM writes, no email, no calls beyond
the S3 reads below.
"""

import base64
import hashlib
import hmac
import json
import os
import time
import urllib.parse
from datetime import datetime, timezone
import boto3

BUCKET = "full-pipeline-cache"
PEOPLE_KEY = "people.json"
INTEREST_KEY = "interest_people.json"
DEALS_KEY = "deals.json"

INVESTOR_LEVEL_FIELD = "custom_label_3923758"
QP_ID = 6950564
IQF_FIELD = "custom_label_3763008"
IQF_OK_IDS = {6496840, 6596073}

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
STRUCTURE_FIELD = "custom_label_3064360"
LAYERS_FIELD = "custom_label_3938743"

STRUCTURE_LABELS = {6250090: "Direct", 5077906: "Fund"}
LAYERS_MAP = {7000228: "1-Layer", 7000229: "2-Layer", 7000230: "3-Layer"}

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

# "Matched or later" live buy-side stages for the Matched Buyers section:
# every known stage id (STAGE_LABELS, above) except Inquiry and Hold, per
# instruction to include matched/firm/transfer-notice/SPA-style stages and
# exclude inquiry/hold/lost/dead. Confirm/LOI Signed sit later than Matched
# in this org's own stage progression (see chadgracia/daily-brief's
# TO_CLOSE_ALL_STAGES / TO_CLOSE_AGED_STAGE ordering), so they're included
# too. Not archived is required in addition, same as the Sellers column's
# "live" definition.
MATCHED_OR_LATER_STAGE_IDS = {
    STAGE_MATCHED, STAGE_FIRM, STAGE_CONFIRM,
    STAGE_LOI_SIGNED, STAGE_TRANSFER_NOTICE, STAGE_SPA_SIGNED,
}

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
    if QP_ID in cf_list(cf, INVESTOR_LEVEL_FIELD):
        return "qp"
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


def get_my_matched_buy_deals(person_id, company):
    """The tenant's BUY deals for one company at Matched-or-later stage,
    for the Matched Buyers section."""
    target = company.strip().lower()
    out = []
    for d in get_my_deals(person_id):
        if (_deal_company_name(d) or "").strip().lower() != target:
            continue
        if d.get("is_archived"):
            continue
        if _deal_stage_id(d) not in MATCHED_OR_LATER_STAGE_IDS:
            continue
        if DEAL_SIDE_BUY_ID in _deal_cf_option_ids(d, DEAL_SIDE_FIELD):
            out.append(d)
    return out


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


def _deal_card_html(deal, company):
    name = _esc(_deal_title(deal))
    sid = _deal_stage_id(deal)
    stage = _esc(STAGE_LABELS.get(sid, str(sid) if sid is not None else "—"))
    size_text = _esc(_deal_size_text(deal))
    gross_text = _esc(_fmt_money(_deal_cf_number(deal, GROSS_FIELD)))

    struct_label = STRUCTURE_LABELS.get(next(iter(_deal_cf_option_ids(deal, STRUCTURE_FIELD)), None))
    layer_label = LAYERS_MAP.get(next(iter(_deal_cf_option_ids(deal, LAYERS_FIELD)), None))
    struct_parts = [p for p in (struct_label, layer_label) if p]
    structure_text = _esc(" · ".join(struct_parts)) if struct_parts else "—"

    deadline = _deal_deadline_text(deal)
    deadline_html = (f'<div class="dc-line">Deadline: {_esc(deadline)}</div>'
                      if deadline else "")

    fees = _fmt_fees(deal)
    fees_html = f'<div class="dc-line">{_esc(fees)}</div>' if fees else ""

    badge_html = _engagement_badge_html(deal, company)

    return f"""<div class="deal-card">
      <div class="deal-card-head">
        <div class="deal-card-title">{name}</div>
        <div class="deal-card-stage">{stage}</div>
      </div>
      <div class="deal-card-metrics">
        <div><span class="dc-label">Size</span><span class="dc-value">{size_text}</span></div>
        <div><span class="dc-label">Gross</span><span class="dc-value">{gross_text}</span></div>
        <div><span class="dc-label">Structure</span><span class="dc-value">{structure_text}</span></div>
      </div>
      {deadline_html}
      {fees_html}
      {badge_html}
    </div>"""


def _matched_buyer_row_html(deal, tenant_person_id, people_by_id):
    linked = _deal_linked_person_ids(deal) - {tenant_person_id}
    buyer_recs = [people_by_id[pid] for pid in linked if pid in people_by_id]

    if buyer_recs:
        names = ", ".join(_esc(_person_display_name(r) or "—") for r in buyer_recs)
        first_cf = buyer_recs[0].get("custom_fields") or {}
        transactor_ids = cf_list(first_cf, TRANSACTOR_TYPE_FIELD)
        entity_text = "Natural Person" if NATURAL_PERSON_ID in transactor_ids else ""
    else:
        names = "—"
        entity_text = ""

    sid = _deal_stage_id(deal)
    stage = _esc(STAGE_LABELS.get(sid, str(sid) if sid is not None else "—"))
    size_text = _esc(_deal_size_text(deal))

    return (
        f'<tr><td>{names}</td>'
        f'<td>{_esc(entity_text)}</td>'
        f'<td class="num">{size_text}</td>'
        f'<td>{stage}</td></tr>'
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


def _nav_html(active_tab, viewer_name, key=None, view_as=None, show_viewer=True):
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
    return f"""<header class="gg-nav">
  <div class="gg-nav-inner">
    <div class="gg-brand">Gracia Group</div>
    <nav class="gg-tabs">
      <a class="{mydeals_cls}" href="{mydeals_href}">My Deals</a>
      <a class="{intros_cls}" href="{intros_href}">Active Intros</a>
      <a class="{demand_cls}" href="{demand_href}">Demand Board</a>
    </nav>
    <div class="gg-viewer">{viewer_html}</div>
  </div>
</header>"""


def render_intros_page(viewer_name, key=None, view_as=None):
    nav = _nav_html("intros", viewer_name, key=key, view_as=view_as)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Active Intros</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0;
    background: #14161a;
    color: #e8eaed;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
  }}
{NAV_CSS}
  .gg-placeholder {{
    max-width: 1000px;
    margin: 96px auto;
    padding: 0 24px;
    text-align: center;
    color: #9aa0ac;
    font-size: 15px;
  }}
</style>
</head>
<body>
{nav}
<div class="gg-placeholder">Your active introductions will appear here soon</div>
</body>
</html>"""


def _my_deal_row_html(deal, key=None, view_as=None):
    company_name = _deal_company_name(deal)
    if company_name:
        company = (f'<a href="{_company_href(company_name, "mydeals", key, view_as)}">'
                   f'{_esc(company_name)}</a>')
    else:
        company = "—"
    name = _esc(_deal_title(deal))

    side_ids = _deal_cf_option_ids(deal, DEAL_SIDE_FIELD)
    if DEAL_SIDE_SELL_ID in side_ids:
        side = "Sell"
    elif DEAL_SIDE_BUY_ID in side_ids:
        side = "Buy"
    else:
        side = "—"

    sid = _deal_stage_id(deal)
    stage = _esc(STAGE_LABELS.get(sid, str(sid) if sid is not None else "—"))

    ticket_max = _deal_cf_number(deal, TICKET_MAX_FIELD)
    ticket_min = _deal_cf_number(deal, TICKET_MIN_FIELD)
    size_val = ticket_max if ticket_max is not None else ticket_min
    size_text = _fmt_money(size_val)

    gross_val = _deal_cf_number(deal, GROSS_FIELD)
    gross_text = _fmt_money(gross_val)

    struct_label = STRUCTURE_LABELS.get(next(iter(_deal_cf_option_ids(deal, STRUCTURE_FIELD)), None))
    layer_label = LAYERS_MAP.get(next(iter(_deal_cf_option_ids(deal, LAYERS_FIELD)), None))
    parts = [p for p in (struct_label, layer_label) if p]
    structure = _esc(" · ".join(parts)) if parts else "—"

    updated = _esc((deal.get("updated_at") or "")[:10] or "—")

    return (
        f'<tr><td class="company">{company}</td>'
        f'<td>{name}</td>'
        f'<td>{side}</td>'
        f'<td>{stage}</td>'
        f'<td class="num" data-sort="{size_val if size_val is not None else -1}">{size_text}</td>'
        f'<td class="num" data-sort="{gross_val if gross_val is not None else -1}">{gross_text}</td>'
        f'<td>{structure}</td>'
        f'<td>{updated}</td></tr>'
    )


def render_my_deals_page(viewer_name, deals=None, tenant_picker=False, key=None, view_as=None):
    nav = _nav_html("mydeals", viewer_name, key=key, view_as=view_as)

    if tenant_picker:
        body_html = (
            '<div class="gg-placeholder">Pick a tenant to preview — '
            'add &amp;view_as=&lt;email&gt; to the URL.</div>'
        )
    elif not deals:
        body_html = '<div class="gg-placeholder">You have no active deals yet.</div>'
    else:
        rows_html = "".join(_my_deal_row_html(d, key=key, view_as=view_as) for d in deals)
        body_html = f"""<div class="card">
    <table id="board">
      <thead>
        <tr>
          <th data-key="company" data-type="string">Company<span class="arrow"></span></th>
          <th data-key="name" data-type="string">Deal<span class="arrow"></span></th>
          <th data-key="side" data-type="string">Buy/Sell<span class="arrow"></span></th>
          <th data-key="stage" data-type="string">Stage<span class="arrow"></span></th>
          <th class="num" data-key="size" data-type="number">Size<span class="arrow"></span></th>
          <th class="num" data-key="gross" data-type="number">Gross price<span class="arrow"></span></th>
          <th data-key="structure" data-type="string">Structure/Layers<span class="arrow"></span></th>
          <th data-key="updated" data-type="string">Last updated<span class="arrow"></span></th>
        </tr>
      </thead>
      <tbody id="board-body">
        {rows_html}
      </tbody>
    </table>
  </div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>My Deals</title>
<style>
{NAV_CSS}
  :root {{
    --bg: #14161a;
    --card: #1c1f26;
    --line: #2a2e37;
    --ink: #e8eaed;
    --muted: #9aa0ac;
    --accent: #4f8cff;
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
  .gg-placeholder {{
    max-width: 1000px;
    margin: 96px auto;
    padding: 0 24px;
    text-align: center;
    color: var(--muted);
    font-size: 15px;
  }}
</style>
</head>
<body>
{nav}
<div class="wrap">
  <h1>My Deals</h1>
  {body_html}
</div>
<script>
(function() {{
  var tbody = document.getElementById('board-body');
  if (!tbody) return;
  var rows = Array.prototype.slice.call(tbody.querySelectorAll('tr'));
  var headers = document.querySelectorAll('#board thead th');
  var sortState = {{ key: null, dir: 1 }};

  function cellSortValue(row, colIndex) {{
    var cell = row.children[colIndex];
    var raw = cell.getAttribute('data-sort');
    return raw !== null ? raw : cell.textContent.trim();
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
      var av = cellSortValue(a, colIndex);
      var bv = cellSortValue(b, colIndex);
      if (type === 'number') {{
        return (parseFloat(av) - parseFloat(bv)) * dir;
      }}
      return av.localeCompare(bv) * dir;
    }});
    rows.forEach(function(row) {{ tbody.appendChild(row); }});
  }}

  headers.forEach(function(h, idx) {{
    h.addEventListener('click', function() {{
      applySort(idx, h.getAttribute('data-key'), h.getAttribute('data-type'));
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


def render_company_page(company, viewer_name, tenant, anon_key_email, ref, key=None, view_as=None):
    """No tab is highlighted (active_tab=None never matches mydeals/intros/
    demand in _nav_html). Section (a) is included only when tenant is not
    None — the same "admin with no view_as" signal render_my_deals_page's
    tenant_picker branch uses, since there is no tenant to scope deals to.
    Section (b) never receives — and so can never render — a buyer's name,
    email, or raw person id; the tile only ever sees the anonymized code,
    tier, ticket range, and a boolean recency flag from _buyer_tile_html."""
    nav = _nav_html(None, viewer_name, key=key, view_as=view_as, show_viewer=False)
    suffix = _tab_qs_suffix(key, view_as)
    back_href = f"?tab={ref}{suffix}"
    back_label = REF_LABELS.get(ref, "My Deals")

    your_deals_html = ""
    matched_buyers_html = ""
    if tenant is not None:
        person_id = tenant.get("person_id")

        sell_deals = get_my_sell_deals(person_id, company) if person_id is not None else []
        if sell_deals:
            deals_body = "".join(_deal_card_html(d, company) for d in sell_deals)
        else:
            deals_body = '<div class="gg-placeholder small">No deals with this company yet.</div>'
        your_deals_html = f"""<section class="cd-section">
    <h2>Your Deals</h2>
    {deals_body}
  </section>"""

        matched_deals = get_my_matched_buy_deals(person_id, company) if person_id is not None else []
        if matched_deals:
            wanted_ids = set()
            for d in matched_deals:
                wanted_ids |= _deal_linked_person_ids(d) - {person_id}
            people_by_id = get_people_by_ids(wanted_ids)
            matched_rows_html = "".join(
                _matched_buyer_row_html(d, person_id, people_by_id) for d in matched_deals
            )
            matched_body = f"""<div class="card">
      <table>
        <thead>
          <tr>
            <th>Buyer name</th><th>Entity/Natural person</th>
            <th class="num">Size</th><th>Stage</th>
          </tr>
        </thead>
        <tbody>{matched_rows_html}</tbody>
      </table>
    </div>"""
        else:
            matched_body = '<div class="gg-placeholder small">No matched buyers yet.</div>'
        matched_buyers_html = f"""<section class="cd-section">
    <h2>Matched Buyers</h2>
    {matched_body}
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
    border-radius: 10px;
    padding: 16px 18px;
    margin-bottom: 12px;
  }}
  .deal-card:last-child {{ margin-bottom: 0; }}
  .deal-card-head {{
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 12px;
    margin-bottom: 10px;
  }}
  .deal-card-title {{ font-size: 15px; font-weight: 600; }}
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
    gap: 20px;
    margin-bottom: 8px;
  }}
  .dc-label {{
    display: block;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    color: var(--muted);
    margin-bottom: 2px;
  }}
  .dc-value {{ font-size: 14px; font-weight: 500; }}
  .dc-line {{ font-size: 13px; color: var(--muted); margin-bottom: 4px; }}
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
</style>
</head>
<body>
{nav}
<div class="wrap">
  <a class="cd-back" href="{back_href}">&larr; Back to {_esc(back_label)}</a>
  <h1>{_esc(company)}</h1>
  {your_deals_html}
  {matched_buyers_html}
  <section class="cd-section">
    <h2>Buyer Demand</h2>
    {buyer_demand_body}
  </section>
  <section class="cd-section">
    <h2>Introduced</h2>
    <div class="gg-placeholder small">Introductions for this company will appear here soon.</div>
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


def render_page(table, viewer_name, key=None, view_as=None):
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
    nav = _nav_html("demand", viewer_name, key=key, view_as=view_as)
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


NOT_ENABLED_MESSAGE = (
    "This dashboard isn't enabled for your account yet — "
    "contact cgracia@rainmakersecurities.com."
)


def lambda_handler(event, context):
    method = (event.get("requestContext", {}).get("http", {}).get("method")
              or event.get("httpMethod") or "GET")
    if method != "GET":
        return _forbidden()

    query = event.get("queryStringParameters") or {}
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

    # Company detail page: same auth resolution as the tabs above, just a
    # different route param.
    company = query.get("company")
    if company:
        ref = query.get("ref") or "mydeals"
        if ref not in REF_LABELS:
            ref = "mydeals"
        body = render_company_page(company, viewer_name, tenant, anon_key_email, ref,
                                    key=nav_key, view_as=nav_view_as)
        return _html_response(body)

    if tab == "demand":
        table = get_company_table()
        body = render_page(table, viewer_name, key=nav_key, view_as=nav_view_as)
    elif tab == "mydeals":
        if tenant is None:
            body = render_my_deals_page(viewer_name, tenant_picker=True,
                                         key=nav_key, view_as=nav_view_as)
        else:
            person_id = tenant.get("person_id")
            deals = get_my_deals(person_id) if person_id is not None else []
            body = render_my_deals_page(viewer_name, deals=deals,
                                         key=nav_key, view_as=nav_view_as)
    else:
        body = render_intros_page(viewer_name, key=nav_key, view_as=nav_view_as)
    return _html_response(body)
