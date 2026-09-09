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


def _nav_html(active_tab, viewer_name, key=None, view_as=None):
    suffix = _tab_qs_suffix(key, view_as)
    mydeals_href = f"?tab=mydeals{suffix}"
    intros_href = f"?tab=intros{suffix}"
    demand_href = f"?tab=demand{suffix}"
    mydeals_cls = "gg-tab active" if active_tab == "mydeals" else "gg-tab"
    intros_cls = "gg-tab active" if active_tab == "intros" else "gg-tab"
    demand_cls = "gg-tab active" if active_tab == "demand" else "gg-tab"
    return f"""<header class="gg-nav">
  <div class="gg-nav-inner">
    <div class="gg-brand">Gracia Group</div>
    <nav class="gg-tabs">
      <a class="{mydeals_cls}" href="{mydeals_href}">My Deals</a>
      <a class="{intros_cls}" href="{intros_href}">Active Intros</a>
      <a class="{demand_cls}" href="{demand_href}">Demand Board</a>
    </nav>
    <div class="gg-viewer">{_esc(viewer_name)}</div>
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


def _my_deal_row_html(deal):
    company = _esc(_deal_company_name(deal) or "—")
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
        rows_html = "".join(_my_deal_row_html(d) for d in deals)
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
        f'<tr><td class="company">{_esc(r["company"])}</td>'
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
    if is_admin_key:
        if view_as:
            tenant = TENANTS.get(view_as.lower())
            if not tenant:
                return _html_response(_message_page("Not enabled", NOT_ENABLED_MESSAGE))
            viewer_name = tenant["name"]
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

    nav_key = query.get("key") if is_admin_key else None
    nav_view_as = view_as or None

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
