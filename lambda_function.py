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

Access gate: query param `key` must equal os.environ["ADMIN_KEY"]. GET only.
Read-only: no S3 writes, no CRM writes, no email, no calls beyond the two S3
reads below.
"""

import json
import os
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


def render_page(table):
    rows_html = "".join(
        f'<tr><td class="company">{_esc(r["company"])}</td>'
        f'<td class="num">{r["total"]}</td>'
        f'<td class="num">{r["qp"]}</td>'
        f'<td class="num">{r["accredited"]}</td>'
        f'<td class="num">{r["unknown"]}</td>'
        f'<td class="num">{r["sellers"]}</td></tr>'
        for r in table
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Demand Board</title>
<style>
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
<div class="wrap">
  <div class="header-row">
    <div>
      <h1>Demand Board</h1>
      <p class="sub">{len(table)} companies with interested buyers</p>
    </div>
    <a class="feature-btn"
       href="mailto:cgracia@graciagroup.com?subject=Syndicator%20Dashboard%20feature%20request">Request a feature</a>
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


def lambda_handler(event, context):
    method = (event.get("requestContext", {}).get("http", {}).get("method")
              or event.get("httpMethod") or "GET")
    if method != "GET":
        return _forbidden()

    admin_key = os.environ.get("ADMIN_KEY")
    query = event.get("queryStringParameters") or {}
    if not admin_key or query.get("key") != admin_key:
        return _forbidden()

    table = get_company_table()
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "text/html; charset=utf-8"},
        "body": render_page(table),
    }
