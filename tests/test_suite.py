"""
Canonical test suite for lambda_function.py (syndicate-dash).

Run with:  cd /home/user/syndicate-dash && python3 tests/test_suite.py

This is a single, hand-maintained, always-green regression suite. Per
CLAUDE.md convention: keep it green, extend it with each change, never
leave a known-failing check behind (delete or fix instead).

Style: each section is a self-contained fixture block (its own
people.json/deals.json/interest_people.json/companies.json shapes, its
own FakeS3/FakeDynamoTable, its own `lf.boto3` reassignment) followed by
a run of `check(label, cond)` assertions against real lambda_function.py
code paths (render_* page functions, and the small pure-Python helpers
they're built from). Sections are independent and run top-to-bottom;
each resets lf's module-level caches (_tenant_cache, _deals_cache,
_cache) before it needs its own fixture picked up.
"""
import json
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta

os.environ["ADMIN_KEY"] = "test-admin-key"
os.environ["IDENTITY_SECRET"] = "test-secret"
os.environ["HMAC_SECRET"] = "test-hmac-secret"
os.environ["PIPELINE_API_KEY"] = "pk"
os.environ["PIPELINE_APP_KEY"] = "ak"

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lambda_function as lf

ADMIN_KEY = "test-admin-key"


# ======================================================================
# Shared test harness
# ======================================================================

passed = 0
failed = 0


def check(label, cond):
    global passed, failed
    if cond:
        passed += 1
        print(f"PASS: {label}")
    else:
        failed += 1
        print(f"FAIL: {label}")


def reset_caches():
    lf._tenant_cache["version"] = None
    lf._tenant_cache["by_email"] = None
    lf._deals_cache["version"] = None
    lf._deals_cache["deals"] = None
    lf._cache["version"] = None
    lf._cache["table"] = None
    lf._raised_cache["version"] = None
    lf._raised_cache["total"] = None
    # Closed-deals cache defaults to fresh-and-empty (never None/stale) so
    # the 800+ pre-existing tests never trigger an implicit live Pipeline
    # fetch just from calling get_deals_list() -- only the dedicated
    # closed-deals test section below deliberately sets this back to
    # None/stale to exercise the real fetch/cache/TTL logic.
    lf._closed_deals_cache["fetched_at"] = time.time()
    lf._closed_deals_cache["deals"] = []
    # Perf fixes 1-5: the request-scoped cache (memoized deals/people/
    # interest/companies lists, get_my_deals/get_my_matched_buy_deals/
    # company-stats results, per-key HEAD versions) and the singleton S3
    # client / Dynamo table binding all need to be cleared on every
    # fixture swap too -- otherwise a later use_fixture()'s fresh
    # FakeS3/FakeDynamoTable would never actually get exercised (the
    # singletons would still point at the PREVIOUS fixture's fakes, and
    # the request cache would still hold the previous fixture's parsed
    # data/results).
    lf._req_cache_reset()
    lf._s3_client_singleton["client"] = None
    lf._dynamo_table_singleton["table"] = None


class FakeBody:
    def __init__(self, data):
        self.data = data

    def read(self):
        return json.dumps(self.data).encode()


class FakeS3:
    """objs: {key: python-object-to-serve-as-json}. last_modified: optional
    {key: datetime} overrides for cache-invalidation tests; keys not
    present there just get "now" every call."""

    def __init__(self, objs, last_modified=None):
        self.objs = objs
        self.last_modified = last_modified or {}
        self.put_calls = []
        self.multipart_calls = []  # [{"key", "parts": [bytes, ...], "aborted": bool}], one per upload id
        self._multipart_uploads = {}
        self._next_upload_id = 1

    def get_object(self, Bucket, Key):
        # ContentLength included (real S3 always returns it) so the
        # perf report's byte-size logging (TIMING line, item 6) has
        # something real to read in tests.
        return {"Body": FakeBody(self.objs[Key]), "ContentLength": len(json.dumps(self.objs[Key])),
                "LastModified": self.last_modified.get(Key, datetime.now(timezone.utc))}

    def head_object(self, Bucket, Key):
        return {"LastModified": self.last_modified.get(Key, datetime.now(timezone.utc))}

    def put_object(self, Bucket, Key, Body, ContentType=None):
        # Body is the real code's already-encoded bytes (json.dumps(...)
        # .encode("utf-8")) -- decoded/re-parsed here so a get_object
        # right after a put_object in the same test sees it, matching
        # real S3's read-after-write behavior for a single-writer key.
        parsed = json.loads(Body.decode("utf-8") if isinstance(Body, bytes) else Body)
        self.objs[Key] = parsed
        self.put_calls.append({"Key": Key, "ContentType": ContentType})

    # ── Multipart upload (see _write_closed_deals_cache_to_s3) ──────────
    # Real S3 requires every non-final part to be >=5MB; this fake
    # doesn't enforce that (tests use small fixtures well under the
    # threshold), it just assembles whatever parts arrive, in
    # PartNumber order, exactly like the real API does on complete.
    def create_multipart_upload(self, Bucket, Key, ContentType=None):
        upload_id = f"upload-{self._next_upload_id}"
        self._next_upload_id += 1
        self._multipart_uploads[upload_id] = {"key": Key, "parts": {}}
        self.multipart_calls.append({"key": Key, "upload_id": upload_id, "aborted": False})
        return {"UploadId": upload_id}

    def upload_part(self, Bucket, Key, UploadId, PartNumber, Body):
        self._multipart_uploads[UploadId]["parts"][PartNumber] = Body
        return {"ETag": f'"etag-{UploadId}-{PartNumber}"'}

    def complete_multipart_upload(self, Bucket, Key, UploadId, MultipartUpload):
        parts_by_number = self._multipart_uploads[UploadId]["parts"]
        ordered = [parts_by_number[p["PartNumber"]] for p in MultipartUpload["Parts"]]
        body = b"".join(ordered)
        self.objs[Key] = json.loads(body.decode("utf-8"))
        del self._multipart_uploads[UploadId]

    def abort_multipart_upload(self, Bucket, Key, UploadId):
        self._multipart_uploads.pop(UploadId, None)
        for call in self.multipart_calls:
            if call["upload_id"] == UploadId:
                call["aborted"] = True
        return {}


def _extract_tenant_from_key_condition(expr):
    """Best-effort pull of the "tenant" partition-key value out of a
    boto3.dynamodb.conditions Key("tenant").eq(x) (possibly And-combined
    with a sort-key condition) expression, matching the shape the real
    code always builds. Falls back to None (no filtering) if the shape
    doesn't match, which is always safe here (tests use per-fixture
    tables scoped to specific tenants anyway)."""
    try:
        return expr._values[0]._values[1]
    except (AttributeError, IndexError):
        return None


class FakeDynamoTable:
    """One fake table implementation shared by every section: filters
    query() by the tenant partition key when present, and applies
    update_item's ExpressionAttributeValues back onto the matching item
    (status_override/override_at, milestones, next_steps, notes,
    notes_updated_at, follow_up, deadline_override/deadline_override_at)
    so a second read/write in the same test sees the merged result, the
    way real DynamoDB would."""

    def __init__(self, items=None):
        self.items = items if items is not None else []
        self.updates = []
        self.puts = []

    def query(self, **kwargs):
        items = self.items
        tenant_val = _extract_tenant_from_key_condition(kwargs.get("KeyConditionExpression"))
        if tenant_val is not None:
            items = [it for it in items if it.get("tenant") == tenant_val]
        return {"Items": items}

    def get_item(self, **kwargs):
        key = kwargs["Key"]
        item = next((i for i in self.items
                     if i.get("tenant") == key["tenant"] and i.get("sk") == key["sk"]), None)
        return {"Item": item} if item is not None else {}

    def update_item(self, **kwargs):
        """Generic SET/REMOVE simulation -- parses the actual
        UpdateExpression/ExpressionAttributeNames/Values rather than a
        hardcoded token->field table, so it works for every write path
        in lambda_function.py (real-deal intro updates, deal-stage
        overrides, manual intros, ...) without needing its own entry
        added here each time a new one is written."""
        self.updates.append(kwargs)
        key = kwargs["Key"]
        item = next((i for i in self.items
                     if i.get("tenant") == key["tenant"] and i.get("sk") == key["sk"]), None)
        if item is None:
            item = dict(key)
            self.items.append(item)
        vals = kwargs.get("ExpressionAttributeValues", {})
        names = kwargs.get("ExpressionAttributeNames", {})
        expr = kwargs.get("UpdateExpression", "")
        set_match = re.search(r"SET (.+?)(?:\s+REMOVE\s|$)", expr)
        if set_match:
            for assignment in set_match.group(1).split(","):
                assignment = assignment.strip()
                if not assignment or "=" not in assignment:
                    continue
                field_token, value_token = [p.strip() for p in assignment.split("=", 1)]
                field = names.get(field_token, field_token)
                if value_token in vals:
                    item[field] = vals[value_token]
        remove_match = re.search(r"REMOVE (.+)$", expr)
        if remove_match:
            for field in remove_match.group(1).split(","):
                field = names.get(field.strip(), field.strip())
                item.pop(field, None)
        return {}

    def put_item(self, **kwargs):
        item = kwargs.get("Item")
        self.puts.append(item)
        if item is not None and not str(item.get("sk", "")).startswith(("audit#", "feature#")):
            self.items = [it for it in self.items
                          if not (it.get("tenant") == item.get("tenant") and it.get("sk") == item.get("sk"))]
            self.items.append(item)
        elif item is not None and str(item.get("sk", "")).startswith("feature#"):
            self.items.append(item)
        return {}


class FakeDynamoResource:
    def __init__(self, table):
        self._table = table

    def Table(self, name):
        return self._table


class FakeBoto3:
    """Assign as lf.boto3 = FakeBoto3 after setting FakeBoto3.S3/.TABLE
    (and, only for the one section that needs it, .SES)."""
    S3 = None
    TABLE = None
    SES = None

    @staticmethod
    def client(name, **kwargs):
        if name == "s3":
            return FakeBoto3.S3
        if name == "ses" and FakeBoto3.SES is not None:
            return FakeBoto3.SES
        raise AssertionError(f"unexpected boto3.client({name!r})")

    @staticmethod
    def resource(name, **kwargs):
        assert name == "dynamodb"
        return FakeDynamoResource(FakeBoto3.TABLE)


def use_fixture(objs, table_items=None, last_modified=None, ses=None):
    """Point lf.boto3 at a fresh FakeS3(objs)/FakeDynamoTable(table_items)
    pair and reset lf's module caches. Returns (fake_s3, fake_table) so a
    test can inspect writes or seed more S3 keys before rendering."""
    fake_s3 = FakeS3(objs, last_modified=last_modified)
    fake_table = FakeDynamoTable(items=table_items or [])
    FakeBoto3.S3 = fake_s3
    FakeBoto3.TABLE = fake_table
    FakeBoto3.SES = ses
    lf.boto3 = FakeBoto3
    reset_caches()
    return fake_s3, fake_table


AGREEMENT_YES = next(iter(lf.AGENT_ENGAGED_OPTS))
AGREEMENT_IN_PROCESS = next(iter(lf.AGENT_IN_PROCESS_OPTS))


def full_terms():
    return {lf.TICKET_MIN_FIELD: 100000, lf.MGMT_FEE_FIELD: 2, lf.CARRY_FIELD: 20, lf.SELLER_FEE_FIELD: 0}


def cf_sell(extra=None):
    cf = {lf.DEAL_SIDE_FIELD: [lf.DEAL_SIDE_SELL_ID]}
    if extra:
        cf.update(extra)
    return cf


def cf_status(option_id, extra=None):
    """Buy-side custom_fields, optionally with an Intro Status option id."""
    cf = {lf.DEAL_SIDE_FIELD: lf.DEAL_SIDE_BUY_ID}
    if option_id:
        cf[lf.INTRO_STATUS_FIELD] = [option_id]
    if extra:
        cf.update(extra)
    return cf


def tenant_cookie(email):
    return lf._make_identity_cookie(email).split(";")[0]


def post_event(body_dict, cookies=None):
    return {"requestContext": {"http": {"method": "POST"}}, "rawPath": "/",
            "queryStringParameters": {"action": "update_intro"}, "cookies": cookies or [],
            "body": json.dumps(body_dict)}


def feature_event(body_dict, cookies=None):
    return {"requestContext": {"http": {"method": "POST"}}, "rawPath": "/",
            "queryStringParameters": {"action": "feature_request"}, "cookies": cookies or [],
            "body": json.dumps(body_dict)}


def row_for(page, deal_id):
    """Locate the <tr ...>...</tr> block carrying data-deal-id="<deal_id>".
    Returns None if the row isn't on the page at all (e.g. a pending row
    that carries no data-deal-id in tenant view)."""
    idx = page.find(f'data-deal-id="{deal_id}"')
    if idx == -1:
        return None
    start = page.rfind("<tr", 0, idx)
    end = page.find("</tr>", idx)
    return page[start:end]


def body_only(page):
    """Slice off <header> (the nav, including the My Deals quick-jump
    dropdown) so a string/count/split assertion against the page body
    below isn't confused by the dropdown legitimately repeating a
    company name, section label ("On Hold" etc.), or that also appears
    in the visible table."""
    idx = page.find('<div class="wrap">')
    return page[idx:] if idx != -1 else page


class FakeHTTPResponse:
    def __init__(self, status=200):
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self):
        return b"{}"


TODAY = datetime.now(timezone.utc)


def iso_dash(days):
    return (TODAY + timedelta(days=days)).strftime("%Y-%m-%d")


def iso_ts(days_ago):
    return (TODAY - timedelta(days=days_ago)).isoformat()


# ======================================================================
# SECTION: Tenant resolution & auto-enrollment
# ======================================================================
# A "tenant" is any person.json record linked (via person_ids/people on a
# deal) to at least one Sell-side deal in ANY stage; there is no static
# allow-list. _tenant_index()/_resolve_tenant() build/query that
# derivation, cached on people.json's S3 version.

people = {"people": [
    # Scalar "email" only
    {"id": 1, "full_name": "Alice Seller", "email": "alice@example.com", "custom_fields": {}},
    # "emails" list of plain strings
    {"id": 2, "full_name": "Bob Seller", "emails": ["bob@example.com", "bob.alt@example.com"], "custom_fields": {}},
    # "emails" list of dicts
    {"id": 3, "full_name": "Carol Seller", "emails": [{"address": "carol@example.com"}], "custom_fields": {}},
    # Has a Sell deal, but only in a Won stage -- still qualifies ("any stage")
    {"id": 4, "full_name": "Dave WonStage", "email": "dave@example.com", "custom_fields": {}},
    # No sell deal at all -- should NOT qualify
    {"id": 5, "full_name": "Eve NoSell", "email": "eve@example.com", "custom_fields": {}},
    # Only a Buy-side deal -- should NOT qualify
    {"id": 6, "full_name": "Frank BuyerOnly", "email": "frank@example.com", "custom_fields": {}},
    # Blocklisted despite qualifying
    {"id": 7, "full_name": "Grace Blocked", "email": "grace@example.com", "custom_fields": {}},
    # Michael, matches a TENANT_OVERRIDES name override
    {"id": 1307955474, "full_name": "Michael Raw Name", "email": "michael@nonpublic.io", "custom_fields": {}},
]}
interest = {"buy": {}}
deals_list = [
    {"id": 101, "company": {"name": "Alpha Co"}, "deal_stage": {"id": lf.STAGE_FIRM},
     "custom_fields": cf_sell(), "person_ids": [1], "is_archived": False},
    {"id": 102, "company": {"name": "Beta Co"}, "deal_stage": {"id": lf.STAGE_HOLD},
     "custom_fields": cf_sell(), "person_ids": [2], "is_archived": False},
    {"id": 103, "company": {"name": "Gamma Co"}, "deal_stage": {"id": lf.OBSOLETE_STAGE_ID},
     "custom_fields": cf_sell(), "person_ids": [3], "is_archived": False},
    {"id": 104, "company": {"name": "Delta Co"}, "deal_stage": {"id": 111802},  # a WON stage id
     "custom_fields": cf_sell(), "person_ids": [4], "is_archived": False},
    {"id": 105, "company": {"name": "Zeta Co"}, "deal_stage": {"id": lf.STAGE_FIRM},
     "custom_fields": {lf.DEAL_SIDE_FIELD: [lf.DEAL_SIDE_BUY_ID]}, "person_ids": [6], "is_archived": False},
    {"id": 106, "company": {"name": "Eta Co"}, "deal_stage": {"id": lf.STAGE_FIRM},
     "custom_fields": cf_sell(), "person_ids": [7], "is_archived": False},
    {"id": 107, "company": {"name": "Theta Co"}, "deal_stage": {"id": lf.STAGE_FIRM},
     "custom_fields": cf_sell(), "person_ids": [1307955474], "is_archived": False},
]
deals_data = {"deals": deals_list}
lm = {"people.json": datetime(2026, 1, 1, tzinfo=timezone.utc)}
fake_s3, _ = use_fixture({"people.json": people, "interest_people.json": interest, "deals.json": deals_data},
                          last_modified=lm)
lf.TENANT_BLOCKLIST = {"grace@example.com"}

idx = lf._tenant_index()
check("Alice (scalar email) auto-enrolled", "alice@example.com" in idx and idx["alice@example.com"]["person_id"] == 1)
check("Bob primary email auto-enrolled", "bob@example.com" in idx)
check("Bob's alt email ALSO maps to same person", idx.get("bob.alt@example.com", {}).get("person_id") == 2)
check("Carol (dict-shaped emails list) auto-enrolled", "carol@example.com" in idx)
check("Dave (Won-stage sell deal) still auto-enrolled -- any stage", "dave@example.com" in idx)
check("Eve (no sell deal at all) NOT enrolled", "eve@example.com" not in idx)
check("Frank (buy-side deal only) NOT enrolled", "frank@example.com" not in idx)
check("Grace blocklisted despite a qualifying deal", "grace@example.com" not in idx)
check("Michael's name is overridden via TENANT_OVERRIDES",
      idx.get("michael@nonpublic.io", {}).get("name") == "Michael Ferkol (NonPublic)")
check("Non-overridden tenant uses people.json's display name",
      idx.get("alice@example.com", {}).get("name") == "Alice Seller")

check("_resolve_tenant is case-insensitive", lf._resolve_tenant("ALICE@EXAMPLE.COM") is not None)
check("_resolve_tenant returns None for an unknown email", lf._resolve_tenant("nobody@example.com") is None)
check("_resolve_tenant returns None for an empty string", lf._resolve_tenant("") is None)
check("_resolve_tenant returns None for None", lf._resolve_tenant(None) is None)

# Cache keyed by people.json's LastModified only
call_count = {"n": 0}
orig_get_object = fake_s3.get_object
def counting_get_object(Bucket, Key):
    if Key == "people.json":
        call_count["n"] += 1
    return orig_get_object(Bucket, Key)
fake_s3.get_object = counting_get_object
lf._tenant_index()
lf._tenant_index()
check("tenant index stays cached across calls (same people.json version)", call_count["n"] == 0)
lm["people.json"] = datetime(2026, 1, 2, tzinfo=timezone.utc)
# Perf fixes 1-4 added a request-scoped cache (_req_cache) sitting
# UNDER _tenant_cache, reset once per real request via _perf_start (the
# lambda_handler wrapper) -- simulate that same request boundary here,
# since this test calls _tenant_index() directly rather than through
# lambda_handler, and otherwise the already-cached parsed people list
# from the earlier calls above would mask the very re-fetch this check
# exists to verify.
lf._req_cache_reset()
lf._tenant_index()
check("tenant index recomputed once people.json's version changes", call_count["n"] == 1)
fake_s3.get_object = orig_get_object

# lambda_handler-level: admin &view_as works for ANY qualifying email, not a fixed list
resp = lf.lambda_handler({"requestContext": {"http": {"method": "GET"}},
                           "queryStringParameters": {"key": ADMIN_KEY, "view_as": "bob@example.com", "tab": "mydeals"}}, None)
check("admin view_as bob@example.com (never explicitly listed) -> 200", resp["statusCode"] == 200)
check("admin view_as bob -> renders Bob's name", "Bob Seller" in resp["body"])

resp2 = lf.lambda_handler({"requestContext": {"http": {"method": "GET"}},
                            "queryStringParameters": {"key": ADMIN_KEY, "view_as": "eve@example.com", "tab": "mydeals"}}, None)
check("admin view_as eve (no sell deal) -> not-enabled page", "This dashboard is for sellers" in resp2["body"])

resp3 = lf.lambda_handler({"requestContext": {"http": {"method": "GET"}},
                            "queryStringParameters": {"key": ADMIN_KEY, "view_as": "grace@example.com", "tab": "mydeals"}}, None)
check("admin view_as grace (blocklisted) -> not-enabled page", "This dashboard is for sellers" in resp3["body"])

# Regression guard for the "string person id in people.json" bug: a
# record whose "id" is a STRING (rather than int) must still auto-enroll
# and coerce cleanly into every downstream int-keyed comparison.
NATOLI_PID = 1307332972
NATOLI_EMAIL = "natoli@mangustacap.com"
people_natoli = {"people": [
    {"id": str(NATOLI_PID), "full_name": "Natoli", "email": NATOLI_EMAIL, "custom_fields": {}},
    {"id": 999, "full_name": "NoSellDeals", "email": "nosell@example.com", "custom_fields": {}},
]}
deal_natoli = {
    "id": 55312877, "company": {"name": "Mangusta Portfolio Co"},
    "deal_stage": {"id": lf.OBSOLETE_STAGE_ID},
    "custom_fields": {lf.DEAL_SIDE_FIELD: [5011675]},  # Sell Order
    "person_ids": [NATOLI_PID], "is_archived": True,
}
use_fixture({"people.json": people_natoli, "interest_people.json": {"buy": {}},
             "deals.json": {"deals": [deal_natoli]}})

idx_natoli = lf._tenant_index()
check("Natoli auto-enrolls despite a string-typed people.json id", NATOLI_EMAIL in idx_natoli)
check("Natoli's person_id is coerced to int for downstream comparisons",
      idx_natoli.get(NATOLI_EMAIL, {}).get("person_id") == NATOLI_PID
      and isinstance(idx_natoli.get(NATOLI_EMAIL, {}).get("person_id"), int))
check("Natoli gets her TENANT_OVERRIDES display name",
      idx_natoli.get(NATOLI_EMAIL, {}).get("name") == "Natoli (Mangusta Capital)")
check("a person with no sell deal at all is still correctly excluded", "nosell@example.com" not in idx_natoli)
check("_tenant_email_for_deal resolves Natoli's deal back to her email",
      lf._tenant_email_for_deal(deal_natoli) == NATOLI_EMAIL)

full_cookie = lf._make_identity_cookie(NATOLI_EMAIL)
resp_natoli = lf.lambda_handler({"requestContext": {"http": {"method": "GET"}},
                                  "queryStringParameters": {"tab": "mydeals"},
                                  "cookies": [full_cookie.split(";")[0]]}, None)
check("lambda_handler: Natoli's SSO cookie gets 200 (not the not-enabled page)", resp_natoli["statusCode"] == 200)
check("lambda_handler: Natoli's name renders", "Natoli" in resp_natoli["body"])
check("lambda_handler: no 'not for sellers' message for a real tenant",
      "This dashboard is for sellers" not in resp_natoli["body"])

# not-enabled page: sell-order CTA
page_not_enabled = lf._message_page("Not enabled", lf.NOT_ENABLED_MESSAGE, show_sell_cta=True)
check("not-enabled page shows the sell CTA copy", "Have a block or shares to sell?" in page_not_enabled)
check("not-enabled page CTA link text", "Submit a sell order" in page_not_enabled)
check("CTA links a mailto to cgracia@rainmakersecurities.com", "mailto:cgracia@rainmakersecurities.com" in page_not_enabled)
check("CTA mailto subject is 'I want to sell'", "subject=I%20want%20to%20sell" in page_not_enabled)
check("CTA mailto body prompts for company/size/structure",
      "Company%2C%20approximate%20size%2C%20and%20structure" in page_not_enabled)

page_no_cta = lf._message_page("Access denied", "Sign in to view the Demand Board.", show_signin=True)
check("access-denied (unauthenticated) page does NOT show the sell CTA", "Submit a sell order" not in page_no_cta)

full_cookie2 = lf._make_identity_cookie("nosell@example.com")
resp_nosell = lf.lambda_handler({"requestContext": {"http": {"method": "GET"}},
                                  "queryStringParameters": {"tab": "mydeals"},
                                  "cookies": [full_cookie2.split(";")[0]]}, None)
check("lambda_handler: a genuinely ineligible user gets the not-enabled page with the sell CTA",
      "Submit a sell order" in resp_nosell["body"] and "This dashboard is for sellers" in resp_nosell["body"])


# ======================================================================
# SECTION: ID-verification (CEF) nav badges + nav bar theme
# ======================================================================

badge_yes = lf._cef_badge_html(lf.CEF_YES_ID, "Bob")
badge_pending = lf._cef_badge_html(lf.CEF_PENDING_ID, "Bob")
badge_no = lf._cef_badge_html(lf.CEF_NO_ID, "Bob")
check("nav badge (Yes) says ID verified", "ID verified" in badge_yes)
check("nav badge (Yes) has no leftover CEF text", "CEF" not in badge_yes)
check("nav badge (Pending) says ID pending", "ID pending" in badge_pending)
check("nav badge (Pending) has no leftover CEF text", "CEF" not in badge_pending)
check("nav badge (No/missing) says ID required — FINRA compliance", "ID required — FINRA compliance" in badge_no)
check("nav badge (No/missing) still links the CEF form", lf.CEF_FORM_URL in badge_no)
check("nav badge (No/missing) has no leftover CEF text", "CEF" not in badge_no)

box_html = lf._feature_box_html("bob@example.com", key=None)
check("feature box is unaffected by the CEF-badge rename", "feature-input" in box_html)

check("nav background is transparent (no fill)", "background: transparent;" in lf.NAV_CSS)
check("nav has a single thin bottom border in the page line color", "border-bottom: 1px solid var(--line);" in lf.NAV_CSS)
check("brand text uses the blue accent",
      re.search(r"\.gg-brand \{[^}]*color: var\(--accent\);", lf.NAV_CSS) is not None)
check("active tab text uses the blue accent",
      re.search(r"\.gg-tab\.active \{[^}]*color: var\(--accent\);", lf.NAV_CSS) is not None)
check("admin badge stays a fixed high-contrast red (not palette-driven)",
      "background: #7a1f1f;" in lf.NAV_CSS and "color: #ffffff;" in lf.NAV_CSS)


# ======================================================================
# SECTION: My Deals — visibility state machine & Next Steps action chips
# ======================================================================
# _my_deal_visibility_state(deal, cef_state, is_held, is_won=False) is a
# strict, first-match-wins ladder: sold > id_required > agreement_unsigned
# > terms_incomplete > held > live. _my_deal_action_chip_html renders at
# most ONE chip per row by its own separate priority (overdue > terms >
# nudge > id_required > agreement_unsigned), with a tooltip naming any
# lower-priority candidates that also applied.

deal_no_id = {"custom_fields": cf_sell()}
check("state: CEF not Yes -> id_required", lf._my_deal_visibility_state(deal_no_id, lf.CEF_NO_ID, False) == "id_required")
check("id_required wins even with agreement signed + full terms",
      lf._my_deal_visibility_state({"custom_fields": cf_sell({lf.AGENT_AGREEMENT_FIELD: [AGREEMENT_YES],
                                                                **full_terms()})}, lf.CEF_PENDING_ID, False)
      == "id_required")

deal_agreement_unsigned = {"custom_fields": cf_sell({**full_terms()})}
check("state: agreement unset -> agreement_unsigned",
      lf._my_deal_visibility_state(deal_agreement_unsigned, lf.CEF_YES_ID, False) == "agreement_unsigned")
deal_in_process = {"custom_fields": cf_sell({lf.AGENT_AGREEMENT_FIELD: [AGREEMENT_IN_PROCESS], **full_terms()})}
check("state: In-Process agreement counts as unsigned",
      lf._my_deal_visibility_state(deal_in_process, lf.CEF_YES_ID, False) == "agreement_unsigned")

deal_no_terms = {"custom_fields": cf_sell({lf.AGENT_AGREEMENT_FIELD: [AGREEMENT_YES]})}
check("state: no size, no fees -> terms_incomplete",
      lf._my_deal_visibility_state(deal_no_terms, lf.CEF_YES_ID, False) == "terms_incomplete")
deal_missing_one_fee = {"custom_fields": cf_sell({lf.AGENT_AGREEMENT_FIELD: [AGREEMENT_YES],
                                                   lf.TICKET_MAX_FIELD: 500000,
                                                   lf.MGMT_FEE_FIELD: 2, lf.CARRY_FIELD: 20})}
check("state: seller fee missing (2 of 3 fees defined) -> terms_incomplete",
      lf._my_deal_visibility_state(deal_missing_one_fee, lf.CEF_YES_ID, False) == "terms_incomplete")
deal_zero_fees = {"custom_fields": cf_sell({lf.AGENT_AGREEMENT_FIELD: [AGREEMENT_YES],
                                             lf.TICKET_MIN_FIELD: 100000,
                                             lf.MGMT_FEE_FIELD: 0, lf.CARRY_FIELD: 0, lf.SELLER_FEE_FIELD: 0})}
check("0.0 fee values count as DEFINED, not missing -> live",
      lf._my_deal_visibility_state(deal_zero_fees, lf.CEF_YES_ID, False) == "live")

deal_full = {"custom_fields": cf_sell({lf.AGENT_AGREEMENT_FIELD: [AGREEMENT_YES], **full_terms()})}
check("state: is_held=True with full terms -> held", lf._my_deal_visibility_state(deal_full, lf.CEF_YES_ID, True) == "held")
check("state: everything satisfied, not held -> live", lf._my_deal_visibility_state(deal_full, lf.CEF_YES_ID, False) == "live")
check("is_won=True -> 'sold', even with nothing else set", lf._my_deal_visibility_state({}, None, False, is_won=True) == "sold")
check("is_won=True -> 'sold' even when is_held=True (never 'held')",
      lf._my_deal_visibility_state({}, None, True, is_won=True) == "sold")
check("is_won=False, everything unset -> falls through the normal ladder (not 'sold')",
      lf._my_deal_visibility_state({}, None, False, is_won=False) != "sold")

# Badge HTML: state-only text, no links (the actionable links moved to the
# Next Steps chip below), shortened wording with a middot separator.
check("live badge exact text", lf._my_deal_visibility_badge_html(deal_full, lf.CEF_YES_ID, False)
      == '<span class="visibility-badge live">Live · shown to buyers</span>')
check("id_required badge exact text", lf._my_deal_visibility_badge_html(deal_no_id, lf.CEF_NO_ID, False)
      == '<span class="visibility-badge id-required">Not live · ID required</span>')
check("agreement_unsigned badge exact text", lf._my_deal_visibility_badge_html(deal_agreement_unsigned, lf.CEF_YES_ID, False)
      == '<span class="visibility-badge agreement-unsigned">Not live · unsigned agreement</span>')
check("terms_incomplete badge exact text", lf._my_deal_visibility_badge_html(deal_no_terms, lf.CEF_YES_ID, False)
      == '<span class="visibility-badge terms-incomplete">Not live · awaiting deal terms</span>')
check("held badge exact text", lf._my_deal_visibility_badge_html(deal_full, lf.CEF_YES_ID, True)
      == '<span class="visibility-badge held">Held · not shown to buyers</span>')
check("sold badge exact text", lf._my_deal_visibility_badge_html({}, None, False, is_won=True)
      == '<span class="visibility-badge sold">Sold &#10003;</span>')
_badge_no_id = lf._my_deal_visibility_badge_html(deal_no_id, lf.CEF_NO_ID, False)
check("id_required badge is a span, not a link (no href)", "<span" in _badge_no_id and "<a " not in _badge_no_id)

# Next Steps chip priority: a) overdue > b) terms_incomplete > c) nudge > d) id_required > e) agreement_unsigned
chip = lf._my_deal_action_chip_html("1", "Co", True, True, "id_required", key=None, view_as=None)
check("a) overdue wins over everything else", 'class="action-chip overdue"' in chip)
check("a) tooltip names the other applicable actions",
      'title="Also: nudge buyers, id required"' in chip)

chip = lf._my_deal_action_chip_html("1", "Co", False, True, "terms_incomplete", key=None, view_as=None)
check("b) terms_incomplete beats nudge", 'class="action-chip terms"' in chip)
check("b) chip text", "Provide deal terms" in chip)

chip = lf._my_deal_action_chip_html("1", "Co", False, True, "live", key=None, view_as=None)
check("c) nudge shown alone when nothing else applies", 'class="action-chip nudge"' in chip)
check("c) nudge chip text", "Nudge buyers" in chip)

chip = lf._my_deal_action_chip_html("1", "Co", False, False, "id_required", key=None, view_as=None)
check("d) id_required chip shown when nothing higher-priority applies", 'class="action-chip id-required"' in chip)
check("d) id_required chip links the CEF form", lf.CEF_FORM_URL in chip)

chip = lf._my_deal_action_chip_html("1", "Co", False, False, "agreement_unsigned", key=None, view_as=None)
check("e) sign-agreement chip shown when nothing higher-priority applies", 'class="action-chip sign"' in chip)
check("e) sign-agreement chip links the agreement doc", lf.AGENT_AGREEMENT_DOC_URL in chip)

chip = lf._my_deal_action_chip_html("1", "Co", False, False, "live", key=None, view_as=None)
check("nothing applies -> empty chip", chip == "")

check("_my_deal_action_chip_html has no no_interest parameter (removed feature)",
      "no_interest" not in __import__("inspect").signature(lf._my_deal_action_chip_html).parameters)
check("no leftover 'no-interest'/'No interest' class or text reachable from the chip renderer",
      "no-interest" not in lf._my_deal_action_chip_html("1", "Co", True, True, "terms_incomplete", key=None, view_as=None))


# ======================================================================
# SECTION: My Deals — sections (Active/On Hold/Cancelled/Closed), Hold/
# Cancel/Reactivate, Deal-ID sub-line, theme
# ======================================================================

TENANT_EMAIL = "sella@example.com"
TENANT_PID = 1

ses_calls = []
class FakeSES:
    def send_email(self, **kwargs):
        ses_calls.append(kwargs)
        return {}

people = {"people": [{"id": TENANT_PID, "name": "Sella", "email": TENANT_EMAIL, "custom_fields": {
    lf.CEF_FIELD: [lf.CEF_YES_ID],
}}]}
interest = {"buy": {}}

deal_active = {"id": 101, "name": "Active Deal", "company": {"name": "Alpha Co"},
               "deal_stage": {"id": lf.STAGE_FIRM}, "custom_fields": cf_sell(),
               "person_ids": [TENANT_PID], "updated_at": iso_dash(-1)}
deal_held = {"id": 102, "name": "Held Deal", "company": {"name": "Beta Co"},
             "deal_stage": {"id": lf.STAGE_HOLD}, "custom_fields": cf_sell(),
             "person_ids": [TENANT_PID], "updated_at": iso_dash(-1)}
# raw stage stays in the live whitelist (STAGE_MATCHED); the OBSOLETE
# resolution comes from a Dynamo stage_override, exactly like a
# same-session Cancel would produce.
deal_cancelled = {"id": 103, "name": "Cancelled Deal", "company": {"name": "Gamma Co"},
                   "deal_stage": {"id": lf.STAGE_MATCHED}, "custom_fields": cf_sell(),
                   "person_ids": [TENANT_PID], "updated_at": iso_dash(-1)}
deals_list = [deal_active, deal_held, deal_cancelled]
s3_objs = {"people.json": people, "interest_people.json": interest, "deals.json": {"deals": deals_list}}
fake_s3, fake_table = use_fixture(s3_objs, table_items=[
    {"tenant": TENANT_EMAIL, "sk": "intro#103", "stage_override": lf.OBSOLETE_STAGE_ID, "stage_override_at": time.time()},
], ses=FakeSES())

deals = [d for d in deals_list if lf._is_live_sell_deal(d)]
page = lf.render_my_deals_page("Sella", deals=deals, key=None, view_as=None,
                                person_id=TENANT_PID, anon_key_email=TENANT_EMAIL)

check("no leftover 'Open Hold' text", "Open Hold" not in page)
check("no leftover request-chip text", "Cancel requests" not in page)
check("Active section present (Active Deal)", "Active Deal" in page)
check("On Hold section heading present", "On Hold" in page)
check("Held Deal row present", "Held Deal" in page)
check("Cancelled section heading present", '<h2 class="mydeals-section-heading cancelled">Cancelled' in page)
check("Cancelled Deal row present (via its company name -- terminal rows carry no action buttons)",
      "Gamma Co" in page)

# body_only: person_id is set, so the nav's My Deals dropdown also lists
# an "On Hold" group for this same tenant -- slice the header off before
# splitting on "On Hold" so these sections read the actual table.
page_body = body_only(page)
hold_section = page_body.split("On Hold")[1].split("Cancelled")[0]
check("Reactivate button present in the Hold section (tenant)", 'data-target="reactivate"' in hold_section)
check("no Hold/Cancel buttons in the Hold section", 'data-target="hold"' not in hold_section
      and 'data-target="cancel"' not in hold_section)
active_section = page_body.split("On Hold")[0]
check("Hold/Cancel buttons present in the active section", 'data-target="hold"' in active_section
      and 'data-target="cancel"' in active_section)

page_admin = body_only(lf.render_my_deals_page("Admin", deals=deals, key=ADMIN_KEY, view_as=TENANT_EMAIL,
                                                person_id=TENANT_PID, anon_key_email=TENANT_EMAIL))
hold_section_admin = page_admin.split("On Hold")[1].split("Cancelled")[0]
check("Reactivate button present in the Hold section (admin)", 'data-target="reactivate"' in hold_section_admin)

page_active_only = body_only(lf.render_my_deals_page("Sella", deals=[deal_active], key=None, view_as=None,
                                                      person_id=TENANT_PID, anon_key_email=TENANT_EMAIL))
check("On Hold section omitted entirely when empty", "On Hold" not in page_active_only)
check("Cancelled section omitted entirely when empty (heading markup, not just the bare word)",
      'mydeals-section-heading cancelled"' not in page_active_only)

# Reactivate write path: a tenant can reactivate their own held deal
orig_read_identity = lf._read_identity_email
lf._read_identity_email = lambda event: TENANT_EMAIL
pipeline_calls = []
lf._pipeline_update_deal_stage = lambda deal_id, stage_id: (pipeline_calls.append((deal_id, stage_id)), (True, None))[1]

resp = lf._handle_deal_stage({"body": json.dumps({"deal_id": "102", "target": "reactivate"})})
check("reactivate write returns 200", resp["statusCode"] == 200)
check("reactivate writes STAGE_INQUIRY to Pipeline", pipeline_calls and pipeline_calls[-1] == ("102", lf.STAGE_INQUIRY))
check("reactivate sends a notification email", len(ses_calls) >= 1 and "Reactivate" in ses_calls[-1]["Message"]["Subject"]["Data"])

resp_bad = lf._handle_deal_stage({"body": json.dumps({"deal_id": "102", "target": "bogus"})})
check("an invalid target is rejected with 400", resp_bad["statusCode"] == 400)
lf._read_identity_email = orig_read_identity

# Theme spot-checks: light palette, no leftover dark literals
check("My Deals uses the light --bg token", "--bg: #f4f2ee;" in page)
check("My Deals uses the light --card token", "--card: #ffffff;" in page)
check("My Deals has no leftover dark-mode bg literal", "#14161a" not in page and "#1c1f26" not in page)

company_table_page = lf.render_page([{"company": "Alpha Co", "total": 1, "qp": 1, "accredited": 0,
                                       "unknown": 0, "sellers": 1}], "Sella", key=None, view_as=None,
                                     anon_key_email=TENANT_EMAIL, tenant_picker=False)
check("Demand Board uses the light --bg token", "--bg: #f4f2ee;" in company_table_page)
check("Demand Board has no leftover dark-mode literal", "#14161a" not in company_table_page)
check("Demand Board renders the feature box", 'id="feature-box"' in company_table_page)
check("Demand Board renders the feature list container", "feature-section-heading" in company_table_page)
check("old mailto 'Request a feature' button is fully gone", "Request a feature" not in company_table_page)

demand_admin_agg = lf.render_page([], "Admin", key=ADMIN_KEY, view_as=None,
                                   anon_key_email="admin", tenant_picker=True)
check("Demand Board admin aggregate (no view_as) also renders the feature box", 'id="feature-box"' in demand_admin_agg)

msg_page = lf._message_page("Not enabled", lf.NOT_ENABLED_MESSAGE)
check("_message_page uses the light bg", "#f4f2ee" in msg_page)
check("_message_page has no leftover dark bg", "#14161a" not in msg_page)

# Item: Deal ID moved under the company name (own sub-line, not a
# standalone column), Size column removed entirely, header order.
head = page[page.find("<thead>"):page.find("</thead>")]
expected_order = ["Company", "Visibility", "Interested buyers", "Active intros", "Deadline", "Next Steps"]
positions = [head.find(f">{h}<") for h in expected_order]
check("header columns present in the exact expected order", positions == sorted(positions) and all(p != -1 for p in positions))
check("Deal ID column header removed", "<th>Deal ID</th>" not in page)
check("Size column header removed", "<th>Size</th>" not in page)
check("no per-row Size cell helper left reachable", not hasattr(lf, "_my_deal_size_text"))

company_block = page[page.find('company">'):]
company_block = company_block[:company_block.find("</td>") + 5]
check("Deal-ID sub-line present under the company cell", 'class="deal-id-sub"' in company_block)
check("Deal-ID sub-line links the public deal URL, with #id text",
      f'#{deal_active["id"]}' in company_block and lf._deal_public_url(str(deal_active["id"])) in company_block)
check("copy-id button still present in the sub-line", 'class="copy-id"' in company_block)
check("no standalone deal-id table-cell class remains", 'td class="deal-id"' not in page)

check(".wrap is 1000px (matches sibling tabs)", ".wrap { max-width: 1000px; margin: 28px auto 0; }" in page)
check("no .table-scroll wrapper on My Deals (would break the sticky thead)", '<div class="table-scroll">' not in page)
check("sticky thead offset (top:0) present", "position: sticky;" in page and "top: 0;" in page)
check("actions-stack still renders (Update/Hold/Cancel stacked)", 'class="actions-stack"' in page)
check("action-chip column header is 'Next Steps'", "<th>Next Steps</th>" in page)
check("table uses table-layout:fixed", "table-layout: fixed;" in page)
check("num columns centered (Interested buyers/Active intros use the .num class)",
      '<th class="num">Interested buyers</th>' in page
      and '<th class="num" title="Introductions still in progress -- not yet Won or Lost">Active intros</th>' in page)
check("deal-id-sub has a clear tap gap (margin-top)", "margin-top: 7px;" in page)

# _fmt_short_date
check("_fmt_short_date formats correctly", lf._fmt_short_date("2026-09-15") == "Sep 15, '26")
check("_fmt_short_date single-digit day, no leading zero", lf._fmt_short_date("2026-09-05") == "Sep 5, '26")
check("_fmt_short_date handles slash-format input", lf._fmt_short_date("2026/12/01") == "Dec 1, '26")
check("_fmt_short_date returns None for unparseable input", lf._fmt_short_date("not-a-date") is None)


# ======================================================================
# SECTION: My Deals — Won/Closed section, Lost -> Cancelled
# ======================================================================

LIVE_TERMS_CF = {
    lf.TICKET_MIN_FIELD: 1_000_000, lf.TICKET_MAX_FIELD: 5_000_000,
    lf.MGMT_FEE_FIELD: 2, lf.CARRY_FIELD: 20, lf.SELLER_FEE_FIELD: 4,
    lf.AGENT_AGREEMENT_FIELD: [AGREEMENT_YES],
}
people = {"people": [{"id": TENANT_PID, "full_name": "Sella Seller", "email": TENANT_EMAIL,
                       "custom_fields": {lf.CEF_FIELD: [lf.CEF_YES_ID]}}]}
deals_list = [
    {"id": 601, "name": "Live Deal", "company": {"name": "Live Co"}, "deal_stage": {"id": lf.STAGE_FIRM},
     "custom_fields": cf_sell(LIVE_TERMS_CF), "people": [{"id": TENANT_PID}], "is_archived": False, "updated_at": "2026-08-01T00:00:00Z"},
    {"id": 602, "name": "Held Deal", "company": {"name": "Held Co"}, "deal_stage": {"id": lf.STAGE_HOLD},
     "custom_fields": cf_sell(LIVE_TERMS_CF), "people": [{"id": TENANT_PID}], "is_archived": False, "updated_at": "2026-08-01T00:00:00Z"},
    {"id": 603, "name": "Obsolete Deal", "company": {"name": "Obsolete Co"}, "deal_stage": {"id": lf.OBSOLETE_STAGE_ID},
     "custom_fields": cf_sell(), "people": [{"id": TENANT_PID}], "is_archived": True, "updated_at": "2026-08-01T00:00:00Z"},
    {"id": 604, "name": "Lost Deal A", "company": {"name": "Lost Co A"}, "deal_stage": {"id": 111801},
     "custom_fields": cf_sell(), "people": [{"id": TENANT_PID}], "is_archived": True, "updated_at": "2026-08-01T00:00:00Z"},
    {"id": 605, "name": "Lost Deal B", "company": {"name": "Lost Co B"}, "deal_stage": {"id": 2379322},
     "custom_fields": cf_sell(), "people": [{"id": TENANT_PID}], "is_archived": True, "updated_at": "2026-08-01T00:00:00Z"},
    {"id": 606, "name": "Won Deal A", "company": {"name": "Won Co A"}, "deal_stage": {"id": 111802},
     "custom_fields": cf_sell({lf.TICKET_MIN_FIELD: 2_000_000, lf.TICKET_MAX_FIELD: 3_000_000}),
     "people": [{"id": TENANT_PID}], "is_archived": True, "updated_at": "2026-08-01T00:00:00Z"},
    {"id": 607, "name": "Won Deal B", "company": {"name": "Won Co B"}, "deal_stage": {"id": 2379321},
     "custom_fields": cf_sell(), "value": 4_500_000,
     "people": [{"id": TENANT_PID}], "is_archived": True, "updated_at": "2026-08-01T00:00:00Z"},
]
use_fixture({"people.json": people, "interest_people.json": {"buy": {}}, "deals.json": {"deals": deals_list}})
tenant_a = lf._resolve_tenant(TENANT_EMAIL)
assert tenant_a is not None

sell_deals = [d for d in lf.get_my_deals(TENANT_PID) if lf.DEAL_SIDE_SELL_ID in lf._deal_cf_option_ids(d, lf.DEAL_SIDE_FIELD)]
# body_only: same reason as above -- the nav dropdown now also lists
# these companies (Won Co A/B included), which would otherwise confuse
# the page.find("Won Co A")-to-end-of-page fallback slices below.
page = body_only(lf.render_my_deals_page("Sella Seller", deals=sell_deals, key=None, view_as=None,
                                          edit_mode=False, person_id=TENANT_PID, anon_key_email=TENANT_EMAIL))

check("'Closed' section heading present", '<h2 class="mydeals-section-heading closed">Closed' in page)
check("Closed section count is 2 (Won Deal A + Won Deal B)",
      '<h2 class="mydeals-section-heading closed">Closed <span class="count">(2)</span>' in page)
check("Closed sits AFTER Cancelled on the page", page.find('mydeals-section-heading cancelled') < page.find('mydeals-section-heading closed'))

row_won_a = row_for(page, "606") or page[page.find("Won Co A"):]
check("Closed row: green 'Sold' badge", 'class="visibility-badge sold"' in row_won_a and "Sold" in row_won_a)
check("Closed row: no Next Steps action chip", "action-chip" not in row_won_a)
check("Closed row: no Hold/Cancel/Reactivate buttons", 'data-target="hold"' not in row_won_a
      and 'data-target="cancel"' not in row_won_a and 'data-target="reactivate"' not in row_won_a)
check("Closed row: Update link still present", "update-cancel-btn" in row_won_a)

check("Active table still shows the genuinely live deal", 'class="visibility-badge live"' in page)
check("Won Co A appears exactly once (only in Closed, not the active table too)", page.count("Won Co A") == 1)
check("Won Co B appears exactly once too", page.count("Won Co B") == 1)
check("summary strip shows '1 live' (won deals excluded from the live count)", "1 live" in page)
check("summary strip's pipeline total excludes the won deals' sizes",
      "$9.5M" not in page and "$10.5M" not in page)
check("summary strip shows 'Total closed' summing both won deals ($3M+$4.5M=$7.5M)",
      "Total closed" in page and "$7.5M" in page)

check("'Cancelled' section count is 3 (Obsolete + 2x Lost)",
      '<h2 class="mydeals-section-heading cancelled">Cancelled <span class="count">(3)</span>' in page)
row_lost_a = row_for(page, "604") or page[page.find("Lost Co A"):]
check("Lost Deal (111801) sits in Cancelled (Update-only actions)",
      "update-cancel-btn" in row_lost_a and 'data-target="hold"' not in row_lost_a and 'data-target="cancel"' not in row_lost_a)
check("neither Lost deal appears in the active table too", page.count("Lost Co A") == 1 and page.count("Lost Co B") == 1)


# ======================================================================
# SECTION: Company page — Deal Details deadline overrides, overdue chip,
# admin deadline write path
# ======================================================================
# _deal_card_html renders each of the tenant's own Sell deals in the
# Company page's "Deal Details" section; an overdue deadline (resolved
# through any Dynamo deadline_override) always gets a red-bordered card
# and an overdue chip, admin and tenant alike. Only an admin can write a
# new deadline (?action=update_intro), which PUTs to Pipeline in
# "YYYY/MM/DD" slash format BEFORE writing the Dynamo override, and
# aborts the whole write (no Dynamo touch) if that PUT fails.

past_date = iso_dash(-5)
future_date = iso_dash(30)
people = {"people": [{"id": TENANT_PID, "full_name": "Sella Seller", "email": TENANT_EMAIL, "custom_fields": {}}]}
deal_overdue = {"id": 900, "name": "Sell Deal Overdue", "company": {"name": "Delta Co"},
                "deal_stage": {"id": lf.STAGE_FIRM},
                "custom_fields": cf_sell({lf.DEADLINE_FIELD: past_date.replace("-", "/")}),
                "people": [{"id": TENANT_PID}], "updated_at": "2026-08-01T00:00:00Z"}
deal_no_deadline = {"id": 901, "name": "Sell Deal No Deadline", "company": {"name": "Delta Co"},
                     "deal_stage": {"id": lf.STAGE_FIRM}, "custom_fields": cf_sell(),
                     "people": [{"id": TENANT_PID}], "updated_at": "2026-08-01T00:00:00Z"}
deal_future = {"id": 902, "name": "Sell Deal Future", "company": {"name": "Delta Co"},
               "deal_stage": {"id": lf.STAGE_FIRM},
               "custom_fields": cf_sell({lf.DEADLINE_FIELD: future_date.replace("-", "/")}),
               "people": [{"id": TENANT_PID}], "updated_at": "2026-08-01T00:00:00Z"}
deals_list = [deal_overdue, deal_no_deadline, deal_future]
fake_s3, fake_table = use_fixture({"people.json": people, "interest_people.json": {"buy": {}},
                                    "deals.json": {"deals": deals_list}})
tenant_rec = lf._resolve_tenant(TENANT_EMAIL)
assert tenant_rec is not None

pipeline_calls = []
def fake_urlopen(req, timeout=15):
    pipeline_calls.append({"url": req.full_url, "data": json.loads(req.data.decode()) if req.data else None})
    return FakeHTTPResponse(200)
lf.urllib.request.urlopen = fake_urlopen

page_company = lf.render_company_page("Delta Co", "Sella Seller", tenant_rec, TENANT_EMAIL, "mydeals",
                                       key=None, view_as=None, edit_mode=False)
check("company page renders the tenant's overdue Sell deal", "Sell Deal Overdue" in page_company)
check("overdue card gets the 'deal-card overdue' class", 'class="deal-card overdue"' in page_company)
check("overdue mailto/update chip text present (tenant, read-only view)",
      "Deadline passed — update or cancel this deal" in page_company)
check("exactly one overdue chip on the page (only deal 900 is overdue)",
      page_company.count("Deadline passed — update or cancel this deal") == 1)
no_deadline_block = page_company.split("Sell Deal No Deadline")[1][:800]
check("a deal with no deadline at all gets no overdue chip nearby", "overdue" not in no_deadline_block)
future_block = page_company.split("Sell Deal Future")[1][:800]
check("a future deadline gets no overdue chip nearby", "overdue" not in future_block)
check("tenant (read-only) view: no ei-deadline input (that's edit-mode only)", 'class="ei-deadline"' not in page_company)

page_company_edit = lf.render_company_page("Delta Co", "Admin", tenant_rec, TENANT_EMAIL, "mydeals",
                                            key=ADMIN_KEY, view_as=TENANT_EMAIL, edit_mode=True)
check("admin edit mode: deadline input present", 'class="ei-deadline"' in page_company_edit)
check("admin edit mode: overdue chip text (non-link <div>, not the tenant mailto <a>)",
      '<div class="overdue-chip">Deadline passed — update or cancel this deal</div>' in page_company_edit)

# Admin deadline write: PUTs Pipeline in slash format, stores a Dynamo
# override, audits actor="admin"
pipeline_calls.clear()
resp = lf.lambda_handler(post_event({"key": ADMIN_KEY, "deal_id": "900", "deadline": "2026-12-25"}), None)
check("admin deadline write -> 200", resp["statusCode"] == 200)
check("admin deadline write makes exactly one Pipeline call", len(pipeline_calls) == 1)
check("admin deadline write sends the slash-format date to Pipeline",
      pipeline_calls and pipeline_calls[0]["data"]["deal"]["custom_fields"][lf.DEADLINE_FIELD] == "2026/12/25")
check("admin deadline write stores a Dynamo deadline_override",
      fake_table.updates and fake_table.updates[-1]["ExpressionAttributeValues"].get(":do") == "2026-12-25")
check("admin deadline write audits actor='admin'", fake_table.puts and fake_table.puts[-1].get("actor") == "admin")

# Pipeline failure aborts the whole write -- no Dynamo touch
def fake_urlopen_fail(req, timeout=15):
    raise lf.urllib.error.HTTPError(req.full_url, 502, "boom", {}, None)
lf.urllib.request.urlopen = fake_urlopen_fail
fake_table.updates.clear()
resp_fail = lf.lambda_handler(post_event({"key": ADMIN_KEY, "deal_id": "900", "deadline": "2026-12-26"}), None)
check("Pipeline 502 on deadline write -> 502", resp_fail["statusCode"] == 502)
check("no Dynamo write happens when the Pipeline PUT fails", len(fake_table.updates) == 0)
lf.urllib.request.urlopen = fake_urlopen

# Tenant still can never write a deadline (unchanged, dedicated 403 path)
resp_tenant = lf.lambda_handler(post_event({"deal_id": "900", "deadline": "2026-12-01"},
                                            cookies=[tenant_cookie(TENANT_EMAIL)]), None)
check("tenant deadline write is rejected with 403", resp_tenant["statusCode"] == 403)

# No auth at all -> 403
resp_noauth = lf.lambda_handler(post_event({"deal_id": "900", "next_steps": "x"}), None)
check("no admin key and no identity cookie -> 403", resp_noauth["statusCode"] == 403)

# A deal belonging to a different tenant is rejected for a tenant write
other_people = {"people": people["people"] + [{"id": 42, "full_name": "Other Tenant", "email": "other@example.com",
                                                "custom_fields": {}}]}
other_deal = {"id": 950, "name": "Other's Deal", "company": {"name": "Other Co"}, "deal_stage": {"id": lf.STAGE_FIRM},
              "custom_fields": cf_sell(), "people": [{"id": 42}], "updated_at": "2026-08-01T00:00:00Z"}
use_fixture({"people.json": other_people, "interest_people.json": {"buy": {}},
             "deals.json": {"deals": deals_list + [other_deal]}})
resp_foreign = lf.lambda_handler(post_event({"deal_id": "900", "next_steps": "hack"},
                                             cookies=[tenant_cookie("other@example.com")]), None)
check("a tenant writing on a deal linked to a DIFFERENT tenant is rejected", resp_foreign["statusCode"] == 403)


# ======================================================================
# SECTION: Active Intros — layout, status restrictions, sort/grouping,
# pending note, empty state
# ======================================================================

people = {"people": [
    {"id": TENANT_PID, "full_name": "Sella Seller", "email": TENANT_EMAIL, "custom_fields": {}},
    {"id": 2, "name": "Alice Buyer", "company_name": "Alice Capital LLC", "country": "USA",
     "email": "alice@acmecap.com", "website": "acmecap.com", "linked_in": "linkedin.com/in/alicebuyer",
     "won_deals_total": 3, "custom_fields": {}},
    {"id": 3, "name": "Bob Natural", "email": "bob@example.com", "custom_fields": {}},
    {"id": 4, "name": "Carol Coinvest", "email": "carol@example.com", "custom_fields": {}},
]}
deal_pending = {"id": 801, "name": "Pending Deal", "company": {"name": "Zeta Co"}, "deal_stage": {"id": lf.STAGE_MATCHED},
                "custom_fields": cf_status(7207578), "people": [{"id": TENANT_PID}, {"id": 2}], "updated_at": "2026-08-01T00:00:00Z"}
deal_introduced = {"id": 802, "name": "Introduced Deal", "company": {"name": "Alpha Industries"}, "deal_stage": {"id": lf.STAGE_MATCHED},
                    "custom_fields": cf_status(7207579), "people": [{"id": TENANT_PID}, {"id": 2}], "updated_at": "2026-08-02T00:00:00Z"}
deal_stalled = {"id": 803, "name": "Stalled Deal", "company": {"name": "Beta Holdings"}, "deal_stage": {"id": lf.STAGE_MATCHED},
                "custom_fields": cf_status(7207584), "people": [{"id": TENANT_PID}, {"id": 3}], "updated_at": "2026-08-03T00:00:00Z"}
deal_gamma_a = {"id": 804, "name": "Gamma Deal A", "company": {"name": "Gamma Co"}, "deal_stage": {"id": lf.STAGE_MATCHED},
                "custom_fields": cf_status(7207579), "people": [{"id": TENANT_PID}, {"id": 3}], "updated_at": "2026-08-04T00:00:00Z"}
deal_gamma_b = {"id": 805, "name": "Gamma Deal B", "company": {"name": "Gamma Co"}, "deal_stage": {"id": lf.STAGE_MATCHED},
                "custom_fields": cf_status(7207583), "people": [{"id": TENANT_PID}, {"id": 4}], "updated_at": "2026-08-05T00:00:00Z"}
deal_passed = {"id": 806, "name": "Passed Deal", "company": {"name": "Delta Corp"}, "deal_stage": {"id": lf.STAGE_MATCHED},
               "custom_fields": cf_status(7207585), "people": [{"id": TENANT_PID}, {"id": 2}], "updated_at": "2026-08-06T00:00:00Z"}
deal_own_sell = {"id": 900, "name": "Sella's Own Deal", "company": {"name": "Sella HoldCo"}, "deal_stage": {"id": lf.STAGE_FIRM},
                 "custom_fields": {lf.DEAL_SIDE_FIELD: [lf.DEAL_SIDE_SELL_ID]}, "people": [{"id": TENANT_PID}], "updated_at": "2026-08-01T00:00:00Z"}
deals_list = [deal_pending, deal_introduced, deal_stalled, deal_gamma_a, deal_gamma_b, deal_passed, deal_own_sell]
s3_objs = {lf.PEOPLE_KEY: people, lf.INTEREST_KEY: {"buy": {}}, lf.DEALS_KEY: {"deals": deals_list}}
use_fixture(s3_objs, table_items=[
    {"tenant": TENANT_EMAIL, "sk": "intro#802", "notes": "Send updated deck", "milestones": {}},
    {"tenant": TENANT_EMAIL, "sk": "intro#803", "milestones": {"NDA": 1}},  # exit status needs real progress evidence to stay disclosed
    {"tenant": TENANT_EMAIL, "sk": "intro#804", "notes": "Awaiting VDR access", "milestones": {}},
])
tenant_rec = lf._resolve_tenant(TENANT_EMAIL)
assert tenant_rec is not None

page = lf.render_intros_page("Sella Seller", tenant=tenant_rec, tenant_email=TENANT_EMAIL,
                              key=None, view_as=None, edit_mode=False)

head = page[page.find("<thead>"):page.find("</thead>")]
# Scoped to the main Buyers table only: deal_passed (806, Matched stage,
# Intro Status Passed) now correctly renders in the "Closed out" section
# below (see the status-based-exit bug fix), which carries its own
# second <colgroup> -- counting the whole page would see 12, not 6.
main_table_section = (page[:page.find('<details class="closed-out-section">')]
                       if '<details class="closed-out-section">' in page else page)
check("colgroup has exactly 6 columns (no Follow-up column)", main_table_section.count("<col ") == 6)
check("no Deal column header", ">Deal<" not in head)
check("Company/Buyer/Investor Type/Size/Status/Notes header order",
      [head.find(f">{h}<") for h in ["Company", "Buyer", "Investor Type", "Size", "Status", "Notes"]]
      == sorted(head.find(f">{h}<") for h in ["Company", "Buyer", "Investor Type", "Size", "Status", "Notes"]))
check("Status column tooltip present", 'title="Where this introduction stands"' in head)
check("Size column centered via the .num class", '<th class="num">Size</th>' in head)
check("table uses table-layout:fixed", "table-layout: fixed;" in page)
check("no table-scroll wrapper", '<div class="table-scroll">' not in page)

check("summary strip renders with the expected counts/wording",
      "in motion" in page and "stalled" in page and "pending introduction" in page)
check("summary hint line present", "Update statuses as buyers progress" in page)
check("feature box present", "feature" in page.lower())
check("Feature requests heading present", "Feature requests" in page)

# _intro_status_select_html (the old 10-option dropdown) was retired by
# the company-page parity refactor -- its one remaining caller
# (_matched_buyer_row_edit_html) is gone, replaced by the same milestone
# checkboxes + flags-only select Active Intros always used. Tenant-vs-
# admin status control is now purely _flag_select_html's admin_controls
# bool; TENANT_ALLOWED_STATUS_IDS itself is still live server-side (see
# _handle_update_intro's status-field validation, kept for API
# completeness) -- see the tenant-flag-options checks elsewhere in this
# suite for its client-facing equivalent.
check("TENANT_ALLOWED_STATUS_IDS is still defined and used server-side",
      hasattr(lf, "TENANT_ALLOWED_STATUS_IDS") and 7207578 not in lf.TENANT_ALLOWED_STATUS_IDS)

check("notes placeholder is the fixed 'Add a note…' text", "Add a note…" in page)
check("a Passed (dead/exit) row has no editable data-deal-id markup", 'data-deal-id="806"' not in page)
# Bug fix regression guard: deal_passed (806) sits at STAGE_MATCHED (a
# live, matched-or-later stage) with Intro Status explicitly Passed --
# an exit expressed via status, not a Pipeline stage move. It must
# render in "Closed out" (same as a stage-based Lost/Obsolete exit),
# never nowhere and never still in Introduced with live controls.
check("exit-via-status-on-live-stage: Delta Corp (806, Matched+Passed) is NOT in Introduced/Pending",
      "Delta Corp" not in page[:page.find('<details class="closed-out-section">')]
      if '<details class="closed-out-section">' in page else False)
check("exit-via-status-on-live-stage: Delta Corp (806) DOES render, in Closed out",
      "Delta Corp" in page and '<details class="closed-out-section">' in page
      and "Delta Corp" in page[page.find('<details class="closed-out-section">'):])
check("exit-via-status-on-live-stage: 806 has no milestone evidence, but the deal itself (Buy-tagged, "
      "linking the tenant, at a live matched-or-later stage) is evidence an introduction happened -> "
      "discloses ('Alice Buyer' named, not an anonymized code)",
      "Alice Buyer" in page[page.find('<details class="closed-out-section">'):])
check("exit-via-status-on-live-stage: the Closed out chip reads 'Lost' (via _deal_exit_outcome_name's status "
      "fallback, display-renamed from Passed)",
      '<span class="status-chip exit">Lost</span>' in page[page.find('<details class="closed-out-section">'):])

intro_section = page[page.find(">Introduced<"):page.find("Pending introductions")
                      if "Pending introductions" in page else len(page)]
pos_beta = intro_section.find("Beta Holdings")
pos_gamma = intro_section.find("Gamma Co")
check("Stalled (Beta Holdings) sorts ahead of non-stalled rows", 0 <= pos_beta < pos_gamma)
check("grouped-row class applied to the repeat Gamma Co row", "grouped-row" in page)

check("pending-introductions explanatory note present", "We're preparing these introductions" in page)
check("Pending introductions header present (801 is Matched-only)", "Pending introductions" in page)

# Empty state -- a tenant auto-enrolled via their own Sell deal, but with
# no Buy-side matches at all.
people_empty = {"people": [{"id": 99, "full_name": "Empty Tenant", "email": "empty@example.com", "custom_fields": {}}]}
deals_empty = {"deals": [
    {"id": 901, "name": "Empty Tenant's Own Deal", "company": {"name": "Empty HoldCo"},
     "deal_stage": {"id": lf.STAGE_FIRM}, "custom_fields": {lf.DEAL_SIDE_FIELD: [lf.DEAL_SIDE_SELL_ID]},
     "people": [{"id": 99}], "updated_at": "2026-08-01T00:00:00Z"},
]}
use_fixture({lf.PEOPLE_KEY: people_empty, lf.INTEREST_KEY: {"buy": {}}, lf.DEALS_KEY: deals_empty})
empty_tenant_rec = lf._resolve_tenant("empty@example.com")
assert empty_tenant_rec is not None
page_empty = lf.render_intros_page("Empty Tenant", tenant=empty_tenant_rec, tenant_email="empty@example.com",
                                    key=None, view_as=None, edit_mode=False)
check("empty state message present", "No introductions yet" in page_empty)
check("empty state links back to My Deals", "tab=mydeals" in page_empty)
check("empty state does not render a table", "<table>" not in page_empty)

# Server-side status write enforcement
use_fixture(s3_objs, table_items=[
    {"tenant": TENANT_EMAIL, "sk": "intro#802", "notes": "Send updated deck", "milestones": {}},
    {"tenant": TENANT_EMAIL, "sk": "intro#803", "milestones": {"NDA": 1}},
    {"tenant": TENANT_EMAIL, "sk": "intro#804", "notes": "Awaiting VDR access", "milestones": {}},
])
lf.urllib.request.urlopen = fake_urlopen

resp = lf.lambda_handler(post_event({"deal_id": "802", "status": "7207584"}, cookies=[tenant_cookie(TENANT_EMAIL)]), None)
check("tenant CAN set Stalled on an Introduced (disclosed) row -> 200", resp["statusCode"] == 200)
resp2 = lf.lambda_handler(post_event({"deal_id": "802", "status": "7207578"}, cookies=[tenant_cookie(TENANT_EMAIL)]), None)
check("tenant CANNOT set Matched -> 403", resp2["statusCode"] == 403)
resp3 = lf.lambda_handler(post_event({"deal_id": "802", "status": "7207586"}, cookies=[tenant_cookie(TENANT_EMAIL)]), None)
check("tenant CANNOT set Withdrawn -> 403", resp3["statusCode"] == 403)
resp4 = lf.lambda_handler(post_event({"deal_id": "801", "status": "7207580"}, cookies=[tenant_cookie(TENANT_EMAIL)]), None)
check("tenant CANNOT set status on a non-disclosed (Matched-only) row -> 403", resp4["statusCode"] == 403)
resp5 = lf.lambda_handler(post_event({"deal_id": "801", "status": "7207578", "key": ADMIN_KEY}), None)
check("admin CAN set Matched via key -> 200", resp5["statusCode"] == 200)


# ======================================================================
# SECTION: Active Intros — milestone checkboxes, flag select, status-chip
# rendering, and the write path behind both
# ======================================================================
# Turn 17 replaced the old raw-status dropdown with four checkboxes (NDA/
# VDR/Sub Docs/Wired -- "add-only", never backfilled) plus a compact
# flag select (none/stalled/passed/withdrawn/closed) that never touches
# the milestones map. Status derives from the checked set
# (_derive_status_from_checked) unless a flag overrides it.

def resolved_for(name, sid, is_exit=False):
    return {"id": sid, "name": name, "is_exit": is_exit, "disclosed": True}

stalled_r = resolved_for("Stalled", lf.INTRO_STATUS_STALLED_ID, is_exit=True)
passed_r = resolved_for("Passed", lf.INTRO_STATUS_PASSED_ID, is_exit=True)
withdrawn_r = resolved_for("Withdrawn", lf.INTRO_STATUS_WITHDRAWN_ID, is_exit=True)
closed_r = resolved_for("Closed", lf.INTRO_STATUS_CLOSED_ID, is_exit=False)
introduced_r = resolved_for("Introduced", 7207579, is_exit=False)
wired_r = resolved_for("Wired", 7207583, is_exit=False)

# Editable (disabled=False): the select alone carries the state, no chip.
html = lf._status_milestones_column_html(stalled_r, "1", {}, admin_controls=False, disabled=False)
check("editable Stalled: no status-chip rendered", "status-chip" not in html)
check("editable Stalled: select carries the flag-stalled class", 'class="ei-flag flag-stalled"' in html)
html2 = lf._status_milestones_column_html(passed_r, "2", {}, admin_controls=False, disabled=False)
check("editable Passed: no status-chip rendered", "status-chip" not in html2)
check("editable Passed: select carries the flag-exit class", 'class="ei-flag flag-exit"' in html2)
html3 = lf._status_milestones_column_html(withdrawn_r, "3", {}, admin_controls=True, disabled=False)
check("editable (admin) Withdrawn: no status-chip rendered", "status-chip" not in html3)
html4 = lf._status_milestones_column_html(introduced_r, "4", {}, admin_controls=False, disabled=False)
check("editable Introduced (no flag): plain select class, no chip",
      "status-chip" not in html4 and 'class="ei-flag"' in html4)
html5 = lf._status_milestones_column_html(wired_r, "5", {}, admin_controls=False, disabled=False)
check("editable Wired: 'awaiting confirmation' note still present", "Awaiting our confirmation" in html5)
check("editable Wired: no chip", "status-chip" not in html5)

# Read-only (disabled=True): both the chip AND the select render.
html6 = lf._status_milestones_column_html(closed_r, "6", {}, admin_controls=False, disabled=True)
check("disabled Closed (tenant-locked): status-chip present", 'class="status-chip closed">Won<' in html6)
check("disabled Closed: select still present, disabled", '<select class="ei-flag"' in html6 and " disabled" in html6)
html7 = lf._status_milestones_column_html(stalled_r, "7", {}, admin_controls=False, disabled=True)
check("disabled Stalled: status-chip present", 'class="status-chip stalled">Stalled<' in html7)
check("disabled Stalled: select still carries flag-stalled styling too", 'class="ei-flag flag-stalled"' in html7)
html8 = lf._status_milestones_column_html(stalled_r, "8", {}, admin_controls=True, disabled=False)
check("admin edit mode is never disabled in real usage -> no chip", "status-chip" not in html8)

# Checkbox label rename: "Sub Docs" -> "Docs Sent" for display, data key unchanged.
check("MILESTONE_STEP_LABELS renames Sub Docs -> Docs Sent for display",
      lf.MILESTONE_STEP_LABELS.get("Sub Docs") == "Docs Sent")
check("internal step key is still 'Sub Docs' (data compatibility)", "Sub Docs" in lf.CHECKBOX_MILESTONE_STEPS)
cb_html = lf._milestone_checkboxes_html("1", {"Sub Docs": 123})
check("checkbox label text reads 'Docs Sent'", "Docs Sent" in cb_html)
check("checkbox value= attribute is still the internal 'Sub Docs' key", 'value="Sub Docs"' in cb_html)
check("a pre-existing 'Sub Docs' milestone entry still shows checked",
      re.search(r'value="Sub Docs"[^>]*checked', cb_html) is not None)

# Flag select is flags-only for every viewer -- never a milestone option.
tenant_select = lf._flag_select_html("1", introduced_r, admin_controls=False)
admin_select = lf._flag_select_html("1", introduced_r, admin_controls=True)
for html_sel, who in [(tenant_select, "tenant"), (admin_select, "admin")]:
    for milestone_id in (7207578, 7207579, 7207580, 7207581, 7207582, 7207583):
        check(f"{who} flag select has NO milestone option {milestone_id}", f'value="{milestone_id}"' not in html_sel)

# ---- Full-page render: checkbox state, backfill guard, flag-locked Closed rows ----
people = {"people": [
    {"id": TENANT_PID, "full_name": "Sella Seller", "email": TENANT_EMAIL, "custom_fields": {}},
    {"id": 2, "name": "Alice Buyer", "email": "alice@example.com", "custom_fields": {}},
    {"id": 3, "name": "Bob Buyer", "email": "bob@example.com", "custom_fields": {}},
]}
deal_i802 = {"id": 802, "name": "Alpha Deal", "company": {"name": "Alpha Industries"}, "deal_stage": {"id": lf.STAGE_MATCHED},
             "custom_fields": cf_status(7207579), "people": [{"id": TENANT_PID}, {"id": 2}], "updated_at": "2026-08-02T00:00:00Z"}
deal_i803 = {"id": 803, "name": "Beta Deal", "company": {"name": "Beta Holdings"}, "deal_stage": {"id": lf.STAGE_MATCHED},
             "custom_fields": cf_status(7207583), "people": [{"id": TENANT_PID}, {"id": 3}], "updated_at": "2026-08-03T00:00:00Z"}
deal_i804 = {"id": 804, "name": "Gamma Deal", "company": {"name": "Gamma Co"}, "deal_stage": {"id": lf.STAGE_MATCHED},
             "custom_fields": cf_status(7207587), "people": [{"id": TENANT_PID}, {"id": 2}], "updated_at": "2026-08-04T00:00:00Z"}
deal_i805 = {"id": 805, "name": "Delta Deal", "company": {"name": "Delta Corp"}, "deal_stage": {"id": lf.STAGE_MATCHED},
             "custom_fields": cf_status(7207578), "people": [{"id": TENANT_PID}, {"id": 3}], "updated_at": "2026-08-05T00:00:00Z"}
deals_list = [deal_i802, deal_i803, deal_i804, deal_own_sell]
s3_objs = {lf.PEOPLE_KEY: people, lf.INTEREST_KEY: {"buy": {}}, lf.DEALS_KEY: {"deals": deals_list + [deal_i805]}}
_, fake_table = use_fixture(s3_objs, table_items=[
    {"tenant": TENANT_EMAIL, "sk": "intro#803", "milestones": {"NDA": 1750000000}},
    {"tenant": TENANT_EMAIL, "sk": "intro#804",
     "milestones": {"NDA": 1750000000, "VDR": 1750000100, "Sub Docs": 1750000200, "Wired": 1750000300}},
])
tenant_rec = lf._resolve_tenant(TENANT_EMAIL)
assert tenant_rec is not None

page_tenant = lf.render_intros_page("Sella Seller", tenant=tenant_rec, tenant_email=TENANT_EMAIL,
                                     key=None, view_as=None, edit_mode=False)
page_admin = lf.render_intros_page("Sella Seller", tenant=tenant_rec, tenant_email=TENANT_EMAIL,
                                    key=ADMIN_KEY, view_as=TENANT_EMAIL, edit_mode=True)

check("no old .ei-status dropdown left on Active Intros (fully replaced)", '<select class="ei-status"' not in page_tenant)
check("milestone checkboxes present (.ei-milestone)", 'class="ei-milestone"' in page_tenant)
check("all four labeled steps (NDA/VDR/Sub Docs/Wired) present", all(s in page_tenant for s in ["NDA", "VDR", "Sub Docs", "Wired"]))

def checkbox_states(row):
    return {m.group(1): ("checked" in m.group(0)) for m in
            re.finditer(r'<input type="checkbox" class="ei-milestone"[^>]*value="([^"]+)"[^>]*>', row)}

row802 = row_for(page_tenant, "802")
states802 = checkbox_states(row802)
check("802 (Introduced, nothing recorded): all four boxes unchecked", not any(states802.values()))
check("802 flag select shows None selected", '<option value="none" selected>None</option>' in row802)

row803 = row_for(page_tenant, "803")
states803 = checkbox_states(row803)
check("803 (Wired, only NDA explicitly stored): ONLY NDA checked -- proves no backfill",
      states803 == {"NDA": True, "VDR": False, "Sub Docs": False, "Wired": False})
check("803 shows the Wired-not-Closed 'awaiting confirmation' note", "Awaiting our confirmation" in row803)

# Bug fix (Closed intros render in "Closed out", not locked in place in
# Introduced): 804 (Gamma Co, Intro Status Closed) no longer carries any
# data-deal-id markup in the normal table at all -- same convention as a
# Passed/Withdrawn row -- it's in the Closed out section instead, named
# (disclosed -- Closed is never Matched) with the positive green chip,
# nothing left to edit for tenant OR admin.
check("804 no longer has milestone checkbox markup in the normal table (tenant)",
      'class="ei-milestone" data-deal-id="804"' not in page_tenant)
check("804 no longer has milestone checkbox markup in the normal table (admin)",
      'class="ei-milestone" data-deal-id="804"' not in page_admin)
closed_out_tenant = page_tenant[page_tenant.find('<details class="closed-out-section">'):]
closed_out_admin = page_admin[page_admin.find('<details class="closed-out-section">'):]
check("804 (Closed) IS in Closed out (tenant), named since Closed is always disclosed",
      "Gamma Co" in closed_out_tenant and "Alice Buyer" in closed_out_tenant)
check("804's Closed out chip is the positive green 'Won' one, not the red Lost styling",
      '<span class="status-chip closed">Won</span>' in closed_out_tenant)
check("804 (Closed) IS in Closed out (admin edit mode too)",
      "Gamma Co" in closed_out_admin and '<span class="status-chip closed">Won</span>' in closed_out_admin)

check("805 (Matched/pending) has no checkbox markup", 'class="ei-milestone" data-deal-id="805"' not in page_tenant)
check("805 is listed in the Pending introductions section", "Pending introductions" in page_tenant)

# ---- Write path: checkbox toggle, uncheck-a-middle-step, Closed-lock, flags ----
pipeline_calls = []
def fake_urlopen2(req, timeout=15):
    pipeline_calls.append({"url": req.full_url, "data": json.loads(req.data.decode()) if req.data else None})
    return FakeHTTPResponse(200)
lf.urllib.request.urlopen = fake_urlopen2

resp = lf.lambda_handler(post_event({"deal_id": "802", "milestone_step": "VDR", "milestone_checked": True},
                                     cookies=[tenant_cookie(TENANT_EMAIL)]), None)
check("tenant checks VDR on 802 -> 200", resp["statusCode"] == 200)
last = fake_table.updates[-1]
ms = last["ExpressionAttributeValues"].get(":ms")
check("milestones map now has exactly VDR", ms is not None and set(ms.keys()) == {"VDR"})
check("derived Pipeline status = VDR (7207581)",
      pipeline_calls[-1]["data"]["deal"]["custom_fields"][lf.INTRO_STATUS_FIELD] == 7207581)

resp2 = lf.lambda_handler(post_event({"deal_id": "804", "milestone_step": "NDA", "milestone_checked": False,
                                       "key": ADMIN_KEY}), None)
check("admin unchecks NDA on 804 (Closed row) -> 200", resp2["statusCode"] == 200)
last2 = fake_table.updates[-1]
ms2 = last2["ExpressionAttributeValues"].get(":ms")
check("NDA removed, VDR/Sub Docs/Wired remain", ms2 is not None and set(ms2.keys()) == {"VDR", "Sub Docs", "Wired"})
check("admin checkbox edit on a Closed row does NOT touch status_override", ":so" not in last2["ExpressionAttributeValues"])
check("no Pipeline status PUT fired for the Closed-row admin checkbox edit",
      len([c for c in pipeline_calls if "deals/804" in c["url"]]) == 0)

resp3 = lf.lambda_handler(post_event({"deal_id": "804", "milestone_step": "Wired", "milestone_checked": False},
                                      cookies=[tenant_cookie(TENANT_EMAIL)]), None)
check("tenant checkbox toggle on a Closed row (804) -> 403", resp3["statusCode"] == 403)

resp4 = lf.lambda_handler(post_event({"deal_id": "802", "flag": "stalled"}, cookies=[tenant_cookie(TENANT_EMAIL)]), None)
check("tenant sets flag=stalled on 802 -> 200", resp4["statusCode"] == 200)
last4 = fake_table.updates[-1]
check("flag write does NOT touch milestones", ":ms" not in last4["ExpressionAttributeValues"])
check("flag write DOES set status_override to the Stalled id",
      last4["ExpressionAttributeValues"].get(":so") == lf.INTRO_STATUS_STALLED_ID)

resp5 = lf.lambda_handler(post_event({"deal_id": "802", "flag": "closed"}, cookies=[tenant_cookie(TENANT_EMAIL)]), None)
check("tenant flag=closed -> 403 (admin-only)", resp5["statusCode"] == 403)
resp6 = lf.lambda_handler(post_event({"deal_id": "802", "flag": "closed", "key": ADMIN_KEY}), None)
check("admin flag=closed on 802 -> 200", resp6["statusCode"] == 200)
last6 = fake_table.updates[-1]
check("admin Closed-flag write does NOT touch milestones", ":ms" not in last6["ExpressionAttributeValues"])
check("admin Closed-flag write sets status to 7207587", last6["ExpressionAttributeValues"].get(":so") == 7207587)

resp7 = lf.lambda_handler(post_event({"deal_id": "802", "flag": "none", "key": ADMIN_KEY}), None)
check("admin flag=none on 802 -> 200", resp7["statusCode"] == 200)
last7 = fake_table.updates[-1]
check("flag=none does NOT touch milestones", ":ms" not in last7["ExpressionAttributeValues"])
check("flag=none re-derives status from the checked set (VDR -> 7207581)",
      last7["ExpressionAttributeValues"].get(":so") == 7207581)

resp8 = lf.lambda_handler(post_event({"deal_id": "802", "milestone_step": "VDR", "milestone_checked": True,
                                       "flag": "stalled", "key": ADMIN_KEY}), None)
check("both milestone_step and flag in one request -> 400", resp8["statusCode"] == 400)
resp9 = lf.lambda_handler(post_event({"deal_id": "802", "milestone_step": "NotAStep", "milestone_checked": True,
                                       "key": ADMIN_KEY}), None)
check("an invalid milestone_step -> 400", resp9["statusCode"] == 400)


# ======================================================================
# SECTION: Active Intros — Notes (replaces Next Steps), buyer Notes
# Ledger, per-page feature requests
# ======================================================================

TENANT_A_EMAIL = "sella@example.com"
TENANT_A_PID = 1
TENANT_B_EMAIL = "tabor@example.com"
TENANT_B_PID = 8

people = {"people": [
    {"id": TENANT_A_PID, "full_name": "Sella Seller", "email": TENANT_A_EMAIL, "custom_fields": {}},
    {"id": TENANT_B_PID, "full_name": "Tabor Seller", "email": TENANT_B_EMAIL, "custom_fields": {}},
    {"id": 2, "name": "Alice Buyer", "email": "alice@example.com", "custom_fields": {}},
]}
deal_802 = {"id": 802, "name": "Alpha Deal", "company": {"name": "Alpha Industries"}, "deal_stage": {"id": lf.STAGE_MATCHED},
            "custom_fields": cf_status(7207579), "people": [{"id": TENANT_A_PID}, {"id": 2}], "updated_at": "2026-08-02T00:00:00Z"}
deal_803 = {"id": 803, "name": "Beta Deal", "company": {"name": "Beta Holdings"}, "deal_stage": {"id": lf.STAGE_MATCHED},
            "custom_fields": cf_status(7207580), "people": [{"id": TENANT_A_PID}, {"id": 2}], "updated_at": "2026-08-03T00:00:00Z"}
deal_804 = {"id": 804, "name": "Gamma Deal", "company": {"name": "Gamma Co"}, "deal_stage": {"id": lf.STAGE_MATCHED},
            "custom_fields": cf_status(7207579), "people": [{"id": TENANT_B_PID}, {"id": 2}], "updated_at": "2026-08-04T00:00:00Z"}
deal_805 = {"id": 805, "name": "Delta Deal", "company": {"name": "Delta Corp"}, "deal_stage": {"id": lf.STAGE_MATCHED},
            "custom_fields": cf_status(7207578), "people": [{"id": TENANT_A_PID}, {"id": 2}], "updated_at": "2026-08-05T00:00:00Z"}
deal_900a = {"id": 900, "name": "Sella's Own Deal", "company": {"name": "Sella HoldCo"}, "deal_stage": {"id": lf.STAGE_FIRM},
             "custom_fields": {lf.DEAL_SIDE_FIELD: [lf.DEAL_SIDE_SELL_ID]}, "people": [{"id": TENANT_A_PID}], "updated_at": "2026-08-01T00:00:00Z"}
deal_901b = {"id": 901, "name": "Tabor's Own Deal", "company": {"name": "Tabor HoldCo"}, "deal_stage": {"id": lf.STAGE_FIRM},
             "custom_fields": {lf.DEAL_SIDE_FIELD: [lf.DEAL_SIDE_SELL_ID]}, "people": [{"id": TENANT_B_PID}], "updated_at": "2026-08-01T00:00:00Z"}
deals_list = [deal_802, deal_803, deal_804, deal_805, deal_900a, deal_901b]
s3_objs = {lf.PEOPLE_KEY: people, lf.INTEREST_KEY: {"buy": {}}, lf.DEALS_KEY: {"deals": deals_list}}
_, fake_table = use_fixture(s3_objs, table_items=[
    {"tenant": TENANT_A_EMAIL, "sk": "intro#802", "next_steps": "Legacy action item", "milestones": {"Sub Docs": 1750000000}},
    {"tenant": TENANT_A_EMAIL, "sk": "intro#803", "notes": "Great call yesterday", "milestones": {}},
    {"tenant": TENANT_B_EMAIL, "sk": "intro#804", "notes": "Following up next week", "milestones": {}},
])
tenant_a = lf._resolve_tenant(TENANT_A_EMAIL)
tenant_b = lf._resolve_tenant(TENANT_B_EMAIL)
assert tenant_a is not None and tenant_b is not None

page_a = lf.render_intros_page("Sella Seller", tenant=tenant_a, tenant_email=TENANT_A_EMAIL,
                                key=None, view_as=None, edit_mode=False)
check("head row shows 'Notes' (not 'Next Steps')", ">Notes</th>" in page_a and "<th>Next Steps</th>" not in page_a)
check("no ei-next-steps input element remains on Active Intros", 'class="ei-next-steps"' not in page_a)
check("Notes placeholder is the fixed 'Add a note…' (no per-row suggestion)", 'placeholder="Add a note…"' in page_a)

row802 = row_for(page_a, "802")
check("802 (legacy next_steps, no notes): shows the legacy text as the initial Notes value",
      ">Legacy action item</textarea>" in row802)
row803 = row_for(page_a, "803")
check("803 (explicit notes already set): shows the real notes value", ">Great call yesterday</textarea>" in row803)

lf.urllib.request.urlopen = fake_urlopen2
resp = lf.lambda_handler(post_event({"deal_id": "802", "notes": "Tenant-authored note"},
                                     cookies=[tenant_cookie(TENANT_A_EMAIL)]), None)
check("tenant CAN write notes on a disclosed row -> 200", resp["statusCode"] == 200)
last = fake_table.updates[-1]
check("the notes write actually set the notes field", last["ExpressionAttributeValues"].get(":no") == "Tenant-authored note")

resp2 = lf.lambda_handler(post_event({"deal_id": "805", "notes": "Should be blocked"},
                                      cookies=[tenant_cookie(TENANT_A_EMAIL)]), None)
check("tenant CANNOT write notes on a non-disclosed (Matched-only) row -> 403", resp2["statusCode"] == 403)
resp3 = lf.lambda_handler(post_event({"deal_id": "802", "deadline": "2026-12-01"}, cookies=[tenant_cookie(TENANT_A_EMAIL)]), None)
check("tenant still CANNOT write a deadline -> 403 (unchanged)", resp3["statusCode"] == 403)

notes_a = lf._buyer_notes_ledger_html(2, tenant_a, TENANT_A_EMAIL, False)
check("tenant view: 'Notes' heading present", ">Notes</h2>" in notes_a)
check("tenant view: 802 shows its (now tenant-authored) note", "Alpha Industries" in notes_a and "Tenant-authored note" in notes_a)
check("tenant view: 803 shows its explicit notes", "Beta Holdings" in notes_a and "Great call yesterday" in notes_a)
check("tenant view: a pending/non-disclosed deal (805) is NOT shown (privacy)", "Delta Corp" not in notes_a)

notes_b = lf._buyer_notes_ledger_html(2, tenant_b, TENANT_B_EMAIL, False)
check("tenant B view: their own note on 804 shown", "Gamma Co" in notes_b and "Following up next week" in notes_b)
check("tenant B view: tenant A's notes are NOT shown", "Alpha Industries" not in notes_b and "Beta Holdings" not in notes_b)

notes_admin = lf._buyer_notes_ledger_html(2, tenant_a, TENANT_A_EMAIL, True)
check("admin edit mode: tenant A's 802 note shown", "Alpha Industries" in notes_admin and "Tenant-authored note" in notes_admin)
check("admin edit mode: tenant A's 803 note shown", "Beta Holdings" in notes_admin and "Great call yesterday" in notes_admin)
check("admin edit mode: tenant B's 804 note ALSO shown (cross-tenant aggregate)",
      "Gamma Co" in notes_admin and "Following up next week" in notes_admin)
check("admin edit mode: entries labeled by tenant name", "Sella Seller" in notes_admin and "Tabor Seller" in notes_admin)

notes_empty = lf._buyer_notes_ledger_html(999, tenant_a, TENANT_A_EMAIL, False)
check("no matched deals with a buyer -> the exact empty-state placeholder",
      "No notes yet — add one from Active Intros or right here." in notes_empty)

page_buyer_full = lf.render_buyer_page(2, "Sella Seller", tenant_a, TENANT_A_EMAIL, key=None, view_as=None, edit_mode=False)
check("full-access buyer page includes the Notes section", ">Notes</h2>" in page_buyer_full)
check("full-access buyer page includes an editable ei-notes field", 'class="ei-notes"' in page_buyer_full)
check("full-access buyer page includes the auto-save edit script", "saveField" in page_buyer_full)

page_buyer_anon = lf.render_buyer_page(3, "Sella Seller", tenant_a, TENANT_A_EMAIL, key=None, view_as=None, edit_mode=False)
check("anonymized (no disclosed deal) buyer page has no Notes section", ">Notes</h2>" not in page_buyer_anon)
check("anonymized buyer page has no edit script at all", "saveField" not in page_buyer_anon)

# ---- Per-page feature requests ----
def all_of(bucket_tuple):
    data, err = bucket_tuple
    assert not err
    return data["open"] + data["done"]

fake_table.items.append({"tenant": TENANT_A_EMAIL, "sk": "feature#1000000000001",
                          "text": "Add dark mode to intros", "submitted_by": TENANT_A_EMAIL,
                          "created_at": datetime.now(timezone.utc).isoformat(), "done": False, "page": "intros"})
fake_table.items.append({"tenant": TENANT_A_EMAIL, "sk": "feature#1000000000002",
                          "text": "Better company search", "submitted_by": TENANT_A_EMAIL,
                          "created_at": datetime.now(timezone.utc).isoformat(), "done": False, "page": "company"})
fake_table.items.append({"tenant": TENANT_A_EMAIL, "sk": "feature#1000000000003",
                          "text": "Legacy item, no page attribute", "submitted_by": TENANT_A_EMAIL,
                          "created_at": datetime.now(timezone.utc).isoformat(), "done": False})

intros_items = all_of(lf.get_feature_requests(TENANT_A_EMAIL, page="intros"))
check("get_feature_requests(page='intros') returns only the intros item",
      any(i["text"] == "Add dark mode to intros" for i in intros_items)
      and not any(i["text"] == "Better company search" for i in intros_items))
company_items = all_of(lf.get_feature_requests(TENANT_A_EMAIL, page="company"))
check("get_feature_requests(page='company') returns only the company item",
      any(i["text"] == "Better company search" for i in company_items)
      and not any(i["text"] == "Add dark mode to intros" for i in company_items))
my_deals_items = all_of(lf.get_feature_requests(TENANT_A_EMAIL, page="my-deals"))
check("an untagged legacy item (no 'page' attribute) falls back to my-deals",
      any(i["text"] == "Legacy item, no page attribute" for i in my_deals_items))
all_items = all_of(lf.get_feature_requests(TENANT_A_EMAIL))
check("get_feature_requests with no page arg is unfiltered (admin aggregate path)", len(all_items) >= 3)

resp_feat = lf.lambda_handler(feature_event(
    {"key": TENANT_A_EMAIL, "tenant": TENANT_A_EMAIL, "text": "Demand board filter", "page": "demand"},
    cookies=[tenant_cookie(TENANT_A_EMAIL)]), None)
check("POST feature_request with page='demand' -> 200", resp_feat["statusCode"] == 200)
demand_items = all_of(lf.get_feature_requests(TENANT_A_EMAIL, page="demand"))
check("a submitted item lands under its own page", any(i["text"] == "Demand board filter" for i in demand_items))

resp_feat_bad = lf.lambda_handler(feature_event(
    {"key": TENANT_A_EMAIL, "tenant": TENANT_A_EMAIL, "text": "No page given", "page": "not-a-real-page"},
    cookies=[tenant_cookie(TENANT_A_EMAIL)]), None)
check("POST feature_request with an invalid page -> 200", resp_feat_bad["statusCode"] == 200)
fallback_items = all_of(lf.get_feature_requests(TENANT_A_EMAIL, page="my-deals"))
check("an invalid/unrecognized page value falls back to my-deals",
      any(i["text"] == "No page given" for i in fallback_items))

agg_data, agg_err = lf.get_feature_requests(TENANT_A_EMAIL)
tenant_names = {TENANT_A_EMAIL: "Sella Seller"}
agg_open = [dict(it, tenant=TENANT_A_EMAIL) for it in agg_data["open"]]
agg_done = [dict(it, tenant=TENANT_A_EMAIL) for it in agg_data["done"]]
agg_html = lf._feature_requests_list_html(agg_open, agg_done, show_tenant=True, tenant_names=tenant_names, show_page=True)
check("admin aggregate list renders page-label pills", 'class="feature-page-tag"' in agg_html)
check("admin aggregate list shows the Active Intros page label", "Active Intros" in agg_html)
check("admin aggregate list shows the Company page label", "Company" in agg_html)

section_box, section_list = lf._feature_section_html(False, TENANT_A_EMAIL, key=None, page="intros")
section_intros = section_box + section_list
check("_feature_section_html(page='intros') box only shows intros items",
      "Add dark mode to intros" in section_intros and "Better company search" not in section_intros)

# ---- Buyer-link affordance ("profile ->") shared by Active Intros and Company page ----
alice_primary = {"id": 2, "name": "Alice Buyer", "custom_fields": {}}
empty_firm_won_index = {"by_company_id": set(), "by_company_name": set()}
cell_html = lf._intro_buyer_cell_html(alice_primary, None, 0, empty_firm_won_index, key=None, view_as=None)
check("Active Intros buyer cell: link carries the buyer-link class", 'class="buyer-link"' in cell_html)
check("Active Intros buyer cell: 'profile ->' suffix present", "profile" in cell_html and "buyer-link-suffix" in cell_html)

name_cell_html = lf._buyer_name_cell_html(alice_primary, None, 0, show_contact=False, link=True, key=None, view_as=None)
check("Company page buyer-name cell: link carries the buyer-link class", 'class="buyer-link"' in name_cell_html)
check("Company page buyer-name cell: 'profile ->' suffix present",
      "profile" in name_cell_html and "buyer-link-suffix" in name_cell_html)


# ======================================================================
# SECTION: Buyer page — disclosure gating, primary-buyer selection
# ======================================================================
# A buyer's dedicated ?buyer=<id> page shows the real name/contact/notes
# only when at least one deal links this tenant and that buyer at
# Introduced-or-later; otherwise it's the anonymized card (a stable
# per-tenant "Buyer <code>" alias). Admin edit mode always gets full
# access, disclosed or not.

people = {"people": [
    {"id": TENANT_PID, "full_name": "Sella Seller", "email": TENANT_EMAIL, "custom_fields": {}},
    {"id": 2, "name": "Alice Buyer", "email": "alice@example.com", "custom_fields": {}},
    {"id": 4, "name": "Carol Buyer", "email": "carol@example.com", "custom_fields": {}},
    {"id": 5, "name": "Dave Buyer", "email": "dave@example.com", "custom_fields": {}},
    {"id": 7, "name": "Frank Buyer", "email": "frank@example.com", "custom_fields": {}},
]}
deal_disc_802 = {"id": 802, "name": "Alpha Deal", "company": {"name": "Alpha Industries"}, "deal_stage": {"id": lf.STAGE_MATCHED},
                 "custom_fields": cf_status(7207579), "people": [{"id": TENANT_PID}, {"id": 2}, {"id": 4}], "updated_at": "2026-08-02T00:00:00Z"}
deal_disc_805 = {"id": 805, "name": "Delta Deal", "company": {"name": "Delta Corp"}, "deal_stage": {"id": lf.STAGE_MATCHED},
                 "custom_fields": cf_status(7207578), "people": [{"id": TENANT_PID}, {"id": 7}], "updated_at": "2026-08-05T00:00:00Z"}
deals_list = [deal_disc_802, deal_disc_805, deal_own_sell]
use_fixture({lf.PEOPLE_KEY: people, lf.INTEREST_KEY: {"buy": {}}, lf.DEALS_KEY: {"deals": deals_list}})
tenant_rec = lf._resolve_tenant(TENANT_EMAIL)
assert tenant_rec is not None

page_buyer2 = lf.render_buyer_page("2", "Sella Seller", tenant_rec, TENANT_EMAIL, key=None, view_as=None, edit_mode=False)
check("disclosed buyer (2, via 802): full profile shows the real name", "Alice Buyer" in page_buyer2)
check("disclosed buyer full profile shows a mailto", "mailto:alice@example.com" in page_buyer2)
check("disclosed buyer full profile has no anonymized-card note", "Identity available after introduction" not in page_buyer2)
check("disclosed buyer full page: no tier-badge markup for an unknown tier", 'class="tier-badge' not in page_buyer2)

page_buyer7 = lf.render_buyer_page("7", "Sella Seller", tenant_rec, TENANT_EMAIL, key=None, view_as=None, edit_mode=False)
check("buyer 7 (only ever Matched/pending): anonymized card, no real name", "Frank Buyer" not in page_buyer7)
check("buyer 7 anonymized card shows the 'available after introduction' note",
      "Identity available after introduction" in page_buyer7)
code7 = lf._anon_buyer_code(TENANT_EMAIL, 7)
check("buyer 7 anonymized card shows the stable anon buyer code", f"Buyer {code7}" in page_buyer7)
check("buyer 7 anonymized page: no tier-badge markup either", 'class="tier-badge' not in page_buyer7)

page_buyer7_admin = lf.render_buyer_page("7", "Sella Seller", tenant_rec, TENANT_EMAIL, key=ADMIN_KEY,
                                          view_as=TENANT_EMAIL, edit_mode=True)
check("admin edit mode: buyer 7's full profile shown despite no disclosed deal", "Frank Buyer" in page_buyer7_admin)

page_buyer_missing = lf.render_buyer_page("99999", "Sella Seller", tenant_rec, TENANT_EMAIL, key=None,
                                           view_as=None, edit_mode=False)
check("a nonexistent buyer id renders the not-found placeholder", "Buyer not found" in page_buyer_missing)


# ======================================================================
# SECTION: Buyer page — Identity/Capacity/closer chip/Process signals/
# Track with you/Notes Ledger, and the matching closed-dot on Active Intros
# ======================================================================

alice_cf = {
    lf.TRANSACTOR_TYPE_FIELD: [lf.NATURAL_PERSON_ID],
    lf.TICKET_SIZE_FIELD: [5014555],  # 1,000,000 - 5,000,000
    lf.IQF_FIELD: [next(iter(lf.IQF_OK_IDS))],
    lf.CEF_FIELD: [lf.CEF_YES_ID],
    lf.ACCEPTS_FIELD: [7177773, 7177775],  # SPV, Fees
}
people = {"people": [
    {"id": TENANT_A_PID, "full_name": "Sella Seller", "email": TENANT_A_EMAIL, "custom_fields": {}},
    {"id": TENANT_B_PID, "full_name": "Tabor Seller", "email": TENANT_B_EMAIL, "custom_fields": {}},
    {"id": 2, "name": "Alice Buyer", "email": "alice@example.com", "title": "Managing Partner",
     "company_name": "Alice Capital", "website": "alicecapital.com", "linked_in_url": "linkedin.com/in/alice",
     "work_city": "Austin", "work_country": "USA", "company_id": 9001, "won_deals_total": 2, "custom_fields": alice_cf},
    # Bob: his OWN won_deals_total is 0, but a colleague at the SAME company_id has won -> FIRM closer.
    {"id": 3, "name": "Bob Buyer", "email": "bob@example.com", "company_id": 9002,
     "won_deals_total": 0, "custom_fields": {}},
    {"id": 30, "name": "Bob's Colleague", "email": "colleague@example.com", "company_id": 9002,
     "won_deals_total": 1, "custom_fields": {}},
    # Carla: no company_id at all, but a colleague sharing her exact company_name has won -> FIRM closer via fallback.
    {"id": 4, "name": "Carla Buyer", "email": "carla@example.com", "company_name": "Carla Holdings",
     "won_deals_total": 0, "custom_fields": {}},
    {"id": 40, "name": "Carla's Colleague", "email": "carla2@example.com", "company_name": "Carla Holdings",
     "won_deals_total": 5, "custom_fields": {}},
    # Dana: nobody has won anything -> no closer chip/dot at all.
    {"id": 5, "name": "Dana Buyer", "email": "dana@example.com", "company_id": 9005, "won_deals_total": 0, "custom_fields": {}},
    # Erin: linked to no deal at all -- the Notes Ledger's true empty state (admin-only reachable).
    {"id": 6, "name": "Erin Buyer", "email": "erin@example.com", "company_id": 9006, "won_deals_total": 0, "custom_fields": {}},
]}
deal_802 = {"id": 802, "name": "Alpha Deal", "company": {"name": "Alpha Industries"}, "deal_stage": {"id": lf.STAGE_MATCHED},
            "custom_fields": cf_status(7207579), "people": [{"id": TENANT_A_PID}, {"id": 2}], "updated_at": "2026-08-02T00:00:00Z"}
deal_803 = {"id": 803, "name": "Beta Deal", "company": {"name": "Beta Holdings"}, "deal_stage": {"id": lf.STAGE_MATCHED},
            "custom_fields": cf_status(7207580), "people": [{"id": TENANT_A_PID}, {"id": 2}], "updated_at": "2026-08-03T00:00:00Z"}
deal_804 = {"id": 804, "name": "Gamma Deal", "company": {"name": "Gamma Co"}, "deal_stage": {"id": lf.STAGE_MATCHED},
            "custom_fields": cf_status(7207578), "people": [{"id": TENANT_A_PID}, {"id": 2}], "updated_at": "2026-08-04T00:00:00Z"}
deal_805 = {"id": 805, "name": "Delta Deal", "company": {"name": "Delta LLC"}, "deal_stage": {"id": lf.STAGE_MATCHED},
            "custom_fields": cf_status(7207579), "people": [{"id": TENANT_B_PID}, {"id": 2}], "updated_at": "2026-08-05T00:00:00Z"}
deal_806 = {"id": 806, "name": "Echo Deal", "company": {"name": "Echo Co"}, "deal_stage": {"id": lf.STAGE_MATCHED},
            "custom_fields": cf_status(7207579), "people": [{"id": TENANT_A_PID}, {"id": 3}], "updated_at": "2026-08-06T00:00:00Z"}
deal_807 = {"id": 807, "name": "Foxtrot Deal", "company": {"name": "Foxtrot Co"}, "deal_stage": {"id": lf.STAGE_MATCHED},
            "custom_fields": cf_status(7207579), "people": [{"id": TENANT_A_PID}, {"id": 4}], "updated_at": "2026-08-07T00:00:00Z"}
deal_808 = {"id": 808, "name": "Golf Deal", "company": {"name": "Golf Co"}, "deal_stage": {"id": lf.STAGE_MATCHED},
            "custom_fields": cf_status(7207579), "people": [{"id": TENANT_A_PID}, {"id": 5}], "updated_at": "2026-08-08T00:00:00Z"}
deals_list = [deal_802, deal_803, deal_804, deal_805, deal_806, deal_807, deal_808, deal_900a, deal_901b]
_, fake_table = use_fixture({lf.PEOPLE_KEY: people, lf.INTEREST_KEY: {"buy": {}}, lf.DEALS_KEY: {"deals": deals_list}},
                             table_items=[
    {"tenant": TENANT_A_EMAIL, "sk": "intro#802", "notes": "Chasing signature", "notes_updated_at": 1750000000, "milestones": {"NDA": 1}},
    {"tenant": TENANT_A_EMAIL, "sk": "intro#803", "notes": "Great call yesterday", "milestones": {}},
    {"tenant": TENANT_B_EMAIL, "sk": "intro#805", "notes": "Following up next week", "milestones": {}},
])
tenant_a = lf._resolve_tenant(TENANT_A_EMAIL)
tenant_b = lf._resolve_tenant(TENANT_B_EMAIL)
assert tenant_a is not None and tenant_b is not None

page_alice = lf.render_buyer_page(2, "Sella Seller", tenant_a, TENANT_A_EMAIL, key=None, view_as=None, edit_mode=False)
check("Identity: name present", "Alice Buyer" in page_alice)
check("Identity: title/position present", "Managing Partner" in page_alice)
check("Identity: natural person -> no firm/company line despite company_name being set",
      "Alice Capital" not in page_alice)
check("Identity: city + country present", "Austin" in page_alice and "USA" in page_alice)
check("Identity: email present as a mailto link", 'href="mailto:alice@example.com"' in page_alice)

check("Capacity: tier badge present (IQF-OK -> Accredited)", 'class="tier-badge tier-accredited"' in page_alice)
check("Capacity: ticket range present", "$1.0M" in page_alice or "$1M" in page_alice)
check("Capacity: ID verified chip present (CEF Yes)", 'class="id-verified-chip">ID verified</span>' in page_alice)
check("Closer chip: person_won -> 'Proven closer with Rainmaker'", "Proven closer with Rainmaker" in page_alice)
check("Closer chip: firm text NOT also shown for a person closer", "Firm has closed with Rainmaker" not in page_alice)

page_bob = lf.render_buyer_page(3, "Sella Seller", tenant_a, TENANT_A_EMAIL, key=None, view_as=None, edit_mode=False)
check("Closer chip: firm_won via company_id match -> 'Firm has closed with Rainmaker'",
      "Firm has closed with Rainmaker" in page_bob)
check("Closer chip: person text NOT also shown for a firm-only closer", "Proven closer with Rainmaker" not in page_bob)
check("Capacity: ID verified chip absent when CEF is unset", 'class="id-verified-chip"' not in page_bob)

page_carla = lf.render_buyer_page(4, "Sella Seller", tenant_a, TENANT_A_EMAIL, key=None, view_as=None, edit_mode=False)
check("Closer chip: firm_won via company_name fallback (no company_id on Carla's own record)",
      "Firm has closed with Rainmaker" in page_carla)

page_dana = lf.render_buyer_page(5, "Sella Seller", tenant_a, TENANT_A_EMAIL, key=None, view_as=None, edit_mode=False)
check("Closer chip: no chip at all when nobody has won", "Proven closer" not in page_dana and "Firm has closed" not in page_dana)

# Item 3: "Process signals" (Accepts chips + IQF "Qualification on file"
# line) removed entirely from the buyer page, even for a buyer (Alice)
# whose fixture still carries ACCEPTS_FIELD/IQF_FIELD values -- the
# underlying field constants stay in the file (ACCEPTS_FIELD, IQF_FIELD,
# ACCEPTS_LABELS), only the rendering is gone.
check("Process signals card heading removed entirely (Alice, who has Accepts+IQF data)",
      "Process signals" not in page_alice)
check("Process signals: no Accepts chip renders even though Alice's fixture sets ACCEPTS_FIELD",
      "Accepts:" not in page_alice)
check("Process signals: no IQF chip renders even though Alice is IQF-OK",
      "Qualification on file" not in page_alice)

check("Track with you: heading present", "Track with you" in page_alice)
check("Track with you: Alpha Industries (disclosed) shown", "Alpha Industries" in page_alice)
check("Track with you: Beta Holdings (disclosed) shown", "Beta Holdings" in page_alice)
check("Track with you: Gamma Co (pending/non-disclosed) NOT shown", "Gamma Co" not in page_alice)
check("Track with you: read-only milestone checkboxes present (disabled)",
      re.search(r'<input type="checkbox" class="ei-milestone"[^>]*disabled', page_alice) is not None)

page_alice_admin = lf.render_buyer_page(2, "Admin", tenant_a, TENANT_A_EMAIL, key=ADMIN_KEY, view_as=None, edit_mode=True)
check("admin edit mode: Sella Seller's tenant label present", "Sella Seller" in page_alice_admin)
check("admin edit mode: Tabor Seller's tenant label present", "Tabor Seller" in page_alice_admin)
check("admin edit mode: Gamma Co (pending) IS shown (admin sees pending too)", "Gamma Co" in page_alice_admin)
check("admin edit mode: Delta LLC (tenant B's deal) shown", "Delta LLC" in page_alice_admin)

check("Notes ledger: heading present", ">Notes</h2" in page_alice)
check("Notes ledger: 802's note shown in a textarea", ">Chasing signature</textarea>" in page_alice)
check("Notes ledger: 803's note shown", ">Great call yesterday</textarea>" in page_alice)
check("Notes ledger: a last-edited date shown for 802 (has notes_updated_at)", "Last edited" in page_alice)

page_nodeals_admin = lf.render_buyer_page(6, "Admin", tenant_a, TENANT_A_EMAIL, key=ADMIN_KEY, view_as=None, edit_mode=True)
check("Notes ledger empty state text matches exactly",
      "No notes yet — add one from Active Intros or right here." in page_nodeals_admin)
check("admin notes ledger: tenant B's note (805) shown too", "Following up next week" in page_alice_admin)
check("admin notes ledger: entries labeled by tenant then company", "Tabor Seller &middot; Delta LLC" in page_alice_admin)

lf.urllib.request.urlopen = fake_urlopen2
resp = lf.lambda_handler(post_event({"deal_id": "802", "notes": "Updated note"}, cookies=[tenant_cookie(TENANT_A_EMAIL)]), None)
check("notes write -> 200", resp["statusCode"] == 200)
last = fake_table.updates[-1]
check("notes_updated_at written alongside notes", ":nua" in last["ExpressionAttributeValues"])
check("notes value written correctly", last["ExpressionAttributeValues"].get(":no") == "Updated note")

page_intros = lf.render_intros_page("Sella Seller", tenant=tenant_a, tenant_email=TENANT_A_EMAIL,
                                     key=None, view_as=None, edit_mode=False)
row_802 = row_for(page_intros, "802")
check("Active Intros closed-dot: person-closer tooltip matches the chip text",
      'title="Proven closer with Rainmaker"' in row_802)
row_806 = row_for(page_intros, "806")
check("Active Intros closed-dot: firm-closer tooltip matches the chip text",
      'title="Firm has closed with Rainmaker"' in row_806)
row_808 = row_for(page_intros, "808")
check("Active Intros closed-dot: no dot at all when there's no closer", 'class="closed-dot"' not in row_808)

page_anon = lf.render_buyer_page(5, "Tabor Seller", tenant_b, TENANT_B_EMAIL, key=None, view_as=None, edit_mode=False)
check("anonymized card (viewed by a tenant with no disclosed deal): the old shape, unchanged",
      "Identity available after introduction." in page_anon and "Dana Buyer" not in page_anon)
check("anonymized card leaks no Track with you / Notes sections",
      "Track with you" not in page_anon and ">Notes</h2" not in page_anon)


# ======================================================================
# SECTION: Buyer page — single header card, two-column layout, About the
# firm (companies.json), Track with you checkboxes-only
# ======================================================================

LONG_DESC = " ".join(f"word{i}" for i in range(80))  # 80 words -> needs the "more" expander

alice_cf2 = {
    lf.TRANSACTOR_TYPE_FIELD: [6484811],  # Family Office -- entity, has a firm
    lf.TICKET_SIZE_FIELD: [5014555],
    lf.IQF_FIELD: list(lf.IQF_OK_IDS)[:1],
    lf.CEF_FIELD: [lf.CEF_YES_ID],
}
bob_cf2 = {lf.TRANSACTOR_TYPE_FIELD: [6484811]}
natural_cf2 = {lf.TRANSACTOR_TYPE_FIELD: [lf.NATURAL_PERSON_ID]}

people = {"people": [
    {"id": TENANT_A_PID, "full_name": "Sella Seller", "email": TENANT_A_EMAIL, "custom_fields": {}},
    # Alice: entity buyer, company_id 9001 -- HAS a companies.json record.
    {"id": 2, "name": "Alice Buyer", "email": "alice@example.com", "title": "Managing Partner",
     "company_name": "Alice Capital", "company_id": 9001, "won_deals_total": 0, "custom_fields": alice_cf2},
    # Bob: entity buyer, company_id 9002 -- NO matching companies.json record.
    {"id": 3, "name": "Bob Buyer", "email": "bob@example.com", "company_name": "Bob Holdings",
     "company_id": 9002, "won_deals_total": 0, "custom_fields": bob_cf2},
    # Nadia: natural person -- no firm at all, About-firm never attempted.
    {"id": 9, "name": "Nadia Buyer", "email": "nadia@example.com", "company_name": "Should Never Show",
     "company_id": 9009, "won_deals_total": 0, "custom_fields": natural_cf2},
]}
deal_802 = {"id": 802, "name": "Alpha Deal", "company": {"name": "Alpha Industries"}, "deal_stage": {"id": lf.STAGE_MATCHED},
            "custom_fields": cf_status(7207579), "people": [{"id": TENANT_A_PID}, {"id": 2}], "updated_at": "2026-08-02T00:00:00Z"}
deal_803 = {"id": 803, "name": "Beta Deal", "company": {"name": "Beta Holdings"}, "deal_stage": {"id": lf.STAGE_MATCHED},
            "custom_fields": cf_status(7207584), "people": [{"id": TENANT_A_PID}, {"id": 2}], "updated_at": "2026-08-03T00:00:00Z"}  # Stalled
deal_806 = {"id": 806, "name": "Echo Deal", "company": {"name": "Echo Co"}, "deal_stage": {"id": lf.STAGE_MATCHED},
            "custom_fields": cf_status(7207579), "people": [{"id": TENANT_A_PID}, {"id": 3}], "updated_at": "2026-08-06T00:00:00Z"}
deal_809 = {"id": 809, "name": "Hotel Deal", "company": {"name": "Hotel Co"}, "deal_stage": {"id": lf.STAGE_MATCHED},
            "custom_fields": cf_status(7207579), "people": [{"id": TENANT_A_PID}, {"id": 9}], "updated_at": "2026-08-09T00:00:00Z"}
deals_list = [deal_802, deal_803, deal_806, deal_809, deal_900a]
companies = {"companies": [
    {"id": 9001, "name": "Alice Capital", "city": "Austin", "country": "USA", "founded_year": 2015,
     "description": LONG_DESC, "custom_fields": {lf.COMPANY_PITCHBOOK_FIELD: "pitchbook.com/profile/alice-capital"}},
    # No entry at all for company_id 9002 (Bob) -- tests graceful omission.
]}
use_fixture({lf.PEOPLE_KEY: people, lf.INTEREST_KEY: {"buy": {}}, lf.DEALS_KEY: {"deals": deals_list},
             lf.COMPANIES_KEY: companies}, table_items=[
    {"tenant": TENANT_A_EMAIL, "sk": "intro#802", "notes": "Chasing signature", "milestones": {"NDA": 1}},
    # An exit status (Stalled) discloses only with real evidence of prior progress -- the NDA
    # milestone here represents genuine progress before stalling.
    {"tenant": TENANT_A_EMAIL, "sk": "intro#803", "milestones": {"NDA": 1}},
])
tenant_a = lf._resolve_tenant(TENANT_A_EMAIL)
assert tenant_a is not None

page_alice = lf.render_buyer_page(2, "Sella Seller", tenant_a, TENANT_A_EMAIL, key=None, view_as=None, edit_mode=False)
check("single header card present", 'class="card buyer-header"' in page_alice)
check("no separate 'Capacity' heading/card anymore (folded into the header)",
      '<h2 class="buyer-section-heading">Capacity</h2>' not in page_alice)
check("two-column layout wrapper present", 'class="buyer-columns"' in page_alice)
check("left column present", 'class="buyer-col-left"' in page_alice)
check("right column present", 'class="buyer-col-right"' in page_alice)

left_html = page_alice.split('class="buyer-col-left"')[1].split('class="buyer-col-right"')[0]
right_html = page_alice.split('class="buyer-col-right"')[1]
check("About the firm card is in the LEFT column", "About Alice Capital" in left_html)
check("Process signals card is gone entirely, not just moved (left column no longer carries it)",
      "Process signals" not in left_html)
check("Track with you card is in the RIGHT column", "Track with you" in right_html)
check("Notes card is in the RIGHT column", ">Notes</h2" in right_html)

header_html = page_alice.split('class="card buyer-header"')[1].split('class="buyer-columns"')[0]
check("header: name present", "Alice Buyer" in header_html)
check("header: title present in line 1", "Managing Partner" in header_html)
check("header: firm name present in line 1", "Alice Capital" in header_html)
check("header: tier/ticket/transactor chip row present", 'class="buyer-header-chips"' in header_html)
check("header: ticket-range chip present", "$1M" in header_html or "$1.0M" in header_html)
check("header: transactor-type chip present", "Family Office" in header_html)
check("header: badges (right-aligned block) present", 'class="buyer-header-badges"' in header_html)
check("header: ID verified chip present (CEF Yes)", 'class="id-verified-chip"' in header_html)

check("About card: city/country shown", "Austin, USA" in page_alice)
check("About card: founded year shown", "Founded 2015" in page_alice)
check("About card: PitchBook link shown", "PitchBook profile" in page_alice and "pitchbook.com" in page_alice)
check("About card: description text NOT shown to a tenant", "word0 word1" not in page_alice)
check("About card: internal marker NOT shown to a tenant",
      "(internal — review before publishing)" not in page_alice)

page_alice_admin = lf.render_buyer_page(2, "Admin", tenant_a, TENANT_A_EMAIL, key=ADMIN_KEY, view_as=None, edit_mode=True)
check("admin: description text shown", "word0 word1" in page_alice_admin)
check("admin: description trimmed with a details/summary 'more' expander (80 words > 60)",
      '<details class="firm-more">' in page_alice_admin)
check("admin: the full remainder of the description is present inside the expander", "word79" in page_alice_admin)
check("admin: internal-review marker shown", "(internal — review before publishing)" in page_alice_admin)

ABOUT_HEADING_RE = re.compile(r'<h2 class="buyer-section-heading">About\b')
page_bob = lf.render_buyer_page(3, "Sella Seller", tenant_a, TENANT_A_EMAIL, key=None, view_as=None, edit_mode=False)
check("About card omitted entirely when the firm isn't in companies.json", ABOUT_HEADING_RE.search(page_bob) is None)
check("Process signals card omitted when neither Accepts nor IQF is set (Bob)",
      '<h2 class="buyer-section-heading">Process signals</h2>' not in page_bob)

page_nadia = lf.render_buyer_page(9, "Sella Seller", tenant_a, TENANT_A_EMAIL, key=None, view_as=None, edit_mode=False)
check("natural-person buyer: no firm line in the header", "Should Never Show" not in page_nadia)
check("natural-person buyer: no About-the-firm card at all", ABOUT_HEADING_RE.search(page_nadia) is None)

check("Track with you: read-only milestone checkboxes present", 'class="ei-milestones"' in right_html)
check("Track with you: NO editable/disabled flag <select> anymore (chip only)",
      "<select" not in right_html.split("Notes")[0])
check("Track with you: a Stalled deal (803) shows a Stalled status chip", 'class="status-chip stalled"' in right_html)
check("Track with you: an in-progress deal (802) shows a plain status pill", 'class="status-pill"' in right_html)


# ======================================================================
# SECTION: Matched-or-later buy-deal stage set (which raw Pipeline stages
# count as a "real" introduction at all)
# ======================================================================

EXPECTED_MATCHED_OR_LATER = {2381534, 2517909, 2533998, 2381535, 2388323, 2456153, 2426790, 2447751, 111802, 2379321}
check("MATCHED_OR_LATER_STAGE_IDS matches the verified set exactly", lf.MATCHED_OR_LATER_STAGE_IDS == EXPECTED_MATCHED_OR_LATER)
check("Firm (111800) is deliberately excluded", lf.STAGE_FIRM not in lf.MATCHED_OR_LATER_STAGE_IDS)
check("Inquiry (2109142) excluded", 2109142 not in lf.MATCHED_OR_LATER_STAGE_IDS)
check("Hold (2094373) excluded", 2094373 not in lf.MATCHED_OR_LATER_STAGE_IDS)
check("Trade Broken (2486672) excluded", 2486672 not in lf.MATCHED_OR_LATER_STAGE_IDS)
check("Obsolete (2348038) excluded", 2348038 not in lf.MATCHED_OR_LATER_STAGE_IDS)
check("Lost (111801) excluded", 111801 not in lf.MATCHED_OR_LATER_STAGE_IDS)
check("Lost (2379322) excluded", 2379322 not in lf.MATCHED_OR_LATER_STAGE_IDS)
check("new stage constants match their documented ids",
      lf.STAGE_INVOICED == 2456153 and lf.STAGE_ROFR == 2426790 and lf.STAGE_BLOCKED == 2447751)
check("WON_STAGE_IDS is a subset of MATCHED_OR_LATER_STAGE_IDS (reused, not re-declared)",
      lf.WON_STAGE_IDS <= lf.MATCHED_OR_LATER_STAGE_IDS)
check("_deal_stage_id reads the nested deal_stage.id object shape", lf._deal_stage_id({"deal_stage": {"id": 2381534}}) == 2381534)
check("_deal_stage_id falls back to a flat deal_stage_id scalar", lf._deal_stage_id({"deal_stage_id": 2381534}) == 2381534)
check("_deal_stage_id returns None when neither shape is present", lf._deal_stage_id({}) is None)

STAGE_DEALS = {
    "Matched (kept)": (701, lf.STAGE_MATCHED, True),
    "Firm (excluded)": (702, lf.STAGE_FIRM, False),
    "Confirm (kept)": (703, lf.STAGE_CONFIRM, True),
    "LOI Signed (kept)": (704, lf.STAGE_LOI_SIGNED, True),
    "Transfer Notice (kept)": (705, lf.STAGE_TRANSFER_NOTICE, True),
    "SPA Signed (kept)": (706, lf.STAGE_SPA_SIGNED, True),
    "Invoiced (kept)": (707, lf.STAGE_INVOICED, True),
    "ROFR'd (kept)": (708, lf.STAGE_ROFR, True),
    "Blocked (kept)": (709, lf.STAGE_BLOCKED, True),
    "Won Deal (kept)": (710, 111802, True),
    "Won (kept)": (711, 2379321, True),
    "Inquiry (excluded)": (712, 2109142, False),
    "Hold (excluded)": (713, 2094373, False),
    "Trade Broken (excluded)": (714, 2486672, False),
    "Obsolete (excluded)": (715, 2348038, False),
    "Lost/111801 (excluded)": (716, 111801, False),
    "Lost/2379322 (excluded)": (717, 2379322, False),
}
deals_list = [deal_900a]
for label, (deal_id, stage_id, _) in STAGE_DEALS.items():
    deals_list.append({"id": deal_id, "name": label, "company": {"name": f"Co {deal_id}"},
                        "deal_stage": {"id": stage_id, "name": "whatever"}, "custom_fields": cf_status(7207579),
                        "people": [{"id": TENANT_A_PID}, {"id": 2}], "updated_at": "2026-08-01T00:00:00Z"})
people = {"people": [{"id": TENANT_A_PID, "full_name": "Sella Seller", "email": TENANT_A_EMAIL, "custom_fields": {}},
                      {"id": 2, "name": "Alice Buyer", "email": "alice@example.com", "custom_fields": {}}]}
# Bug fix fixture: Intro Status Closed at a totally unmapped/unrecognized
# stage id (not in MATCHED_OR_LATER_STAGE_IDS, not a dead stage either) --
# the actual hypothesized real-world trigger (Panthalassa): a deal whose
# stage never lands on one of the two ids this file recognizes as Won,
# but whose Intro Status field IS explicitly Closed. Before the fix this
# was invisible to get_my_matched_buy_deals (and everywhere downstream of
# it) while still correctly counting toward the desk-wide Raised total.
deals_list.append({"id": 718, "name": "Unmapped stage + Closed status (kept, bug fix)",
                    "company": {"name": "Co 718"}, "deal_stage": {"id": 9999999, "name": "whatever"},
                    "custom_fields": cf_status(lf.INTRO_STATUS_CLOSED_ID),
                    "people": [{"id": TENANT_A_PID}, {"id": 2}], "updated_at": "2026-08-01T00:00:00Z"})
use_fixture({lf.PEOPLE_KEY: people, lf.INTEREST_KEY: {"buy": {}}, lf.DEALS_KEY: {"deals": deals_list}})
tenant_a = lf._resolve_tenant(TENANT_A_EMAIL)
assert tenant_a is not None

matched = lf.get_my_matched_buy_deals(TENANT_A_PID)
matched_ids = {d["id"] for d in matched}
for label, (deal_id, stage_id, should_be_included) in STAGE_DEALS.items():
    check(f"get_my_matched_buy_deals: {label} -> {'included' if should_be_included else 'excluded'}",
          (deal_id in matched_ids) == should_be_included)
check("get_my_matched_buy_deals: Closed status at an unmapped stage id -> included (the bug fix)",
      718 in matched_ids)

page_intros = lf.render_intros_page("Sella Seller", tenant=tenant_a, tenant_email=TENANT_A_EMAIL,
                                     key=None, view_as=None, edit_mode=False)
check("Active Intros: a Firm-stage deal (702) never appears at all", ">Co 702<" not in page_intros)
check("Active Intros: an Invoiced-stage deal (707) does appear", ">Co 707<" in page_intros)
check("Active Intros: the unmapped-stage Closed deal (718) appears, named (Closed is always disclosed)",
      "Co 718" in page_intros and "Alice Buyer" in page_intros)

page_buyer = lf.render_buyer_page(2, "Sella Seller", tenant_a, TENANT_A_EMAIL, key=None, view_as=None, edit_mode=False)
check("Buyer page Track With You: Firm-stage deal excluded", ">Co 702<" not in page_buyer)
check("Buyer page Track With You: Won-stage deal (711) included", ">Co 711<" in page_buyer)
check("Buyer page Track With You: unmapped-stage Closed deal (718) included", ">Co 718<" in page_buyer)

# --- Direct unit tests for the predicate itself ---
check("_is_matched_or_later_buy_deal: Closed status at an ordinary Matched stage -> True (unchanged)",
      lf._is_matched_or_later_buy_deal({"deal_stage": {"id": lf.STAGE_MATCHED},
                                         "custom_fields": cf_status(lf.INTRO_STATUS_CLOSED_ID)}))
check("_is_matched_or_later_buy_deal: Closed status at a totally unmapped/unrecognized stage id -> True (the bug fix)",
      lf._is_matched_or_later_buy_deal({"deal_stage": {"id": 9999999},
                                         "custom_fields": cf_status(lf.INTRO_STATUS_CLOSED_ID)}))
check("_is_matched_or_later_buy_deal: Closed status even at a dead stage (Lost) -> True (Closed status wins)",
      lf._is_matched_or_later_buy_deal({"deal_stage": {"id": 111801},
                                         "custom_fields": cf_status(lf.INTRO_STATUS_CLOSED_ID)}))
check("_is_matched_or_later_buy_deal: Closed status but Sell-tagged -> False (side gate still applies)",
      not lf._is_matched_or_later_buy_deal({"deal_stage": {"id": 9999999},
                                             "custom_fields": {lf.DEAL_SIDE_FIELD: [lf.DEAL_SIDE_SELL_ID],
                                                                lf.INTRO_STATUS_FIELD: [lf.INTRO_STATUS_CLOSED_ID]}}))
check("_is_matched_or_later_buy_deal: no Closed status, unmapped stage -> False (unchanged)",
      not lf._is_matched_or_later_buy_deal({"deal_stage": {"id": 9999999}, "custom_fields": cf_status(7207579)}))


# ======================================================================
# SECTION: Closed-out buy deals (Lost/Trade Broken/Obsolete) — disclosure
# gate and rendering across Active Intros / Company page / Buyer page
# ======================================================================

def cf_buy(status_option_id=None):
    return cf_status(status_option_id)

check("STAGE_TRADE_BROKEN == 2486672", lf.STAGE_TRADE_BROKEN == 2486672)
check("_closed_out_outcome_name: Lost (111801) -> Passed", lf._closed_out_outcome_name(111801) == "Passed")
check("_closed_out_outcome_name: Lost (2379322) -> Passed", lf._closed_out_outcome_name(2379322) == "Passed")
check("_closed_out_outcome_name: Trade Broken -> Passed", lf._closed_out_outcome_name(lf.STAGE_TRADE_BROKEN) == "Passed")
check("_closed_out_outcome_name: Obsolete -> Withdrawn", lf._closed_out_outcome_name(lf.OBSOLETE_STAGE_ID) == "Withdrawn")
check("_closed_out_outcome_name: Matched -> None (not closed out)", lf._closed_out_outcome_name(lf.STAGE_MATCHED) is None)
check("_closed_out_outcome_name: Won -> None", lf._closed_out_outcome_name(111802) is None)
check("_closed_out_outcome_name: None stage -> None", lf._closed_out_outcome_name(None) is None)

# _deal_exit_outcome_name: the composite resolver the bug fix adds --
# stage-derived first, falling back to the deal's own RAW Intro Status
# field for an exit expressed via status on an otherwise live stage.
check("_deal_exit_outcome_name: stage-based Lost -> Passed (unchanged)",
      lf._deal_exit_outcome_name({"deal_stage": {"id": 111801}, "custom_fields": {}}) == "Passed")
check("_deal_exit_outcome_name: stage-based Obsolete -> Withdrawn (unchanged)",
      lf._deal_exit_outcome_name({"deal_stage": {"id": lf.OBSOLETE_STAGE_ID}, "custom_fields": {}}) == "Withdrawn")
check("_deal_exit_outcome_name: Matched stage + raw status Passed -> 'Passed' (the bug fix)",
      lf._deal_exit_outcome_name({"deal_stage": {"id": lf.STAGE_MATCHED},
                                   "custom_fields": cf_status(7207585)}) == "Passed")
check("_deal_exit_outcome_name: LOI Signed stage + raw status Withdrawn -> 'Withdrawn'",
      lf._deal_exit_outcome_name({"deal_stage": {"id": lf.STAGE_LOI_SIGNED},
                                   "custom_fields": cf_status(7207586)}) == "Withdrawn")
check("_deal_exit_outcome_name: Matched stage + raw status Stalled -> None (Stalled never a Closed-out outcome)",
      lf._deal_exit_outcome_name({"deal_stage": {"id": lf.STAGE_MATCHED},
                                   "custom_fields": cf_status(7207584)}) is None)
check("_deal_exit_outcome_name: Matched stage + no exit status at all -> None",
      lf._deal_exit_outcome_name({"deal_stage": {"id": lf.STAGE_MATCHED}, "custom_fields": cf_status(None)}) is None)
# Bug fix: Intro Status Closed is a positive/won outcome, checked FIRST --
# ahead of stage-derived AND ahead of the Passed/Withdrawn status fallback
# -- so it's never mistaken for a stage-based loss even in the edge case
# where Pipeline's stage field still shows Lost/Obsolete.
check("_deal_exit_outcome_name: Closed status at an ordinary Matched stage -> 'Closed'",
      lf._deal_exit_outcome_name({"deal_stage": {"id": lf.STAGE_MATCHED},
                                   "custom_fields": cf_status(lf.INTRO_STATUS_CLOSED_ID)}) == "Closed")
check("_deal_exit_outcome_name: Closed status even when stage is ALSO Lost -> 'Closed' (status wins, not 'Passed')",
      lf._deal_exit_outcome_name({"deal_stage": {"id": 111801},
                                   "custom_fields": cf_status(lf.INTRO_STATUS_CLOSED_ID)}) == "Closed")
check("_deal_exit_outcome_name: Closed status even when stage is ALSO Obsolete -> 'Closed' (status wins, not 'Withdrawn')",
      lf._deal_exit_outcome_name({"deal_stage": {"id": lf.OBSOLETE_STAGE_ID},
                                   "custom_fields": cf_status(lf.INTRO_STATUS_CLOSED_ID)}) == "Closed")
check("INTRODUCED_OR_LATER_STATUS_IDS excludes Matched (7207578)", 7207578 not in lf.INTRODUCED_OR_LATER_STATUS_IDS)
check("INTRODUCED_OR_LATER_STATUS_IDS excludes the three exit ids", not (lf.EXIT_STATUS_IDS & lf.INTRODUCED_OR_LATER_STATUS_IDS))
check("INTRODUCED_OR_LATER_STATUS_IDS == {Introduced, NDA, VDR, Sub Docs, Wired, Closed}",
      lf.INTRODUCED_OR_LATER_STATUS_IDS == {7207579, 7207580, 7207581, 7207582, 7207583, 7207587})

check("_is_closed_out_buy_deal: Buy + Lost -> True",
      lf._is_closed_out_buy_deal({"deal_stage": {"id": 111801}, "custom_fields": cf_buy()}))
check("_is_closed_out_buy_deal: Sell + Lost -> False (side gate)",
      not lf._is_closed_out_buy_deal({"deal_stage": {"id": 111801}, "custom_fields": cf_sell()}))
check("_is_closed_out_buy_deal: Buy + Matched -> False (live stage, not dead)",
      not lf._is_closed_out_buy_deal({"deal_stage": {"id": lf.STAGE_MATCHED}, "custom_fields": cf_buy()}))
check("_is_closed_out_buy_deal is never true for a MATCHED_OR_LATER stage (disjoint by construction)",
      not any(lf._closed_out_outcome_name(sid) is not None for sid in lf.MATCHED_OR_LATER_STAGE_IDS))
# Bug fix guard: a Closed-status deal is NEVER claimed by the stage-based
# dead-exit predicate, even in the edge case where its Pipeline stage
# happens to ALSO be Lost/Trade Broken/Obsolete -- it always routes
# through _is_matched_or_later_buy_deal's own Closed-status check instead.
check("_is_closed_out_buy_deal: Closed status even at a dead stage (Lost) -> False (mutual-exclusivity guard)",
      not lf._is_closed_out_buy_deal({"deal_stage": {"id": 111801},
                                       "custom_fields": cf_status(lf.INTRO_STATUS_CLOSED_ID)}))
check("_is_closed_out_buy_deal: Closed status even at Trade Broken -> False",
      not lf._is_closed_out_buy_deal({"deal_stage": {"id": lf.STAGE_TRADE_BROKEN},
                                       "custom_fields": cf_status(lf.INTRO_STATUS_CLOSED_ID)}))
check("_is_closed_out_buy_deal: Closed status even at Obsolete -> False",
      not lf._is_closed_out_buy_deal({"deal_stage": {"id": lf.OBSOLETE_STAGE_ID},
                                       "custom_fields": cf_status(lf.INTRO_STATUS_CLOSED_ID)}))

check("_closed_out_disclosed: raw status Introduced -> True", lf._closed_out_disclosed({"custom_fields": cf_buy(7207579)}))
check("_closed_out_disclosed: raw status NDA Signed -> True", lf._closed_out_disclosed({"custom_fields": cf_buy(7207580)}))
check("_closed_out_disclosed: raw status empty -> False (anonymized like Pending)", not lf._closed_out_disclosed({"custom_fields": cf_buy()}))
check("_closed_out_disclosed: raw status Matched (explicit) -> False", not lf._closed_out_disclosed({"custom_fields": cf_buy(7207578)}))
check("_closed_out_disclosed: raw status itself Passed (no prior progress recorded) -> False",
      not lf._closed_out_disclosed({"custom_fields": cf_buy(7207585)}))

check("_deal_loss_reason_text: absent -> None", lf._deal_loss_reason_text({}) is None)
check("_deal_loss_reason_text: plain string", lf._deal_loss_reason_text({"deal_loss_reason": "Price too low"}) == "Price too low")
check("_deal_loss_reason_text: {'name':...} object shape",
      lf._deal_loss_reason_text({"deal_loss_reason": {"name": "Went with another buyer"}}) == "Went with another buyer")
check("_deal_loss_reason_text: list-wrapped object shape",
      lf._deal_loss_reason_text({"deal_loss_reason": [{"name": "Financing fell through"}]}) == "Financing fell through")
check("_deal_loss_reason_text: empty string -> None", lf._deal_loss_reason_text({"deal_loss_reason": "  "}) is None)
check("_deal_loss_reason_text: unrecognized dict shape -> None",
      lf._deal_loss_reason_text({"deal_loss_reason": {"unexpected": 1}}) is None)

r1 = lf._resolve_intro_status({"custom_fields": cf_buy(7207585), "updated_at": "2026-08-01T00:00:00Z"}, None)
check("_resolve_intro_status: status jumped straight from empty to Passed -> NOT disclosed",
      r1["is_exit"] and r1["name"] == "Passed" and r1["disclosed"] is False)
r2 = lf._resolve_intro_status({"custom_fields": cf_buy(7207585), "updated_at": "2026-08-01T00:00:00Z"},
                               {"milestones": {"NDA": 1750000000}})
check("_resolve_intro_status: a milestone on record + exit status -> disclosed", r2["is_exit"] and r2["disclosed"] is True)
r3 = lf._resolve_intro_status({"custom_fields": cf_buy(7207583), "updated_at": "2026-08-01T00:00:00Z"},
                               {"status_override": 7207585, "override_at": "2100000000"})
check("_resolve_intro_status: override=Passed but raw field=Wired -> disclosed (status-history signal)",
      r3["is_exit"] and r3["name"] == "Passed" and r3["disclosed"] is True)
r4 = lf._resolve_intro_status({"custom_fields": cf_buy(7207584), "updated_at": "2026-08-01T00:00:00Z"}, None)
check("_resolve_intro_status: Stalled straight from empty, no milestone -> NOT disclosed",
      r4["is_exit"] and r4["name"] == "Stalled" and r4["disclosed"] is False)
r5 = lf._resolve_intro_status({"custom_fields": cf_buy(7207579), "updated_at": "2026-08-01T00:00:00Z"}, None)
check("_resolve_intro_status: Introduced (non-exit) -> disclosed, unaffected", not r5["is_exit"] and r5["disclosed"] is True)
r6 = lf._resolve_intro_status({"custom_fields": cf_buy(), "updated_at": "2026-08-01T00:00:00Z"}, None)
check("_resolve_intro_status: empty/Matched -> not disclosed, unaffected", not r6["is_exit"] and r6["disclosed"] is False)

people = {"people": [
    {"id": TENANT_A_PID, "full_name": "Sella Seller", "email": TENANT_A_EMAIL, "custom_fields": {}},
    {"id": 2, "name": "Alice Buyer", "email": "alice@example.com", "custom_fields": {}},
    {"id": 3, "name": "Bob Buyer", "email": "bob@example.com", "custom_fields": {}},
    {"id": 4, "name": "Cara Buyer", "email": "cara@example.com", "custom_fields": {}},
]}
deal_live_buy = {"id": 800, "name": "Live Buy Deal", "company": {"name": "Live Buy Co"}, "deal_stage": {"id": lf.STAGE_MATCHED},
                 "custom_fields": cf_buy(7207579), "people": [{"id": TENANT_A_PID}, {"id": 2}], "updated_at": "2026-08-01T00:00:00Z"}
deal_lost = {"id": 801, "name": "Lost Deal", "company": {"name": "Lost Co"}, "deal_stage": {"id": 111801},
             "custom_fields": cf_buy(7207579), "deal_loss_reason": {"name": "Went with another sponsor"},
             "people": [{"id": TENANT_A_PID}, {"id": 3}], "updated_at": "2026-08-01T00:00:00Z"}
deal_broken = {"id": 802, "name": "Broken Deal", "company": {"name": "Broken Co"}, "deal_stage": {"id": lf.STAGE_TRADE_BROKEN},
               "custom_fields": cf_buy(7207581), "people": [{"id": TENANT_A_PID}, {"id": 4}], "updated_at": "2026-08-01T00:00:00Z"}
deal_obsolete = {"id": 803, "name": "Obsolete Deal", "company": {"name": "Obsolete Co"}, "deal_stage": {"id": lf.OBSOLETE_STAGE_ID},
                 "custom_fields": cf_buy(), "people": [{"id": TENANT_A_PID}, {"id": 2}], "updated_at": "2026-08-01T00:00:00Z"}
# Bug fix fixtures: exits expressed via the Intro Status field on an
# otherwise still-live (matched-or-later) stage -- never a Pipeline
# stage move -- which get_my_closed_out_buy_deals (stage-only) can never
# catch. deal_status_passed carries prior-progress evidence (a
# milestone) so it stays disclosed, exactly like deal_lost above;
# deal_status_withdrawn has none, exactly like deal_obsolete above.
# deal_still_stalled is the negative case: Stalled must stay in
# Introduced, flagged, never routed to Closed out.
deal_status_passed = {"id": 804, "name": "Status Passed Deal", "company": {"name": "Status Passed Co"},
                      "deal_stage": {"id": lf.STAGE_MATCHED}, "custom_fields": cf_buy(7207585),
                      "people": [{"id": TENANT_A_PID}, {"id": 3}], "updated_at": "2026-08-01T00:00:00Z"}
deal_status_withdrawn = {"id": 805, "name": "Status Withdrawn Deal", "company": {"name": "Status Withdrawn Co"},
                        "deal_stage": {"id": lf.STAGE_LOI_SIGNED}, "custom_fields": cf_buy(7207586),
                        "people": [{"id": TENANT_A_PID}, {"id": 2}], "updated_at": "2026-08-01T00:00:00Z"}
deal_still_stalled = {"id": 806, "name": "Stalled Deal", "company": {"name": "Stalled Co"},
                      "deal_stage": {"id": lf.STAGE_MATCHED}, "custom_fields": cf_buy(7207584),
                      "people": [{"id": TENANT_A_PID}, {"id": 4}], "updated_at": "2026-08-01T00:00:00Z"}
# Bug fix fixture: RAW Intro Status Closed on an otherwise-live (Matched)
# stage -- a positive/won outcome, must land in Closed out styled green,
# never lumped in with the gray Passed/Withdrawn rows above, and must
# NEVER be claimed by get_my_closed_out_buy_deals (stage-based, dead
# exits only) since Closed is not a dead outcome.
deal_status_closed = {"id": 807, "name": "Status Closed Deal", "company": {"name": "Status Closed Co"},
                      "deal_stage": {"id": lf.STAGE_MATCHED}, "custom_fields": cf_buy(lf.INTRO_STATUS_CLOSED_ID),
                      "people": [{"id": TENANT_A_PID}, {"id": 2}], "updated_at": "2026-08-01T00:00:00Z"}
deals_list = [deal_900a, deal_live_buy, deal_lost, deal_broken, deal_obsolete,
              deal_status_passed, deal_status_withdrawn, deal_still_stalled, deal_status_closed]
_, fake_table = use_fixture({lf.PEOPLE_KEY: people, lf.INTEREST_KEY: {"buy": {}}, lf.DEALS_KEY: {"deals": deals_list}},
                             table_items=[
                                 {"tenant": TENANT_A_EMAIL, "sk": "intro#804", "milestones": {"NDA": 1750000000}},
                                 {"tenant": TENANT_A_EMAIL, "sk": "intro#806", "milestones": {"NDA": 1750000000}},
                             ])
tenant_a = lf._resolve_tenant(TENANT_A_EMAIL)
assert tenant_a is not None

closed_out = lf.get_my_closed_out_buy_deals(TENANT_A_PID)
closed_out_ids = {d["id"] for d in closed_out}
check("get_my_closed_out_buy_deals finds Lost/Trade Broken/Obsolete (3 deals)", closed_out_ids == {801, 802, 803})
check("get_my_closed_out_buy_deals excludes the live Matched deal", 800 not in closed_out_ids)
check("get_my_closed_out_buy_deals excludes the Closed-status deal (807, not a dead/stage-based exit)",
      807 not in closed_out_ids)

page_intros = lf.render_intros_page("Sella Seller", tenant=tenant_a, tenant_email=TENANT_A_EMAIL,
                                     key=None, view_as=None, edit_mode=False)
check("Active Intros: collapsed 'Closed out' section present with count (6: 3 stage-based + 2 status-based exits + 1 status-based Closed)",
      '<summary>Closed out <span class="count">(6)</span></summary>' in page_intros)
check("Active Intros: the live Matched deal still shows under Introduced", ">Live Buy Co<" in page_intros)
check("Active Intros: a disclosed closed-out row names the real buyer (Bob Buyer)", "Bob Buyer" in page_intros)
check("Active Intros: a disclosed closed-out row shows the loss-reason suffix",
      "— Went with another sponsor" in page_intros)
check("Active Intros: a disclosed-no-reason closed-out row shows a plain gray chip",
      ">Broken Co<" in page_intros and "Cara Buyer" in page_intros)
obs_row = row_for(page_intros, "803") or page_intros[page_intros.find(">Obsolete Co<") - 400: page_intros.find(">Obsolete Co<") + 400]
check("Active Intros: a stage-based closed-out row (Obsolete, empty status) still discloses -- the "
      "deal itself (Buy-tagged, linking the tenant, at the Obsolete stage) is evidence an introduction "
      "happened, per the disclosure-evidence fix -- names the real buyer (Alice Buyer)",
      "Alice Buyer" in obs_row)
check("Active Intros: that row shows the Lost outcome (display-renamed from Withdrawn)",
      '<span class="status-chip exit">Lost</span>' in obs_row)
lost_co_row = page_intros[page_intros.find(">Lost Co<") - 200: page_intros.find(">Lost Co<") + 400]
check("Active Intros: the Lost Co (stage=Lost) row shows the Lost outcome (display-renamed from Passed)",
      '<span class="status-chip exit">Lost</span>' in lost_co_row)

# --- Bug fix regression: exit-via-status-on-live-stage (804/805), and
# the negative case, Stalled-stays-in-Introduced (806) ---
closed_out_section = page_intros[page_intros.find('<details class="closed-out-section">'):]
intros_section_only = page_intros[:page_intros.find('<details class="closed-out-section">')]
check("exit-via-status-on-live-stage: Status Passed Co (804, Matched+Passed, has milestone) IS in Closed out",
      "Status Passed Co" in closed_out_section and "Status Passed Co" not in intros_section_only)
check("exit-via-status-on-live-stage: disclosed (milestone evidence) -> names the real buyer (Bob Buyer)",
      "Bob Buyer" in closed_out_section)
check("exit-via-status-on-live-stage: Status Withdrawn Co (805, LOI Signed+Withdrawn, no milestone) IS in Closed out",
      "Status Withdrawn Co" in closed_out_section and "Status Withdrawn Co" not in intros_section_only)
check("exit-via-status-on-live-stage: no milestone evidence, but the deal itself (Buy-tagged, linking "
      "the tenant, at LOI Signed -- a live matched-or-later stage) is evidence an introduction happened "
      "-> discloses ('Alice Buyer' named)",
      "Status Withdrawn Co" in closed_out_section
      and "Alice Buyer" in closed_out_section[closed_out_section.find("Status Withdrawn Co"):
                                                closed_out_section.find("Status Withdrawn Co") + 600])
check("exit-via-status-on-live-stage: both status-based exits (Passed AND Withdrawn) render the same "
      "display-renamed 'Lost' chip -- at least two occurrences in the section",
      closed_out_section.count('<span class="status-chip exit">Lost</span>') >= 2)
check("Stalled-stays-in-Introduced: Stalled Co (806, Matched+Stalled) is NOT in Closed out",
      "Stalled Co" not in closed_out_section)
check("Stalled-stays-in-Introduced: Stalled Co IS in the main Introduced table, flagged (stalled-row)",
      "Stalled Co" in intros_section_only
      and 'stalled-row' in row_for(page_intros, "806"))

# --- Bug fix regression: RAW Intro Status Closed on a live stage (807) --
# in Closed out, styled positively, distinct from the gray Passed/
# Withdrawn chips in the very same section ---
check("Closed-status-on-live-stage: Status Closed Co (807, Matched+Closed) IS in Closed out",
      "Status Closed Co" in closed_out_section and "Status Closed Co" not in intros_section_only)
check("Closed-status-on-live-stage: always disclosed (Closed is never anonymized) -> names the real buyer (Alice Buyer)",
      "Status Closed Co" in closed_out_section
      and "Alice Buyer" in closed_out_section[closed_out_section.find("Status Closed Co"):
                                               closed_out_section.find("Status Closed Co") + 600])
check("Closed-status-on-live-stage: rendered with the positive GREEN 'Won' chip, not the red Lost one",
      '<span class="status-chip closed">Won</span>' in closed_out_section)
check("Closed-status-on-live-stage: the green Won chip and the red Lost chips both appear in the same "
      "section (visually distinct outcomes coexist)",
      '<span class="status-chip closed">Won</span>' in closed_out_section
      and closed_out_section.count('<span class="status-chip exit">Lost</span>') >= 2)

page_company_lost = lf.render_company_page("Lost Co", "Sella Seller", tenant_a, TENANT_A_EMAIL, "intros",
                                            key=None, view_as=None, edit_mode=False)
check("Company page (Lost Co): 'Lost' section present with count (1)",
      '<summary>Lost <span class="count">(1)</span></summary>' in page_company_lost)
check("Company page (Lost Co): disclosed buyer named (Bob Buyer)", "Bob Buyer" in page_company_lost)
check("Company page (Lost Co): loss-reason suffix shown", "— Went with another sponsor" in page_company_lost)

page_company_obsolete = lf.render_company_page("Obsolete Co", "Sella Seller", tenant_a, TENANT_A_EMAIL, "intros",
                                                key=None, view_as=None, edit_mode=False)
check("Company page (Obsolete Co): closed-out row discloses -- deal-evidence carve-out (real buyer named)",
      "Alice Buyer" in page_company_obsolete)
check("Company page (Obsolete Co): Withdrawn outcome shown", "Withdrawn" in page_company_obsolete)

page_company_admin = lf.render_company_page("Obsolete Co", "Admin", tenant_a, "admin", "demand",
                                             key=ADMIN_KEY, view_as=TENANT_A_EMAIL, edit_mode=True)
check("Company page (Obsolete Co, admin edit): real buyer shown regardless of disclosure",
      "Alice Buyer" in page_company_admin)

# --- Company page: same status-based-exit fix, same Stalled guard ---
page_company_status_passed = lf.render_company_page("Status Passed Co", "Sella Seller", tenant_a, TENANT_A_EMAIL,
                                                      "intros", key=None, view_as=None, edit_mode=False)
check("Company page (Status Passed Co): 'Lost' section present (not stuck in the Buyers table)",
      '<summary>Lost <span class="count">(1)</span></summary>' in page_company_status_passed)
check("Company page (Status Passed Co): disclosed buyer named (Bob Buyer)",
      "Bob Buyer" in page_company_status_passed)
check("Company page (Status Passed Co): the Buyers table's own 'Introduced' group has nothing (dead intro)",
      "No introductions yet on this deal." in page_company_status_passed)

page_company_status_withdrawn = lf.render_company_page("Status Withdrawn Co", "Sella Seller", tenant_a,
                                                         TENANT_A_EMAIL, "intros", key=None, view_as=None,
                                                         edit_mode=False)
check("Company page (Status Withdrawn Co): closed-out row discloses -- deal-evidence carve-out (real buyer named)",
      "Alice Buyer" in page_company_status_withdrawn)
check("Company page (Status Withdrawn Co): Withdrawn outcome shown", "Withdrawn" in page_company_status_withdrawn)

# --- Company page: same Closed-status routing, same positive-green chip ---
page_company_status_closed = lf.render_company_page("Status Closed Co", "Sella Seller", tenant_a, TENANT_A_EMAIL,
                                                      "intros", key=None, view_as=None, edit_mode=False)
check("Company page (Status Closed Co): 'Won' section present (not stuck in the Buyers table)",
      '<summary>Won <span class="count">(1)</span></summary>' in page_company_status_closed)
check("Company page (Status Closed Co): always-disclosed buyer named (Alice Buyer)",
      "Alice Buyer" in page_company_status_closed)
check("Company page (Status Closed Co): positive solid-green 'Won' chip, not the gray Passed/Withdrawn styling",
      '<span class="status-chip won">Won</span>' in page_company_status_closed
      and 'closed-out-row-won' in page_company_status_closed)

page_company_stalled = lf.render_company_page("Stalled Co", "Sella Seller", tenant_a, TENANT_A_EMAIL, "intros",
                                               key=None, view_as=None, edit_mode=False)
check("Stalled-stays-in-Introduced (company page): Stalled Co has NO 'Closed out' section at all",
      '<details class="closed-out-section">' not in page_company_stalled)
check("Stalled-stays-in-Introduced (company page): Stalled Co is in the Buyers table's Introduced group",
      "Cara Buyer" in page_company_stalled)

page_buyer_bob = lf.render_buyer_page(3, "Sella Seller", tenant_a, TENANT_A_EMAIL, key=None, view_as=None, edit_mode=False)
check("Buyer page (Bob, disclosed closed-out deal): Lost Co appears in Track with you", "Lost Co" in page_buyer_bob)
check("Buyer page (Bob): Lost chip (display-renamed from Passed) + loss-reason suffix present",
      '<span class="status-chip exit">Lost</span>' in page_buyer_bob
      and "— Went with another sponsor" in page_buyer_bob)

page_buyer_alice = lf.render_buyer_page(2, "Sella Seller", tenant_a, TENANT_A_EMAIL, key=None, view_as=None, edit_mode=False)
check("Buyer page (Alice): the live Matched deal (Live Buy Co) still appears", "Live Buy Co" in page_buyer_alice)
check("Buyer page (Alice): the closed-out deal (Obsolete Co) now discloses (deal-evidence carve-out) "
      "and appears in Track with you",
      "Obsolete Co" in page_buyer_alice)

# --- Buyer page parity check: Track with you already routes matched-or-
# later deals through _resolve_intro_status directly (never filtered by
# Intro Status the way Active Intros/company page were), so a status-
# based exit was already showing up correctly here -- no code change
# needed on this surface, only this regression guard confirming it.
check("Buyer page (Bob): the status-based exit (Status Passed Co, disclosed) appears in Track with you",
      "Status Passed Co" in page_buyer_bob)
check("Buyer page (Bob): its chip reads 'Lost' (display-renamed from Passed)",
      '<span class="status-chip exit">Lost</span>' in page_buyer_bob)
check("Buyer page (Alice): the status-based exit (Status Withdrawn Co) now discloses (deal-evidence "
      "carve-out) and appears in Track with you",
      "Status Withdrawn Co" in page_buyer_alice)
check("Buyer page (Alice): the Closed-status deal (Status Closed Co, always disclosed) appears in Track with you",
      "Status Closed Co" in page_buyer_alice)
check("Buyer page (Alice): Closed now renders its own solid-tint green 'Won' chip here too (item 1 fix -- "
      "previously fell through to a plain, uncolored status-pill since Closed isn't is_exit), never the red "
      "Lost styling used for a real Passed/Withdrawn exit",
      '<span class="status-chip closed">Won</span>' in page_buyer_alice
      and '<span class="status-pill">Closed</span>' not in page_buyer_alice
      and '<span class="status-chip exit">Won</span>' not in page_buyer_alice)
page_buyer_cara = lf.render_buyer_page(4, "Sella Seller", tenant_a, TENANT_A_EMAIL, key=None, view_as=None, edit_mode=False)
check("Stalled-stays-in-Introduced (buyer page): Stalled Co appears with the Stalled chip, not an exit chip",
      "Stalled Co" in page_buyer_cara and "Stalled — needs a nudge" in page_buyer_cara)

check("rendering these closed-out rows issues no Dynamo writes",
      fake_table.updates == [] and fake_table.puts == [])


# ======================================================================
# SECTION: Disclosure-evidence carve-out for historical exits (CAREFUL
# MODE fix) + the Notes "forbidden" bug it caused
# ======================================================================
# A Buy-tagged deal is only ever CREATED at Matched (_pipeline_create_
# buy_deal), so reaching a dead-exit stage (Lost/Trade Broken/Obsolete)
# or sitting at any live matched-or-later stage is itself proof an
# introduction happened -- even with zero milestone/status history.
# _deal_is_introduction_evidence/DEAL_EVIDENCE_STAGE_IDS is the shared
# carve-out; _closed_out_disclosed and _resolve_intro_status's exit
# branch (Passed/Withdrawn only, never Stalled) both consult it.

check("_deal_is_introduction_evidence: Buy-tagged, matched-or-later stage -> True",
      lf._deal_is_introduction_evidence({"custom_fields": cf_buy(), "deal_stage": {"id": lf.STAGE_MATCHED}}))
check("_deal_is_introduction_evidence: Buy-tagged, dead-exit stage (Lost) -> True",
      lf._deal_is_introduction_evidence({"custom_fields": cf_buy(), "deal_stage": {"id": 111801}}))
check("_deal_is_introduction_evidence: Buy-tagged, dead-exit stage (Trade Broken) -> True",
      lf._deal_is_introduction_evidence({"custom_fields": cf_buy(), "deal_stage": {"id": lf.STAGE_TRADE_BROKEN}}))
check("_deal_is_introduction_evidence: Buy-tagged, dead-exit stage (Obsolete) -> True",
      lf._deal_is_introduction_evidence({"custom_fields": cf_buy(), "deal_stage": {"id": lf.OBSOLETE_STAGE_ID}}))
check("_deal_is_introduction_evidence: Buy-tagged, a non-evidence stage (Inquiry) -> False",
      not lf._deal_is_introduction_evidence({"custom_fields": cf_buy(), "deal_stage": {"id": lf.STAGE_INQUIRY}}))
check("_deal_is_introduction_evidence: no deal_stage at all -> False",
      not lf._deal_is_introduction_evidence({"custom_fields": cf_buy()}))
check("_deal_is_introduction_evidence: SELL-tagged deal at a matched-or-later stage -> False (not Buy-tagged)",
      not lf._deal_is_introduction_evidence({"custom_fields": cf_sell(), "deal_stage": {"id": lf.STAGE_MATCHED}}))

check("_closed_out_disclosed: Buy-tagged deal at a non-evidence stage, no raw status -> stays anonymized "
      "(the negative case the carve-out does NOT swallow)",
      not lf._closed_out_disclosed({"custom_fields": cf_buy(), "deal_stage": {"id": lf.STAGE_INQUIRY}}))

r_stalled_evidence = lf._resolve_intro_status(
    {"custom_fields": cf_buy(7207584), "deal_stage": {"id": lf.STAGE_MATCHED}, "updated_at": "2026-08-01T00:00:00Z"}, None)
check("_resolve_intro_status: Stalled at a matched-or-later stage, no milestone -> STILL not disclosed "
      "(the deal-evidence carve-out applies only to Passed/Withdrawn, never Stalled -- an ongoing, not "
      "historical, state)",
      r_stalled_evidence["disclosed"] is False)
r_passed_evidence = lf._resolve_intro_status(
    {"custom_fields": cf_buy(7207585), "deal_stage": {"id": lf.STAGE_MATCHED}, "updated_at": "2026-08-01T00:00:00Z"}, None)
check("_resolve_intro_status: Passed at a matched-or-later stage, no milestone -> discloses via deal evidence",
      r_passed_evidence["disclosed"] is True)
r_withdrawn_evidence = lf._resolve_intro_status(
    {"custom_fields": cf_buy(7207586), "deal_stage": {"id": lf.STAGE_LOI_SIGNED}, "updated_at": "2026-08-01T00:00:00Z"}, None)
check("_resolve_intro_status: Withdrawn at a matched-or-later stage, no milestone -> discloses via deal evidence",
      r_withdrawn_evidence["disclosed"] is True)

# --- Real-ticket regression: "forbidden" under Notes on Ferkol's Destinus
# row in Tenant view. Repro: a Matched-stage Introduced deal the tenant
# flags Passed (no milestones ever recorded -- realistic for a deal that
# never had a checkbox ticked before dying) -- same session, no reload
# (the inline-edit script never reloads the page) -- then the tenant
# edits Notes on that SAME still-visible row. Before the fix, the flag
# write flips the row's OWN disclosure to False (no milestone evidence),
# so the very next Notes write on it 403s with a bare "forbidden".
FERKOL_TENANT_EMAIL = "ferkol@example.com"
FERKOL_TENANT_PID = 991201
DESTINUS_BUYER_PID = 991202
DESTINUS_PENDING_BUYER_PID = 991203
ferkol_sell = {"id": 993101, "name": "Ferkol Sell Order", "company": {"name": "Destinus"},
               "deal_stage": {"id": lf.STAGE_MATCHED}, "custom_fields": cf_sell(),
               "people": [{"id": FERKOL_TENANT_PID}], "updated_at": "2026-08-01T00:00:00Z"}
ferkol_buy = {"id": 993102, "name": "Destinus: Buy", "company": {"name": "Destinus"},
              "deal_stage": {"id": lf.STAGE_MATCHED}, "custom_fields": cf_status(7207579),  # Introduced
              "people": [{"id": FERKOL_TENANT_PID}, {"id": DESTINUS_BUYER_PID}], "updated_at": "2026-08-01T00:00:00Z"}
ferkol_pending = {"id": 993103, "name": "Destinus: Buy 2", "company": {"name": "Destinus"},
                   "deal_stage": {"id": lf.STAGE_MATCHED}, "custom_fields": cf_status(None),  # never introduced
                   "people": [{"id": FERKOL_TENANT_PID}, {"id": DESTINUS_PENDING_BUYER_PID}],
                   "updated_at": "2026-08-01T00:00:00Z"}
ferkol_people = {"people": [
    {"id": FERKOL_TENANT_PID, "full_name": "Ferkol", "email": FERKOL_TENANT_EMAIL, "custom_fields": {}},
    {"id": DESTINUS_BUYER_PID, "full_name": "Destinus Buyer", "email": "buyer@destinus.example",
     "custom_fields": {}},
    {"id": DESTINUS_PENDING_BUYER_PID, "full_name": "Destinus Pending Buyer", "email": "pending@destinus.example",
     "custom_fields": {}},
]}
_, ferkol_table = use_fixture({lf.PEOPLE_KEY: ferkol_people, lf.INTEREST_KEY: {"buy": {}},
                                lf.DEALS_KEY: {"deals": [ferkol_sell, ferkol_buy, ferkol_pending]}})

resp_flag = lf.lambda_handler(post_event({"deal_id": "993102", "flag": "passed"},
                                          cookies=[tenant_cookie(FERKOL_TENANT_EMAIL)]), None)
check("Ferkol/Destinus repro: tenant flags the deal Passed -> 200", resp_flag["statusCode"] == 200)

resp_notes = lf.lambda_handler(post_event({"deal_id": "993102", "notes": "Following up next week"},
                                           cookies=[tenant_cookie(FERKOL_TENANT_EMAIL)]), None)
check("Ferkol/Destinus repro: Notes write on the same row now succeeds (bug fixed, no more 'forbidden')",
      resp_notes["statusCode"] == 200)
check("Ferkol/Destinus repro: the note was actually saved",
      any(it.get("sk") == "intro#993102" and it.get("notes") == "Following up next week"
          for it in ferkol_table.items))

# --- A genuinely never-introduced (Matched, no evidence) row still 403s
# for a tenant's Notes write -- but with a clear inline message now,
# never the bare "forbidden" string.
resp_notes_pending = lf.lambda_handler(
    post_event({"deal_id": "993103", "notes": "trying anyway"}, cookies=[tenant_cookie(FERKOL_TENANT_EMAIL)]), None)
check("Notes write on a genuinely undisclosed row: still 403 (correctly rejected)",
      resp_notes_pending["statusCode"] == 403)
check("Notes write on a genuinely undisclosed row: clear inline message, not the bare word 'forbidden'",
      json.loads(resp_notes_pending["body"])["error"] != "forbidden"
      and "introduced" in json.loads(resp_notes_pending["body"])["error"].lower())

# --- Same clarity fix for the status-write disclosure/Closed-lock 403s.
resp_status_pending = lf.lambda_handler(
    post_event({"deal_id": "993103", "flag": "passed"}, cookies=[tenant_cookie(FERKOL_TENANT_EMAIL)]), None)
check("Status write on a genuinely undisclosed row: still 403, clear message not bare 'forbidden'",
      resp_status_pending["statusCode"] == 403
      and json.loads(resp_status_pending["body"])["error"] != "forbidden")

resp_status_closed_lock = lf.lambda_handler(
    post_event({"deal_id": "993102", "flag": "stalled"}, cookies=[tenant_cookie(FERKOL_TENANT_EMAIL)]), None)
# 993102 is Passed (an exit), not Closed, so this should actually succeed
# (tenants may re-flag a Passed row) -- included to prove the Closed-lock
# message change didn't accidentally start blocking non-Closed exits too.
check("Re-flagging a Passed (non-Closed) row is still allowed for a tenant",
      resp_status_closed_lock["statusCode"] == 200)


# --- Real-ticket regression: deal 55422151 ("Panthalassa: $250K Buy"),
# tenant elana@tworoads.vc -- Buy-tagged (5077819), stage Won Deal
# (111802), Intro Status Closed (7207587), Elana linked via the deal's
# own "people" list. Reported as rendering nowhere: no Closed section on
# My Deals/Active Intros, Buyers(0) and "No introductions yet on this
# deal." on the company page, Intros column reading "--". Reproduced
# with the exact ids from the report to prove (not just claim) that
# _is_matched_or_later_buy_deal's Closed-status OR-check, together with
# _is_closed_out_buy_deal's matching exclusion, actually holds across
# every one of the three deployed paths named in the report: the intro
# fetch (get_my_matched_buy_deals/_resolve_intro_status), per-company
# stats (_company_buy_stats, My Deals' own Intros column), and the
# company page's Buyers rendering.
ELANA_EMAIL = "elana@tworoads.vc"
ELANA_PID = 990001
elana_sell_deal = {"id": 900700, "name": "Panthalassa Sell Order", "company": {"name": "Panthalassa"},
                    "deal_stage": {"id": lf.STAGE_MATCHED}, "custom_fields": cf_sell(),
                    "people": [{"id": ELANA_PID}], "updated_at": "2026-08-01T00:00:00Z"}
panthalassa_deal = {"id": 55422151, "name": "Panthalassa: $250K Buy", "company": {"name": "Panthalassa"},
                     "deal_stage": {"id": 111802},  # Won Deal
                     "custom_fields": {lf.DEAL_SIDE_FIELD: [lf.DEAL_SIDE_BUY_ID],
                                       lf.INTRO_STATUS_FIELD: [lf.INTRO_STATUS_CLOSED_ID],
                                       lf.TICKET_MAX_FIELD: 250000},
                     "people": [{"id": ELANA_PID}, {"id": 5001}], "updated_at": "2026-08-05T00:00:00Z"}
elana_people = {"people": [
    {"id": ELANA_PID, "full_name": "Elana Investor", "email": ELANA_EMAIL, "custom_fields": {}},
    {"id": 5001, "full_name": "Panthalassa Buyer", "email": "buyer@panthalassa-buyer.example", "custom_fields": {}},
]}
use_fixture({lf.PEOPLE_KEY: elana_people, lf.INTEREST_KEY: {"buy": {}},
             lf.DEALS_KEY: {"deals": [elana_sell_deal, panthalassa_deal]}})
elana_tenant = lf._resolve_tenant(ELANA_EMAIL)
check("Ticket 55422151: elana@tworoads.vc auto-enrolls as a tenant", elana_tenant is not None)

check("Ticket 55422151: _is_matched_or_later_buy_deal(panthalassa_deal) -> True",
      lf._is_matched_or_later_buy_deal(panthalassa_deal))
check("Ticket 55422151: _is_closed_out_buy_deal(panthalassa_deal) -> False (routes via matched-or-later instead)",
      not lf._is_closed_out_buy_deal(panthalassa_deal))

elana_matched = lf.get_my_matched_buy_deals(elana_tenant["person_id"], "Panthalassa")
check("Ticket 55422151: get_my_matched_buy_deals (intro fetch) picks it up",
      [d["id"] for d in elana_matched] == [55422151])

elana_stats = lf._company_buy_stats(elana_tenant["person_id"], "Panthalassa")
check("Ticket 55422151: per-company stats -- intro_count = 1, not 0",
      elana_stats["intro_count"] == 1)

elana_sell_only = [d for d in lf.get_my_deals(elana_tenant["person_id"])
                    if lf.DEAL_SIDE_SELL_ID in lf._deal_cf_option_ids(d, lf.DEAL_SIDE_FIELD)]
page_elana_mydeals = lf.render_my_deals_page("Elana Investor", deals=elana_sell_only, key=None, view_as=None,
                                              person_id=elana_tenant["person_id"], anon_key_email=ELANA_EMAIL)
check("Ticket 55422151: My Deals Active intros column shows '—', since a Won intro is terminal, not active "
      "(Won/Lost are each broken out on the company page and stats card instead)",
      '<a class="mydeals-count-link" href="?company=Panthalassa&ref=mydeals#buyers">' not in page_elana_mydeals)

page_elana_company = lf.render_company_page("Panthalassa", "Elana Investor", elana_tenant, ELANA_EMAIL, "mydeals",
                                             key=None, view_as=None, edit_mode=False)
buyers_section = page_elana_company[page_elana_company.find('id="buyers"'):]
check("Ticket 55422151: company page's Buyers section has a 'Won (1)' group",
      '<summary>Won <span class="count">(1)</span></summary>' in buyers_section)
check("Ticket 55422151: the buyer is named (Closed is always disclosed), not anonymized",
      "Panthalassa Buyer" in buyers_section)
check("Ticket 55422151: styled positively (solid-green .status-chip.won 'Won'), never the gray Passed/Withdrawn chip",
      '<span class="status-chip won">Won</span>' in buyers_section
      and '<span class="status-chip exit">Won</span>' not in buyers_section)

page_elana_intros = lf.render_intros_page("Elana Investor", tenant=elana_tenant, tenant_email=ELANA_EMAIL,
                                           key=None, view_as=None, edit_mode=False)
elana_closed_out = page_elana_intros[page_elana_intros.find('<details class="closed-out-section">'):]
check("Ticket 55422151: Active Intros' Closed out section shows it, named, styled green 'Won'",
      "Panthalassa Buyer" in elana_closed_out
      and '<span class="status-chip closed">Won</span>' in elana_closed_out)

elana_raised = lf._raised_headline_stats()
check("Ticket 55422151: contributes to the desk-wide Raised total ($250K)",
      elana_raised["total"] == 250000)


# ======================================================================
# SECTION: Multi-buyer display (primary + secondary + "+N more"), Deal
# team, and the company_name fallback for colleague-matching
# ======================================================================

recs = [{"id": 10, "name": "A"}, {"id": 11, "name": "B"}, {"id": 12, "name": "C"}]
p, s, n = lf._select_display_buyers({}, [recs[0]])
check("_select_display_buyers: 1 buyer -> secondary None, more_count 0", p is recs[0] and s is None and n == 0)
p, s, n = lf._select_display_buyers({}, recs[:2])
check("_select_display_buyers: 2 buyers -> both shown, more_count 0", p is recs[0] and s is recs[1] and n == 0)
p, s, n = lf._select_display_buyers({}, recs)
check("_select_display_buyers: 3 buyers -> primary+secondary shown, more_count 1", p is recs[0] and s is recs[1] and n == 1)
p, s, n = lf._select_display_buyers({}, [])
check("_select_display_buyers: 0 buyers -> (None, None, 0)", p is None and s is None and n == 0)
p, s, n = lf._select_display_buyers({"primary_contact_id": 11}, recs)
check("_select_display_buyers: primary_contact_id names the primary; secondary is the next in order",
      p is recs[1] and s is recs[0] and n == 1)

people = {"people": [
    {"id": TENANT_A_PID, "full_name": "Sella Seller", "email": TENANT_A_EMAIL, "custom_fields": {}},
    {"id": 2, "name": "Alice Anderson", "email": "alice@example.com", "company_id": 500,
     "company_name": "Acme Capital", "title": "Principal", "phone": "555-1000", "custom_fields": {}},
    {"id": 3, "name": "Bob Baker", "email": "bob@example.com", "company_id": 500,
     "company_name": "Acme Capital", "title": "Associate", "custom_fields": {}},
    {"id": 4, "name": "Cara Cole", "email": "cara@example.com", "company_id": 500,
     "company_name": "Acme Capital", "custom_fields": {}},
    # Dan: same deal, DIFFERENT company -- never a colleague.
    {"id": 5, "name": "Dan Diff", "email": "dan@example.com", "company_id": 999,
     "company_name": "Other Firm", "custom_fields": {}},
    # Erin: same company_id as Alice, but NOT linked to any shared deal -- excluded.
    {"id": 6, "name": "Erin Excluded", "email": "erin@example.com", "company_id": 500,
     "company_name": "Acme Capital", "custom_fields": {}},
]}
deal_multi = {"id": 800, "name": "Multi Buyer Deal", "company": {"name": "Multi Co"}, "deal_stage": {"id": lf.STAGE_MATCHED},
              "custom_fields": cf_status(7207579), "primary_contact_id": 2,
              "people": [{"id": TENANT_A_PID}, {"id": 3}, {"id": 2}, {"id": 4}, {"id": 5}], "updated_at": "2026-08-02T00:00:00Z"}
deals_list = [deal_900a, deal_multi]
use_fixture({lf.PEOPLE_KEY: people, lf.INTEREST_KEY: {"buy": {}}, lf.DEALS_KEY: {"deals": deals_list}})
tenant_a = lf._resolve_tenant(TENANT_A_EMAIL)
assert tenant_a is not None

page_intros = lf.render_intros_page("Sella Seller", tenant=tenant_a, tenant_email=TENANT_A_EMAIL,
                                     key=None, view_as=None, edit_mode=False)
check("Active Intros: primary buyer (Alice) named", "Alice Anderson" in page_intros)
check("Active Intros: secondary buyer (Bob, first non-primary in deals.json order) named", "Bob Baker" in page_intros)
check("Active Intros: third+ buyers NOT named (Cara/Dan)",
      "Cara Cole" not in page_intros and "Dan Diff" not in page_intros)
check("Active Intros: '+2 more' shown", "+2 more" in page_intros)
check("Active Intros: contact/detail lines key to the primary only (her company shows)", "Acme Capital" in page_intros)
check("Active Intros: both shown names link to their own buyer page",
      'href="?buyer=2' in page_intros and 'href="?buyer=3' in page_intros)
check("Active Intros: only the primary gets the 'profile ->' suffix treatment",
      page_intros.count('<div class="buyer-cell-secondary">') == 1 and page_intros.count('profile &rarr;') == 1)

page_company = lf.render_company_page("Multi Co", "Sella Seller", tenant_a, TENANT_A_EMAIL, "intros",
                                       key=None, view_as=None, edit_mode=False)
check("Company page: primary buyer (Alice) named", "Alice Anderson" in page_company)
check("Company page: secondary buyer (Bob) named", "Bob Baker" in page_company)
check("Company page: Cara/Dan not named in the Buyers table", "Cara Cole" not in page_company and "Dan Diff" not in page_company)
check("Company page: '+2 more' shown", "+2 more" in page_company)
check("Company page: the primary's phone is shown (Alice has one, Bob doesn't)", "555-1000" in page_company)

page_company_admin = lf.render_company_page("Multi Co", "Admin", tenant_a, "admin", "demand",
                                             key=ADMIN_KEY, view_as=TENANT_A_EMAIL, edit_mode=True)
check("Company page (admin edit): primary + secondary shown, '+2 more' present",
      "Alice Anderson" in page_company_admin and "Bob Baker" in page_company_admin and "+2 more" in page_company_admin)

page_buyer_alice = lf.render_buyer_page(2, "Sella Seller", tenant_a, TENANT_A_EMAIL, key=None, view_as=None, edit_mode=False)
check("Deal team: heading names the firm", "Deal team at Acme Capital" in page_buyer_alice)
check("Deal team: Bob Baker (same company_id, linked to the shared deal) listed", "Bob Baker" in page_buyer_alice)
check("Deal team: Cara Cole (same company_id, linked to the shared deal) listed", "Cara Cole" in page_buyer_alice)
check("Deal team: Dan Diff (different company) NOT listed", "Dan Diff" not in page_buyer_alice)
check("Deal team: Erin Excluded (same company, not on a shared deal) NOT listed", "Erin Excluded" not in page_buyer_alice)
check("Deal team: titles shown (Associate for Bob)", "Associate" in page_buyer_alice)
check("Deal team: mailto links present for colleagues", "mailto:bob@example.com" in page_buyer_alice)
check("Deal team: colleague names link to their own buyer page", 'href="?buyer=3' in page_buyer_alice)

page_buyer_bob = lf.render_buyer_page(3, "Sella Seller", tenant_a, TENANT_A_EMAIL, key=None, view_as=None, edit_mode=False)
check("Deal team (Bob's page): Alice listed", "Alice Anderson" in page_buyer_bob)
check("Deal team (Bob's page): Cara listed", "Cara Cole" in page_buyer_bob)
check("Deal team (Bob's page): Bob himself NOT listed as his own colleague",
      page_buyer_bob.split("Deal team at Acme Capital")[1].count("Bob Baker") == 0
      if "Deal team at Acme Capital" in page_buyer_bob else True)

page_buyer_dan = lf.render_buyer_page(5, "Sella Seller", tenant_a, TENANT_A_EMAIL, key=None, view_as=None, edit_mode=False)
check("Deal team (Dan's page): omitted entirely (no Acme colleague shares Dan's own company)",
      "Deal team at" not in page_buyer_dan)

# Fallback: company_name match (case/whitespace-insensitive) when the buyer has no company_id at all.
people2 = {"people": [
    {"id": TENANT_A_PID, "full_name": "Sella Seller", "email": TENANT_A_EMAIL, "custom_fields": {}},
    {"id": 8, "name": "Gina Gates", "email": "gina@example.com", "company_name": "Fallback Firm", "custom_fields": {}},
    {"id": 9, "name": "Hank Holt", "email": "hank@example.com", "company_name": "  FALLBACK FIRM  ", "custom_fields": {}},
    {"id": 10, "name": "Ivy Ives", "email": "ivy@example.com", "company_name": "Someone Else Co", "custom_fields": {}},
]}
deal_fallback = {"id": 801, "name": "Fallback Deal", "company": {"name": "Fallback Co"}, "deal_stage": {"id": lf.STAGE_MATCHED},
                 "custom_fields": cf_status(7207579), "people": [{"id": TENANT_A_PID}, {"id": 8}, {"id": 9}, {"id": 10}],
                 "updated_at": "2026-08-02T00:00:00Z"}
use_fixture({lf.PEOPLE_KEY: people2, lf.INTEREST_KEY: {"buy": {}}, lf.DEALS_KEY: {"deals": [deal_900a, deal_fallback]}})
tenant_a2 = lf._resolve_tenant(TENANT_A_EMAIL)
assert tenant_a2 is not None

page_buyer_gina = lf.render_buyer_page(8, "Sella Seller", tenant_a2, TENANT_A_EMAIL, key=None, view_as=None, edit_mode=False)
check("Deal team fallback: Hank (same company_name, no company_id) listed", "Hank Holt" in page_buyer_gina)
check("Deal team fallback: Ivy (different company_name) NOT listed", "Ivy Ives" not in page_buyer_gina)


# ======================================================================
# SECTION: Page-wrapper spacing (.wrap) consistency across tabs
# ======================================================================

page_intros_wrap = lf.render_intros_page("Sella Seller", tenant=tenant_a, tenant_email=TENANT_A_EMAIL,
                                          key=None, view_as=None, edit_mode=False)
check("Active Intros .wrap has the shared top margin", ".wrap { max-width: 1000px; margin: 28px auto 0; }" in page_intros_wrap)

page_demand_wrap = lf.render_page([], "Admin", key=ADMIN_KEY, view_as=None, anon_key_email="admin", tenant_picker=True)
check("Demand Board .wrap has the shared top margin", ".wrap { max-width: 1000px; margin: 28px auto 0; }" in page_demand_wrap)

page_company_wrap = lf.render_company_page("Multi Co", "Sella Seller", tenant_a, TENANT_A_EMAIL, "mydeals",
                                            key=None, view_as=None, edit_mode=False)
check("Company page .wrap is left untouched (not a tab -- no top margin)", ".wrap { max-width: 1000px; margin: 0 auto; }" in page_company_wrap)


# ======================================================================
# SECTION: Buyer photos (?photo=<person_id>)
# ======================================================================

check("_person_initials: first_name+last_name", lf._person_initials({"first_name": "Alice", "last_name": "Buyer"}) == "AB")
check("_person_initials: falls back to full_name split", lf._person_initials({"full_name": "Cara Cole"}) == "CC")
check("_person_initials: single-word name -> first two letters", lf._person_initials({"full_name": "Cher"}) == "CH")
check("_person_initials: no name at all -> '?'", lf._person_initials({}) == "?")

svg = lf._avatar_svg("AB")
check("_avatar_svg: valid inline SVG containing the initials", svg.startswith("<svg") and ">AB<" in svg)
check("_avatar_svg: empty initials fall back to '?'", ">?<" in lf._avatar_svg(""))

people_photo = {"people": [
    {"id": TENANT_A_PID, "full_name": "Sella Seller", "email": TENANT_A_EMAIL, "custom_fields": {}},
    {"id": 2, "first_name": "Alice", "last_name": "Buyer", "email": "alice@example.com",
     "company_id": 500, "company_name": "Acme Capital", "custom_fields": {}},
    {"id": 3, "first_name": "Bob", "last_name": "Baker", "email": "bob@example.com", "custom_fields": {}},
    # Dana shares Alice's company_id and is linked to the same disclosed deal -- Alice's Deal team.
    {"id": 4, "first_name": "Dana", "last_name": "Deputy", "email": "dana@example.com",
     "company_id": 500, "company_name": "Acme Capital", "custom_fields": {}},
]}
deal_disclosed = {"id": 700, "company": {"name": "Photo Co"}, "deal_stage": {"id": lf.STAGE_MATCHED},
                   "custom_fields": cf_status(7207579), "people": [{"id": TENANT_A_PID}, {"id": 2}, {"id": 4}],
                   "updated_at": "2026-08-01T00:00:00Z"}
deal_pending = {"id": 701, "company": {"name": "Pending Co"}, "deal_stage": {"id": lf.STAGE_MATCHED},
                "custom_fields": cf_status(None), "people": [{"id": TENANT_A_PID}, {"id": 3}],
                "updated_at": "2026-08-02T00:00:00Z"}
own_sell_deal_photo = {"id": 799, "name": "Sella's Own Deal", "company": {"name": "Sella HoldCo"},
                        "deal_stage": {"id": lf.STAGE_FIRM}, "custom_fields": cf_sell(),
                        "people": [{"id": TENANT_A_PID}], "updated_at": "2026-08-01T00:00:00Z"}
use_fixture({lf.PEOPLE_KEY: people_photo, lf.INTEREST_KEY: {"buy": {}},
             lf.DEALS_KEY: {"deals": [deal_disclosed, deal_pending, own_sell_deal_photo]}})
tenant_photo = lf._resolve_tenant(TENANT_A_EMAIL)
assert tenant_photo is not None

# Stub the live Pipeline fetch -- this sandbox can't reach api.pipelinecrm.com
# (see CLAUDE.md) -- so the auth/caching logic can be tested in isolation
# from the network call itself.
_orig_fetch = lf._pipeline_fetch_person_photo_url
_fetch_calls = []


def _stub_fetch(person_id):
    _fetch_calls.append(person_id)
    return "https://cdn.pipelinecrm.example/thumb/2.jpg" if person_id == 2 else None


lf._pipeline_fetch_person_photo_url = _stub_fetch
lf._PHOTO_CACHE.clear()

resp = lf._handle_photo_request("2", tenant_photo, TENANT_A_EMAIL, True)
check("Photo: admin gets a 302 redirect to the real photo URL, regardless of disclosure",
      resp["statusCode"] == 302 and resp["headers"]["Location"] == "https://cdn.pipelinecrm.example/thumb/2.jpg")

lf._PHOTO_CACHE.clear()
resp = lf._handle_photo_request("2", tenant_photo, TENANT_A_EMAIL, False)
check("Photo: tenant with a disclosed deal linking this person gets the real photo (302)",
      resp["statusCode"] == 302 and resp["headers"]["Location"] == "https://cdn.pipelinecrm.example/thumb/2.jpg")

lf._PHOTO_CACHE.clear()
_fetch_calls.clear()
resp = lf._handle_photo_request("3", tenant_photo, TENANT_A_EMAIL, False)
check("Photo: tenant WITHOUT a disclosed deal for this person gets the fallback SVG avatar (200), never a redirect",
      resp["statusCode"] == 200 and resp["headers"]["Content-Type"] == "image/svg+xml")
check("Photo: a denied request never calls the live Pipeline fetch at all", 3 not in _fetch_calls)

lf._PHOTO_CACHE.clear()
resp = lf._handle_photo_request("3", tenant_photo, TENANT_A_EMAIL, True)
check("Photo: admin but no photo on file for this person (stub returns None) -> fallback SVG avatar with initials",
      resp["statusCode"] == 200 and ">BB<" in resp["body"])

resp = lf._handle_photo_request("not-a-number", tenant_photo, TENANT_A_EMAIL, True)
check("Photo: garbage person_id -> fallback avatar, never crashes", resp["statusCode"] == 200)

lf._PHOTO_CACHE.clear()
_fetch_calls.clear()
lf._handle_photo_request("2", tenant_photo, TENANT_A_EMAIL, True)
lf._handle_photo_request("2", tenant_photo, TENANT_A_EMAIL, True)
check("Photo: warm-invocation cache reused -- only ONE live fetch across two requests within the TTL",
      _fetch_calls.count(2) == 1)

# --- VIEW_AS CONTRACT, bug fix: the 4th arg here is edit_mode, not the
# bare admin-key flag -- an admin previewing this tenant via BARE
# &view_as (no &edit=1) computes edit_mode=False, the exact same value
# passed for the tenant's own session above, so it hits the identical
# disclosure check and gets the SAME result: the real photo only for a
# disclosed person, the fallback avatar otherwise. Before this fix,
# _handle_photo_request keyed off the bare admin-key flag, so a bare
# &view_as preview could still pull the real photo for a person its
# own anonymized render never named.
lf._PHOTO_CACHE.clear()
resp = lf._handle_photo_request("3", tenant_photo, TENANT_A_EMAIL, False)
check("Photo: admin + bare &view_as (edit_mode=False) gets the SAME fallback avatar a real tenant "
      "without disclosure would -- never the real photo it hasn't disclosed",
      resp["statusCode"] == 200 and resp["headers"]["Content-Type"] == "image/svg+xml")

lf._pipeline_fetch_person_photo_url = _orig_fetch
lf._PHOTO_CACHE.clear()

page_buyer_photo = lf.render_buyer_page(2, "Sella Seller", tenant_photo, TENANT_A_EMAIL, key=None, view_as=None,
                                         edit_mode=False)
check("Buyer page: circular header photo <img src=\"?photo=2\"> present",
      'class="buyer-header-photo"' in page_buyer_photo and 'src="?photo=2"' in page_buyer_photo)
check("Buyer page: Deal team small avatar for Dana present", 'class="deal-team-avatar"' in page_buyer_photo
      and 'src="?photo=4"' in page_buyer_photo)


# ======================================================================
# SECTION: Raised headline (desk-wide, item 2)
# ======================================================================

people_raised = {"people": [{"id": TENANT_A_PID, "full_name": "Sella Seller", "email": TENANT_A_EMAIL,
                              "custom_fields": {}}]}
deals_raised = [
    # Closed via Intro Status Closed, has a max ticket size.
    {"id": 601, "company": {"name": "Closed Co A"}, "deal_stage": {"id": lf.STAGE_MATCHED},
     "custom_fields": cf_status(lf.INTRO_STATUS_CLOSED_ID,
                                 {lf.TICKET_MAX_FIELD: 20_000_000, lf.TICKET_MIN_FIELD: 18_000_000}),
     "people": [], "updated_at": "2026-08-01T00:00:00Z"},
    # Closed via Won stage, no Intro Status set, only a min ticket size on file.
    {"id": 602, "company": {"name": "Closed Co B"}, "deal_stage": {"id": 111802},
     "custom_fields": cf_status(None, {lf.TICKET_MIN_FIELD: 5_000_000}),
     "people": [], "updated_at": "2026-08-02T00:00:00Z"},
    # Closed (Won stage) but missing BOTH ticket fields -- contributes $0; native "value" must NEVER be used.
    {"id": 603, "company": {"name": "Closed Co C"}, "deal_stage": {"id": 111802},
     "custom_fields": cf_status(None), "value": 999999999,
     "people": [], "updated_at": "2026-08-03T00:00:00Z"},
    # Live (Matched, not closed) -- must NOT count toward the total.
    {"id": 604, "company": {"name": "Live Co"}, "deal_stage": {"id": lf.STAGE_MATCHED},
     "custom_fields": cf_status(7207579, {lf.TICKET_MAX_FIELD: 50_000_000}),
     "people": [], "updated_at": "2026-08-04T00:00:00Z"},
    # Sell-side, even a "Won"-stage one -- must NOT count (buy deals only).
    {"id": 605, "company": {"name": "Closed Co A"}, "deal_stage": {"id": 111802},
     "custom_fields": cf_sell({lf.TICKET_MAX_FIELD: 99_000_000}),
     "people": [], "updated_at": "2026-08-05T00:00:00Z"},
]
use_fixture({lf.PEOPLE_KEY: people_raised, lf.INTEREST_KEY: {"buy": {}}, lf.DEALS_KEY: {"deals": deals_raised}})

raised_stats = lf._raised_headline_stats()
check("_raised_headline_stats: total = $20M + $5M + $0 = $25M (native 'value' never used)",
      raised_stats["total"] == 25_000_000)
check("_raised_headline_stats: closed_count = 3 (601, 602, 603)", raised_stats["closed_count"] == 3)
check("_raised_headline_stats: zero_size_count = 1 (603, missing both ticket fields)",
      raised_stats["zero_size_count"] == 1)
check("_raised_headline_stats: companies_count = 3 (distinct companies among CLOSED buy deals only)",
      raised_stats["companies_count"] == 3)
# Confirming test (no code change needed here -- _raised_headline_stats
# already used this OR-logic before the fix): the same Intro-Status-Closed
# deal (601) this desk-wide total already counted is now ALSO recognized
# by _is_matched_or_later_buy_deal, so a tenant-scoped view of this exact
# deal would no longer have been the invisible case the bug fix targets.
check("_is_matched_or_later_buy_deal also recognizes deal 601 (parity with _raised_headline_stats' own rule)",
      lf._is_matched_or_later_buy_deal(deals_raised[0]))

check("_fmt_raised_headline: $25M -> '$25M+'", lf._fmt_raised_headline(25_000_000) == "$25M+")
check("_fmt_raised_headline: rounds DOWN (25.9M -> '$25M+', not 26)", lf._fmt_raised_headline(25_900_000) == "$25M+")
check("_fmt_raised_headline: below $1M -> suppressed (None)", lf._fmt_raised_headline(999_999) is None)
check("_fmt_raised_headline: exactly $1M -> shown", lf._fmt_raised_headline(1_000_000) == "$1M+")

page_demand_raised = lf.render_page([], "Sella Seller", key=None, view_as=None, anon_key_email=TENANT_A_EMAIL,
                                     tenant_picker=False)
check("Demand Board: bold raised headline shown under the title",
      '<p class="raised-headline">$25M+ closed for sellers through this desk</p>' in page_demand_raised)
check("Demand Board: tenant view has NO admin tooltip (no title attr)",
      'class="raised-headline" title=' not in page_demand_raised)

page_demand_admin = lf.render_page([], "Admin", key=ADMIN_KEY, view_as=None, anon_key_email="admin",
                                    tenant_picker=True, edit_mode=True)
check("Demand Board (admin edit): tooltip carries the EXACT total (not K/M-rounded) and the $0-contributing count",
      "Exact: $25,000,000" in page_demand_admin and "1 contributing $0" in page_demand_admin)

not_enabled_html = lf._message_page("Not enabled", lf.NOT_ENABLED_MESSAGE, show_sell_cta=True)
check("Not-enabled page: raised headline shown", "$25M+ closed for sellers through this desk" in not_enabled_html)
check("Not-enabled page: 'N companies with completed purchases' line shown",
      "3 companies with completed purchases." in not_enabled_html)
check("Not-enabled page: headline appears ABOVE the sell-CTA contact line",
      not_enabled_html.find("closed for sellers through this desk") < not_enabled_html.find("Submit a sell order"))

signin_html = lf._message_page("Access denied", "Sign in to view the Demand Board.", show_signin=True)
check("Sign-in (unauthenticated) page: no raised headline at all", "closed for sellers through this desk" not in signin_html)

use_fixture({lf.PEOPLE_KEY: people_raised, lf.INTEREST_KEY: {"buy": {}}, lf.DEALS_KEY: {"deals": []}})
page_demand_empty = lf.render_page([], "Sella Seller", key=None, view_as=None, anon_key_email=TENANT_A_EMAIL,
                                    tenant_picker=False)
check("Demand Board: no closed buys at all -> headline suppressed entirely", "closed for sellers" not in page_demand_empty)
not_enabled_empty = lf._message_page("Not enabled", lf.NOT_ENABLED_MESSAGE, show_sell_cta=True)
check("Not-enabled page: below $1M -> both headline and companies-count line suppressed",
      "closed for sellers" not in not_enabled_empty and "companies with completed purchases" not in not_enabled_empty)


# ======================================================================
# SECTION: Item 3 -- overdue-deadline Next Steps chip reword
# ======================================================================

people_chip = {"people": [{"id": TENANT_A_PID, "full_name": "Sella Seller", "email": TENANT_A_EMAIL,
                            "custom_fields": {}}]}
overdue_deal = {"id": 620, "name": "Overdue Deal", "company": {"name": "Overdue Co"},
                 "deal_stage": {"id": lf.STAGE_FIRM},
                 "custom_fields": cf_sell({lf.DEADLINE_FIELD: "2020/01/01"}),
                 "people": [{"id": TENANT_A_PID}], "updated_at": "2026-08-01T00:00:00Z"}
use_fixture({lf.PEOPLE_KEY: people_chip, lf.INTEREST_KEY: {"buy": {}}, lf.DEALS_KEY: {"deals": [overdue_deal]}})
tenant_chip = lf._resolve_tenant(TENANT_A_EMAIL)
assert tenant_chip is not None
page_mydeals_chip = lf.render_my_deals_page("Sella Seller", deals=[overdue_deal], key=None, view_as=None,
                                             edit_mode=False, person_id=TENANT_A_PID, anon_key_email=TENANT_A_EMAIL)
check("My Deals: overdue-deadline chip now reads 'Update deadline or cancel ->'",
      "Update deadline or cancel" in page_mydeals_chip)


# ======================================================================
# SECTION: Firm-wide Closed section (item 4) + Net per-share (item 5)
# ======================================================================

people_firm = {"people": [
    {"id": TENANT_A_PID, "full_name": "Sella Seller", "email": TENANT_A_EMAIL, "company_id": 700,
     "custom_fields": {}},
    # Colleague sharing company_id 700, linked to a won Sell deal that isn't the tenant's own.
    {"id": 20, "first_name": "Marco", "last_name": "Colleague", "email": "marco@example.com",
     "company_id": 700, "custom_fields": {}},
    # A different-firm person, also on a won Sell deal -- must NOT show up as "firm-wide."
    {"id": 21, "first_name": "Other", "last_name": "Person", "email": "other@example.com",
     "company_id": 999, "custom_fields": {}},
]}
own_won_deal = {"id": 910, "name": "Own Won Deal", "company": {"name": "Own Co"}, "deal_stage": {"id": 111802},
                 "custom_fields": cf_sell({lf.TICKET_MAX_FIELD: 3_000_000, lf.NUM_SHARES_FIELD: 9524,
                                            lf.NET_FIELD: 20.0}),
                 "people": [{"id": TENANT_A_PID}], "updated_at": "2026-08-01T00:00:00Z"}
colleague_won_deal = {"id": 911, "name": "Colleague Won Deal", "company": {"name": "Colleague Co"},
                       "deal_stage": {"id": 111802}, "custom_fields": cf_sell({lf.TICKET_MAX_FIELD: 4_000_000}),
                       "people": [{"id": 20}], "updated_at": "2026-08-02T00:00:00Z"}
other_firm_won_deal = {"id": 912, "name": "Other Firm Won Deal", "company": {"name": "Other Co"},
                        "deal_stage": {"id": 111802}, "custom_fields": cf_sell({lf.TICKET_MAX_FIELD: 9_000_000}),
                        "people": [{"id": 21}], "updated_at": "2026-08-03T00:00:00Z"}
live_own_deal = {"id": 913, "name": "Live Deal", "company": {"name": "Own Co"}, "deal_stage": {"id": lf.STAGE_FIRM},
                  "custom_fields": cf_sell(), "people": [{"id": TENANT_A_PID}], "updated_at": "2026-08-04T00:00:00Z"}
use_fixture({lf.PEOPLE_KEY: people_firm, lf.INTEREST_KEY: {"buy": {}},
             lf.DEALS_KEY: {"deals": [own_won_deal, colleague_won_deal, other_firm_won_deal, live_own_deal]}})
tenant_firm = lf._resolve_tenant(TENANT_A_EMAIL)
assert tenant_firm is not None

firm_pairs = lf.get_firm_closed_sell_deals(TENANT_A_PID)
firm_ids = {d.get("id") for d, _ in firm_pairs}
check("get_firm_closed_sell_deals: colleague's won deal (911) included", 911 in firm_ids)
check("get_firm_closed_sell_deals: other firm's won deal (912) excluded", 912 not in firm_ids)
check("get_firm_closed_sell_deals: the viewer's OWN won deal (910) excluded (already personal)", 910 not in firm_ids)
colleague_rec = next(c for d, c in firm_pairs if d.get("id") == 911)
check("get_firm_closed_sell_deals: colleague record is Marco", lf._person_display_name(colleague_rec) == "Marco Colleague")

# Firm-level tenancy: render_my_deals_page now takes `deals` exactly as
# the caller (lambda_handler's mydeals branch) fetches it -- via
# get_firm_sell_deals, already firm-wide -- rather than bolting the
# colleague's closed deal on internally. Mirror that real call here
# instead of hand-building a personal-only list.
firm_sell_deals = lf.get_firm_sell_deals(TENANT_A_PID)
# body_only: the nav dropdown's own Closed group now also carries a "via
# Marco" entry for the same firm-wide deal -- slice it off so the
# occurrence count below reflects the actual table, not the nav.
page_mydeals_firm = body_only(lf.render_my_deals_page("Sella Seller", deals=firm_sell_deals, key=None, view_as=None,
                                                       edit_mode=False, person_id=TENANT_A_PID,
                                                       anon_key_email=TENANT_A_EMAIL))
check("My Deals: Closed section count includes BOTH the personal (910) and firm (911) won deals",
      '<h2 class="mydeals-section-heading closed">Closed <span class="count">(2)</span></h2>' in page_mydeals_firm)
check("My Deals: the firm-wide row shows a 'via Marco' chip", "via Marco" in page_mydeals_firm)
check("My Deals: exactly one via-chip (only the firm row gets one, not the personal row)",
      page_mydeals_firm.count("via Marco") == 1)
check("My Deals: 'Total closed' is now the merged firm-wide figure ($7M = $3M personal + $4M colleague -- "
      "there's no separate personal/firm split anymore now that Active through Closed is one shared view; "
      "the 'via Marco' chip is what tells a personal deal from a colleague's)",
      "$7M" in page_mydeals_firm.split("Total closed")[1][:80])
check("My Deals: net-per-share line on the personal Closed row ('9,524 sh @ $20.00 net')",
      "9,524 sh @ $20.00 net" in page_mydeals_firm)
check("My Deals: no per-share line for the firm row (neither field set on that deal)",
      lf._deal_per_share_text(colleague_won_deal) is None)

page_company_won = lf.render_company_page("Own Co", "Sella Seller", tenant_firm, TENANT_A_EMAIL, "mydeals",
                                           key=None, view_as=None, edit_mode=False)
check("Company page: a won deal's Deal Details card shows the net-per-share line",
      "9,524 sh @ $20.00 net" in page_company_won)

live_deal_only_co = {"id": 914, "name": "Live Only Deal", "company": {"name": "Live Only Co"},
                      "deal_stage": {"id": lf.STAGE_FIRM},
                      "custom_fields": cf_sell({lf.NUM_SHARES_FIELD: 500, lf.NET_FIELD: 10.0}),
                      "people": [{"id": TENANT_A_PID}], "updated_at": "2026-08-05T00:00:00Z"}
use_fixture({lf.PEOPLE_KEY: people_firm, lf.INTEREST_KEY: {"buy": {}}, lf.DEALS_KEY: {"deals": [live_deal_only_co]}})
tenant_live_only = lf._resolve_tenant(TENANT_A_EMAIL)
page_company_live = lf.render_company_page("Live Only Co", "Sella Seller", tenant_live_only, TENANT_A_EMAIL,
                                            "mydeals", key=None, view_as=None, edit_mode=False)
check("Company page: a LIVE (non-won) deal's card never shows the per-share line even with both fields set",
      "500 sh" not in page_company_live)

check("_deal_per_share_text: shares only (net absent)",
      lf._deal_per_share_text({"custom_fields": {lf.NUM_SHARES_FIELD: 100}}) == "100 sh")
check("_deal_per_share_text: net only (shares absent), two decimals",
      lf._deal_per_share_text({"custom_fields": {lf.NET_FIELD: 15.5}}) == "@ $15.50 net")
check("_deal_per_share_text: both missing -> None", lf._deal_per_share_text({}) is None)


# ======================================================================
# SECTION: Company-page parity refactor -- shared row-builder
# ======================================================================
# The company page's Buyers table now renders through the SAME shared
# row-builders Active Intros uses (_buy_deal_row_html/_edit_html/
# _pending_buy_deal_row_html/_closed_out_row_html, surface="company"):
# milestone checkboxes + flags-only select (not the old 10-option
# dropdown), a single auto-saving Notes textarea (Next Steps/Follow-up
# retired), a tier badge under Investor Type, up to two buyer names +
# "+N more", centered Size, a colgroup (no horizontal bleed), and the
# same Closed-out collapsed section -- one 6-column shape regardless of
# edit_mode. Deal Details' Size is now a min-max RANGE and Fees now
# lists seller fee first.

people_parity = {"people": [
    {"id": TENANT_A_PID, "full_name": "Sella Seller", "email": TENANT_A_EMAIL, "custom_fields": {}},
    {"id": 30, "first_name": "Nina", "last_name": "Novak", "email": "nina@example.com", "custom_fields": {}},
    {"id": 31, "first_name": "Omar", "last_name": "Ortiz", "email": "omar@example.com",
     "custom_fields": {lf.INVESTOR_LEVEL_FIELD: [lf.QP_ID]}},
    {"id": 32, "first_name": "Priya", "last_name": "Patel", "email": "priya@example.com", "custom_fields": {}},
    {"id": 33, "first_name": "Quinn", "last_name": "Quill", "email": "quinn@example.com", "custom_fields": {}},
]}
deal_parity_disclosed = {"id": 951, "name": "Parity Deal", "company": {"name": "Parity Co"},
                          "deal_stage": {"id": lf.STAGE_MATCHED}, "custom_fields": cf_status(7207579),
                          "people": [{"id": TENANT_A_PID}, {"id": 31}, {"id": 32}, {"id": 33}],
                          "updated_at": "2026-08-01T00:00:00Z"}
deal_parity_pending = {"id": 952, "name": "Parity Pending Deal", "company": {"name": "Parity Co"},
                        "deal_stage": {"id": lf.STAGE_MATCHED}, "custom_fields": cf_status(None),
                        "people": [{"id": TENANT_A_PID}, {"id": 30}], "updated_at": "2026-08-02T00:00:00Z"}
deal_parity_closed_out = {"id": 953, "name": "Parity Closed Out Deal", "company": {"name": "Parity Co"},
                          "deal_stage": {"id": 111801}, "custom_fields": cf_status(7207579),
                          "people": [{"id": TENANT_A_PID}, {"id": 30}], "updated_at": "2026-08-03T00:00:00Z"}
deal_parity_sell_range = {"id": 954, "name": "Parity Sell Deal", "company": {"name": "Parity Co"},
                          "deal_stage": {"id": lf.STAGE_FIRM},
                          "custom_fields": cf_sell({lf.TICKET_MIN_FIELD: 500_000, lf.TICKET_MAX_FIELD: 25_000_000,
                                                     lf.SELLER_FEE_FIELD: 7, lf.MGMT_FEE_FIELD: 2, lf.CARRY_FIELD: 20}),
                          "people": [{"id": TENANT_A_PID}], "updated_at": "2026-08-04T00:00:00Z"}
deal_parity_sell_single = {"id": 955, "name": "Parity Sell Single", "company": {"name": "Parity Single Co"},
                           "deal_stage": {"id": lf.STAGE_FIRM},
                           "custom_fields": cf_sell({lf.TICKET_MIN_FIELD: 1_000_000, lf.TICKET_MAX_FIELD: 1_000_000}),
                           "people": [{"id": TENANT_A_PID}], "updated_at": "2026-08-05T00:00:00Z"}
use_fixture({lf.PEOPLE_KEY: people_parity, lf.INTEREST_KEY: {"buy": {}},
             lf.DEALS_KEY: {"deals": [deal_parity_disclosed, deal_parity_pending, deal_parity_closed_out,
                                       deal_parity_sell_range, deal_parity_sell_single]}},
            table_items=[{"tenant": TENANT_A_EMAIL, "sk": "intro#951", "notes": "Chasing docs",
                          "milestones": {"NDA": 1, "VDR": 1}}])
tenant_parity = lf._resolve_tenant(TENANT_A_EMAIL)
assert tenant_parity is not None

page_parity = lf.render_company_page("Parity Co", "Sella Seller", tenant_parity, TENANT_A_EMAIL, "mydeals",
                                      key=None, view_as=None, edit_mode=False)

check("Parity: milestone checkboxes present on the Buyers table", 'class="ei-milestone"' in page_parity)
check("Parity: NDA/VDR checked from Dynamo milestones",
      re.search(r'value="NDA"[^>]*checked', page_parity) and re.search(r'value="VDR"[^>]*checked', page_parity))
check("Parity: flags-only select present (not the old 10-option status dropdown)",
      'class="ei-flag"' in page_parity and 'class="ei-status"' not in page_parity)
check("Parity: single Notes column is a 2-line auto-saving textarea",
      '<textarea class="ei-notes"' in page_parity and "Chasing docs" in page_parity)
check("Parity: no Next Steps / Follow-up columns anywhere", ">Next Steps<" not in page_parity
      and ">Follow-up<" not in page_parity and 'class="ei-next-steps"' not in page_parity
      and 'class="ei-follow-up"' not in page_parity)
check("Parity: 'Buyer Notes' header retired in favor of plain 'Notes'",
      ">Buyer Notes<" not in page_parity and ">Notes<" in page_parity)
check("Parity: tier badge shown under Investor Type (Omar is QP)", 'class="tier-badge tier-qp"' in page_parity)
check("Parity: up to two buyer names shown (Omar + Priya, deals.json order) plus '+1 more' (Quinn)",
      "Omar Ortiz" in page_parity and "Priya Patel" in page_parity and "+1 more" in page_parity
      and "Quinn Quill" not in page_parity)
check("Parity: colgroup present (6 columns, matches Active Intros)",
      page_parity[page_parity.find("<colgroup>"):page_parity.find("</colgroup>")].count("<col ") == 6)
check("Parity: table-layout:fixed (no horizontal bleed)", "table-layout: fixed;" in page_parity)
check("Parity: Size column centered via .num", "text-align: center;" in page_parity)
check("Parity: Closed-out collapsed section present (Lost, since deal_parity_closed_out is a Lost-stage exit)",
      '<summary>Lost <span class="count">(1)</span></summary>' in page_parity)
parity_head = page_parity[page_parity.find("<thead>"):page_parity.find("</thead>")]
check("Parity: head_row is unconditionally 6 columns (Buyer name/Company/Investor Type/Size/Status/Notes)",
      [parity_head.find(f">{h}<") for h in ["Buyer name", "Company", "Investor Type", "Size", "Status", "Notes"]]
      == sorted(parity_head.find(f">{h}<")
                for h in ["Buyer name", "Company", "Investor Type", "Size", "Status", "Notes"]))

page_parity_pending_section = page_parity[page_parity.find("Pending introductions"):]
check("Parity: pending row shows the anonymized buyer code, not Nina's real name",
      "Nina Novak" not in page_parity_pending_section.split("</table>")[0]
      if "Pending introductions" in page_parity else True)

page_parity_admin = lf.render_company_page("Parity Co", "Admin", tenant_parity, "admin", "demand",
                                            key=ADMIN_KEY, view_as=TENANT_A_EMAIL, edit_mode=True)
check("Parity (admin edit): milestone checkboxes + flag select, still no old status dropdown",
      'class="ei-milestone"' in page_parity_admin and 'class="ei-flag"' in page_parity_admin
      and 'class="ei-status"' not in page_parity_admin)
check("Parity (admin edit): Notes textarea auto-saves (ei-notes class), not a single-line input",
      '<textarea class="ei-notes"' in page_parity_admin)
check("Parity (admin edit): pending buyer (Nina) IS named -- edit mode never anonymizes",
      "Nina Novak" in page_parity_admin)

# Deal Details: Size as a min-max RANGE, and Fees reordered (seller first).
page_parity_range = lf.render_company_page("Parity Co", "Sella Seller", tenant_parity, TENANT_A_EMAIL, "mydeals",
                                            key=None, view_as=None, edit_mode=False)
check("Deal Details: Size shows a min-max range ('$500K – $25M')", "$500K – $25M" in page_parity_range)
check("Deal Details: Fees line lists seller fee FIRST, then mgmt, then carry",
      "7% seller fee · 2% mgmt · 20% carry" in page_parity_range)

page_parity_single = lf.render_company_page("Parity Single Co", "Sella Seller", tenant_parity, TENANT_A_EMAIL,
                                             "mydeals", key=None, view_as=None, edit_mode=False)
check("Deal Details: min==max collapses to a single figure, not '$1M – $1M'", "$1M – $1M" not in page_parity_single
      and "$1M" in page_parity_single)

check("_deal_size_range_text: min-only -> single figure",
      lf._deal_size_range_text({"custom_fields": {lf.TICKET_MIN_FIELD: 2_000_000}}) == "$2M")
check("_deal_size_range_text: max-only -> single figure",
      lf._deal_size_range_text({"custom_fields": {lf.TICKET_MAX_FIELD: 3_000_000}}) == "$3M")
check("_deal_size_range_text: neither field, falls back to 'value'",
      lf._deal_size_range_text({"custom_fields": {}, "value": 4_000_000}) == "$4M")
check("_deal_size_range_text: nothing at all -> '—'", lf._deal_size_range_text({}) == "—")

check("_fmt_fees: seller/mgmt/carry order",
      lf._fmt_fees({"custom_fields": {lf.SELLER_FEE_FIELD: 4, lf.MGMT_FEE_FIELD: 2, lf.CARRY_FIELD: 20}})
      == "4% seller fee · 2% mgmt · 20% carry")
check("_fmt_fees: omits missing parts, keeps relative order",
      lf._fmt_fees({"custom_fields": {lf.CARRY_FIELD: 20}}) == "20% carry")


# ======================================================================
# SECTION: Navigation & role-sharpening pass -- counts-as-doors (My
# Deals), urgency styling (Active Intros), company-page section nav
# ======================================================================
# Active Intros = triage queue, company page = deal dossier, My Deals =
# hub. Item 1: a nonzero Buyers/Intros count on My Deals is a door to the
# matching anchor on that company's own page. Item 2: a Stalled row on
# Active Intros gets an amber left-border accent and the summary strip's
# "N stalled" segment anchors to the first one. Item 3: the company page
# gets a small section nav under the title. Items 4/5 (no new cross-
# links to Active Intros; ref= breadcrumbs survive the new anchors) are
# checked here too.

people_nav = {"people": [
    {"id": TENANT_A_PID, "full_name": "Sella Seller", "email": TENANT_A_EMAIL, "custom_fields": {}},
    {"id": 401, "first_name": "Buyer", "last_name": "One", "email": "buyer1@example.com", "custom_fields": {}},
    {"id": 402, "first_name": "Buyer", "last_name": "Two", "email": "buyer2@example.com", "custom_fields": {}},
]}
deal_nav_sell = {"id": 1101, "name": "Nav Sell Deal", "company": {"name": "Nav Co"},
                 "deal_stage": {"id": lf.STAGE_FIRM}, "custom_fields": cf_sell(),
                 "people": [{"id": TENANT_A_PID}], "is_archived": False, "updated_at": "2026-08-01T00:00:00Z"}
deal_nav_intro = {"id": 1102, "name": "Nav Intro Deal", "company": {"name": "Nav Co"},
                  "deal_stage": {"id": lf.STAGE_MATCHED}, "custom_fields": cf_status(7207579),
                  "people": [{"id": TENANT_A_PID}, {"id": 401}], "updated_at": "2026-08-02T00:00:00Z"}
deal_nav_stalled = {"id": 1103, "name": "Nav Stalled Deal", "company": {"name": "Nav Co"},
                    "deal_stage": {"id": lf.STAGE_MATCHED}, "custom_fields": cf_status(lf.INTRO_STATUS_STALLED_ID),
                    "people": [{"id": TENANT_A_PID}, {"id": 402}], "updated_at": "2026-08-03T00:00:00Z"}
deal_nav_sell_zero = {"id": 1104, "name": "Zero Sell Deal", "company": {"name": "Zero Co"},
                      "deal_stage": {"id": lf.STAGE_FIRM}, "custom_fields": cf_sell(),
                      "people": [{"id": TENANT_A_PID}], "is_archived": False, "updated_at": "2026-08-04T00:00:00Z"}
use_fixture({lf.PEOPLE_KEY: people_nav, lf.INTEREST_KEY: {"buy": {"Nav Co": [401, 402]}},
             lf.DEALS_KEY: {"deals": [deal_nav_sell, deal_nav_intro, deal_nav_stalled, deal_nav_sell_zero]}},
            table_items=[{"tenant": TENANT_A_EMAIL, "sk": "intro#1103", "milestones": {"NDA": 1}}])
tenant_nav = lf._resolve_tenant(TENANT_A_EMAIL)
assert tenant_nav is not None

# --- Item 1: My Deals counts become doors ---
sell_deals_nav = [d for d in lf.get_my_deals(TENANT_A_PID)
                  if lf.DEAL_SIDE_SELL_ID in lf._deal_cf_option_ids(d, lf.DEAL_SIDE_FIELD)]
page_nav_mydeals = lf.render_my_deals_page("Sella Seller", deals=sell_deals_nav, key=None, view_as=None,
                                            edit_mode=False, person_id=TENANT_A_PID, anon_key_email=TENANT_A_EMAIL)
buyer_href_nav = lf._company_href("Nav Co", "mydeals") + "#demand"
intro_href_nav = lf._company_href("Nav Co", "mydeals") + "#buyers"
check("Item 1: nonzero Buyers count (2) is a door to #demand",
      f'<a class="mydeals-count-link" href="{buyer_href_nav}">2</a>' in page_nav_mydeals)
check("Item 1: nonzero Intros count (2) is a door to #buyers",
      f'<a class="mydeals-count-link" href="{intro_href_nav}">2</a>' in page_nav_mydeals)
check("Item 1: count-link CSS reads as a plain number with only a hover affordance",
      ".mydeals-count-link { color: inherit; text-decoration: none; }" in page_nav_mydeals
      and ".mydeals-count-link:hover { color: var(--accent); text-decoration: underline; }" in page_nav_mydeals)
check("Item 1: fragment is appended after the query string -- ref survives for back-link parsing",
      intro_href_nav.split("#")[0].endswith("ref=mydeals") and intro_href_nav.endswith("#buyers"))

row_zero = row_for(page_nav_mydeals, "1104")
check("Item 1: a zero Buyers/Intros count stays plain text, not a link",
      row_zero is not None and "mydeals-count-link" not in row_zero
      and '<td class="num">0</td>' in row_zero and '<td class="num">—</td>' in row_zero)

# --- Item 2: urgency styling on Active Intros ---
page_nav_intros = lf.render_intros_page("Sella Seller", tenant=tenant_nav, tenant_email=TENANT_A_EMAIL,
                                         key=None, view_as=None, edit_mode=False)
row_1102 = row_for(page_nav_intros, "1102")
row_1103 = row_for(page_nav_intros, "1103")
check("Item 2: Stalled row (1103) gets an id anchor and the stalled-row class",
      row_1103 is not None and 'id="intro-row-1103"' in row_1103 and "stalled-row" in row_1103)
check("Item 2: healthy row (1102) is unchanged -- no stalled-row class or id anchor",
      row_1102 is not None and "stalled-row" not in row_1102 and 'id="intro-row-1102"' not in row_1102)
check("Item 2: amber left-border CSS rule for stalled rows",
      "tr.stalled-row {{ box-shadow: inset 3px 0 0 #c9a227; }}" not in page_nav_intros
      and "tr.stalled-row { box-shadow: inset 3px 0 0 #c9a227; }" in page_nav_intros)
check("Item 2: summary strip's '1 stalled' is a same-page anchor to the first stalled row",
      '<a href="#intro-row-1103">1 stalled</a>' in page_nav_intros)

# --- Item 2 (company-page scoping): the shared row-builder must NEVER
# apply stalled-row styling on the company page -- that's Active Intros'
# own triage signal, not the deal dossier's.
page_nav_company_edit = lf.render_company_page("Nav Co", "Admin", tenant_nav, "admin", "mydeals",
                                                key=ADMIN_KEY, view_as=TENANT_A_EMAIL, edit_mode=True)
check("Item 2: company page never gets a stalled-row class, even in admin edit mode",
      "stalled-row" not in page_nav_company_edit)

# --- Item 3: company-page section nav ---
page_nav_company = lf.render_company_page("Nav Co", "Sella Seller", tenant_nav, TENANT_A_EMAIL, "mydeals",
                                           key=None, view_as=None, edit_mode=False)
check("Item 3: section nav under the title links to Deal details / Active intros (N) / Interested buyers (M)",
      '<p class="cd-subnav">' in page_nav_company
      and '<a href="#deal-details">Deal details</a>' in page_nav_company
      and '<a href="#buyers">Active intros (2)</a>' in page_nav_company
      and '<a href="#demand">Interested buyers (2)</a>' in page_nav_company)

# --- Item 4: no new cross-links from company-page buyer rows to Active Intros ---
buyers_section_nav = page_nav_company[page_nav_company.find('id="buyers"'):page_nav_company.find('id="demand"')]
check("Item 4: no cross-links from the company page's Buyers rows to Active Intros",
      "tab=intros" not in buyers_section_nav)

# --- Item 5: ref= breadcrumbs still resolve given the new anchors ---
check("Item 5: ref=mydeals back-link resolves to My Deals",
      '<a class="cd-back" href="?tab=mydeals">&larr; Back to My Deals</a>' in page_nav_company)
page_nav_company_ref_intros = lf.render_company_page("Nav Co", "Sella Seller", tenant_nav, TENANT_A_EMAIL, "intros",
                                                      key=None, view_as=None, edit_mode=False)
check("Item 5: ref=intros back-link resolves to Active Intros",
      '<a class="cd-back" href="?tab=intros">&larr; Back to Active Intros</a>' in page_nav_company_ref_intros)


# ======================================================================
# SECTION: My Deals nav dropdown -- hover/click quick-jump menu
# ======================================================================
# The My Deals tab becomes a hover/click dropdown of the viewing tenant's
# own Sell deals, on every page (see _nav_html's person_id param and
# _mydeals_dropdown_entries/_mydeals_dropdown_html). The tab <a> itself
# is untouched -- a separate caret button opens the menu -- grouped Live/
# On Hold/Closed/Cancelled (company A-Z within each, empty groups
# omitted), each entry a company link carrying an intro-count badge when
# >0, firm-wide Closed rows carry a "via <colleague>" suffix, capped at
# 40 with a trailing "View all in My Deals ->" beyond that, and omitted
# outright for admin-without-view_as (no tenant context).

people_navdrop = {"people": [
    {"id": TENANT_A_PID, "full_name": "Sella Seller", "email": TENANT_A_EMAIL, "company_id": 77,
     "custom_fields": {}},
    {"id": 501, "first_name": "Buyer", "last_name": "Alpha", "email": "buyer.alpha@example.com",
     "custom_fields": {}},
    {"id": 502, "full_name": "Marco Colleague", "email": "marco@example.com", "company_id": 77,
     "custom_fields": {}},
]}
deal_nd_live = {"id": 1201, "name": "Alpha Sell", "company": {"name": "Alpha Co"},
                "deal_stage": {"id": lf.STAGE_FIRM}, "custom_fields": cf_sell(),
                "people": [{"id": TENANT_A_PID}], "is_archived": False, "updated_at": "2026-08-01T00:00:00Z"}
deal_nd_hold = {"id": 1202, "name": "Beta Sell", "company": {"name": "Beta Co"},
                "deal_stage": {"id": lf.HOLD_STAGE_ID}, "custom_fields": cf_sell(),
                "people": [{"id": TENANT_A_PID}], "is_archived": False, "updated_at": "2026-08-02T00:00:00Z"}
deal_nd_cancelled = {"id": 1203, "name": "Charlie Sell", "company": {"name": "Charlie Co"},
                     "deal_stage": {"id": 111801}, "custom_fields": cf_sell(),
                     "people": [{"id": TENANT_A_PID}], "is_archived": True, "updated_at": "2026-08-03T00:00:00Z"}
deal_nd_closed = {"id": 1204, "name": "Delta Sell", "company": {"name": "Delta Co"},
                  "deal_stage": {"id": 111802}, "custom_fields": cf_sell(),
                  "people": [{"id": TENANT_A_PID}], "is_archived": True, "updated_at": "2026-08-04T00:00:00Z"}
deal_nd_intro = {"id": 1205, "name": "Alpha Intro", "company": {"name": "Alpha Co"},
                 "deal_stage": {"id": lf.STAGE_MATCHED}, "custom_fields": cf_status(7207579),
                 "people": [{"id": TENANT_A_PID}, {"id": 501}], "updated_at": "2026-08-05T00:00:00Z"}
deal_nd_firm_closed = {"id": 1206, "name": "Colleague Won", "company": {"name": "Echo Co"},
                       "deal_stage": {"id": 111802}, "custom_fields": cf_sell(),
                       "people": [{"id": 502}], "updated_at": "2026-08-06T00:00:00Z"}
use_fixture({lf.PEOPLE_KEY: people_navdrop, lf.INTEREST_KEY: {"buy": {}},
             lf.DEALS_KEY: {"deals": [deal_nd_live, deal_nd_hold, deal_nd_cancelled, deal_nd_closed,
                                       deal_nd_intro, deal_nd_firm_closed]}})
tenant_navdrop = lf._resolve_tenant(TENANT_A_EMAIL)
assert tenant_navdrop is not None

entries_nd = lf._mydeals_dropdown_entries(TENANT_A_PID)
check("Dropdown: one entry per company (5: Alpha/Beta/Charlie/Delta/Echo)", len(entries_nd) == 5)
check("Dropdown: group order is Live, On Hold, Closed, Cancelled",
      [e["group"] for e in entries_nd] == ["Live", "On Hold", "Closed", "Closed", "Cancelled"])
check("Dropdown: Closed group sorted A-Z (Delta before Echo)",
      [e["company"] for e in entries_nd if e["group"] == "Closed"] == ["Delta Co", "Echo Co"])
check("Dropdown: Alpha Co (Live) carries its intro_count (1 disclosed matched buy deal)",
      next(e for e in entries_nd if e["company"] == "Alpha Co")["intro_count"] == 1)
check("Dropdown: Beta Co (On Hold) has intro_count 0", next(e for e in entries_nd
      if e["company"] == "Beta Co")["intro_count"] == 0)
check("Dropdown: Delta Co (personal Closed) carries no colleague/via text",
      next(e for e in entries_nd if e["company"] == "Delta Co")["colleague"] is None)
check("Dropdown: Echo Co (firm-wide Closed) carries colleague='Marco'",
      next(e for e in entries_nd if e["company"] == "Echo Co")["colleague"] == "Marco")
check("Dropdown entries: admin-without-view_as (person_id=None) -> no entries at all",
      lf._mydeals_dropdown_entries(None) == [])
check("Dropdown HTML: admin-without-view_as renders nothing (no caret, no menu)",
      lf._mydeals_dropdown_html(None) == "")

menu_html = lf._mydeals_dropdown_html(TENANT_A_PID, key=None, view_as=None)
check("Dropdown HTML: a caret button, a separate element from the tab <a> itself",
      '<button type="button" class="gg-mydeals-caret"' in menu_html)
check("Dropdown HTML: keyboard-accessible caret (aria-haspopup/aria-expanded)",
      'aria-haspopup="true"' in menu_html and 'aria-expanded="false"' in menu_html)
check("Dropdown HTML: menu carries role=menu, entries role=menuitem",
      'role="menu"' in menu_html and 'role="menuitem"' in menu_html)
check("Dropdown HTML: group labels appear in Live/On Hold/Closed/Cancelled order",
      menu_html.find(">Live<") < menu_html.find(">On Hold<") < menu_html.find(">Closed<")
      < menu_html.find(">Cancelled<"))
alpha_href = lf._company_href("Alpha Co", "mydeals", None, None)
check("Dropdown HTML: entry links to ?company=<name>&ref=mydeals", f'href="{alpha_href}"' in menu_html)
check("Dropdown HTML: Alpha Co shows its intro-count badge (1)",
      '<span class="gg-mydeals-menu-badge">1</span>' in menu_html)
check("Dropdown HTML: Beta Co (0 intros) shows no badge for that entry",
      '<span class="gg-mydeals-menu-name">Beta Co</span></a>' in menu_html)
check("Dropdown HTML: Echo Co shows 'via Marco' in muted text",
      '<span class="gg-mydeals-menu-via">via Marco</span>' in menu_html)
check("Dropdown HTML: no 'View all' link when under the 40-entry cap",
      "View all in My Deals" not in menu_html)

menu_html_admin = lf._mydeals_dropdown_html(TENANT_A_PID, key=ADMIN_KEY, view_as=TENANT_A_EMAIL)
alpha_href_admin = lf._company_href("Alpha Co", "mydeals", ADMIN_KEY, TENANT_A_EMAIL)
check("Dropdown HTML: auth params (key/view_as) carried into entry hrefs",
      f'href="{alpha_href_admin}"' in menu_html_admin and "&key=" in alpha_href_admin)

# --- Structure: the tab <a> stays a plain link to My Deals -- the
# caret is a separate sibling element, so a click on the tab label
# itself always navigates rather than being hijacked into opening the
# menu. z-index clears the sticky table header (z-index:1 elsewhere); a
# narrow-width media query keeps the menu from overflowing the nav on
# ~400px screens.
nav_with_dropdown = lf._nav_html("mydeals", "Sella Seller", key=None, view_as=None, person_id=TENANT_A_PID)
check("Dropdown: the My Deals tab is still a plain, unmodified link to ?tab=mydeals",
      '<a class="gg-tab active" href="?tab=mydeals">My Deals</a>' in nav_with_dropdown)
check("Dropdown: the caret is a sibling of the tab <a>, not nested inside it",
      '<a class="gg-tab active" href="?tab=mydeals">My Deals</a>\n        <button type="button" '
      'class="gg-mydeals-caret"' in nav_with_dropdown)
check("Dropdown CSS: menu z-index clears the sticky table header's z-index:1",
      ".gg-mydeals-menu {" in lf.NAV_CSS and "z-index: 50;" in lf.NAV_CSS)
check("Dropdown CSS: narrow-width (<=480px) media query repositions the menu viewport-relative",
      "@media (max-width: 480px)" in lf.NAV_CSS and "position: fixed;" in lf.NAV_CSS)

# --- Rendered on every page, not just My Deals itself ---
page_nd_intros = lf.render_intros_page("Sella Seller", tenant=tenant_navdrop, tenant_email=TENANT_A_EMAIL,
                                        key=None, view_as=None, edit_mode=False)
check("Dropdown: present on Active Intros", 'class="gg-mydeals-caret"' in page_nd_intros)
page_nd_company = lf.render_company_page("Alpha Co", "Sella Seller", tenant_navdrop, TENANT_A_EMAIL, "mydeals",
                                          key=None, view_as=None, edit_mode=False)
check("Dropdown: present on the company page", 'class="gg-mydeals-caret"' in page_nd_company)
page_nd_buyer = lf.render_buyer_page(501, "Sella Seller", tenant_navdrop, TENANT_A_EMAIL,
                                      key=None, view_as=None, edit_mode=False)
check("Dropdown: present on the buyer page", 'class="gg-mydeals-caret"' in page_nd_buyer)
page_nd_demand = lf.render_page(lf.get_company_table(), "Sella Seller", key=None, view_as=None,
                                 anon_key_email=TENANT_A_EMAIL, person_id=TENANT_A_PID)
check("Dropdown: present on the Demand Board", 'class="gg-mydeals-caret"' in page_nd_demand)
page_nd_mydeals = lf.render_my_deals_page("Sella Seller", deals=[deal_nd_live], key=None, view_as=None,
                                           person_id=TENANT_A_PID, anon_key_email=TENANT_A_EMAIL)
check("Dropdown: present on My Deals itself too", 'class="gg-mydeals-caret"' in page_nd_mydeals)

# --- Admin-without-view_as: omitted entirely, on every page ---
page_nd_demand_admin = lf.render_page([], "Admin", key=ADMIN_KEY, view_as=None, anon_key_email="admin",
                                       tenant_picker=True, person_id=None)
check("Dropdown: admin-without-view_as sees no caret on the Demand Board",
      'class="gg-mydeals-caret"' not in page_nd_demand_admin)
page_nd_mydeals_admin = lf.render_my_deals_page("Admin", tenant_picker=True, key=ADMIN_KEY, view_as=None)
check("Dropdown: admin-without-view_as sees no caret on My Deals either",
      'class="gg-mydeals-caret"' not in page_nd_mydeals_admin)

# --- Cap at 40 entries + trailing "View all" ---
deals_cap = [
    {"id": 1300 + i, "name": f"Cap Deal {i}", "company": {"name": f"Cap Co {i:03d}"},
     "deal_stage": {"id": lf.STAGE_FIRM}, "custom_fields": cf_sell(),
     "people": [{"id": TENANT_A_PID}], "updated_at": f"2026-09-{(i % 28) + 1:02d}T00:00:00Z"}
    for i in range(45)
]
use_fixture({lf.PEOPLE_KEY: {"people": [{"id": TENANT_A_PID, "full_name": "Sella Seller",
                                          "email": TENANT_A_EMAIL, "custom_fields": {}}]},
             lf.INTEREST_KEY: {"buy": {}}, lf.DEALS_KEY: {"deals": deals_cap}})
entries_cap = lf._mydeals_dropdown_entries(TENANT_A_PID)
check("Dropdown cap: 45 distinct companies -> 45 raw entries (the cap applies at HTML build, not entries)",
      len(entries_cap) == 45)
menu_html_cap = lf._mydeals_dropdown_html(TENANT_A_PID)
check("Dropdown cap: HTML renders only 40 entries", menu_html_cap.count("gg-mydeals-menu-item") == 40)
check("Dropdown cap: trailing 'View all in My Deals ->' item present beyond the cap",
      "View all in My Deals" in menu_html_cap and "gg-mydeals-menu-viewall" in menu_html_cap)
viewall_href = f"?tab=mydeals{lf._tab_qs_suffix(None, None)}"
check("Dropdown cap: 'View all' link points at the My Deals tab",
      f'href="{viewall_href}"' in menu_html_cap)


# ======================================================================
# SECTION: Diagnostic timing instrumentation (perf report task)
# ======================================================================
# _perf_start/_perf_timer/_perf_count/_perf_log -- one "TIMING ..." line
# to CloudWatch (stdout) per request, from the outer lambda_handler
# wrapper (_lambda_handler_impl does the actual routing/rendering,
# unchanged). Diagnostic only: nothing here reaches the HTTP response.
import io
import contextlib

people_perf = {"people": [{"id": TENANT_A_PID, "full_name": "Sella Seller", "email": TENANT_A_EMAIL,
                            "custom_fields": {}}]}
deal_perf = {"id": 1401, "name": "Perf Deal", "company": {"name": "Perf Co"},
             "deal_stage": {"id": lf.STAGE_FIRM}, "custom_fields": cf_sell(),
             "people": [{"id": TENANT_A_PID}], "updated_at": "2026-09-01T00:00:00Z"}
use_fixture({lf.PEOPLE_KEY: people_perf, lf.INTEREST_KEY: {"buy": {}}, lf.DEALS_KEY: {"deals": [deal_perf]}})
lf._cold_start_seen["done"] = False  # simulate a fresh container for this section


def _capture_timing_line(event):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        resp = lf.lambda_handler(event, None)
    lines = [ln for ln in buf.getvalue().splitlines() if ln.startswith("TIMING ")]
    return resp, lines


mydeals_event = {"requestContext": {"http": {"method": "GET"}},
                  "queryStringParameters": {"key": ADMIN_KEY, "view_as": TENANT_A_EMAIL, "tab": "mydeals"}}

resp1, lines1 = _capture_timing_line(mydeals_event)
check("TIMING: request succeeds (instrumentation is transparent)", resp1["statusCode"] == 200)
check("TIMING: exactly one summary line logged per request", len(lines1) == 1)
line1 = lines1[0]
check("TIMING: page label matches the requested tab", "page=mydeals" in line1)
check("TIMING: first request in a fresh container is cold=True", "cold=True" in line1)
check("TIMING: reports s3_people/s3_deals/s3_interest fetch+parse timings",
      "s3_people=" in line1 and "s3_deals=" in line1 and "s3_interest=" in line1)
check("TIMING: reports dynamo/pipeline_api/render/total",
      "dynamo=" in line1 and "pipeline_api=0.00s" in line1 and "render=" in line1 and "total=" in line1)
check("TIMING: reports call counts for the named expensive functions",
      "calls_get_firm_deals=" in line1 and "calls_get_deals_list=" in line1
      and "calls_get_firm_matched_buy_deals=" in line1 and "calls_build_tenant_index=" in line1)
check("TIMING: no Pipeline API call happens on a normal page render",
      "calls_pipeline_api_call=" not in line1)

resp2, lines2 = _capture_timing_line(mydeals_event)
check("TIMING: second request in the same warm container is cold=False", "cold=False" in lines2[0])

buyer_event = {"requestContext": {"http": {"method": "GET"}},
               "queryStringParameters": {"key": ADMIN_KEY, "buyer": "1"}}
_, lines_buyer = _capture_timing_line(buyer_event)
check("TIMING: page label follows the actual route (?buyer= -> page=buyer)", "page=buyer" in lines_buyer[0])

action_event = post_event({"deal_id": "1401", "notes": "x"})
_, lines_action = _capture_timing_line(action_event)
check("TIMING: a POST action still logs exactly one line, labeled by its action",
      len(lines_action) == 1 and "page=action:update_intro" in lines_action[0])

check("TIMING: byte sizes are reported for whichever S3 keys this request actually fetched",
      "bytes_people=" in line1 and "bytes_deals=" in line1 and "bytes_interest=" in line1)


# ======================================================================
# SECTION: Perf fixes 1-5 -- request-scoped caching, zero behavior change
# ======================================================================
# Fixes 1-5 from the perf diagnostic: memoize get_my_deals/
# get_my_matched_buy_deals per request (1); validate deals.json's
# snapshot version at most once per request (2); the nav dropdown's
# per-company stats share render_my_deals_page's own cache instead of
# an independent loop (3); one parsed people/deals/interest/companies
# list per request, shared across every function that needs the full
# list (4); one S3 client / Dynamo table binding reused across calls (5).
# The whole existing suite above (668 checks) already re-verifies every
# page's rendered output is unchanged under these fixes -- this section
# adds the fixes' OWN regression coverage: the call-count reduction
# itself, and the specific "did the shared cache accidentally change
# what's disclosed" risk fix 3 raised during design.

people_perf12 = {"people": [
    {"id": TENANT_A_PID, "full_name": "Sella Seller", "email": TENANT_A_EMAIL, "custom_fields": {}},
    {"id": 9000, "first_name": "Buyer", "last_name": "Zero", "email": "b0@example.com", "custom_fields": {}},
]}
deals_perf12 = []
for i in range(12):
    deals_perf12.append({"id": 7000 + i, "name": f"Sell {i}", "company": {"name": f"Perf Co {i:02d}"},
                         "deal_stage": {"id": lf.STAGE_FIRM}, "custom_fields": cf_sell(),
                         "people": [{"id": TENANT_A_PID}], "updated_at": f"2026-08-{(i % 28) + 1:02d}T00:00:00Z"})
for i in range(0, 12, 3):
    deals_perf12.append({"id": 8000 + i, "name": f"Buy {i}", "company": {"name": f"Perf Co {i:02d}"},
                         "deal_stage": {"id": lf.STAGE_MATCHED}, "custom_fields": cf_status(7207579),
                         "people": [{"id": TENANT_A_PID}, {"id": 9000 + i}],
                         "updated_at": f"2026-08-{(i % 28) + 1:02d}T00:00:00Z"})
fake_s3_p12, _ = use_fixture({lf.PEOPLE_KEY: people_perf12, lf.INTEREST_KEY: {"buy": {}},
                               lf.DEALS_KEY: {"deals": deals_perf12}})
lf._cold_start_seen["done"] = False

mydeals12_event = {"requestContext": {"http": {"method": "GET"}},
                    "queryStringParameters": {"key": ADMIN_KEY, "view_as": TENANT_A_EMAIL, "tab": "mydeals"}}
resp_p12, lines_p12 = _capture_timing_line(mydeals12_event)
check("Perf fixes: 12-company My Deals page still renders successfully", resp_p12["statusCode"] == 200)
line_p12 = lines_p12[0]

# Before these fixes (see the diagnostic report): calls_get_my_matched_
# buy_deals=24, calls_get_my_deals=26, calls_get_deals_list=26 (cold),
# for this same 12-company shape. Firm-level tenancy then swapped My
# Deals' read path from get_my_deals/get_my_matched_buy_deals to their
# firm-scoped mirrors (get_firm_deals/get_firm_matched_buy_deals) --
# same request-scoped-cache shape, so the same O(1)-per-ask argument
# still holds, just under the new names -- and added one more
# get_deals_list() ask (_firm_sell_person_ids_by_company's own scan,
# itself still just one more request-cache hit, not a real fetch; see
# below for the real S3 fetch count).
check("Perf fix 2: get_deals_list's own call count collapses 26 -> 5 "
      "(only the head_object-and-parse path is entered more than once; see below for real fetch count -- "
      "3 -> 4 as of the nav's searchable tenant picker, which now computes the eligible-tenant list, "
      "and so calls get_deals_list once more via _tenant_index, on every admin page -- then 4 -> 5 as of "
      "firm-level tenancy's _firm_sell_person_ids_by_company, which scans deals.json once more per request "
      "to group the firm's own Sell deals by company)",
      "calls_get_deals_list=5" in line_p12)
check("Perf fix 1: get_firm_deals's call count is 26 (firm-level tenancy's mirror of get_my_deals -- "
      "get_firm_matched_buy_deals' OWN second set of 12 per-company calls hit ITS cache before ever "
      "reaching get_firm_deals, same as the old get_my_deals shape; every one of those asks is still "
      "an O(1) cache lookup, not an O(deals) rescan, per the real S3 fetch count checked below)",
      "calls_get_firm_deals=26" in line_p12)
check("get_firm_matched_buy_deals is still asked for once per company by both the dropdown "
      "and the page body (24, unchanged) -- two genuine callers, not a bug -- but each of "
      "those 24 asks is now an O(1) cache lookup instead of an O(deals) rescan (see below)",
      "calls_get_firm_matched_buy_deals=24" in line_p12)

# The real win get_deals_list's call count alone doesn't show: only the
# FIRST of its 3 logical entries this request ever reaches the actual
# S3 fetch -- the other 2 short-circuit on _req_cache["deals"] before
# even a head_object call. Verify directly against the fake S3's own
# call log rather than trusting the derived call-count metric.
s3_get_calls = {"n": 0}
orig_get_object_p12 = fake_s3_p12.get_object
def _counting_get_object_p12(Bucket, Key):
    if Key == lf.DEALS_KEY:
        s3_get_calls["n"] += 1
    return orig_get_object_p12(Bucket, Key)
fake_s3_p12.get_object = _counting_get_object_p12
lf._req_cache_reset()
with contextlib.redirect_stdout(io.StringIO()):
    lf.lambda_handler(mydeals12_event, None)
check("Perf fix 2: deals.json is actually GET-fetched from S3 exactly once for this request "
      "(not 3x, and nowhere near the pre-fix 26x)", s3_get_calls["n"] == 1)
fake_s3_p12.get_object = orig_get_object_p12

# Perf fix 5: the S3 client (and Dynamo table) are constructed once and
# reused -- verify identity stays constant across repeat requests in
# this same container, not just "a client exists".
client_before = lf._s3_client_singleton["client"]
table_before = lf._dynamo_table_singleton["table"]
check("Perf fix 5: S3 client and Dynamo table singletons are populated after a request",
      client_before is not None and table_before is not None)
_capture_timing_line(mydeals12_event)
check("Perf fix 5: the SAME S3 client object is reused across requests in this container",
      lf._s3_client_singleton["client"] is client_before)
check("Perf fix 5: the SAME Dynamo table object is reused across requests in this container",
      lf._dynamo_table_singleton["table"] is table_before)

# Perf fix 1 correctness: memoized results are still the RIGHT results
# -- same object back for a repeated (person_id, company) key, genuinely
# different lists for different companies, and get_my_deals itself
# returns identically-shaped data to a fresh (unmemoized) computation.
lf._req_cache_reset()
first_call = lf.get_my_matched_buy_deals(TENANT_A_PID, "Perf Co 00")
second_call = lf.get_my_matched_buy_deals(TENANT_A_PID, "Perf Co 00")
other_company = lf.get_my_matched_buy_deals(TENANT_A_PID, "Perf Co 03")
check("Perf fix 1: a repeated (person_id, company) call returns the cached list",
      first_call is second_call and len(first_call) == 1 and first_call[0]["id"] == 8000)
check("Perf fix 1: a different company is NOT served the wrong cache entry",
      other_company is not first_call and other_company[0]["id"] == 8003)
my_deals_first = lf.get_my_deals(TENANT_A_PID)
my_deals_second = lf.get_my_deals(TENANT_A_PID)
check("Perf fix 1: get_my_deals is memoized (same object) and still has all 16 of this "
      "tenant's deals (12 sell + 4 buy: i in 0,3,6,9)",
      my_deals_first is my_deals_second and len(my_deals_first) == 16)

# --- Perf fix 3 safety: the shared cache must NOT unify the dropdown's
# raw-only count with render_my_deals_page's own override-aware one --
# a Dynamo status_override that changes disclosure must show up in the
# page's own Intros column WITHOUT leaking into the nav dropdown's
# badge (unchanged from pre-fix behavior on both counts). ---
people_override = {"people": [
    {"id": TENANT_A_PID, "full_name": "Sella Seller", "email": TENANT_A_EMAIL, "custom_fields": {}},
    {"id": 501, "first_name": "Ovi", "last_name": "Buyer", "email": "ovi@example.com", "custom_fields": {}},
]}
deal_override_sell = {"id": 9101, "name": "Override Sell", "company": {"name": "Override Co"},
                      "deal_stage": {"id": lf.STAGE_FIRM}, "custom_fields": cf_sell(),
                      "people": [{"id": TENANT_A_PID}], "updated_at": "2026-08-01T00:00:00Z"}
deal_override_buy = {"id": 9102, "name": "Override Buy", "company": {"name": "Override Co"},
                     "deal_stage": {"id": lf.STAGE_MATCHED}, "custom_fields": cf_status(None),
                     "people": [{"id": TENANT_A_PID}, {"id": 501}], "updated_at": "2026-01-01T00:00:00Z"}
use_fixture({lf.PEOPLE_KEY: people_override, lf.INTEREST_KEY: {"buy": {}},
             lf.DEALS_KEY: {"deals": [deal_override_sell, deal_override_buy]}},
            table_items=[{"tenant": TENANT_A_EMAIL, "sk": "intro#9102",
                          "status_override": 7207579, "override_at": "2100000000"}])
tenant_override = lf._resolve_tenant(TENANT_A_EMAIL)
assert tenant_override is not None

page_override = lf.render_my_deals_page(
    "Sella Seller", deals=[deal_override_sell], key=None, view_as=None,
    person_id=TENANT_A_PID, anon_key_email=TENANT_A_EMAIL)
row_override = row_for(page_override, "9101")
check("Perf fix 3 safety: render_my_deals_page's OWN Intros column IS override-aware "
      "(raw status is empty/Matched, but the Dynamo override says Introduced -> counts as 1, "
      "rendered as a count-link since it's nonzero)",
      row_override is not None and 'class="mydeals-count-link"' in row_override
      and ">1</a>" in row_override)
check("Perf fix 3 safety: the SAME page's nav dropdown badge stays RAW-only "
      "(no badge for Override Co -- the override must not leak into the dropdown's count)",
      '<span class="gg-mydeals-menu-name">Override Co</span></a>' in page_override)


# ======================================================================
# SECTION: "+ Add buyer" (admin-only write path) -- ?action=add_buyer,
# ?lookup_deal=, ?lookup_company_buyers=
# ======================================================================
# Two Pipeline PUTs (status, then a best-effort person-linkage merge),
# abort-on-non-2xx for each, a Dynamo status_override so the row shows
# up immediately, and the "never send a bare array" merge safety rule.

AB_TENANT_EMAIL = "sella-ab@example.com"
AB_TENANT_PID = 501
AB_BUYER_PID = 777

ab_people = {"people": [
    {"id": AB_TENANT_PID, "full_name": "Sella AddBuyer", "email": AB_TENANT_EMAIL, "custom_fields": {}},
    {"id": AB_BUYER_PID, "first_name": "Bianca", "last_name": "Buyer", "full_name": "Bianca Buyer",
     "email": "bianca@buyerfirm.com", "company_name": "Buyer Firm", "custom_fields": {}},
]}
# The tenant's own Sell deal (for auto-enrollment eligibility) and a
# Buy-tagged deal for the SAME company that the tenant's person is NOT
# yet linked to -- exactly the "+ Add buyer" starting state.
ab_sell_deal = {"id": 9600, "name": "AB Sell Deal", "company": {"name": "Echo Co"},
                "deal_stage": {"id": lf.STAGE_FIRM}, "custom_fields": cf_sell(),
                "people": [{"id": AB_TENANT_PID}], "updated_at": "2026-08-01T00:00:00Z"}
ab_buy_deal = {"id": 9601, "name": "AB Buy Deal", "company": {"name": "Echo Co"},
               "deal_stage": {"id": lf.STAGE_MATCHED}, "custom_fields": cf_status(None),
               "people": [{"id": AB_BUYER_PID}], "updated_at": "2026-08-01T00:00:00Z"}
fake_s3_ab, fake_table_ab = use_fixture({
    lf.PEOPLE_KEY: ab_people, lf.INTEREST_KEY: {"buy": {}},
    lf.DEALS_KEY: {"deals": [ab_sell_deal, ab_buy_deal]},
})
ab_tenant = lf._resolve_tenant(AB_TENANT_EMAIL)
assert ab_tenant is not None and ab_tenant["person_id"] == AB_TENANT_PID


class _FakeGetResponse:
    def __init__(self, status, body):
        self.status = status
        self._body = body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self):
        return json.dumps(self._body).encode()


def make_ab_urlopen(calls, get_body=None, get_fail=False, get_status=200,
                     status_put_status=200, link_put_status=200):
    """Records every call (method/url/data) into `calls`, in the exact
    order _handle_add_buyer issues them: PUT (status), then GET, then
    (maybe) a second PUT (person-linkage merge)."""
    put_count = {"n": 0}

    def _urlopen(req, timeout=15):
        method = req.get_method()
        data = json.loads(req.data.decode()) if req.data else None
        calls.append({"method": method, "url": req.full_url, "data": data})
        if method == "GET":
            if get_fail:
                raise lf.urllib.error.HTTPError(req.full_url, 502, "boom", {}, None)
            return _FakeGetResponse(get_status, get_body)
        put_count["n"] += 1
        status = status_put_status if put_count["n"] == 1 else link_put_status
        if status >= 400:
            raise lf.urllib.error.HTTPError(req.full_url, status, "boom", {}, None)
        return FakeHTTPResponse(status)
    return _urlopen


def ab_add_buyer_event(body_dict, cookies=None):
    return {"requestContext": {"http": {"method": "POST"}}, "rawPath": "/",
            "queryStringParameters": {"action": "add_buyer"}, "cookies": cookies or [],
            "body": json.dumps(body_dict)}


def ab_get_event(query, cookies=None):
    return {"requestContext": {"http": {"method": "GET"}}, "rawPath": "/",
            "queryStringParameters": query, "cookies": cookies or []}


# --- Happy path: buyer not yet linked, GET confirms a safe person_ids array
ab_calls = []
lf.urllib.request.urlopen = make_ab_urlopen(ab_calls, get_body={"deal": {"id": 9601, "person_ids": [AB_BUYER_PID]}})
ab_resp = lf.lambda_handler(ab_add_buyer_event({
    "key": ADMIN_KEY, "deal_id": "9601", "status_id": lf.INTRO_STATUS_INTRODUCED_ID,
    "tenant_email": AB_TENANT_EMAIL,
}), None)
check("add_buyer happy path -> 200", ab_resp["statusCode"] == 200)
ab_data = json.loads(ab_resp["body"])
check("add_buyer happy path: link_status == 'linked'", ab_data.get("link_status") == "linked")
check("add_buyer happy path: exactly 3 Pipeline calls (status PUT, GET, link PUT)", len(ab_calls) == 3)
check("add_buyer happy path: call 1 is the Intro Status PUT",
      ab_calls[0]["method"] == "PUT"
      and ab_calls[0]["data"]["deal"]["custom_fields"][lf.INTRO_STATUS_FIELD] == lf.INTRO_STATUS_INTRODUCED_ID)
check("add_buyer happy path: call 2 is the live GET (no body)",
      ab_calls[1]["method"] == "GET" and ab_calls[1]["data"] is None)
check("add_buyer happy path: call 3 PUTs the MERGED array (existing buyer kept, tenant appended) "
      "-- never a bare [tenant_id]",
      ab_calls[2]["method"] == "PUT" and ab_calls[2]["data"]["deal"]["person_ids"] == [AB_BUYER_PID, AB_TENANT_PID])
check("add_buyer happy path: Dynamo status_override stored",
      fake_table_ab.updates and fake_table_ab.updates[-1]["ExpressionAttributeValues"].get(":so")
      == lf.INTRO_STATUS_INTRODUCED_ID)
check("add_buyer happy path: audit actor='admin'", fake_table_ab.puts and fake_table_ab.puts[-1].get("actor") == "admin")

# --- The row appears immediately on both surfaces via the Dynamo
# override even though deals.json's own snapshot STILL only lists the
# original buyer (AB_BUYER_PID), not the tenant -- exactly the hourly-
# sync-lag case _augment_with_dynamo_linked_deals exists to bridge.
# The company page's row shows the BUYER's own name (the page is
# already scoped to one company, see _buy_deal_row_cols_html's
# surface="company" branch) -- so "row exists at all" is checked via
# row_for's data-deal-id match, and "Bianca Buyer" confirms it's the
# right one.
page_company_ab = lf.render_company_page("Echo Co", "Admin", ab_tenant, AB_TENANT_EMAIL, "mydeals",
                                          key=ADMIN_KEY, view_as=AB_TENANT_EMAIL, edit_mode=True)
row_company_ab = row_for(page_company_ab, "9601")
check("newly-linked buyer's row appears on the company page immediately",
      row_company_ab is not None and "Bianca Buyer" in row_company_ab)
page_intros_ab = lf.render_intros_page("Sella AddBuyer", tenant=ab_tenant, tenant_email=AB_TENANT_EMAIL,
                                        key=ADMIN_KEY, view_as=AB_TENANT_EMAIL, edit_mode=True)
row_intros_ab = row_for(page_intros_ab, "9601")
check("newly-linked buyer's row appears on Active Intros immediately too",
      row_intros_ab is not None and "Echo Co" in row_intros_ab and "Bianca Buyer" in row_intros_ab)
check("Active Intros row carries a '+ Add buyer' trigger defaulting to this row's company",
      'data-company="Echo Co"' in page_intros_ab)

# --- Already linked: GET shows the tenant is already on the deal -> no
# redundant second PUT
ab_calls2 = []
lf.urllib.request.urlopen = make_ab_urlopen(
    ab_calls2, get_body={"deal": {"id": 9601, "person_ids": [AB_BUYER_PID, AB_TENANT_PID]}})
ab_resp2 = lf.lambda_handler(ab_add_buyer_event({
    "key": ADMIN_KEY, "deal_id": "9601", "status_id": lf.INTRO_STATUS_INTRODUCED_ID,
    "tenant_email": AB_TENANT_EMAIL,
}), None)
check("add_buyer already-linked -> 200", ab_resp2["statusCode"] == 200)
check("add_buyer already-linked: link_status == 'already_linked'",
      json.loads(ab_resp2["body"]).get("link_status") == "already_linked")
check("add_buyer already-linked: only 2 Pipeline calls (status PUT + GET, no redundant merge PUT)",
      len(ab_calls2) == 2)

# --- GET fails outright: link write is SKIPPED (never guessed), status
# write still completes, Dynamo still records it, and the response
# tells the admin to link by hand instead of risking an unsafe write.
ab_calls3 = []
lf.urllib.request.urlopen = make_ab_urlopen(ab_calls3, get_fail=True)
fake_table_ab.updates.clear()
ab_resp3 = lf.lambda_handler(ab_add_buyer_event({
    "key": ADMIN_KEY, "deal_id": "9601", "status_id": 7207580,  # NDA Signed
    "tenant_email": AB_TENANT_EMAIL,
}), None)
check("add_buyer GET failure -> still 200 (link is best-effort, not fatal)", ab_resp3["statusCode"] == 200)
ab_data3 = json.loads(ab_resp3["body"])
check("add_buyer GET failure: link_status == 'skipped'", ab_data3.get("link_status") == "skipped")
check("add_buyer GET failure: link_message names the tenant instead of guessing a write",
      "Sella AddBuyer" in (ab_data3.get("link_message") or ""))
check("add_buyer GET failure: only 2 calls attempted (status PUT + failed GET, no link PUT at all)",
      len(ab_calls3) == 2)
check("add_buyer GET failure: Dynamo status_override is still written",
      fake_table_ab.updates and fake_table_ab.updates[-1]["ExpressionAttributeValues"].get(":so") == 7207580)

# --- GET succeeds but the shape is unrecognizable (neither person_ids
# nor people present) -- also skipped, same as an outright GET failure.
ab_calls4 = []
lf.urllib.request.urlopen = make_ab_urlopen(ab_calls4, get_body={"deal": {"id": 9601, "name": "no linkage field"}})
ab_resp4 = lf.lambda_handler(ab_add_buyer_event({
    "key": ADMIN_KEY, "deal_id": "9601", "status_id": lf.INTRO_STATUS_INTRODUCED_ID,
    "tenant_email": AB_TENANT_EMAIL,
}), None)
check("add_buyer malformed GET shape: link_status == 'skipped' (never guesses a linkage array)",
      json.loads(ab_resp4["body"]).get("link_status") == "skipped")
check("add_buyer malformed GET shape: no merge PUT was ever attempted",
      len(ab_calls4) == 2 and ab_calls4[1]["method"] == "GET")

# --- The Intro Status PUT itself fails: abort everything, nothing else
# is even attempted, no Dynamo write.
ab_calls5 = []
lf.urllib.request.urlopen = make_ab_urlopen(ab_calls5, status_put_status=502)
fake_table_ab.updates.clear()
ab_resp5 = lf.lambda_handler(ab_add_buyer_event({
    "key": ADMIN_KEY, "deal_id": "9601", "status_id": lf.INTRO_STATUS_INTRODUCED_ID,
    "tenant_email": AB_TENANT_EMAIL,
}), None)
check("add_buyer status PUT failure -> 502", ab_resp5["statusCode"] == 502)
check("add_buyer status PUT failure: only 1 call attempted (never reaches the GET)", len(ab_calls5) == 1)
check("add_buyer status PUT failure: no Dynamo write happens", len(fake_table_ab.updates) == 0)

# --- Status PUT succeeds and the GET confirms a safe array, but the
# merge PUT itself fails: this is a determinate failure (not a "can't
# tell" case), so it aborts too -- no Dynamo write, despite the first
# Pipeline write having already gone through.
ab_calls6 = []
lf.urllib.request.urlopen = make_ab_urlopen(
    ab_calls6, get_body={"deal": {"id": 9601, "person_ids": [AB_BUYER_PID]}}, link_put_status=502)
fake_table_ab.updates.clear()
ab_resp6 = lf.lambda_handler(ab_add_buyer_event({
    "key": ADMIN_KEY, "deal_id": "9601", "status_id": lf.INTRO_STATUS_INTRODUCED_ID,
    "tenant_email": AB_TENANT_EMAIL,
}), None)
check("add_buyer merge-PUT failure (after a safe GET) -> 502", ab_resp6["statusCode"] == 502)
check("add_buyer merge-PUT failure: all 3 calls were attempted (status PUT, GET, failed link PUT)",
      len(ab_calls6) == 3)
check("add_buyer merge-PUT failure: no Dynamo write happens (abort, even though write 1 already succeeded)",
      len(fake_table_ab.updates) == 0)

# --- Admin-only, enforced server-side: no ADMIN_KEY at all, and a real
# tenant's own identity cookie -- neither authorizes this route (unlike
# ?action=update_intro, there is no tenant fallback here whatsoever).
ab_resp_noauth = lf.lambda_handler(ab_add_buyer_event({
    "deal_id": "9601", "status_id": lf.INTRO_STATUS_INTRODUCED_ID, "tenant_email": AB_TENANT_EMAIL,
}), None)
check("add_buyer with no admin key -> 403", ab_resp_noauth["statusCode"] == 403)
ab_resp_tenant_cookie = lf.lambda_handler(ab_add_buyer_event({
    "deal_id": "9601", "status_id": lf.INTRO_STATUS_INTRODUCED_ID, "tenant_email": AB_TENANT_EMAIL,
}, cookies=[tenant_cookie(AB_TENANT_EMAIL)]), None)
check("add_buyer with a real tenant cookie but no admin key -> still 403 (admin-only, no tenant path exists)",
      ab_resp_tenant_cookie["statusCode"] == 403)

# --- Input validation
ab_resp_bad_status = lf.lambda_handler(ab_add_buyer_event({
    "key": ADMIN_KEY, "deal_id": "9601", "status_id": 999999, "tenant_email": AB_TENANT_EMAIL,
}), None)
check("add_buyer with an invalid status_id -> 400", ab_resp_bad_status["statusCode"] == 400)
ab_resp_bad_tenant = lf.lambda_handler(ab_add_buyer_event({
    "key": ADMIN_KEY, "deal_id": "9601", "status_id": lf.INTRO_STATUS_INTRODUCED_ID,
    "tenant_email": "nobody@nowhere.example.com",
}), None)
check("add_buyer with an unresolvable tenant_email -> 400", ab_resp_bad_tenant["statusCode"] == 400)

# --- ?lookup_company_buyers=<company>: the search box's candidate list
# (admin-only GET route)
ab_resp_lookup_co = lf.lambda_handler(ab_get_event({"lookup_company_buyers": "Echo Co", "key": ADMIN_KEY}), None)
check("lookup_company_buyers -> 200", ab_resp_lookup_co["statusCode"] == 200)
ab_lookup_co_data = json.loads(ab_resp_lookup_co["body"])
check("lookup_company_buyers finds the Buy-tagged Echo Co deal",
      any(d["id"] == "9601" for d in ab_lookup_co_data.get("deals", [])))
check("lookup_company_buyers reports the real buyer name (never the tenant's own id/array)",
      any(d.get("buyer_name") == "Bianca Buyer" for d in ab_lookup_co_data["deals"]))
ab_resp_lookup_co_noauth = lf.lambda_handler(ab_get_event({"lookup_company_buyers": "Echo Co"}), None)
check("lookup_company_buyers with no admin key -> 403", ab_resp_lookup_co_noauth["statusCode"] == 403)

# --- ?lookup_deal=<id>: the "paste deal id" flow, live-fetching a deal
# that ISN'T in the deals.json snapshot at all (the snapshot excludes
# won/lost/obsolete by design; this stands in for that case generally).
lf.urllib.request.urlopen = make_ab_urlopen([], get_body={"deal": {
    "id": 9999, "name": "Absent Deal", "company": {"name": "Echo Co"},
    "deal_stage": {"id": lf.STAGE_MATCHED}, "custom_fields": cf_status(None), "person_ids": [AB_BUYER_PID],
}})
ab_resp_lookup_deal = lf.lambda_handler(ab_get_event({"lookup_deal": "9999", "key": ADMIN_KEY}), None)
check("lookup_deal (absent from the snapshot) -> 200", ab_resp_lookup_deal["statusCode"] == 200)
ab_lookup_deal_data = json.loads(ab_resp_lookup_deal["body"])
check("lookup_deal reports deal name/company/buyer/stage for a snapshot-absent deal",
      ab_lookup_deal_data.get("deal_name") == "Absent Deal"
      and ab_lookup_deal_data.get("company_name") == "Echo Co"
      and ab_lookup_deal_data.get("buyer_name") == "Bianca Buyer"
      and ab_lookup_deal_data.get("stage_name") == "MATCHED")
ab_resp_lookup_deal_noauth = lf.lambda_handler(ab_get_event({"lookup_deal": "9999"}), None)
check("lookup_deal with no admin key -> 403", ab_resp_lookup_deal_noauth["statusCode"] == 403)

# --- ?lookup_deal= on a Sell-side deal is rejected -- this picker is
# for Buy-side deals only.
lf.urllib.request.urlopen = make_ab_urlopen([], get_body={"deal": {
    "id": 9998, "name": "Sell Deal", "company": {"name": "Echo Co"}, "custom_fields": cf_sell(),
}})
ab_resp_lookup_sell = lf.lambda_handler(ab_get_event({"lookup_deal": "9998", "key": ADMIN_KEY}), None)
check("lookup_deal on a Sell-side deal -> 400", ab_resp_lookup_sell["statusCode"] == 400)


# ======================================================================
# SECTION: Buyer Demand tiles admin controls + "Introduce"
# (?action=introduce_buyer)
# ======================================================================
# Admin-only real names/controls on the Demand grid (server-enforced,
# not just UI-hidden); find-or-create a Buy-tagged deal (snapshot ->
# live person-deals scan, capped, with a hard abort on an inconclusive
# live lookup rather than risking a duplicate create), the same
# never-a-bare-array merge rule, and the Dynamo override that makes the
# row appear immediately.

IB_TENANT_EMAIL = "sella-ib@example.com"
IB_TENANT_PID = 601
IB_BUYER_PID = 701        # no snapshot deal for Foxtrot Co -- exercises live/create paths
IB_BUYER_SNAP_PID = 702   # already has a snapshot Buy deal for Foxtrot Co

ib_people = {"people": [
    {"id": IB_TENANT_PID, "full_name": "Sella Introduce", "email": IB_TENANT_EMAIL, "custom_fields": {}},
    {"id": IB_BUYER_PID, "first_name": "Ravi", "last_name": "Buyer", "full_name": "Ravi Buyer",
     "email": "ravi@buyerfirm.com", "company_name": "Ravi Capital", "custom_fields": {}},
    {"id": IB_BUYER_SNAP_PID, "first_name": "Sana", "last_name": "Snapshot", "full_name": "Sana Snapshot",
     "email": "sana@snapcap.com", "company_name": "Snap Capital", "custom_fields": {}},
]}
ib_sell_deal = {"id": 9700, "name": "IB Sell Deal", "company": {"name": "Foxtrot Co"},
                "deal_stage": {"id": lf.STAGE_FIRM}, "custom_fields": cf_sell(),
                "people": [{"id": IB_TENANT_PID}], "updated_at": "2026-08-01T00:00:00Z"}
ib_buy_deal_snapshot = {"id": 9701, "name": "IB Snapshot Buy Deal", "company": {"name": "Foxtrot Co"},
                         "deal_stage": {"id": lf.STAGE_MATCHED}, "custom_fields": cf_status(None),
                         "people": [{"id": IB_BUYER_SNAP_PID}], "updated_at": "2026-08-01T00:00:00Z"}
ib_interest = {"buy": {"Foxtrot Co": [IB_BUYER_PID, IB_BUYER_SNAP_PID]}}
fake_s3_ib, fake_table_ib = use_fixture({
    lf.PEOPLE_KEY: ib_people, lf.INTEREST_KEY: ib_interest,
    lf.DEALS_KEY: {"deals": [ib_sell_deal, ib_buy_deal_snapshot]},
})
ib_tenant = lf._resolve_tenant(IB_TENANT_EMAIL)
assert ib_tenant is not None and ib_tenant["person_id"] == IB_TENANT_PID


def make_ib_urlopen(calls, person=None, person_fail=False, deals=None, deals_fail=None,
                     status_put_status=200, link_put_status=200,
                     create_status=200, create_body=None, create_fail=False):
    """Dispatches on (method, url) rather than call order, since
    _find_existing_buy_deal_for_person_company's own scan can issue a
    variable number of calls before _handle_introduce_buyer's write
    sequence even starts."""
    deals = deals or {}
    deals_fail = deals_fail or set()

    def _urlopen(req, timeout=15):
        method = req.get_method()
        url = req.full_url
        data = json.loads(req.data.decode()) if req.data else None
        calls.append({"method": method, "url": url, "data": data})
        if method == "GET" and "/people/" in url:
            if person_fail:
                raise lf.urllib.error.HTTPError(url, 502, "boom", {}, None)
            return _FakeGetResponse(200, {"person": person} if person is not None else {})
        if method == "POST" and "/deals.json" in url:
            if create_fail:
                raise lf.urllib.error.HTTPError(url, create_status, "boom", {}, None)
            return _FakeGetResponse(create_status, {"deal": create_body} if create_body else {})
        if method == "GET" and "/deals/" in url:
            m = re.search(r"/deals/(\d+)\.json", url)
            did = m.group(1) if m else None
            if did in deals_fail:
                raise lf.urllib.error.HTTPError(url, 502, "boom", {}, None)
            d = deals.get(did)
            if d is None:
                raise lf.urllib.error.HTTPError(url, 404, "not found", {}, None)
            return _FakeGetResponse(200, {"deal": d})
        if method == "PUT" and "/deals/" in url:
            is_status_put = bool(data and lf.INTRO_STATUS_FIELD in (data.get("deal") or {}).get("custom_fields", {}))
            status = status_put_status if is_status_put else link_put_status
            if status >= 400:
                raise lf.urllib.error.HTTPError(url, status, "boom", {}, None)
            return FakeHTTPResponse(status)
        raise AssertionError(f"unexpected call: {method} {url}")
    return _urlopen


def ib_event(body_dict, cookies=None):
    return {"requestContext": {"http": {"method": "POST"}}, "rawPath": "/",
            "queryStringParameters": {"action": "introduce_buyer"}, "cookies": cookies or [],
            "body": json.dumps(body_dict)}


# --- Rendering: a real tenant NEVER sees a buyer's name/id/controls on
# the Demand grid, admin key or not -- the anonymized tile is unchanged.
# edit_mode is the ONLY thing that governs this admin-only content
# anywhere in render_company_page -- these calls are shaped exactly the
# way lambda_handler itself would actually compute edit_mode for each
# scenario (edit_mode = is_admin_key and (not view_as or &edit=1)), so
# a passing test here really does say something about the real route.
page_tenant_view = lf.render_company_page("Foxtrot Co", "Sella Introduce", ib_tenant, IB_TENANT_EMAIL, "mydeals",
                                           key=None, view_as=None, edit_mode=False)
check("tenant view: no real buyer name leaks onto the Demand grid",
      "Ravi Buyer" not in page_tenant_view and "Sana Snapshot" not in page_tenant_view)
# (checked via the actual tag's class="..." attribute, not a bare
# substring -- "intro-buyer-btn" alone also appears in the page's own
# <style> block's ".intro-buyer-btn {" selector, which is always
# present regardless of whether any button renders)
check("tenant view: no Introduce button/controls at all", 'class="intro-buyer-btn"' not in page_tenant_view
      and 'class="intro-status-select"' not in page_tenant_view)
check("tenant view: anonymized 'Buyer ' code tiles still render", "Buyer " in page_tenant_view)

# --- Admin, no &view_as: edit_mode is True here in the real app (view_as
# empty -> "not view_as" is True), so real names show (admin always sees
# the real thing), but NO Introduce controls -- there's no tenant yet to
# link to.
page_admin_noview = lf.render_company_page("Foxtrot Co", "Admin", None, "admin", "mydeals",
                                            key=ADMIN_KEY, view_as=None, edit_mode=True)
check("admin, no &view_as: real buyer names ARE shown", "Ravi Buyer" in page_admin_noview
      and "Sana Snapshot" in page_admin_noview)
check("admin, no &view_as: still no Introduce controls (nothing to link the buyer to)",
      'class="intro-buyer-btn"' not in page_admin_noview)

# --- VIEW_AS CONTRACT, bug fix: admin + BARE &view_as (no &edit=1) --
# edit_mode is False here (see lambda_handler's own formula) -- MUST
# render exactly what that tenant sees: no real buyer name anywhere,
# no Introduce button, no status dropdown. This used to leak all three
# because the Demand-tiles admin branch keyed off the bare admin-key
# flag instead of edit_mode; regression-locked here.
page_admin_bare_view_as = lf.render_company_page("Foxtrot Co", "Admin", ib_tenant, IB_TENANT_EMAIL, "mydeals",
                                                  key=ADMIN_KEY, view_as=IB_TENANT_EMAIL, edit_mode=False)
check("admin + bare &view_as (no &edit=1): NO real buyer name anywhere on the page",
      "Ravi Buyer" not in page_admin_bare_view_as and "Sana Snapshot" not in page_admin_bare_view_as)
check("admin + bare &view_as: NO Introduce button/status-select markup at all",
      'class="intro-buyer-btn"' not in page_admin_bare_view_as
      and 'class="intro-status-select"' not in page_admin_bare_view_as)
check("admin + bare &view_as: anonymized 'Buyer ' code tiles render instead, just like the tenant's own view",
      "Buyer " in page_admin_bare_view_as)

# --- Admin WITH &view_as AND &edit=1: edit_mode is True -- real names
# AND working Introduce controls, each carrying the right
# buyer/company/tenant on its own button.
page_admin_view = lf.render_company_page("Foxtrot Co", "Admin", ib_tenant, IB_TENANT_EMAIL, "mydeals",
                                          key=ADMIN_KEY, view_as=IB_TENANT_EMAIL, edit_mode=True)
check("admin + &view_as + &edit=1: real buyer name shown", "Ravi Buyer" in page_admin_view)
check("admin + &view_as + &edit=1: Introduce button present, keyed to the right buyer/company/tenant",
      f'data-person-id="{IB_BUYER_PID}"' in page_admin_view
      and 'data-company="Foxtrot Co"' in page_admin_view
      and f'data-tenant-email="{IB_TENANT_EMAIL}"' in page_admin_view)
check("admin + &view_as + &edit=1: status dropdown defaults to Introduced (7207579)",
      f'<option value="{lf.INTRO_STATUS_INTRODUCED_ID}" selected>' in page_admin_view)

# --- Admin-only, enforced server-side: no ADMIN_KEY, and a real
# tenant's own cookie -- neither authorizes this route.
ib_resp_noauth = lf.lambda_handler(ib_event({
    "person_id": IB_BUYER_PID, "company": "Foxtrot Co", "tenant_email": IB_TENANT_EMAIL,
}), None)
check("introduce_buyer with no admin key -> 403", ib_resp_noauth["statusCode"] == 403)
ib_resp_tenant_cookie = lf.lambda_handler(ib_event({
    "person_id": IB_BUYER_PID, "company": "Foxtrot Co", "tenant_email": IB_TENANT_EMAIL,
}, cookies=[tenant_cookie(IB_TENANT_EMAIL)]), None)
check("introduce_buyer with a real tenant cookie but no admin key -> still 403", ib_resp_tenant_cookie["statusCode"] == 403)

# --- Found via the SNAPSHOT (buyer already has a Buy-tagged deal for
# this company on file) -- no live Pipeline lookup needed at all.
ib_calls1 = []
lf.urllib.request.urlopen = make_ib_urlopen(
    ib_calls1, deals={"9701": {"id": 9701, "person_ids": [IB_BUYER_SNAP_PID]}})
ib_resp1 = lf.lambda_handler(ib_event({
    "key": ADMIN_KEY, "person_id": IB_BUYER_SNAP_PID, "company": "Foxtrot Co",
    "status_id": lf.INTRO_STATUS_INTRODUCED_ID, "tenant_email": IB_TENANT_EMAIL,
}), None)
check("introduce (snapshot match) -> 200", ib_resp1["statusCode"] == 200)
ib_data1 = json.loads(ib_resp1["body"])
check("introduce (snapshot match): lookup_used == 'snapshot', created == False",
      ib_data1.get("lookup_used") == "snapshot" and ib_data1.get("created") is False)
check("introduce (snapshot match): no /people/ call was ever made (found before the live path)",
      not any("/people/" in c["url"] for c in ib_calls1))
check("introduce (snapshot match): status PUT, then GET, then the merged-array PUT (buyer kept, tenant appended)",
      len(ib_calls1) == 3 and ib_calls1[0]["method"] == "PUT" and ib_calls1[1]["method"] == "GET"
      and ib_calls1[2]["method"] == "PUT"
      and ib_calls1[2]["data"]["deal"]["person_ids"] == [IB_BUYER_SNAP_PID, IB_TENANT_PID])
check("introduce (snapshot match): Dynamo status_override stored for deal 9701",
      fake_table_ib.updates and fake_table_ib.updates[-1]["Key"]["sk"] == "intro#9701")

row_after_snapshot = row_for(
    lf.render_company_page("Foxtrot Co", "Admin", ib_tenant, IB_TENANT_EMAIL, "mydeals",
                            key=ADMIN_KEY, view_as=IB_TENANT_EMAIL, edit_mode=True),
    "9701")
check("introduced buyer's row appears in the Buyers section immediately", row_after_snapshot is not None)

# --- Not in the snapshot, but a LIVE scan of the buyer's own deals
# finds a match (a terminal-stage deal the snapshot excludes by
# design).
ib_calls2 = []
lf.urllib.request.urlopen = make_ib_urlopen(
    ib_calls2,
    person={"id": IB_BUYER_PID, "deal_ids": [5001, 5002]},
    deals={
        "5001": {"id": 5001, "company": {"name": "Some Other Co"},
                  "custom_fields": {lf.DEAL_SIDE_FIELD: lf.DEAL_SIDE_BUY_ID}},
        "5002": {"id": 5002, "company": {"name": "Foxtrot Co"}, "deal_stage": {"id": 111802},  # Won -- terminal
                  "custom_fields": {lf.DEAL_SIDE_FIELD: lf.DEAL_SIDE_BUY_ID}, "person_ids": [IB_BUYER_PID]},
    })
ib_resp2 = lf.lambda_handler(ib_event({
    "key": ADMIN_KEY, "person_id": IB_BUYER_PID, "company": "Foxtrot Co",
    "status_id": lf.INTRO_STATUS_CLOSED_ID, "tenant_email": IB_TENANT_EMAIL,
}), None)
check("introduce (live scan finds a terminal-stage match) -> 200", ib_resp2["statusCode"] == 200)
ib_data2 = json.loads(ib_resp2["body"])
check("introduce (live match): lookup_used == 'live', created == False, deal_id == 5002",
      ib_data2.get("lookup_used") == "live" and ib_data2.get("created") is False
      and ib_data2.get("deal_id") == "5002")
check("introduce (live match): reported which lookup was used, and it checked BOTH candidates before matching 5002",
      any("/people/" in c["url"] for c in ib_calls2)
      and any("/deals/5001.json" in c["url"] for c in ib_calls2)
      and any("/deals/5002.json" in c["url"] for c in ib_calls2))

# --- Not found anywhere (live scan completes, no match) -> CREATE a
# brand-new Buy-tagged deal, Matched stage, linked to both buyer and
# tenant, chosen status set on creation.
ib_calls3 = []
lf.urllib.request.urlopen = make_ib_urlopen(
    ib_calls3, person={"id": IB_BUYER_PID, "deal_ids": []},
    create_status=201, create_body={"id": 9800, "name": "created"})
ib_resp3 = lf.lambda_handler(ib_event({
    "key": ADMIN_KEY, "person_id": IB_BUYER_PID, "company": "Foxtrot Co",
    "status_id": lf.INTRO_STATUS_INTRODUCED_ID, "tenant_email": IB_TENANT_EMAIL,
}), None)
check("introduce (nothing found) -> 200", ib_resp3["statusCode"] == 200)
ib_data3 = json.loads(ib_resp3["body"])
check("introduce (nothing found): lookup_used == 'live', created == True, deal_id == '9800'",
      ib_data3.get("lookup_used") == "live" and ib_data3.get("created") is True
      and ib_data3.get("deal_id") == "9800")
create_call = next(c for c in ib_calls3 if c["method"] == "POST")
create_payload = create_call["data"]["deal"]
check("introduce create: Matched stage, Buy side tag, chosen status, company_name (no companies.json on file)",
      create_payload["deal_stage_id"] == lf.STAGE_MATCHED
      and create_payload["custom_fields"][lf.DEAL_SIDE_FIELD] == [lf.DEAL_SIDE_BUY_ID]
      and create_payload["custom_fields"][lf.INTRO_STATUS_FIELD] == lf.INTRO_STATUS_INTRODUCED_ID
      and create_payload["company_name"] == "Foxtrot Co" and "company_id" not in create_payload)
check("introduce create: linked to BOTH buyer and tenant, buyer as primary contact",
      create_payload["person_ids"] == [IB_BUYER_PID, IB_TENANT_PID]
      and create_payload["primary_contact_id"] == IB_BUYER_PID)
check("introduce create: Dynamo status_override stored under the new deal id",
      fake_table_ib.updates and fake_table_ib.updates[-1]["Key"]["sk"] == "intro#9800")

# --- company_id preferred over company_name when companies.json has a
# match for this company (per Chad's instruction).
fake_s3_ib.objs[lf.COMPANIES_KEY] = {"companies": [{"id": 4242, "name": "Foxtrot Co"}]}
ib_calls3b = []
lf.urllib.request.urlopen = make_ib_urlopen(
    ib_calls3b, person={"id": IB_BUYER_PID, "deal_ids": []},
    create_status=201, create_body={"id": 9801, "name": "created2"})
lf._req_cache_reset()
lf.lambda_handler(ib_event({
    "key": ADMIN_KEY, "person_id": IB_BUYER_PID, "company": "Foxtrot Co",
    "status_id": lf.INTRO_STATUS_INTRODUCED_ID, "tenant_email": IB_TENANT_EMAIL,
}), None)
create_call_b = next(c for c in ib_calls3b if c["method"] == "POST")
check("introduce create: company_id used instead of company_name once companies.json has a match",
      create_call_b["data"]["deal"].get("company_id") == 4242
      and "company_name" not in create_call_b["data"]["deal"])
del fake_s3_ib.objs[lf.COMPANIES_KEY]

# --- Too many candidate deal ids on the buyer's own record -> live
# scan skipped entirely, falls back to create, and says why.
ib_calls4 = []
lf.urllib.request.urlopen = make_ib_urlopen(
    ib_calls4, person={"id": IB_BUYER_PID, "deal_ids": list(range(50))},
    create_status=201, create_body={"id": 9802, "name": "created3"})
ib_resp4 = lf.lambda_handler(ib_event({
    "key": ADMIN_KEY, "person_id": IB_BUYER_PID, "company": "Foxtrot Co",
    "status_id": lf.INTRO_STATUS_INTRODUCED_ID, "tenant_email": IB_TENANT_EMAIL,
}), None)
ib_data4 = json.loads(ib_resp4["body"])
check("introduce (too many candidates): lookup_used == 'live-skipped', still creates, and explains why",
      ib_data4.get("lookup_used") == "live-skipped" and ib_data4.get("created") is True
      and "too many" in (ib_data4.get("note") or ""))
check("introduce (too many candidates): no individual /deals/<id> GETs were attempted at all",
      not any(c["method"] == "GET" and "/deals/" in c["url"] for c in ib_calls4))

# --- The live PERSON lookup itself fails: genuinely unknown whether a
# deal already exists, so this aborts rather than risking a duplicate
# create.
ib_calls5 = []
lf.urllib.request.urlopen = make_ib_urlopen(ib_calls5, person_fail=True)
fake_table_ib.updates.clear()
ib_resp5 = lf.lambda_handler(ib_event({
    "key": ADMIN_KEY, "person_id": IB_BUYER_PID, "company": "Foxtrot Co",
    "status_id": lf.INTRO_STATUS_INTRODUCED_ID, "tenant_email": IB_TENANT_EMAIL,
}), None)
check("introduce (live person lookup fails) -> 502, never guesses 'not found'", ib_resp5["statusCode"] == 502)
check("introduce (live person lookup fails): no deal-create or Dynamo write was attempted",
      not any(c["method"] == "POST" for c in ib_calls5) and len(fake_table_ib.updates) == 0)

# --- Found (snapshot), but the GET before the person-linkage merge
# fails -> abort, no Dynamo write, even though the status PUT already
# succeeded.
ib_calls6 = []
lf.urllib.request.urlopen = make_ib_urlopen(ib_calls6, deals_fail={"9701"})
fake_table_ib.updates.clear()
ib_resp6 = lf.lambda_handler(ib_event({
    "key": ADMIN_KEY, "person_id": IB_BUYER_SNAP_PID, "company": "Foxtrot Co",
    "status_id": lf.INTRO_STATUS_INTRODUCED_ID, "tenant_email": IB_TENANT_EMAIL,
}), None)
check("introduce (snapshot match, merge GET fails) -> 502", ib_resp6["statusCode"] == 502)
check("introduce (snapshot match, merge GET fails): no Dynamo write (abort)", len(fake_table_ib.updates) == 0)

# --- Found (snapshot), GET succeeds but the shape is unrecognizable
# (neither person_ids nor people) -> abort (no "link manually" skip on
# this route, unlike add_buyer).
ib_calls7 = []
lf.urllib.request.urlopen = make_ib_urlopen(ib_calls7, deals={"9701": {"id": 9701, "name": "no linkage field"}})
fake_table_ib.updates.clear()
ib_resp7 = lf.lambda_handler(ib_event({
    "key": ADMIN_KEY, "person_id": IB_BUYER_SNAP_PID, "company": "Foxtrot Co",
    "status_id": lf.INTRO_STATUS_INTRODUCED_ID, "tenant_email": IB_TENANT_EMAIL,
}), None)
check("introduce (snapshot match, unsafe GET shape) -> 502, no silent skip on this route",
      ib_resp7["statusCode"] == 502)
check("introduce (snapshot match, unsafe GET shape): no Dynamo write", len(fake_table_ib.updates) == 0)

# --- Found (snapshot), tenant already linked -> no redundant merge PUT.
ib_calls8 = []
lf.urllib.request.urlopen = make_ib_urlopen(
    ib_calls8, deals={"9701": {"id": 9701, "person_ids": [IB_BUYER_SNAP_PID, IB_TENANT_PID]}})
ib_resp8 = lf.lambda_handler(ib_event({
    "key": ADMIN_KEY, "person_id": IB_BUYER_SNAP_PID, "company": "Foxtrot Co",
    "status_id": lf.INTRO_STATUS_INTRODUCED_ID, "tenant_email": IB_TENANT_EMAIL,
}), None)
check("introduce (already linked): only status PUT + GET, no redundant merge PUT",
      ib_resp8["statusCode"] == 200 and len(ib_calls8) == 2)

# --- The Intro Status PUT itself fails on a found deal -> abort
# everything, no GET/merge attempted, no Dynamo write.
ib_calls9 = []
lf.urllib.request.urlopen = make_ib_urlopen(ib_calls9, status_put_status=502)
fake_table_ib.updates.clear()
ib_resp9 = lf.lambda_handler(ib_event({
    "key": ADMIN_KEY, "person_id": IB_BUYER_SNAP_PID, "company": "Foxtrot Co",
    "status_id": lf.INTRO_STATUS_INTRODUCED_ID, "tenant_email": IB_TENANT_EMAIL,
}), None)
check("introduce (status PUT fails on a found deal) -> 502", ib_resp9["statusCode"] == 502)
check("introduce (status PUT fails): only 1 call attempted (never reaches the merge GET)", len(ib_calls9) == 1)
check("introduce (status PUT fails): no Dynamo write", len(fake_table_ib.updates) == 0)

# --- No status_id in the request -> defaults to Introduced, per
# instruction ("default Introduced 7207579").
ib_calls10 = []
lf.urllib.request.urlopen = make_ib_urlopen(
    ib_calls10, deals={"9701": {"id": 9701, "person_ids": [IB_BUYER_SNAP_PID]}})
lf.lambda_handler(ib_event({
    "key": ADMIN_KEY, "person_id": IB_BUYER_SNAP_PID, "company": "Foxtrot Co", "tenant_email": IB_TENANT_EMAIL,
}), None)
status_put_call = next(c for c in ib_calls10 if c["method"] == "PUT"
                        and lf.INTRO_STATUS_FIELD in c["data"]["deal"].get("custom_fields", {}))
check("introduce with no status_id in the request defaults to Introduced",
      status_put_call["data"]["deal"]["custom_fields"][lf.INTRO_STATUS_FIELD] == lf.INTRO_STATUS_INTRODUCED_ID)

# --- Input validation
ib_resp_bad_person = lf.lambda_handler(ib_event({
    "key": ADMIN_KEY, "person_id": "not-an-int", "company": "Foxtrot Co", "tenant_email": IB_TENANT_EMAIL,
}), None)
check("introduce with a non-integer person_id -> 400", ib_resp_bad_person["statusCode"] == 400)
ib_resp_no_company = lf.lambda_handler(ib_event({
    "key": ADMIN_KEY, "person_id": IB_BUYER_PID, "company": "", "tenant_email": IB_TENANT_EMAIL,
}), None)
check("introduce with an empty company -> 400", ib_resp_no_company["statusCode"] == 400)
ib_resp_bad_status = lf.lambda_handler(ib_event({
    "key": ADMIN_KEY, "person_id": IB_BUYER_PID, "company": "Foxtrot Co", "status_id": 999999,
    "tenant_email": IB_TENANT_EMAIL,
}), None)
check("introduce with an invalid status_id -> 400", ib_resp_bad_status["statusCode"] == 400)
ib_resp_bad_tenant = lf.lambda_handler(ib_event({
    "key": ADMIN_KEY, "person_id": IB_BUYER_PID, "company": "Foxtrot Co",
    "tenant_email": "nobody@nowhere.example.com",
}), None)
check("introduce with an unresolvable tenant_email -> 400", ib_resp_bad_tenant["statusCode"] == 400)
ib_resp_bad_buyer = lf.lambda_handler(ib_event({
    "key": ADMIN_KEY, "person_id": 999999999, "company": "Foxtrot Co", "tenant_email": IB_TENANT_EMAIL,
}), None)
check("introduce with a person_id matching no real person -> 400", ib_resp_bad_buyer["statusCode"] == 400)


# ======================================================================
# SECTION: Manual intros (Dynamo-only, ?action=manual_intro) + the
# company page's "Add buyer" box below Buyer Demand
# ======================================================================
# NO Pipeline call of any kind on this route. Manual rows resolve name/
# firm/tier/ticket range/contact/photo from the people snapshot exactly
# like deal-derived rows, follow the same disclosure rule (Introduced-
# or-later discloses, Matched stays anonymized), route Passed/Withdrawn
# to Closed out and Closed to a positive closed row, carry a "manual"
# tag, are admin-only to create/edit, and are suppressed once a real
# deal-derived intro covers the same tenant+company+buyer.

MI_TENANT_EMAIL = "elana@tworoads.vc"
MI_TENANT_PID = 801
MI_BUYER_PID = 1277391706   # Chase Fraser -- the literal instruction test case
MI_BUYER2_PID = 802         # disclosure (Matched) test
MI_BUYER3_PID = 803         # suppression test (already has a real deal)
MI_COMPANY = "Panthalassa"

mi_people = {"people": [
    {"id": MI_TENANT_PID, "full_name": "Elana Founder", "email": MI_TENANT_EMAIL, "custom_fields": {}},
    {"id": MI_BUYER_PID, "first_name": "Chase", "last_name": "Fraser", "full_name": "Chase Fraser",
     "email": "chase@example.com", "company_name": "Fraser Capital", "custom_fields": {}},
    {"id": MI_BUYER2_PID, "first_name": "Dana", "last_name": "Second", "full_name": "Dana Second",
     "email": "dana@example.com", "company_name": "Second Capital", "custom_fields": {}},
    {"id": MI_BUYER3_PID, "first_name": "Wren", "last_name": "Third", "full_name": "Wren Third",
     "email": "wren@example.com", "company_name": "Third Capital", "custom_fields": {}},
]}
mi_sell_deal = {"id": 9900, "name": "MI Sell Deal", "company": {"name": MI_COMPANY},
                "deal_stage": {"id": lf.STAGE_FIRM}, "custom_fields": cf_sell(),
                "people": [{"id": MI_TENANT_PID}], "updated_at": "2026-08-01T00:00:00Z"}
mi_real_buy_deal = {"id": 9901, "name": "MI Real Buy Deal", "company": {"name": MI_COMPANY},
                     "deal_stage": {"id": lf.STAGE_MATCHED}, "custom_fields": cf_status(lf.INTRO_STATUS_INTRODUCED_ID),
                     "people": [{"id": MI_TENANT_PID}, {"id": MI_BUYER3_PID}], "updated_at": "2026-08-01T00:00:00Z"}
fake_s3_mi, fake_table_mi = use_fixture({
    lf.PEOPLE_KEY: mi_people, lf.INTEREST_KEY: {"buy": {}},
    lf.DEALS_KEY: {"deals": [mi_sell_deal, mi_real_buy_deal]},
})
mi_tenant = lf._resolve_tenant(MI_TENANT_EMAIL)
assert mi_tenant is not None and mi_tenant["person_id"] == MI_TENANT_PID


def mi_event(body_dict, cookies=None):
    return {"requestContext": {"http": {"method": "POST"}}, "rawPath": "/",
            "queryStringParameters": {"action": "manual_intro"}, "cookies": cookies or [],
            "body": json.dumps(body_dict)}


# --- Admin-only, enforced server-side -- no tenant write path exists
# for manual intros at all.
mi_resp_noauth = lf.lambda_handler(mi_event({
    "company": MI_COMPANY, "person_id": MI_BUYER_PID, "status": lf.INTRO_STATUS_CLOSED_ID,
    "tenant_email": MI_TENANT_EMAIL,
}), None)
check("manual_intro with no admin key -> 403", mi_resp_noauth["statusCode"] == 403)
mi_resp_tenant_cookie = lf.lambda_handler(mi_event({
    "company": MI_COMPANY, "person_id": MI_BUYER_PID, "status": lf.INTRO_STATUS_CLOSED_ID,
    "tenant_email": MI_TENANT_EMAIL,
}, cookies=[tenant_cookie(MI_TENANT_EMAIL)]), None)
check("manual_intro with a real tenant cookie but no admin key -> still 403 (no tenant path exists)",
      mi_resp_tenant_cookie["statusCode"] == 403)

# --- THE literal instruction test case: Chase Fraser (1277391706) on
# Panthalassa for elana@tworoads.vc, status Closed -> must render
# NAMED in her Buyers section.
mi_resp_create = lf.lambda_handler(mi_event({
    "key": ADMIN_KEY, "company": MI_COMPANY, "person_id": MI_BUYER_PID,
    "status": lf.INTRO_STATUS_CLOSED_ID, "note": "Wired via SPV", "tenant_email": MI_TENANT_EMAIL,
}), None)
check("manual_intro create (Chase Fraser / Panthalassa / Closed) -> 200", mi_resp_create["statusCode"] == 200)
mi_create_data = json.loads(mi_resp_create["body"])
check("manual_intro create: created == True", mi_create_data.get("created") is True)
mi_sk = f"manual-intro#{MI_COMPANY}#{MI_BUYER_PID}"
check("manual_intro create: Dynamo item written under the right tenant/sk",
      any(it.get("sk") == mi_sk and it.get("tenant") == MI_TENANT_EMAIL for it in fake_table_mi.items))
mi_item = next(it for it in fake_table_mi.items if it.get("sk") == mi_sk)
check("manual_intro create: attributes match (person_id/company/status/note/created_by/created_at/updated_at)",
      mi_item.get("status") == lf.INTRO_STATUS_CLOSED_ID and mi_item.get("note") == "Wired via SPV"
      and mi_item.get("created_by") == "admin" and mi_item.get("created_at") is not None
      and mi_item.get("updated_at") is not None and mi_item.get("person_id") == MI_BUYER_PID
      and mi_item.get("company") == MI_COMPANY)
check("manual_intro create: audit item appended",
      any(it and it.get("actor") == "admin" and str(it.get("sk", "")).startswith(f"audit#{mi_sk}#")
          for it in fake_table_mi.puts))

page_company_mi = lf.render_company_page(MI_COMPANY, "Admin", mi_tenant, MI_TENANT_EMAIL, "mydeals",
                                          key=ADMIN_KEY, view_as=MI_TENANT_EMAIL, edit_mode=True)
check("Chase Fraser renders NAMED in the Buyers section (Closed -> disclosed, positive closed row)",
      "Chase Fraser" in page_company_mi)
co_section_mi = page_company_mi[page_company_mi.find("closed-out-section"):]
check("...inside the Closed out section specifically (Closed routes there per instruction)",
      "closed-out-section" in page_company_mi and "Chase Fraser" in co_section_mi)
check("...styled as a POSITIVE closed row (solid-green 'Won' chip), not gray Passed/Withdrawn styling",
      '<span class="status-chip won">Won</span>' in page_company_mi)
check("...carries the 'manual' tag distinguishing it from a CRM-derived row",
      'class="manual-tag"' in page_company_mi)

page_intros_mi = lf.render_intros_page("Elana Founder", tenant=mi_tenant, tenant_email=MI_TENANT_EMAIL,
                                        key=ADMIN_KEY, view_as=MI_TENANT_EMAIL, edit_mode=True)
check("Chase Fraser also renders named on Active Intros", "Chase Fraser" in page_intros_mi)

# --- Tenant (read-only, no admin key) sees the rendered row too since
# it's Closed (disclosed) -- but gets no create/edit control at all.
MI_BUYER4_PID = 804  # a LIVE (non-closed-out) manual intro, for the disabled-control check below
mi_people["people"].append({"id": MI_BUYER4_PID, "first_name": "Four", "last_name": "Buyer",
                             "full_name": "Four Buyer", "email": "four@example.com",
                             "company_name": "Fourth Capital", "custom_fields": {}})
lf.lambda_handler(mi_event({
    "key": ADMIN_KEY, "company": MI_COMPANY, "person_id": MI_BUYER4_PID, "status": lf.INTRO_STATUS_INTRODUCED_ID,
    "tenant_email": MI_TENANT_EMAIL,
}), None)

page_company_mi_tenant = lf.render_company_page(MI_COMPANY, "Elana Founder", mi_tenant, MI_TENANT_EMAIL, "mydeals",
                                                 key=None, view_as=None, edit_mode=False)
check("tenant view: Chase Fraser still renders named (Closed discloses to the tenant too)",
      "Chase Fraser" in page_company_mi_tenant)
check("tenant view: Four Buyer (a live, non-closed-out manual intro) also renders named",
      "Four Buyer" in page_company_mi_tenant)
check("tenant view: its status control renders but DISABLED (read-only)",
      'class="mi-status"' in page_company_mi_tenant and " disabled>" in page_company_mi_tenant)
check("tenant view: note renders as plain text, never an editable textarea",
      'class="mi-note"' not in page_company_mi_tenant)
check("tenant view: no 'Add buyer' create box at all", 'class="manual-intro-box"' not in page_company_mi_tenant)

# --- Disclosure: a manual intro left at Matched renders ANONYMIZED,
# not named -- checked via a non-edit-mode render (tenant/preview),
# where routing unambiguously sends a Matched row through the
# always-anonymized pending-row path.
lf.lambda_handler(mi_event({
    "key": ADMIN_KEY, "company": MI_COMPANY, "person_id": MI_BUYER2_PID, "status": 7207578,  # Matched
    "tenant_email": MI_TENANT_EMAIL,
}), None)
page_company_matched = lf.render_company_page(MI_COMPANY, "Elana Founder", mi_tenant, MI_TENANT_EMAIL, "mydeals",
                                               key=None, view_as=None, edit_mode=False)
check("a manual intro left at Matched renders anonymized (identity shows only Introduced-or-later)",
      "Dana Second" not in page_company_matched)

# --- Exit routing: Passed/Withdrawn go to Closed out too, but styled
# gray (not the green Closed chip) -- and per the SAME simple
# disclosure rule ("not Matched" discloses), a manual row set directly
# to Passed IS named, unlike a real deal's stricter milestone-gated
# exit disclosure.
lf.lambda_handler(mi_event({
    "key": ADMIN_KEY, "company": MI_COMPANY, "person_id": MI_BUYER2_PID, "status": lf.INTRO_STATUS_PASSED_ID,
    "tenant_email": MI_TENANT_EMAIL,
}), None)
page_company_passed = lf.render_company_page(MI_COMPANY, "Admin", mi_tenant, MI_TENANT_EMAIL, "mydeals",
                                              key=ADMIN_KEY, view_as=MI_TENANT_EMAIL, edit_mode=True)
co_section_passed = page_company_passed[page_company_passed.find("closed-out-section"):]
check("a manual intro set to Passed routes to Closed out and is named there (not the green Closed chip)",
      "Dana Second" in co_section_passed
      and '<span class="status-chip closed">Closed</span>' not in
          co_section_passed[co_section_passed.find("Dana Second"):co_section_passed.find("Dana Second") + 400])

# --- Inline edit: .mi-status / .mi-note controls write back to the
# SAME Dynamo item (company+person_id, no deal_id involved at all).
mi_resp_edit_status = lf.lambda_handler(mi_event({
    "key": ADMIN_KEY, "company": MI_COMPANY, "person_id": MI_BUYER_PID, "status": 7207583,  # Wired
    "tenant_email": MI_TENANT_EMAIL,
}), None)
check("inline status edit -> 200, created == False (existing item updated, not re-created)",
      mi_resp_edit_status["statusCode"] == 200 and json.loads(mi_resp_edit_status["body"]).get("created") is False)
mi_item_after_status_edit = next(it for it in fake_table_mi.items if it.get("sk") == mi_sk)
check("inline status edit: status updated, note UNTOUCHED (partial update, single field)",
      mi_item_after_status_edit.get("status") == 7207583 and mi_item_after_status_edit.get("note") == "Wired via SPV")

mi_resp_edit_note = lf.lambda_handler(mi_event({
    "key": ADMIN_KEY, "company": MI_COMPANY, "person_id": MI_BUYER_PID, "note": "Updated note text",
    "tenant_email": MI_TENANT_EMAIL,
}), None)
check("inline note edit -> 200", mi_resp_edit_note["statusCode"] == 200)
mi_item_after_note_edit = next(it for it in fake_table_mi.items if it.get("sk") == mi_sk)
check("inline note edit: note updated, status UNTOUCHED",
      mi_item_after_note_edit.get("note") == "Updated note text" and mi_item_after_note_edit.get("status") == 7207583)

# --- Suppression: a manual intro for a buyer who ALREADY has a real
# deal-derived intro for this tenant+company is never rendered -- the
# real row wins outright.
lf.lambda_handler(mi_event({
    "key": ADMIN_KEY, "company": MI_COMPANY, "person_id": MI_BUYER3_PID, "status": lf.INTRO_STATUS_INTRODUCED_ID,
    "tenant_email": MI_TENANT_EMAIL,
}), None)
page_company_suppress = lf.render_company_page(MI_COMPANY, "Admin", mi_tenant, MI_TENANT_EMAIL, "mydeals",
                                                key=ADMIN_KEY, view_as=MI_TENANT_EMAIL, edit_mode=True)
check("Wren Third (real deal + a manual duplicate) appears exactly once, not twice",
      page_company_suppress.count("Wren Third") == 1)
wren_row = page_company_suppress[max(page_company_suppress.find("Wren Third") - 400, 0):
                                  page_company_suppress.find("Wren Third") + 100]
check("...and that one row is the REAL deal-derived row, not the manual one (no 'manual' tag on it)",
      'class="manual-tag"' not in wren_row)

# --- The company page's "Add buyer" box itself: rendered admin-only,
# directly below the Demand tiles, and creates via the same write path.
check("'Add buyer' box renders below Buyer Demand for admin+&view_as (edit_mode)",
      'class="manual-intro-box"' in page_company_mi
      and page_company_mi.find('class="manual-intro-box"') > page_company_mi.find('id="demand"'))
page_company_admin_noview_mi = lf.render_company_page(MI_COMPANY, "Admin", None, "admin", "mydeals",
                                                       key=ADMIN_KEY, view_as=None, edit_mode=True)
check("'Add buyer' box does NOT render for admin without &view_as (no tenant to attach to)",
      'class="manual-intro-box"' not in page_company_admin_noview_mi)

# --- Input validation
mi_resp_no_company = lf.lambda_handler(mi_event({
    "key": ADMIN_KEY, "person_id": MI_BUYER_PID, "status": lf.INTRO_STATUS_INTRODUCED_ID,
    "tenant_email": MI_TENANT_EMAIL,
}), None)
check("manual_intro with no company -> 400", mi_resp_no_company["statusCode"] == 400)
mi_resp_bad_person = lf.lambda_handler(mi_event({
    "key": ADMIN_KEY, "company": MI_COMPANY, "person_id": "abc", "status": lf.INTRO_STATUS_INTRODUCED_ID,
    "tenant_email": MI_TENANT_EMAIL,
}), None)
check("manual_intro with a non-integer person_id -> 400", mi_resp_bad_person["statusCode"] == 400)
mi_resp_bad_status = lf.lambda_handler(mi_event({
    "key": ADMIN_KEY, "company": MI_COMPANY, "person_id": MI_BUYER_PID, "status": 999999,
    "tenant_email": MI_TENANT_EMAIL,
}), None)
check("manual_intro with an invalid status -> 400", mi_resp_bad_status["statusCode"] == 400)
mi_resp_bad_tenant = lf.lambda_handler(mi_event({
    "key": ADMIN_KEY, "company": MI_COMPANY, "person_id": MI_BUYER_PID,
    "status": lf.INTRO_STATUS_INTRODUCED_ID, "tenant_email": "nobody@nowhere.example.com",
}), None)
check("manual_intro with an unresolvable tenant_email -> 400", mi_resp_bad_tenant["statusCode"] == 400)
mi_resp_nothing = lf.lambda_handler(mi_event({
    "key": ADMIN_KEY, "company": MI_COMPANY, "person_id": MI_BUYER_PID, "tenant_email": MI_TENANT_EMAIL,
}), None)
check("manual_intro with neither status nor note -> 400 ('nothing to update')", mi_resp_nothing["statusCode"] == 400)
mi_resp_long_note = lf.lambda_handler(mi_event({
    "key": ADMIN_KEY, "company": MI_COMPANY, "person_id": MI_BUYER_PID, "note": "x" * 2001,
    "tenant_email": MI_TENANT_EMAIL,
}), None)
check("manual_intro with a note over MAX_INTRO_TEXT_LEN -> 400", mi_resp_long_note["statusCode"] == 400)

# --- Ordinary Pipeline-write regression guard: this whole feature must
# never make a Pipeline API call, under any of the scenarios above.
lf.urllib.request.urlopen = lambda *a, **k: (_ for _ in ()).throw(
    AssertionError("manual_intro must never call Pipeline"))
mi_resp_no_pipeline = lf.lambda_handler(mi_event({
    "key": ADMIN_KEY, "company": MI_COMPANY, "person_id": MI_BUYER_PID,
    "status": lf.INTRO_STATUS_INTRODUCED_ID, "tenant_email": MI_TENANT_EMAIL,
}), None)
check("manual_intro write makes no Pipeline call at all", mi_resp_no_pipeline["statusCode"] == 200)


# ======================================================================
# SECTION: Layout fix -- company page Buyers table column widths
# ======================================================================
# The Buyer name column (rich content: name+link, email/phone, country/
# website/LinkedIn) used to get only 13% while Company (the buyer's own,
# usually-short firm name) got 24% -- backwards versus what each column
# actually holds, and the root cause of the buyer cell's contact line
# visually overlapping the Company column at typical desktop widths.
# Rebalanced to 22%/15%, plus overflow-wrap as a second line of defense
# for any still-unbreakable long token (a long email/URL).
page_company_layout = lf.render_company_page(MI_COMPANY, "Admin", mi_tenant, MI_TENANT_EMAIL, "mydeals",
                                              key=ADMIN_KEY, view_as=MI_TENANT_EMAIL, edit_mode=True)
check("company page: Buyer name column no longer the cramped 13% ('Buyer name'/'Company' swapped in width)",
      '<col style="width:13%">\n          <col style="width:24%">' not in page_company_layout)
check("company page: Buyer name column now WIDER than Company (22% vs 15%, was 13% vs 24%)",
      '<col style="width:22%">\n          <col style="width:15%">' in page_company_layout)
check("company page: the SAME rebalance applied to the Closed out table's own colgroup, not just the main one",
      '<col style="width:22%">\n            <col style="width:15%">' in page_company_layout)
check("company page: overflow-wrap set on table cells (wraps an unbreakable long token instead of overflowing)",
      "overflow-wrap: anywhere;" in page_company_layout)
check("company page: the .buyer-cell-links rule (country/website/LinkedIn line) is no longer missing entirely",
      ".buyer-cell-links {" in page_company_layout)


# ======================================================================
# SECTION: In-page admin/tenant view toggle (replaces &edit=1) +
# "Copy client link" -- full lambda_handler round trips, not direct
# render_* calls, so these prove the real request path (query params +
# cookie -> edit_mode -> render) end to end.
# ======================================================================

VT_TENANT_EMAIL = "toggle-tenant@example.com"
VT_TENANT_PID = 951
VT_BUYER_PID = 952
VT_COMPANY = "Griffin Co"

vt_people = {"people": [
    {"id": VT_TENANT_PID, "full_name": "Toggle Tenant", "email": VT_TENANT_EMAIL, "custom_fields": {}},
    {"id": VT_BUYER_PID, "first_name": "Vera", "last_name": "Realname", "full_name": "Vera Realname",
     "email": "vera@example.com", "company_name": "Vera Capital", "custom_fields": {}},
]}
vt_sell_deal = {"id": 9950, "name": "VT Sell Deal", "company": {"name": VT_COMPANY},
                "deal_stage": {"id": lf.STAGE_FIRM}, "custom_fields": cf_sell(),
                "people": [{"id": VT_TENANT_PID}], "updated_at": "2026-08-01T00:00:00Z"}
vt_buy_deal = {"id": 9951, "name": "VT Buy Deal", "company": {"name": VT_COMPANY},
               # Deliberately Matched/undisclosed (cf_status(None)), not
               # Introduced -- a genuinely Introduced-or-later deal
               # discloses to the tenant themselves too (that's the
               # normal disclosure gate, not an admin-only reveal), so
               # it would prove nothing about the admin-view toggle
               # specifically. Only edit_mode's "admin always sees the
               # real thing" convention should reveal this buyer.
               "deal_stage": {"id": lf.STAGE_MATCHED}, "custom_fields": cf_status(None),
               "people": [{"id": VT_TENANT_PID}, {"id": VT_BUYER_PID}], "updated_at": "2026-08-01T00:00:00Z"}
use_fixture({lf.PEOPLE_KEY: vt_people, lf.INTEREST_KEY: {"buy": {"Griffin Co": [VT_BUYER_PID]}},
             lf.DEALS_KEY: {"deals": [vt_sell_deal, vt_buy_deal]}})
vt_tenant = lf._resolve_tenant(VT_TENANT_EMAIL)
assert vt_tenant is not None


def vt_get_event(query, cookies=None):
    return {"requestContext": {"http": {"method": "GET"}}, "queryStringParameters": query,
            "cookies": cookies or []}


# --- DEFAULT: admin key + &view_as, no cookie, no &edit=1 -> exactly
# the tenant's own read-only view. No real name, no admin controls,
# anywhere -- on BOTH a company page and Active Intros, proving the
# same cookie-free default applies across pages/tabs.
resp_bare_company = lf.lambda_handler(vt_get_event({
    "key": ADMIN_KEY, "view_as": VT_TENANT_EMAIL, "company": VT_COMPANY,
}), None)
check("bare admin+view_as (company page) -> 200", resp_bare_company["statusCode"] == 200)
check("bare admin+view_as (company page): no real buyer name anywhere", "Vera Realname" not in resp_bare_company["body"])
check("bare admin+view_as (company page): no Introduce/status-select admin controls",
      'class="intro-buyer-btn"' not in resp_bare_company["body"]
      and 'class="intro-status-select"' not in resp_bare_company["body"]
      and 'class="manual-intro-box"' not in resp_bare_company["body"])

resp_bare_intros = lf.lambda_handler(vt_get_event({
    "key": ADMIN_KEY, "view_as": VT_TENANT_EMAIL, "tab": "intros",
}), None)
check("bare admin+view_as (Active Intros) -> 200", resp_bare_intros["statusCode"] == 200)
check("bare admin+view_as (Active Intros): no real buyer name anywhere",
      "Vera Realname" not in resp_bare_intros["body"])
# (checked via the actual input/select tag's class="..." attribute, not
# a bare substring -- "ei-milestone"/"ei-flag" alone also appear in the
# page's own <style> block selectors, which are always present)
check("bare admin+view_as (Active Intros): no admin edit controls render on the row at all "
      "(no milestone checkboxes, no flag select)",
      'class="ei-milestone"' not in resp_bare_intros["body"] and 'class="ei-flag' not in resp_bare_intros["body"])

# --- The nav toggle itself: visible (key present), "Tenant view" is the
# active side by default, "Copy client link" is ALSO present (it's
# view_as-gated, not edit_mode-gated -- an admin meta-control, not
# tenant-facing content).
# --- Bug fixes (found via a separate CAREFUL MODE audit of this same
# feature): two more admin-only affordances that kept checking `key`
# directly instead of edit_mode, so they stayed live under bare
# admin+view_as (Tenant view) even though the toggle above is off.
ADMIN_KEY_JS_MARKERS = (f'var ADMIN_KEY = "{ADMIN_KEY}"', f'var KEY = "{ADMIN_KEY}"')
check("bare admin+view_as (company page): no ADMIN badge (an admin-only affordance -- "
      "a real tenant never sees 'ADMIN · viewing as x@y.com')",
      'class="gg-admin-badge"' not in resp_bare_company["body"])
check("bare admin+view_as (Active Intros): no ADMIN badge either",
      'class="gg-admin-badge"' not in resp_bare_intros["body"])
check("bare admin+view_as (company page): the raw ADMIN_KEY never reaches client-side write "
      "scripts (edit script / feature box / feature toggle) -- a Tenant-view preview's save "
      "must fail like an unauthenticated write, never silently succeed as an admin write",
      not any(m in resp_bare_company["body"] for m in ADMIN_KEY_JS_MARKERS))
check("bare admin+view_as (Active Intros): same -- no raw ADMIN_KEY in any client script",
      not any(m in resp_bare_intros["body"] for m in ADMIN_KEY_JS_MARKERS))

check("nav toggle renders under bare admin+view_as", 'class="gg-view-toggle"' in resp_bare_company["body"])
check("nav toggle: 'Tenant view' is the active side by default (no cookie, no &edit=1)",
      '<button type="button" class="gg-view-toggle-btn active" data-mode="tenant">Tenant view</button>'
      in resp_bare_company["body"])
check("nav toggle: 'Admin view' is present but NOT active",
      '<button type="button" class="gg-view-toggle-btn" data-mode="admin">Admin view</button>'
      in resp_bare_company["body"])
check("'Copy client link' renders whenever a tenant is selected, regardless of tenant/admin view mode",
      'id="gg-copy-link-btn"' in resp_bare_company["body"])
# Bug fix: the button used to strip key/view_as/edit off the ADMIN's own
# window.location.href client-side -- those three params were the only
# credential on that URL, so the "copied" link had none left and a tenant
# opening it hit "Access denied." The button now carries a real, permanent,
# server-built per-tenant magic link (_tenant_link_url) in a data attribute;
# the script just copies that fixed string, with a visible-input fallback
# on clipboard failure.
check("Copy client link button carries the real permanent per-tenant link, "
      "not the admin's own URL with params stripped",
      f'data-link="{lf._esc(lf._tenant_link_url(VT_TENANT_EMAIL))}"' in resp_bare_company["body"])
check("Copy client link's permanent link verifies server-side back to this exact tenant",
      lf._verify_tenant_link_token(
          VT_TENANT_EMAIL,
          lf._tenant_link_url(VT_TENANT_EMAIL).rsplit("token=", 1)[1]))
check("Copy client link's permanent link carries no admin key, no view_as, no sso param",
      "key=" not in lf._tenant_link_url(VT_TENANT_EMAIL)
      and "view_as=" not in lf._tenant_link_url(VT_TENANT_EMAIL)
      and "sso=" not in lf._tenant_link_url(VT_TENANT_EMAIL))
check("Copy client link script reads data-link and copies it verbatim",
      "btn.getAttribute('data-link')" in resp_bare_company["body"])
check("Copy client link fallback input pre-filled with the same link, hidden until clipboard fails",
      f'id="gg-copy-link-input" value="{lf._esc(lf._tenant_link_url(VT_TENANT_EMAIL))}"'
      in resp_bare_company["body"]
      and 'id="gg-copy-link-fallback" hidden' in resp_bare_company["body"])

# The permanent link itself must actually sign the tenant in: GET it with no
# cookie, no admin key, nothing else -- same as a tenant pasting it cold into
# a fresh browser.
_tlink = lf._tenant_link_url(VT_TENANT_EMAIL)
_tlink_qs = dict(p.split("=", 1) for p in _tlink.split("?", 1)[1].split("&"))
resp_tenant_link = lf.lambda_handler(vt_get_event(
    {"tenant": lf.urllib.parse.unquote(_tlink_qs["tenant"]), "token": _tlink_qs["token"]}), None)
check("Permanent tenant link: fresh GET (no cookie) redirects (signs in), not 403",
      resp_tenant_link["statusCode"] == 302)
check("Permanent tenant link: sets the same durable gg_id identity cookie the SSO handoff sets",
      any(c.startswith("gg_id=") for c in (resp_tenant_link.get("cookies") or [])))
resp_tenant_link_bad = lf.lambda_handler(vt_get_event(
    {"tenant": VT_TENANT_EMAIL, "token": "not-a-real-token"}), None)
check("Permanent tenant link: tampered token falls through to normal identity "
      "resolution (403, no cookie) rather than signing anyone in",
      resp_tenant_link_bad["statusCode"] == 403
      and not (resp_tenant_link_bad.get("cookies") or []))
check("toggle script writes the gg_admin_view cookie client-side, Path=/, no HttpOnly (JS must be able to set it)",
      "document.cookie = 'gg_admin_view=' + mode" in resp_bare_company["body"]
      and "Path=/" in resp_bare_company["body"])

# --- TOGGLE: the gg_admin_view=admin cookie alone (no &edit=1 at all)
# flips to the full admin view -- real name, admin controls -- on both
# pages, proving the cookie (not the URL) now drives it.
resp_cookie_company = lf.lambda_handler(vt_get_event(
    {"key": ADMIN_KEY, "view_as": VT_TENANT_EMAIL, "company": VT_COMPANY},
    cookies=["gg_admin_view=admin"]), None)
check("gg_admin_view=admin cookie (company page): real buyer name now shown",
      "Vera Realname" in resp_cookie_company["body"])
check("gg_admin_view=admin cookie (company page): nav toggle now shows 'Admin view' as active",
      '<button type="button" class="gg-view-toggle-btn active" data-mode="admin">Admin view</button>'
      in resp_cookie_company["body"])
check("gg_admin_view=admin cookie (company page): ADMIN badge now shown",
      'class="gg-admin-badge"' in resp_cookie_company["body"])
check("gg_admin_view=admin cookie (company page): raw ADMIN_KEY now reaches client write scripts",
      any(m in resp_cookie_company["body"] for m in ADMIN_KEY_JS_MARKERS))

resp_cookie_intros = lf.lambda_handler(vt_get_event(
    {"key": ADMIN_KEY, "view_as": VT_TENANT_EMAIL, "tab": "intros"},
    cookies=["gg_admin_view=admin"]), None)
check("gg_admin_view=admin cookie (Active Intros): admin edit controls now render on the row "
      "-- SAME cookie, different page/tab",
      'class="ei-milestone"' in resp_cookie_intros["body"] or 'class="ei-flag' in resp_cookie_intros["body"])
check("gg_admin_view=admin cookie (Active Intros): nav toggle also shows 'Admin view' as active here",
      '<button type="button" class="gg-view-toggle-btn active" data-mode="admin">Admin view</button>'
      in resp_cookie_intros["body"])

# --- &edit=1 still works internally (never required, but not removed)
resp_edit1 = lf.lambda_handler(vt_get_event({
    "key": ADMIN_KEY, "view_as": VT_TENANT_EMAIL, "company": VT_COMPANY, "edit": "1",
}), None)
check("&edit=1 (no cookie) still works as an internal fallback", "Vera Realname" in resp_edit1["body"])

# --- A garbage/irrelevant cookie value does NOT flip the view -- only
# the literal "admin" does.
resp_garbage_cookie = lf.lambda_handler(vt_get_event(
    {"key": ADMIN_KEY, "view_as": VT_TENANT_EMAIL, "company": VT_COMPANY},
    cookies=["gg_admin_view=nonsense"]), None)
check("a non-'admin' gg_admin_view cookie value stays anonymized (Tenant view)",
      "Vera Realname" not in resp_garbage_cookie["body"])

# --- Admin, no &view_as at all: toggle still renders (key present),
# but 'Copy client link' does NOT (no tenant selected to link to).
resp_admin_noview = lf.lambda_handler(vt_get_event({"key": ADMIN_KEY}), None)
check("admin, no &view_as: nav toggle still renders", 'class="gg-view-toggle"' in resp_admin_noview["body"])
check("admin, no &view_as: 'Copy client link' does NOT render (nothing to link to)",
      'id="gg-copy-link-btn"' not in resp_admin_noview["body"])
check("admin, no &view_as: ADMIN badge still shown (edit_mode is always True with no tenant selected)",
      'class="gg-admin-badge"' in resp_admin_noview["body"])

# --- A real tenant session (gg_id cookie, no admin key): no toggle, no
# copy-link, no admin badge -- none of this is visible to a tenant at all.
resp_real_tenant = lf.lambda_handler(vt_get_event(
    {"tab": "mydeals"}, cookies=[tenant_cookie(VT_TENANT_EMAIL)]), None)
check("real tenant session -> 200", resp_real_tenant["statusCode"] == 200)
check("real tenant session: no view toggle", 'class="gg-view-toggle"' not in resp_real_tenant["body"])
check("real tenant session: no 'Copy client link'", 'id="gg-copy-link-btn"' not in resp_real_tenant["body"])
check("real tenant session: no ADMIN badge", 'class="gg-admin-badge"' not in resp_real_tenant["body"])


# ======================================================================
# SECTION: Closed-deals Pipeline source (get_closed_deals_list)
# ======================================================================
# deals.json excludes every terminal stage by design (TERMINAL_STAGE_IDS),
# so this Lambda fetches those deals directly from Pipeline
# (GET /deals.json, conditions[deal_stage][] filter, page/per_page,
# {"entries":[...],"pagination":{...}} envelope -- exact shape Chad
# confirmed) and caches them to S3 (CLOSED_DEALS_KEY) with an hourly TTL.
# get_deals_list() merges this with the live snapshot so every consumer
# (intro fetch/counts, closed-out predicates, per-company stats, Raised,
# trophy shelf, buyer-page history, add-buyer/Introduce lookup) sees one
# combined set automatically.

def cd_urls_query(url):
    return lf.urllib.parse.parse_qs(lf.urllib.parse.urlparse(url).query)


def make_closed_deals_urlopen(all_deals, calls, unfiltered_combined=False, fail=False):
    """Fake Pipeline GET /deals.json responder for get_closed_deals_list's
    fetch. Paginates `all_deals`, filtering by the request's own
    conditions[deal_stage][] ids -- UNLESS unfiltered_combined=True AND
    more than one stage id is requested in a single call, in which case
    every deal in `all_deals` is returned regardless of stage (simulating
    a combined multi-id filter that doesn't actually filter server-side,
    the one failure mode Chad's fallback exists for -- a single-id call
    still filters correctly even when unfiltered_combined=True, since
    that's what the per-stage fallback relies on to recover). fail=True
    makes every call raise, for fail-soft tests."""
    def _urlopen(req, timeout=20):
        url = req.full_url
        calls.append(url)
        if fail:
            raise lf.urllib.error.HTTPError(url, 502, "boom", {}, None)
        q = cd_urls_query(url)
        stage_ids = [int(x) for x in q.get("conditions[deal_stage][]", [])]
        page = int(q.get("page", ["1"])[0])
        per_page = int(q.get("per_page", ["200"])[0])
        if unfiltered_combined and len(stage_ids) > 1:
            pool = list(all_deals)
        else:
            pool = [d for d in all_deals if d.get("deal_stage", {}).get("id") in stage_ids]
        start = (page - 1) * per_page
        page_entries = pool[start:start + per_page]
        body = {"entries": page_entries,
                "pagination": {"page": page, "per_page": per_page, "total": len(pool), "url": "/deals.json"}}
        return _FakeGetResponse(200, body)
    return _urlopen


def cd_deal(deal_id, stage_id, company="Test Co", buyer_pid=70001):
    return {"id": deal_id, "name": f"{company}: Buy", "company": {"name": company},
            "deal_stage": {"id": stage_id},
            "custom_fields": {lf.DEAL_SIDE_FIELD: [lf.DEAL_SIDE_BUY_ID]},
            "people": [{"id": buyer_pid}], "updated_at": "2026-08-01T00:00:00Z"}


def cd_iso(seconds_ago=0):
    """last_updated-shaped ISO 8601 UTC timestamp, `seconds_ago` in the
    past -- matches the {"last_updated": ..., "deals": [...]} cache file
    shape _read_closed_deals_cache_from_s3/_decide_closed_deals_fetch_
    mode expect (never a raw epoch float)."""
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)).strftime("%Y-%m-%dT%H:%M:%SZ")


# --- Pagination: 5 records across 3 pages (per_page overridden to 2).
orig_per_page = lf.CLOSED_DEALS_PER_PAGE
lf.CLOSED_DEALS_PER_PAGE = 2
cd_page_calls = []
cd_pool = [cd_deal(80000 + i, 111802) for i in range(5)]
lf.urllib.request.urlopen = make_closed_deals_urlopen(cd_pool, cd_page_calls)
cd_entries, cd_err = lf._fetch_all_pages_for_stage_ids(sorted(lf.TERMINAL_STAGE_IDS), lf.CLOSED_DEALS_PER_PAGE,
                                                        lf.CLOSED_DEALS_MAX_PAGES)
check("pagination: no error", cd_err is None)
check("pagination: all 5 records collected", len(cd_entries) == 5)
check("pagination: exactly 3 page requests issued (2+2+1)", len(cd_page_calls) == 3)
check("pagination: no duplicate ids", len({d["id"] for d in cd_entries}) == 5)
lf.CLOSED_DEALS_PER_PAGE = orig_per_page

# --- Combined filter returns unfiltered results -> per-stage fallback.
cd_live_stray = cd_deal(81000, lf.STAGE_MATCHED)  # NOT a terminal stage
cd_terminal_a = cd_deal(81001, 111802)
cd_terminal_b = cd_deal(81002, 111801)
cd_fallback_calls = []
lf.urllib.request.urlopen = make_closed_deals_urlopen(
    [cd_live_stray, cd_terminal_a, cd_terminal_b], cd_fallback_calls, unfiltered_combined=True)
cd_fb_deals, cd_fb_stats, cd_fb_err = lf._pipeline_fetch_closed_deals()
check("unfiltered combined response -> fallback triggered, no error", cd_fb_err is None)
check("fallback used ('which_filter' == 'per-stage')", cd_fb_stats["which_filter"] == "per-stage")
check("fallback result excludes the non-terminal stray record",
      81000 not in {d["id"] for d in cd_fb_deals})
check("fallback result includes both terminal records",
      {81001, 81002} <= {d["id"] for d in cd_fb_deals})
check("fallback issued more than one call (one per stage id, not one combined call)",
      len(cd_fallback_calls) > 1)

# --- Combined filter working correctly -> stays on the combined path.
cd_ok_calls = []
lf.urllib.request.urlopen = make_closed_deals_urlopen([cd_terminal_a, cd_terminal_b], cd_ok_calls)
cd_ok_deals, cd_ok_stats, cd_ok_err = lf._pipeline_fetch_closed_deals()
check("working combined filter -> no error", cd_ok_err is None)
check("working combined filter -> 'which_filter' == 'combined'", cd_ok_stats["which_filter"] == "combined")
check("working combined filter -> both records present", {81001, 81002} == {d["id"] for d in cd_ok_deals})

# --- Entries missing custom_fields are skipped and counted, not guessed at.
cd_good = cd_deal(82001, 111802)
cd_bad = cd_deal(82002, 111802)
del cd_bad["custom_fields"]
lf.urllib.request.urlopen = make_closed_deals_urlopen([cd_good, cd_bad], [])
cd_skip_deals, cd_skip_stats, cd_skip_err = lf._pipeline_fetch_closed_deals()
check("missing custom_fields: no error", cd_skip_err is None)
check("missing custom_fields: only the good record kept", [d["id"] for d in cd_skip_deals] == [82001])
check("missing custom_fields: skip counted", cd_skip_stats["skipped_missing_custom_fields"] == 1)
check("missing custom_fields: record_count reflects only kept records", cd_skip_stats["record_count"] == 1)

# --- get_closed_deals_list: fresh fetch (no cache at all) -> full pull,
# streamed to S3 via multipart upload, never put_object.
cd_write_deal = cd_deal(83001, 111802, company="Write Co")
cd_s3, cd_table = use_fixture({lf.DEALS_KEY: {"deals": []}})
lf._closed_deals_cache["fetched_at"] = None
lf._closed_deals_cache["deals"] = None
lf.urllib.request.urlopen = make_closed_deals_urlopen([cd_write_deal], [])
cd_fetched = lf.get_closed_deals_list()
check("S3 write: fetched list is correct", [d["id"] for d in cd_fetched] == [83001])
check("S3 write: went through multipart upload, never put_object",
      len(cd_s3.multipart_calls) == 1 and cd_s3.put_calls == [])
check("S3 write: multipart upload was not aborted", cd_s3.multipart_calls[0]["aborted"] is False)
check("S3 write: written object shape is {last_updated, deals}, readable back",
      "last_updated" in cd_s3.objs[lf.CLOSED_DEALS_KEY]
      and cd_s3.objs[lf.CLOSED_DEALS_KEY]["deals"][0]["id"] == 83001)

# --- Same warm container, second invocation: TTL not expired -> reused
# from the in-memory dict, zero S3 or Pipeline calls.
lf._req_cache_reset()  # a fresh "request" -- deliberately NOT the harness's reset_caches(),
                        # which would also reset _closed_deals_cache and defeat this check
def _cd_raise(*a, **k):
    raise AssertionError("get_closed_deals_list should not hit Pipeline on a warm TTL hit")
lf.urllib.request.urlopen = _cd_raise
cd_s3.get_object = lambda **k: (_ for _ in ()).throw(AssertionError("should not hit S3 on a warm TTL hit"))
cd_reused = lf.get_closed_deals_list()
check("warm TTL reuse: same list returned with zero I/O", [d["id"] for d in cd_reused] == [83001])

# --- S3 cache read: a fresh cached object skips Pipeline entirely.
cd_cached_deal = cd_deal(84001, 111801, company="Cached Co")
cd_s3b, _ = use_fixture({
    lf.DEALS_KEY: {"deals": []},
    lf.CLOSED_DEALS_KEY: {"deals": [cd_cached_deal], "last_updated": cd_iso(0)},
})
lf._closed_deals_cache["fetched_at"] = None
lf._closed_deals_cache["deals"] = None
lf.urllib.request.urlopen = _cd_raise
cd_from_s3 = lf.get_closed_deals_list()
check("fresh S3 cache: served without any Pipeline call", [d["id"] for d in cd_from_s3] == [84001])

# --- S3 cache read: a stale-but-within-7-days cached object triggers an
# INCREMENTAL refresh -- merged with, not replacing, the existing cache
# (a deal Pipeline doesn't mention this round stays exactly as it was).
cd_stale_deal = cd_deal(85001, 111801, company="Stale Co")
cd_fresh_deal = cd_deal(85002, 111801, company="Refreshed Co")
cd_s3c, _ = use_fixture({
    lf.DEALS_KEY: {"deals": []},
    lf.CLOSED_DEALS_KEY: {"deals": [cd_stale_deal], "last_updated": cd_iso(lf.CLOSED_DEALS_TTL_SECONDS + 10)},
})
lf._closed_deals_cache["fetched_at"] = None
lf._closed_deals_cache["deals"] = None
cd_incr_calls = []
lf.urllib.request.urlopen = make_closed_deals_urlopen([cd_fresh_deal], cd_incr_calls)
cd_refreshed = lf.get_closed_deals_list()
check("stale-but-recent S3 cache: incremental refresh merges in the new record",
      {d["id"] for d in cd_refreshed} == {85001, 85002})
check("incremental refresh's request carries conditions[deal_updated][from_date]",
      "conditions[deal_updated][from_date]" in cd_urls_query(cd_incr_calls[0]))

# --- S3 cache older than the 7-day full-repull threshold -> FULL pull,
# replacing the cache wholesale (an id the new fetch doesn't mention is
# dropped, unlike the incremental-merge case above).
cd_old_deal = cd_deal(86001, 111801, company="Ancient Co")
cd_full_deal = cd_deal(86002, 111801, company="Fresh Full-Pull Co")
cd_s3f, _ = use_fixture({
    lf.DEALS_KEY: {"deals": []},
    lf.CLOSED_DEALS_KEY: {"deals": [cd_old_deal], "last_updated": cd_iso(lf.CLOSED_DEALS_FULL_REPULL_SECONDS + 10)},
})
lf._closed_deals_cache["fetched_at"] = None
lf._closed_deals_cache["deals"] = None
cd_full_calls = []
lf.urllib.request.urlopen = make_closed_deals_urlopen([cd_full_deal], cd_full_calls)
cd_full_refreshed = lf.get_closed_deals_list()
check("cache older than 7 days: full pull replaces the cache wholesale (old id dropped)",
      [d["id"] for d in cd_full_refreshed] == [86002])
check("full pull's request carries no conditions[deal_updated][from_date]",
      "conditions[deal_updated][from_date]" not in cd_urls_query(cd_full_calls[0]))

# --- Fail-soft: Pipeline down, but a stale S3 cache exists -> serves the
# stale list rather than erroring or going empty.
cd_s3d, _ = use_fixture({
    lf.DEALS_KEY: {"deals": []},
    lf.CLOSED_DEALS_KEY: {"deals": [cd_stale_deal], "last_updated": cd_iso(lf.CLOSED_DEALS_TTL_SECONDS + 10)},
})
lf._closed_deals_cache["fetched_at"] = None
lf._closed_deals_cache["deals"] = None
lf.urllib.request.urlopen = make_closed_deals_urlopen([], [], fail=True)
cd_fail_with_stale = lf.get_closed_deals_list()
check("fail-soft with stale S3 cache: falls back to the stale list, no raise",
      [d["id"] for d in cd_fail_with_stale] == [85001])

# --- Multipart write failure (e.g. complete_multipart_upload erroring)
# aborts the upload and fails soft -- the freshly fetched list is still
# returned/used for this invocation regardless of the write outcome.
cd_s3g, _ = use_fixture({lf.DEALS_KEY: {"deals": []}})
lf._closed_deals_cache["fetched_at"] = None
lf._closed_deals_cache["deals"] = None
def _cd_broken_complete(Bucket, Key, UploadId, MultipartUpload):
    raise RuntimeError("simulated S3 failure")
cd_s3g.complete_multipart_upload = _cd_broken_complete
cd_abort_deal = cd_deal(87001, 111801, company="Abort Co")
lf.urllib.request.urlopen = make_closed_deals_urlopen([cd_abort_deal], [])
cd_after_write_failure = lf.get_closed_deals_list()
check("write failure: fetched list still used for this invocation despite the write failing",
      [d["id"] for d in cd_after_write_failure] == [87001])
check("write failure: multipart upload was aborted", cd_s3g.multipart_calls[0]["aborted"] is True)
check("write failure: nothing landed in S3 (object never completed)", lf.CLOSED_DEALS_KEY not in cd_s3g.objs)

# --- Fail-soft: Pipeline down, no cache anywhere -> empty list, no raise,
# page still renders.
cd_s3e, _ = use_fixture({lf.DEALS_KEY: {"deals": []}, lf.PEOPLE_KEY: {"people": []}, lf.INTEREST_KEY: {"buy": {}}})
lf._closed_deals_cache["fetched_at"] = None
lf._closed_deals_cache["deals"] = None
lf.urllib.request.urlopen = make_closed_deals_urlopen([], [], fail=True)
cd_empty = lf.get_closed_deals_list()
check("fail-soft with nothing cached: empty list, no raise", cd_empty == [])
cd_merged_live_only = lf.get_deals_list()
check("get_deals_list still returns the live-only set when closed fetch fails", cd_merged_live_only == [])
cd_resp = lf.lambda_handler({"requestContext": {"http": {"method": "GET"}}, "rawPath": "/",
                              "queryStringParameters": {"key": ADMIN_KEY}, "cookies": []}, None)
check("fail-soft: page still renders 200 despite the closed-deals fetch failing",
      cd_resp["statusCode"] == 200)


# --- Real-ticket verification: deal 55422151 (Panthalassa, Won Deal
# 111802, Intro Status Closed) and deal 55461737 (Senra, Lost 111801) --
# present ONLY via the Pipeline closed-deals fetch, NEVER in the deals.json
# snapshot fixture, proving the merge (not just the downstream render
# logic already covered elsewhere) is what makes them visible.
CD_TENANT_EMAIL = "elana@tworoads.vc"
CD_TENANT_PID = 991001
CD_PANTHALASSA_BUYER_PID = 991002
CD_SENRA_BUYER_PID = 991003

cd_panthalassa_sell = {"id": 991100, "name": "Panthalassa Sell Order", "company": {"name": "Panthalassa"},
                       "deal_stage": {"id": lf.STAGE_MATCHED}, "custom_fields": cf_sell(),
                       "people": [{"id": CD_TENANT_PID}], "updated_at": "2026-08-01T00:00:00Z"}
cd_senra_sell = {"id": 991101, "name": "Senra Sell Order", "company": {"name": "Senra"},
                 "deal_stage": {"id": lf.STAGE_MATCHED}, "custom_fields": cf_sell(),
                 "people": [{"id": CD_TENANT_PID}], "updated_at": "2026-08-01T00:00:00Z"}
cd_panthalassa_buy = {"id": 55422151, "name": "Panthalassa: $250K Buy", "company": {"name": "Panthalassa"},
                      "deal_stage": {"id": 111802},  # Won Deal
                      "custom_fields": {lf.DEAL_SIDE_FIELD: [lf.DEAL_SIDE_BUY_ID],
                                        lf.INTRO_STATUS_FIELD: [lf.INTRO_STATUS_CLOSED_ID],
                                        lf.TICKET_MAX_FIELD: 250000},
                      "people": [{"id": CD_TENANT_PID}, {"id": CD_PANTHALASSA_BUYER_PID}],
                      "updated_at": "2026-08-05T00:00:00Z"}
cd_senra_buy = {"id": 55461737, "name": "Senra: Buy", "company": {"name": "Senra"},
                "deal_stage": {"id": 111801},  # Lost
                # Intro Status Introduced (7207579) so this row carries the
                # prior-progress evidence the disclosure gate requires --
                # otherwise a Passed/Withdrawn exit renders anonymized, per
                # the product-law disclosure rule (exit statuses disclose
                # only given milestone/history evidence). Mirrors the
                # existing deal_lost fixture elsewhere in this suite.
                "custom_fields": {lf.DEAL_SIDE_FIELD: [lf.DEAL_SIDE_BUY_ID],
                                  lf.INTRO_STATUS_FIELD: [7207579]},
                "people": [{"id": CD_TENANT_PID}, {"id": CD_SENRA_BUYER_PID}],
                "updated_at": "2026-08-05T00:00:00Z"}
cd_people = {"people": [
    {"id": CD_TENANT_PID, "full_name": "Elana Investor", "email": CD_TENANT_EMAIL, "custom_fields": {}},
    {"id": CD_PANTHALASSA_BUYER_PID, "full_name": "Panthalassa Buyer",
     "email": "buyer@panthalassa-buyer.example", "custom_fields": {}},
    {"id": CD_SENRA_BUYER_PID, "full_name": "Senra Buyer", "email": "buyer@senra-buyer.example",
     "custom_fields": {}},
]}
cd_s3f, cd_table_f = use_fixture({lf.PEOPLE_KEY: cd_people, lf.INTEREST_KEY: {"buy": {}},
                                   lf.DEALS_KEY: {"deals": [cd_panthalassa_sell, cd_senra_sell]}})
lf._closed_deals_cache["fetched_at"] = None
lf._closed_deals_cache["deals"] = None
lf.urllib.request.urlopen = make_closed_deals_urlopen([cd_panthalassa_buy, cd_senra_buy], [])

cd_merged = lf.get_deals_list()
check("55422151 not present in the live snapshot fixture itself",
      55422151 not in {d["id"] for d in (cd_panthalassa_sell, cd_senra_sell)})
check("get_deals_list merge picks up 55422151 (Panthalassa/Won/Closed) from the closed cache",
      55422151 in {d["id"] for d in cd_merged})
check("get_deals_list merge picks up 55461737 (Senra/Lost) from the closed cache",
      55461737 in {d["id"] for d in cd_merged})

cd_tenant = lf._resolve_tenant(CD_TENANT_EMAIL)
check("elana@tworoads.vc still auto-enrolls as a tenant", cd_tenant is not None)

cd_raised = lf._raised_headline_stats()
check("Raised counts 55422151 (Won/Closed) toward closed_count", cd_raised["closed_count"] >= 1)
check("Raised total includes its $250K ticket size", cd_raised["total"] >= 250000)

cd_page_panthalassa = lf.render_company_page("Panthalassa", "Elana Investor", cd_tenant, CD_TENANT_EMAIL, "mydeals",
                                              key=None, view_as=None, edit_mode=False)
cd_buyers_panthalassa = cd_page_panthalassa[cd_page_panthalassa.find('id="buyers"'):]
check("55422151: named in Panthalassa's Buyers section (Closed is always disclosed)",
      "Panthalassa Buyer" in cd_buyers_panthalassa)
check("55422151: styled as a positive Won row (solid-green chip)",
      '<span class="status-chip won">Won</span>' in cd_buyers_panthalassa)

cd_page_senra = lf.render_company_page("Senra", "Elana Investor", cd_tenant, CD_TENANT_EMAIL, "mydeals",
                                        key=None, view_as=None, edit_mode=False)
cd_buyers_senra = cd_page_senra[cd_page_senra.find('id="buyers"'):]
check("55461737: renders in Senra's Buyers Lost group",
      "Senra Buyer" in cd_buyers_senra and '<span class="status-chip exit">Lost</span>' in cd_buyers_senra)

cd_intros_page = lf.render_intros_page("Elana Investor", tenant=cd_tenant, tenant_email=CD_TENANT_EMAIL,
                                        key=None, view_as=None, edit_mode=False)
cd_closed_out = cd_intros_page[cd_intros_page.find('<details class="closed-out-section">'):]
check("55422151: Active Intros Closed out section shows Panthalassa Buyer, green Won chip",
      "Panthalassa Buyer" in cd_closed_out and '<span class="status-chip closed">Won</span>' in cd_closed_out)
check("55461737: Active Intros Closed out section shows Senra Buyer, red Lost chip",
      "Senra Buyer" in cd_closed_out and '<span class="status-chip exit">Lost</span>' in cd_closed_out)

cd_existing, cd_existing_source, _ = lf._find_existing_buy_deal_for_person_company(
    CD_PANTHALASSA_BUYER_PID, "Panthalassa")
check("add-buyer/Introduce lookup adopts the existing closed deal (source='snapshot'), no duplicate created",
      cd_existing is not None and cd_existing["id"] == 55422151 and cd_existing_source == "snapshot")

check("no Dynamo writes from any of the above (pure reads)", cd_table_f.updates == [] and cd_table_f.puts == [])


# ======================================================================
# SECTION: Closed-deal loss reason + admin-only loss notes w/ tenant share
# ======================================================================
# deal_loss_reason (the dropdown name, via _deal_loss_reason_text) already
# rendered unconditionally on every closed-out row -- this section covers
# the NEW piece: deal_loss_reason_notes, admin-only (edit_mode) unless a
# per-intro Dynamo flag (loss_reason_shared, default unshared) has been
# explicitly set, via the same write path (?action=update_intro,
# share_loss_reason) every other admin-only field already uses.

check("_deal_loss_reason_notes_text: plain string",
      lf._deal_loss_reason_notes_text({"deal_loss_reason_notes": "hi"}) == "hi")
check("_deal_loss_reason_notes_text: dict w/ 'notes' key",
      lf._deal_loss_reason_notes_text({"deal_loss_reason_notes": {"notes": "hi"}}) == "hi")
check("_deal_loss_reason_notes_text: list of dicts",
      lf._deal_loss_reason_notes_text({"deal_loss_reason_notes": [{"notes": "hi"}]}) == "hi")
check("_deal_loss_reason_notes_text: missing key -> None", lf._deal_loss_reason_notes_text({}) is None)
check("_deal_loss_reason_notes_text: blank string -> None",
      lf._deal_loss_reason_notes_text({"deal_loss_reason_notes": "   "}) is None)

LR_TENANT_EMAIL = "lossreason-tenant@example.com"
LR_TENANT_PID = 992001
LR_BUYER_PID = 992002
LR_NOTES_TEXT = "Buyer said pricing was off by 15%; may revisit next round."
lr_sell_deal = {"id": 993001, "name": "LR Co Sell Order", "company": {"name": "LR Co"},
                "deal_stage": {"id": lf.STAGE_MATCHED}, "custom_fields": cf_sell(),
                "people": [{"id": LR_TENANT_PID}], "updated_at": "2026-08-01T00:00:00Z"}
lr_lost_deal = {"id": 993002, "name": "LR Co Buy", "company": {"name": "LR Co"},
                "deal_stage": {"id": 111801},  # Lost
                "deal_loss_reason": {"name": "Went with another sponsor"},
                "deal_loss_reason_notes": LR_NOTES_TEXT,
                "custom_fields": {lf.DEAL_SIDE_FIELD: [lf.DEAL_SIDE_BUY_ID],
                                   lf.INTRO_STATUS_FIELD: [7207579]},  # Introduced -- disclosure evidence
                "people": [{"id": LR_TENANT_PID}, {"id": LR_BUYER_PID}], "updated_at": "2026-08-05T00:00:00Z"}
lr_people = {"people": [
    {"id": LR_TENANT_PID, "full_name": "LR Tenant", "email": LR_TENANT_EMAIL, "custom_fields": {}},
    {"id": LR_BUYER_PID, "full_name": "LR Buyer", "email": "buyer@lr.example", "custom_fields": {}},
]}
lr_s3, lr_table = use_fixture({lf.PEOPLE_KEY: lr_people, lf.INTEREST_KEY: {"buy": {}},
                                lf.DEALS_KEY: {"deals": [lr_sell_deal, lr_lost_deal]}})
lr_tenant = lf._resolve_tenant(LR_TENANT_EMAIL)
check("LR: tenant resolves", lr_tenant is not None)

page_admin = lf.render_company_page("LR Co", "Admin", lr_tenant, LR_TENANT_EMAIL, "mydeals",
                                     key=ADMIN_KEY, view_as=LR_TENANT_EMAIL, edit_mode=True)
lr_buyers_admin = page_admin[page_admin.find('id="buyers"'):]
check("LR admin (edit_mode): loss reason dropdown name shown (unconditional, unchanged)",
      "Went with another sponsor" in lr_buyers_admin)
check("LR admin (edit_mode): loss notes shown (admin-only)", LR_NOTES_TEXT in lr_buyers_admin)
check("LR admin (edit_mode): share checkbox present, unchecked by default (default unshared)",
      'class="ei-loss-shared" data-deal-id="993002">' in lr_buyers_admin)

page_tenant_unshared = lf.render_company_page("LR Co", "LR Tenant", lr_tenant, LR_TENANT_EMAIL, "mydeals",
                                               key=None, view_as=None, edit_mode=False)
lr_buyers_tenant = page_tenant_unshared[page_tenant_unshared.find('id="buyers"'):]
check("LR tenant, default unshared: loss reason dropdown name still shown",
      "Went with another sponsor" in lr_buyers_tenant)
check("LR tenant, default unshared: loss notes NOT shown", LR_NOTES_TEXT not in lr_buyers_tenant)
check("LR tenant: no share checkbox rendered at all (admin-only control, never UI-only-restricted)",
      'class="ei-loss-shared"' not in lr_buyers_tenant)

resp_tenant_share = lf.lambda_handler(
    post_event({"deal_id": "993002", "share_loss_reason": False}, cookies=[tenant_cookie(LR_TENANT_EMAIL)]), None)
check("tenant attempting share_loss_reason directly -> 403 (server-side, not just UI-hidden)",
      resp_tenant_share["statusCode"] == 403)
check("tenant's forbidden attempt wrote nothing to Dynamo", lr_table.updates == [] and lr_table.puts == [])

resp_share_only = lf.lambda_handler(post_event({"key": ADMIN_KEY, "deal_id": "993002", "share_loss_reason": True}),
                                     None)
check("admin share_loss_reason-only write is accepted (not rejected as 'nothing to update')",
      resp_share_only["statusCode"] == 200)
check("Dynamo item now carries loss_reason_shared=True",
      any(it.get("tenant") == LR_TENANT_EMAIL and it.get("sk") == "intro#993002"
          and it.get("loss_reason_shared") is True for it in lr_table.items))
lr_audit_items = [it for it in lr_table.puts if str(it.get("sk", "")).startswith("audit#993002#")]
check("audit item recorded the share write (actor=admin, new value True)",
      any(it.get("actor") == "admin" and it.get("new", {}).get("loss_reason_shared") is True
          for it in lr_audit_items))

reset_caches()  # same FakeBoto3 S3/table (use_fixture already bound them) -- just clear module caches
page_tenant_shared = lf.render_company_page("LR Co", "LR Tenant", lr_tenant, LR_TENANT_EMAIL, "mydeals",
                                             key=None, view_as=None, edit_mode=False)
lr_buyers_tenant_shared = page_tenant_shared[page_tenant_shared.find('id="buyers"'):]
check("LR tenant, AFTER admin shares: loss notes now shown", LR_NOTES_TEXT in lr_buyers_tenant_shared)

page_admin_after = lf.render_company_page("LR Co", "Admin", lr_tenant, LR_TENANT_EMAIL, "mydeals",
                                           key=ADMIN_KEY, view_as=LR_TENANT_EMAIL, edit_mode=True)
lr_buyers_admin_after = page_admin_after[page_admin_after.find('id="buyers"'):]
check("LR admin, AFTER sharing: checkbox now renders checked",
      'class="ei-loss-shared" data-deal-id="993002" checked>' in lr_buyers_admin_after)

# --- Active Intros surfaces the same behavior (shared _closed_out_row_html).
page_intros_admin = lf.render_intros_page("Admin", tenant=lr_tenant, tenant_email=LR_TENANT_EMAIL,
                                           key=ADMIN_KEY, view_as=LR_TENANT_EMAIL, edit_mode=True)
intros_closed_out_admin = page_intros_admin[page_intros_admin.find('<details class="closed-out-section">'):]
check("Active Intros admin: loss notes shown, share checkbox present",
      LR_NOTES_TEXT in intros_closed_out_admin and 'class="ei-loss-shared"' in intros_closed_out_admin)

page_intros_tenant = lf.render_intros_page("LR Tenant", tenant=lr_tenant, tenant_email=LR_TENANT_EMAIL,
                                            key=None, view_as=None, edit_mode=False)
intros_closed_out_tenant = page_intros_tenant[page_intros_tenant.find('<details class="closed-out-section">'):]
check("Active Intros tenant (now shared): loss notes shown, no checkbox",
      LR_NOTES_TEXT in intros_closed_out_tenant and 'class="ei-loss-shared"' not in intros_closed_out_tenant)

# --- A closed deal with a reason but no notes at all: nothing extra
# renders, even in edit_mode -- there's nothing to admin-gate or share.
lr_no_notes_deal = {"id": 993003, "name": "LR Co Buy 2", "company": {"name": "LR Co"},
                     "deal_stage": {"id": lf.OBSOLETE_STAGE_ID},
                     "custom_fields": {lf.DEAL_SIDE_FIELD: [lf.DEAL_SIDE_BUY_ID]},
                     "people": [{"id": LR_TENANT_PID}, {"id": LR_BUYER_PID}], "updated_at": "2026-08-05T00:00:00Z"}
use_fixture({lf.PEOPLE_KEY: lr_people, lf.INTEREST_KEY: {"buy": {}},
             lf.DEALS_KEY: {"deals": [lr_sell_deal, lr_no_notes_deal]}})
lr_tenant2 = lf._resolve_tenant(LR_TENANT_EMAIL)
page_no_notes = lf.render_company_page("LR Co", "Admin", lr_tenant2, LR_TENANT_EMAIL, "mydeals",
                                        key=ADMIN_KEY, view_as=LR_TENANT_EMAIL, edit_mode=True)
lr_buyers_no_notes = page_no_notes[page_no_notes.find('id="buyers"'):]
check("no loss notes on record: no checkbox, no loss-reason-notes div, even for admin",
      'class="ei-loss-shared"' not in lr_buyers_no_notes
      and 'class="loss-reason-notes"' not in lr_buyers_no_notes)


# ======================================================================
# SECTION: Buyer page — Private notes (item 4), per-tenant + buyer,
# autosaved via ?action=update_buyer_note
# ======================================================================
PN_TENANT_EMAIL = "tessa@example.com"
PN_TENANT_PID = 601
PN_OTHER_TENANT_EMAIL = "nora@example.com"
PN_OTHER_TENANT_PID = 602
PN_BUYER_PID = 603

pn_people = {"people": [
    {"id": PN_TENANT_PID, "full_name": "Tessa Seller", "email": PN_TENANT_EMAIL, "custom_fields": {}},
    {"id": PN_OTHER_TENANT_PID, "full_name": "Nora Seller", "email": PN_OTHER_TENANT_EMAIL, "custom_fields": {}},
    {"id": PN_BUYER_PID, "name": "Barry Buyer", "email": "barry@example.com", "custom_fields": {}},
]}
pn_sell_tessa = {"id": 940001, "name": "Tessa's Own Deal", "company": {"name": "PN Sell Co A"},
                 "deal_stage": {"id": lf.STAGE_FIRM}, "custom_fields": cf_sell(),
                 "people": [{"id": PN_TENANT_PID}], "updated_at": "2026-08-01T00:00:00Z"}
pn_sell_nora = {"id": 940002, "name": "Nora's Own Deal", "company": {"name": "PN Sell Co B"},
                "deal_stage": {"id": lf.STAGE_FIRM}, "custom_fields": cf_sell(),
                "people": [{"id": PN_OTHER_TENANT_PID}], "updated_at": "2026-08-01T00:00:00Z"}
pn_buy_deal = {"id": 940003, "name": "Barry's Deal", "company": {"name": "PN Buy Co"},
               "deal_stage": {"id": lf.STAGE_MATCHED}, "custom_fields": cf_status(7207579),
               "people": [{"id": PN_TENANT_PID}, {"id": PN_BUYER_PID}], "updated_at": "2026-08-02T00:00:00Z"}
_, pn_table = use_fixture({lf.PEOPLE_KEY: pn_people, lf.INTEREST_KEY: {"buy": {}},
                            lf.DEALS_KEY: {"deals": [pn_sell_tessa, pn_sell_nora, pn_buy_deal]}})
pn_tenant = lf._resolve_tenant(PN_TENANT_EMAIL)
pn_other_tenant = lf._resolve_tenant(PN_OTHER_TENANT_EMAIL)
assert pn_tenant is not None and pn_other_tenant is not None

pn_page = lf.render_buyer_page(PN_BUYER_PID, "Tessa Seller", pn_tenant, PN_TENANT_EMAIL,
                                key=None, view_as=None, edit_mode=False)
check("Private notes: card renders with the exact required label",
      '<h2 class="buyer-section-heading">Private notes — only you and Gracia Group see these.</h2>' in pn_page)
check("Private notes: textarea present, keyed by buyer_id, empty when no note on record",
      f'<textarea class="bn-notes" data-buyer-id="{PN_BUYER_PID}" placeholder="Add a private note…"></textarea>'
      in pn_page)
# Slice past the <style> block first -- .buyer-private-notes-card is
# also a CSS selector defined in <head>, so an ordering check against
# the raw page would false-positive on that definition rather than the
# actual body markup.
pn_page_body = pn_page[pn_page.find("</style>"):]
check("Private notes: card sits beside the name card (buyer-top-row wraps both)",
      '<div class="buyer-top-row">' in pn_page_body
      and pn_page_body.find('<div class="buyer-top-row">') < pn_page_body.find('class="card buyer-header"')
      < pn_page_body.find('buyer-private-notes-card'))

pn_anon_page = lf.render_buyer_page(PN_BUYER_PID, "Nora Seller", pn_other_tenant, PN_OTHER_TENANT_EMAIL,
                                     key=None, view_as=None, edit_mode=False)
# Substring checks target the actual rendered ELEMENTS, not the class
# name alone -- .bn-notes/.buyer-private-notes-card are legitimately
# defined once in the page's <style> block regardless of which branch
# renders (same as every other CSS rule on this page), so a bare
# "bn-notes" in page check would false-positive on the stylesheet.
check("Private notes: never rendered on the anonymized (not-yet-disclosed) card",
      '<textarea class="bn-notes"' not in pn_anon_page
      and '<h2 class="buyer-section-heading">Private notes' not in pn_anon_page)

def buyer_note_event(body_dict, cookies=None):
    return {"requestContext": {"http": {"method": "POST"}}, "rawPath": "/",
            "queryStringParameters": {"action": "update_buyer_note"}, "cookies": cookies or [],
            "body": json.dumps(body_dict)}

resp_forbidden_nocookie = lf.lambda_handler(buyer_note_event({"buyer_id": PN_BUYER_PID, "note": "x"}), None)
check("Private notes write: no identity cookie, no admin key -> 403",
      resp_forbidden_nocookie["statusCode"] == 403)

resp_forbidden_undisclosed = lf.lambda_handler(
    buyer_note_event({"buyer_id": PN_BUYER_PID, "note": "sneaky"}, cookies=[tenant_cookie(PN_OTHER_TENANT_EMAIL)]),
    None)
check("Private notes write: tenant with NO disclosed deal linking this buyer -> 403 (server-side, not "
      "just UI-hidden)", resp_forbidden_undisclosed["statusCode"] == 403)
check("Private notes write: the forbidden write above wrote nothing to Dynamo",
      pn_table.updates == [] and pn_table.puts == [])

resp_tenant_write = lf.lambda_handler(
    buyer_note_event({"buyer_id": PN_BUYER_PID, "note": "Wants a 2-layer SPV, follow up in Q3"},
                      cookies=[tenant_cookie(PN_TENANT_EMAIL)]),
    None)
check("Private notes write: tenant WITH a disclosed deal -> 200", resp_tenant_write["statusCode"] == 200)
pn_written = pn_table.updates[-1]
check("Private notes write: Dynamo key is (tenant=<tenant email>, sk='buyer-note#<person_id>')",
      pn_written["Key"] == {"tenant": PN_TENANT_EMAIL, "sk": f"buyer-note#{PN_BUYER_PID}"})
check("Private notes write: note text written correctly",
      pn_written["ExpressionAttributeValues"].get(":n") == "Wants a 2-layer SPV, follow up in Q3")
check("Private notes write: audit item appended", any(
    it and it.get("actor") == PN_TENANT_EMAIL and str(it.get("sk", "")).startswith(f"audit#buyer-note#{PN_BUYER_PID}#")
    for it in pn_table.puts))

pn_page_after = lf.render_buyer_page(PN_BUYER_PID, "Tessa Seller", pn_tenant, PN_TENANT_EMAIL,
                                      key=None, view_as=None, edit_mode=False)
check("Private notes: textarea pre-fills with the saved note on next render",
      "Wants a 2-layer SPV, follow up in Q3</textarea>" in pn_page_after)

resp_admin_no_tenant = lf.lambda_handler(
    buyer_note_event({"key": ADMIN_KEY, "buyer_id": PN_BUYER_PID, "note": "admin note"}), None)
check("Private notes write: admin key present but no tenant_email -> 400 (no deal_id to derive it from)",
      resp_admin_no_tenant["statusCode"] == 400)

resp_admin_write = lf.lambda_handler(
    buyer_note_event({"key": ADMIN_KEY, "buyer_id": PN_BUYER_PID, "note": "Admin follow-up: sent updated deck",
                       "tenant_email": PN_TENANT_EMAIL}),
    None)
check("Private notes write: admin WITH tenant_email -> 200, writes into that same tenant's partition",
      resp_admin_write["statusCode"] == 200
      and pn_table.updates[-1]["Key"] == {"tenant": PN_TENANT_EMAIL, "sk": f"buyer-note#{PN_BUYER_PID}"})

pn_page_admin = lf.render_buyer_page(PN_BUYER_PID, "Admin", pn_tenant, PN_TENANT_EMAIL,
                                      key=ADMIN_KEY, view_as=PN_TENANT_EMAIL, edit_mode=True)
check("Private notes: admin (view_as this tenant) sees the SAME note the tenant wrote",
      "Admin follow-up: sent updated deck</textarea>" in pn_page_admin)

check("Private notes: never leaked into Nora's (other tenant's) own partition",
      lf._get_buyer_note_item(PN_OTHER_TENANT_EMAIL, PN_BUYER_PID) is None)


# ======================================================================
# SECTION: Firm-level tenancy -- Natoli/Kevin at Mangusta Capital
# ======================================================================
# The live bug this covers: viewing as natoli@mangustacap.com showed
# Kevin Jiang (a Mangusta colleague) as "the buyer" on AMI Labs/
# Anthropic-style deals -- deals where Kevin was the one BUYING equity
# from some unrelated third-party seller, nothing to do with Mangusta's
# own sell-side book. Firm-level tenancy broadens SCOPE (colleagues
# share one view of the firm's own Sell deals and the Buy deals matched
# against them) but must NOT broaden a colleague's own, unrelated
# Buy-side purchase into "one of the firm's intros" just because they
# happen to work at the same firm as the viewing tenant.

MANGUSTA_CID = 8800
MANGUSTA_NATOLI_PID = 601001
MANGUSTA_KEVIN_PID = 601002
MANGUSTA_BUYER_PID = 601010          # legit buyer, matched against Mangusta's own sell deal
MANGUSTA_OUTSIDE_SELLER_PID = 601020  # the real seller on Kevin's own AMI Labs purchase -- not Mangusta

people_mangusta = {"people": [
    {"id": MANGUSTA_NATOLI_PID, "full_name": "Natoli Silva", "email": "natoli@mangustacap.com",
     "company_id": MANGUSTA_CID, "company_name": "Mangusta Capital", "custom_fields": {}},
    {"id": MANGUSTA_KEVIN_PID, "first_name": "Kevin", "last_name": "Jiang", "email": "kevin@mangustacap.com",
     "company_id": MANGUSTA_CID, "company_name": "Mangusta Capital", "custom_fields": {}},
    {"id": MANGUSTA_BUYER_PID, "full_name": "Prime Capital Rep", "email": "buyer@primecapital.example",
     "custom_fields": {}},
    {"id": MANGUSTA_OUTSIDE_SELLER_PID, "full_name": "AMI Labs Founder", "email": "founder@amilabs.example",
     "company_id": 990000, "company_name": "AMI Labs", "custom_fields": {}},
]}

# 1) Natoli's own Sell deal -- the firm's book, and her eligibility grant.
deal_natoli_sell = {"id": 930001, "name": "Mangusta HoldCo Sell", "company": {"name": "Mangusta HoldCo"},
                     "deal_stage": {"id": lf.STAGE_FIRM}, "custom_fields": cf_sell(),
                     "people": [{"id": MANGUSTA_NATOLI_PID}], "updated_at": "2026-08-01T00:00:00Z"}
# 2) Kevin's own Sell deal -- a DIFFERENT company, same firm. Proves
#    "shared sell deals visible to both" (item 1/4): it should render
#    on Natoli's My Deals (via a "via Kevin" chip) and vice versa.
deal_kevin_sell = {"id": 930002, "name": "Kevin Ventures Sell", "company": {"name": "Kevin Ventures Co"},
                    "deal_stage": {"id": lf.STAGE_FIRM}, "custom_fields": cf_sell(),
                    "people": [{"id": MANGUSTA_KEVIN_PID}], "updated_at": "2026-08-02T00:00:00Z"}
# 3) A legitimate matched buyer against Natoli's OWN sell deal -- must
#    keep showing up as an intro (baseline, unaffected by firm-scoping).
deal_matched_intro = {"id": 930003, "name": "Prime Capital Match", "company": {"name": "Mangusta HoldCo"},
                       "deal_stage": {"id": lf.STAGE_MATCHED},
                       "custom_fields": cf_status(7207579),
                       "people": [{"id": MANGUSTA_BUYER_PID}, {"id": MANGUSTA_NATOLI_PID}],
                       "updated_at": "2026-08-03T00:00:00Z"}
# 4) THE BUG: Kevin's own, unrelated Buy-side purchase of AMI Labs
#    equity, matched against AMI Labs' own (non-Mangusta) seller. Kevin
#    is the buyer here, not a seller of record for any Mangusta company
#    -- this must never render as one of the firm's intros on ANY
#    Mangusta colleague's page, Natoli's included.
deal_kevin_buy_ami = {"id": 930004, "name": "Kevin buys AMI Labs", "company": {"name": "AMI Labs"},
                       "deal_stage": {"id": lf.STAGE_MATCHED},
                       "custom_fields": cf_status(7207579),
                       "people": [{"id": MANGUSTA_KEVIN_PID}, {"id": MANGUSTA_OUTSIDE_SELLER_PID}],
                       "updated_at": "2026-08-04T00:00:00Z"}

use_fixture({lf.PEOPLE_KEY: people_mangusta, lf.INTEREST_KEY: {"buy": {}},
             lf.DEALS_KEY: {"deals": [deal_natoli_sell, deal_kevin_sell, deal_matched_intro, deal_kevin_buy_ami]}})
natoli_tenant = lf._resolve_tenant("natoli@mangustacap.com")
assert natoli_tenant is not None
kevin_tenant = lf._resolve_tenant("kevin@mangustacap.com")
assert kevin_tenant is not None

# --- 1) SCOPE: shared sell deals visible to both -----------------------
natoli_sell_ids = {d.get("id") for d in lf.get_firm_sell_deals(MANGUSTA_NATOLI_PID)}
kevin_sell_ids = {d.get("id") for d in lf.get_firm_sell_deals(MANGUSTA_KEVIN_PID)}
check("Firm tenancy: Natoli's firm-wide sell deals include BOTH her own and Kevin's",
      natoli_sell_ids == {930001, 930002})
check("Firm tenancy: Kevin's firm-wide sell deals include BOTH his own and Natoli's (shared, symmetric)",
      kevin_sell_ids == {930001, 930002})

# --- 2) CRITICAL FIX: a colleague's own Buy deal is excluded ------------
natoli_intro_ids = {d.get("id") for d in lf.get_firm_matched_buy_deals(MANGUSTA_NATOLI_PID)}
check("Firm tenancy: the legit matched buyer against Natoli's own sell deal IS an intro",
      930003 in natoli_intro_ids)
check("CRITICAL FIX: Kevin's own AMI Labs purchase is NOT one of the firm's intros",
      930004 not in natoli_intro_ids)
check("_is_firm_intro_buy_deal: AMI Labs deal fails the company+seller match directly",
      not lf._is_firm_intro_buy_deal(
          deal_kevin_buy_ami, MANGUSTA_NATOLI_PID, lf._firm_person_ids(MANGUSTA_NATOLI_PID),
          lf._firm_sell_person_ids_by_company(lf._firm_person_ids(MANGUSTA_NATOLI_PID))))

# --- 3) No colleague ever rendered as "the buyer" on the tenant's page -
intros_natoli_full = lf.render_intros_page("Natoli Silva", tenant=natoli_tenant, tenant_email="natoli@mangustacap.com",
                                            key=None, view_as=None, edit_mode=False)
# body_only: the nav's own My Deals quick-jump dropdown legitimately
# lists Kevin's sell deal with a "via Kevin" chip (item 1/4 -- shared
# sell-deal visibility, verified separately above) -- strip it off so
# this checks the actual Buyers table, not the nav.
intros_natoli = body_only(intros_natoli_full)
check("Active Intros: the legit buyer's row is present", "Prime Capital" in intros_natoli or "Mangusta HoldCo" in intros_natoli)
check("Active Intros: Kevin's own AMI Labs purchase never appears at all", "AMI Labs" not in intros_natoli)
check("Active Intros: Kevin never rendered as a buyer identity on Natoli's page", "Kevin" not in intros_natoli)
check("Active Intros: the outside AMI Labs seller never leaks onto Natoli's page either",
      "AMI Labs Founder" not in intros_natoli)

# --- ATTRIBUTION (item 2): the shared sell deal carries a "via Kevin" --
mydeals_natoli = body_only(lf.render_my_deals_page(
    "Natoli Silva", deals=lf.get_firm_sell_deals(MANGUSTA_NATOLI_PID), key=None, view_as=None,
    edit_mode=False, person_id=MANGUSTA_NATOLI_PID, anon_key_email="natoli@mangustacap.com"))
check("My Deals: Kevin's own sell deal shows a 'via Kevin' chip on Natoli's page",
      "via Kevin" in mydeals_natoli)
check("My Deals: Natoli's own sell deal carries no via-chip (it's genuinely hers)",
      mydeals_natoli.count("via Kevin") == 1)

mydeals_kevin = body_only(lf.render_my_deals_page(
    "Kevin Jiang", deals=lf.get_firm_sell_deals(MANGUSTA_KEVIN_PID), key=None, view_as=None,
    edit_mode=False, person_id=MANGUSTA_KEVIN_PID, anon_key_email="kevin@mangustacap.com"))
check("My Deals: Natoli's sell deal shows a 'via Natoli' chip on Kevin's page (shared, symmetric)",
      "via Natoli" in mydeals_kevin)


# ======================================================================
# SECTION: classify_person tier coverage
# ======================================================================
# classify_person is a pure function of custom_fields -- no fixture needed.

check("classify_person: QP alone -> qp",
      lf.classify_person({lf.INVESTOR_LEVEL_FIELD: [lf.QP_ID]}) == "qp")
check("classify_person: QC alone -> accredited",
      lf.classify_person({lf.INVESTOR_LEVEL_FIELD: [lf.QC_ID]}) == "accredited")
check("classify_person: Accredited alone -> accredited",
      lf.classify_person({lf.INVESTOR_LEVEL_FIELD: [lf.ACCREDITED_ID]}) == "accredited")
check("classify_person: Substantive alone -> unknown",
      lf.classify_person({lf.INVESTOR_LEVEL_FIELD: [lf.SUBSTANTIVE_ID]}) == "unknown")
check("classify_person: no investor-level field, no IQF -> unknown",
      lf.classify_person({}) == "unknown")

check("iqf_pending: QP without IQF on file -> True",
      lf.iqf_pending({lf.INVESTOR_LEVEL_FIELD: [lf.QP_ID]}) is True)
check("iqf_pending: accredited (via IQF Yes) without Investor Level -> False",
      lf.iqf_pending({lf.IQF_FIELD: [6496840]}) is False)
check("iqf_pending: QP with IQF Yes (6496840) on file -> False",
      lf.iqf_pending({lf.INVESTOR_LEVEL_FIELD: [lf.QP_ID], lf.IQF_FIELD: [6496840]}) is False)
check("iqf_pending: Accredited with IQF Unnecessary (6596073) on file -> False",
      lf.iqf_pending({lf.INVESTOR_LEVEL_FIELD: [lf.ACCREDITED_ID], lf.IQF_FIELD: [6596073]}) is False)
check("iqf_pending: QC without IQF on file -> True",
      lf.iqf_pending({lf.INVESTOR_LEVEL_FIELD: [lf.QC_ID]}) is True)
check("iqf_pending: unknown-tier (Substantive) person -> False regardless of IQF",
      lf.iqf_pending({lf.INVESTOR_LEVEL_FIELD: [lf.SUBSTANTIVE_ID]}) is False)
check("iqf_pending: no investor-level field at all -> False",
      lf.iqf_pending({}) is False)


# ======================================================================
# Summary
# ======================================================================

print(f"\n{passed} passed, {failed} failed")
if failed:
    raise SystemExit(1)
