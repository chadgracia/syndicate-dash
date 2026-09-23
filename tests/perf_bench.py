"""
Performance harness for lambda_function.py (not part of the regression
suite). Builds a synthetic data set at production scale (~16k people),
serves it through fake S3/DynamoDB clients with simulated network
latency, and times each top-level tab cold (fresh container) and warm
(repeat click), reporting the per-phase TIMING breakdown and the top 5
functions by exclusive-ish cost (cProfile tottime within lambda_function).

Run:  python3 tests/perf_bench.py            (phase breakdown per tab)
      python3 tests/perf_bench.py --profile  (+ top-5 function costs)
"""
import cProfile
import contextlib
import importlib
import io
import json
import os
import pstats
import random
import sys
import time
from datetime import datetime, timezone, timedelta

os.environ.setdefault("ADMIN_KEY", "bench-admin-key")
os.environ.setdefault("IDENTITY_SECRET", "bench-secret")
os.environ.setdefault("HMAC_SECRET", "bench-hmac")
os.environ.setdefault("PIPELINE_API_KEY", "pk")
os.environ.setdefault("PIPELINE_APP_KEY", "ak")
sys.path.insert(0, os.environ.get("LF_DIR") or os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lambda_function as lf  # noqa: E402

S3_GET_BASE, S3_BYTES_PER_S, S3_HEAD, DDB_CALL = 0.030, 80e6, 0.015, 0.010
N_PEOPLE, N_SELL, N_BUY, N_CLOSED, N_COMPANIES = 16000, 900, 3600, 1500, 700
TEAM_DOMAIN, TEAM_FIRM = "mangusta.com", "Mangusta Capital"


def build_data(seed=7):
    rnd = random.Random(seed)
    companies = [f"Target {i:04d} Inc" for i in range(N_COMPANIES)]
    people = []
    for i in range(N_PEOPLE):
        pid = 100000 + i
        cf = {lf.INVESTOR_LEVEL_FIELD: [rnd.choice([lf.QP_ID, lf.ACCREDITED_ID, lf.QC_ID])],
              lf.CEF_FIELD: [rnd.choice([lf.CEF_YES_ID, lf.CEF_NO_ID])],
              "custom_label_3052210": [rnd.randint(1, 8)], "custom_label_3759163": [6484810],
              "custom_label_3322093": [rnd.randint(1, 50)], "custom_label_3801446": "Headline • point • point"}
        people.append({"id": pid, "first_name": f"First{i}", "last_name": f"Last{i}", "full_name": f"First{i} Last{i}",
                       "email": f"p{i}@firm{i % 3000}.com", "emails": [{"address": f"p{i}@alt{i % 3000}.com"}],
                       "company_id": 900000 + i % 3000, "company_name": f"Firm {i % 3000}",
                       "position": "Partner", "phone": "555-0100", "summary": "x" * 400,
                       "updated_at": "2026/08/01 10:00:00 -0700", "custom_fields": cf})
    # Team tenant: 4 members at mangusta.com, one firm.
    team_ids = [99000 + k for k in range(4)]
    for k, pid in enumerate(team_ids):
        people.append({"id": pid, "full_name": f"Team{k} Member", "email": f"m{k}@{TEAM_DOMAIN}",
                       "company_id": 777777, "company_name": TEAM_FIRM,
                       "custom_fields": {lf.CEF_FIELD: [lf.CEF_YES_ID]}, "updated_at": "2026/08/01 10:00:00 -0700"})
    buyer_pool = [100000 + i for i in range(N_PEOPLE)]
    deals, closed = [], []
    did = 5000000
    team_companies = companies[:22]
    team_sells = []

    def sell(company, owner, stage=lf.STAGE_FIRM):
        nonlocal did
        did += 1
        cf = {lf.DEAL_SIDE_FIELD: [lf.DEAL_SIDE_SELL_ID], lf.TICKET_MIN_FIELD: 1e6, lf.TICKET_MAX_FIELD: 5e6,
              lf.MGMT_FEE_FIELD: 2, lf.CARRY_FIELD: 20, lf.SELLER_FEE_FIELD: 3,
              lf.AGENT_AGREEMENT_FIELD: [lf.AGENT_AGREEMENT_SELL_SIGNED_ID], lf.DEADLINE_FIELD: "2026/12/01"}
        return {"id": did, "name": f"{company} block", "company": {"name": company}, "deal_stage": {"id": stage},
                "custom_fields": cf, "people": [{"id": owner}], "value": 1000,
                "updated_at": "2026-08-01T00:00:00Z", "created_at": "2026/03/02 09:00:00 +0000"}

    def buy(company, owner, buyer, status, stage=lf.STAGE_MATCHED):
        nonlocal did
        did += 1
        cf = {lf.DEAL_SIDE_FIELD: lf.DEAL_SIDE_BUY_ID, lf.TICKET_MAX_FIELD: 1e6}
        if status:
            cf[lf.INTRO_STATUS_FIELD] = [status]
        return {"id": did, "name": f"{company} buy", "company": {"name": company}, "deal_stage": {"id": stage},
                "custom_fields": cf, "people": [{"id": owner}, {"id": buyer}],
                "updated_at": f"2026-08-{rnd.randint(1, 28):02d}T00:00:00Z"}

    for i, company in enumerate(team_companies):
        d = sell(company, team_ids[i % 4])
        deals.append(d)
        team_sells.append(d)
    statuses = [7207578, 7207579, 7207581, 7207582, 7207583, 7207584, None]
    for i in range(160):
        deals.append(buy(team_companies[i % 22], team_ids[i % 4], rnd.choice(buyer_pool), rnd.choice(statuses)))
    other_sellers = buyer_pool[:N_SELL]
    for i in range(N_SELL - 22):
        deals.append(sell(rnd.choice(companies), other_sellers[i]))
    for i in range(N_BUY - 160):
        deals.append(buy(rnd.choice(companies), rnd.choice(other_sellers), rnd.choice(buyer_pool),
                         rnd.choice(statuses)))
    for i in range(N_CLOSED):
        owner = team_ids[i % 4] if i < 30 else rnd.choice(other_sellers)
        company = team_companies[i % 22] if i < 30 else rnd.choice(companies)
        if i % 2:
            closed.append(sell(company, owner, stage=rnd.choice([111802, lf.OBSOLETE_STAGE_ID, 111801])))
        else:
            closed.append(buy(company, owner, rnd.choice(buyer_pool), lf.INTRO_STATUS_CLOSED_ID, stage=111802))
    interest = {"buy": {c: rnd.sample(buyer_pool, 30) for c in companies}}
    now_iso = datetime.now(timezone.utc).isoformat()
    objs = {
        lf.PEOPLE_KEY: {"people": people},
        lf.DEALS_KEY: {"deals": deals},
        lf.INTEREST_KEY: interest,
        lf.COMPANIES_KEY: {"companies": [{"id": i, "name": c} for i, c in enumerate(companies)]},
        lf.CLOSED_DEALS_KEY: {"deals": closed, "last_updated": now_iso},
    }
    items = []
    for d in deals[22:182]:
        owner_email = f"m{team_ids.index(d['people'][0]['id'])}@{TEAM_DOMAIN}"
        items.append({"tenant": owner_email, "sk": f"intro#{d['id']}", "notes": "n", "milestones": {"NDA": 1}})
    for k in range(4):
        for j in range(5):
            items.append({"tenant": f"m{k}@{TEAM_DOMAIN}", "sk": f"feature#{1700000000000 + j}", "text": "idea",
                          "status": "open", "page": "my-deals"})
    return objs, items, team_sells


class Body:
    def __init__(self, raw):
        self.raw = raw

    def read(self):
        return self.raw


class BenchS3:
    def __init__(self, objs):
        self.raw = {k: json.dumps(v).encode() for k, v in objs.items()}
        self.lm = {k: datetime(2026, 9, 1, tzinfo=timezone.utc) for k in objs}
        self.gets = {}
        self.heads = 0

    def get_object(self, Bucket, Key):
        raw = self.raw[Key]
        time.sleep(S3_GET_BASE + len(raw) / S3_BYTES_PER_S)
        self.gets[Key] = self.gets.get(Key, 0) + 1
        return {"Body": Body(raw), "ContentLength": len(raw), "LastModified": self.lm[Key],
                "ETag": f'"{Key}-v1"'}

    def head_object(self, Bucket, Key):
        time.sleep(S3_HEAD)
        self.heads += 1
        if Key not in self.raw:
            raise KeyError(Key)
        return {"LastModified": self.lm[Key], "ETag": f'"{Key}-v1"', "ContentLength": len(self.raw[Key])}

    def put_object(self, Bucket, Key, Body, ContentType=None):
        self.raw[Key] = Body if isinstance(Body, bytes) else Body.encode()
        self.lm[Key] = datetime.now(timezone.utc)


class BenchTable:
    def __init__(self, items):
        self.items = items
        self.calls = 0

    def _tenant(self, expr):
        try:
            return expr._values[0]._values[1]
        except (AttributeError, IndexError):
            return None

    def query(self, **kw):
        time.sleep(DDB_CALL)
        self.calls += 1
        t = self._tenant(kw.get("KeyConditionExpression"))
        return {"Items": [i for i in self.items if t is None or i.get("tenant") == t]}

    def scan(self, **kw):
        time.sleep(DDB_CALL)
        self.calls += 1
        return {"Items": list(self.items) if kw.get("Segment", 0) == 0 else []}

    def get_item(self, **kw):
        time.sleep(DDB_CALL)
        self.calls += 1
        k = kw["Key"]
        it = next((i for i in self.items if i["tenant"] == k["tenant"] and i["sk"] == k["sk"]), None)
        return {"Item": it} if it else {}

    def batch_get_item(self, RequestItems):
        time.sleep(DDB_CALL)
        self.calls += 1
        (name, req), = RequestItems.items()
        want = {(k["tenant"], k["sk"]) for k in req["Keys"]}
        return {"Responses": {name: [i for i in self.items if (i["tenant"], i["sk"]) in want]},
                "UnprocessedKeys": {}}


class BenchBoto3:
    S3 = None
    TABLE = None

    @staticmethod
    def client(name, **kw):
        assert name == "s3", name
        return BenchBoto3.S3

    @staticmethod
    def resource(name, **kw):
        class R:
            def Table(self, _n):
                return BenchBoto3.TABLE

            def batch_get_item(self, RequestItems):
                return BenchBoto3.TABLE.batch_get_item(RequestItems=RequestItems)
        return R()


def fresh_container(objs, items, s3=None):
    """A new Lambda container: module state reset, fakes bound. Pass the
    previous container's s3 to keep S3-side state (e.g. people-slim.json)."""
    mod = importlib.reload(lf)
    BenchBoto3.S3 = s3 or BenchS3(objs)
    BenchBoto3.TABLE = BenchTable(items)
    mod.boto3 = BenchBoto3
    return mod


def event(mod, q, email):
    ev = {"requestContext": {"http": {"method": "GET"}}, "rawPath": "/", "queryStringParameters": q}
    if email:
        ev["cookies"] = [mod._make_identity_cookie(email).split(";")[0]]
    return ev


ADMIN = os.environ["ADMIN_KEY"]
TABS = [("overview", {"tab": "overview"}, True), ("mydeals", {"tab": "mydeals"}, True),
        ("intros", {"tab": "intros"}, True), ("demand", {"tab": "demand"}, True),
        ("company", {"company": "Target 0003 Inc"}, True),
        ("adm-mydeals", {"key": ADMIN, "tab": "mydeals"}, False),
        ("adm-view_as", {"key": ADMIN, "view_as": f"m0@{TEAM_DOMAIN}", "tab": "overview"}, False)]


def run_one(mod, q, email, profile=False):
    buf = io.StringIO()
    prof = cProfile.Profile() if profile else None
    t0 = time.perf_counter()
    with contextlib.redirect_stdout(buf):
        if prof:
            prof.enable()
        resp = mod.lambda_handler(event(mod, q, email), None)
        if prof:
            prof.disable()
    wall = time.perf_counter() - t0
    assert resp["statusCode"] == 200, (q, resp["statusCode"], resp.get("body", "")[:300])
    line = next((ln for ln in buf.getvalue().splitlines() if ln.startswith("TIMING ")), "")
    fields = dict(p.split("=", 1) for p in line.split()[1:] if "=" in p)
    top = []
    if prof:
        st = pstats.Stats(prof)
        rows = []
        for (fn, ln, name), (cc, nc, tt, ct, callers) in st.stats.items():
            if fn.endswith("lambda_function.py") and name not in ("lambda_handler", "_lambda_handler_impl", "wrapper"):
                rows.append((ct, tt, nc, name))
        # top 5 by cumulative among "leaf-ish" heavy functions: rank by tottime
        rows.sort(key=lambda r: -r[1])
        top = rows[:5]
    return wall, fields, top


def main():
    profile = "--profile" in sys.argv
    email = f"m0@{TEAM_DOMAIN}"
    objs, items, _ = build_data()
    sizes = {k: len(json.dumps(v)) for k, v in objs.items()}
    print("data sizes:", {k: f"{v / 1e6:.1f}MB" for k, v in sizes.items()})
    phases = ("s3", "tenancy", "model", "dynamo", "render", "total")
    print(f"{'tab':11} {'run':5} {'wall':>7} " + " ".join(f"{p:>8}" for p in phases) + "  s3gets heads ddb")
    shared_s3 = None
    for name, q, as_tenant in TABS:
        mod = fresh_container(objs, items, s3=shared_s3)
        shared_s3 = BenchBoto3.S3   # later containers see what earlier ones wrote (slim index)
        for run in ("cold", "warm", "warm2"):
            s3, tbl = BenchBoto3.S3, BenchBoto3.TABLE
            g0, h0, d0 = sum(s3.gets.values()), s3.heads, tbl.calls
            if run == "warm2":
                time.sleep(0.01)
            wall, f, top = run_one(mod, q, email if as_tenant else None, profile=profile and run != "warm2")
            print(f"{name:11} {run:5} {wall:7.2f} " + " ".join(f"{float(f.get(p, '0').rstrip('s')):8.3f}" for p in phases)
                  + f"  {sum(s3.gets.values()) - g0:6d} {s3.heads - h0:5d} {tbl.calls - d0:3d}")
            for ct, tt, nc, fn in top:
                print(f"{'':18}top: {fn:42} self={tt:6.2f}s cum={ct:6.2f}s calls={nc}")


if __name__ == "__main__":
    main()
