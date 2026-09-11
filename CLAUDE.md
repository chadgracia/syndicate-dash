# syndicate-dash — working context (read this first, trust it, do not re-verify)

Seller-facing dashboard Lambda (function name `syndicate-dash`, us-east-1, Function URL). One file: lambda_function.py. Deploy: push to main → GitHub Actions OIDC → update-function-code. Chad is a coding novice: never suggest terminal steps for him; console clicks only.

## Speed rules — read before every task
Default to FAST MODE. A task is fast unless Chad writes "CAREFUL MODE" or it changes client-facing disclosure, money math, write paths, or emails.

FAST MODE means:
- Touch only the functions named in the instruction. Do not sweep the file, audit adjacent code, or look for related problems.
- No new tests. Run tests/test_suite.py ONCE at the end; if green, commit.
- No written report. Reply with at most three lines: what changed, anything that failed, anything you had to guess.
- No screenshots, no diff reviews, no before/after tables, no benchmark fixtures.
- No re-verification of anything already stated in CLAUDE.md or in the instruction.
- If something looks wrong outside the task's scope, note it in one line — do not fix it.
- If a required fact is missing, stop and ask in one sentence.

CAREFUL MODE (only when stated, or when the change touches disclosure/money/writes/email): full verification, regression tests for the changed behavior, and a brief report of root cause and risks.

## Hard conventions
- All literal { } in f-string CSS/JS are doubled. Run `python3 -m py_compile lambda_function.py` before every commit.
- No screenshots/Playwright unless the task explicitly asks.
- Secrets ONLY from env vars: ADMIN_KEY, IDENTITY_SECRET, PIPELINE_API_KEY, PIPELINE_APP_KEY, HMAC_SECRET. Never hardcode values in this repo.
- Pipeline API writes: query-string auth (?api_key=&app_key=), never Basic/Bearer.
- This sandbox cannot reach api.pipelinecrm.com or live S3. Never fetch them; never invent ids. If a needed id/shape is not in this file or the code, STOP and ask Chad to paste JSON.
- Do not read sibling repos to re-verify anything listed here — it is already verified against live data.
- Tests: single suite in tests/test_suite.py. Keep it green; extend it with each change; never leave known-failing tests behind.

## Data sources (S3, bucket full-pipeline-cache, hourly)
- people.json: {"people":[{id, email, first_name, last_name, full_name, company_id, company_name, position, phone, mobile, website, linked_in_url, work_country/home_country, summary(INTERNAL — never render to tenants), updated_at "YYYY/MM/DD ...", won_deals_total, custom_fields{custom_label_N: scalar-or-list}}]}
- interest_people.json: {"buy": {"<company name>": [person_id,...]}, "last_updated"}
- deals.json: deals carry deal_stage as nested object {"id":...}; helper _deal_stage_id handles it. People linkage: "people" list of dicts else flat "person_ids" (never both). Native "value" = OUR COMMISSION, never purchase size. Purchase size = max 3064645 else min 3065488. primary_contact_id exists on API records; verify snapshot survival per use.
- companies.json: has won_deals_total but it is ALWAYS 0 for buyer firms (deals attach to target companies) — never use it for buyer-firm logic; firm-level facts come from people-loop over shared company_id.

## Verified Pipeline field/option ids (final — do not re-derive)
Deal: side tag 1958 (Buy 5077819, Sell 5011675) · Agent Agreement 3714334 (Yes-Sell 6354277, Yes-Buy 6354274, InProc 6354283/6354280) · Mgmt fee 3940558 · Carry 3940559 · Seller fee 3940560 · Partner fee 3940561 (NEVER client-facing) · Net 3064369 · Gross 3064339 · Min size 3065488 · Max size 3064645 · Structure 3064360 · Layers 3938743 · Deadline 4006402 (date, slash format) · Fund Exemption 4006089 (3c1 7200027, 3c7 7200028, Other 7201486) · Intro Status 4008329: Matched 7207578, Introduced 7207579, NDA 7207580, VDR 7207581, Docs Sent 7207582, Wired 7207583, Stalled 7207584, Passed 7207585, Withdrawn 7207586, Closed 7207587.
Person: Investor Level 3923758 (QP 6950564, Substantive 7162165=Unknown tier) · IQF 3763008 (Yes 6496840, Unnecessary 6596073) · Ticket size 3052210 (TICKET_SIZE_MAP) · Transactor Type 3759163 (TRANSACTOR_TYPE_LABELS; Natural Person 6484810, Syndicator 6859893) · CEF/ID 3796440 (No 6600513, Pending 6600514, Yes 6600515, N/A 6600516) · Accepts 3998063 (SPV 7177773, 2-Layer 7177774, Fees 7177775, Forwards 7177776, Commons 7177777) · Buy Interest 3322093.
Company: PitchBook URL 3320818 · description (INTERNAL until published).
Stages: Inquiry 2109142, Firm 111800, Matched 2381534, LOI 2517909, Transfer Notice 2533998, SPA 2381535, Confirm 2388323, ROFR-ed 2426790, Blocked 2447751, Invoiced 2456153, Hold 2094373, Trade Broken 2486672, Obsolete 2348038, Lost 111801/2379322, Won 111802/2379321. MATCHED_OR_LATER and WON sets are in code — authoritative.

## Product law (never violate without an explicit instruction from Chad)
- Two-deal model: SELL deal = tenant's master record (My Deals). BUY deals = one per matched buyer; sole source of buyer rows/counts.
- Tenancy: any authenticated email matching a person with ≥1 Sell-tagged deal (ANY stage). TENANT_OVERRIDES names, TENANT_BLOCKLIST denies.
- Disclosure gate: buyer identity/contact renders ONLY when Intro Status resolves past Matched, with exit statuses disclosing only given milestone/history evidence. Anonymized rows: 4-char HMAC(IDENTITY_SECRET, "{email}|{person_id}") codes, per-tenant. Never leak identity in HTML/attributes/comments on anonymous rows.
- Never client-facing: Partner Fee, internal summaries/descriptions/notes, won-deal amounts/counts/dates of ANY counterparty (only qualitative "Proven closer"/"Firm has closed"), other tenants' anything, other companies' interests of a buyer.
- Writes: Pipeline PUT first, abort all on failure; Dynamo (table syndicate-dash, tenant/sk) overrides newer-wins; audit items on every write; tenant rights are restricted server-side (403), never UI-only.
- Emails: SES from agent@agent.graciagroup.com to cgracia@rainmakersecurities.com ONLY. No other recipients ever.
