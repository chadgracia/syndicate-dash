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
Person: Investor Level 3923758 (QP 6950564, QC 7209227=Accredited tier, Accredited 6950563=Accredited tier, Substantive 7162165=Unknown tier; tier trusts this field — IQF 3763008 renders only as an 'IQF needed' flag) · IQF 3763008 (Yes 6496840, Unnecessary 6596073) · Ticket size 3052210 (TICKET_SIZE_MAP) · Transactor Type 3759163 (TRANSACTOR_TYPE_LABELS; Natural Person 6484810, Syndicator 6859893) · CEF/ID 3796440 (No 6600513, Pending 6600514, Yes 6600515, N/A 6600516) · Accepts 3998063 (SPV 7177773, 2-Layer 7177774, Fees 7177775, Forwards 7177776, Commons 7177777) · Buy Interest 3322093 · Public Bio 3801446 (plain text "headline • point • point"; split on "•" only; TENANT-VISIBLE; authoring rules in docs/BIO_RULES.md).
Company: PitchBook URL 3320818 · description (INTERNAL until published).
Stages: Inquiry 2109142, Firm 111800, Matched 2381534, LOI 2517909, Transfer Notice 2533998, SPA 2381535, Confirm 2388323, ROFR-ed 2426790, Blocked 2447751, Invoiced 2456153, Hold 2094373, Trade Broken 2486672, Obsolete 2348038, Lost 111801/2379322, Won 111802/2379321. MATCHED_OR_LATER and WON sets are in code — authoritative.

## Product law (never violate without an explicit instruction from Chad)
- Two-deal model: SELL deal = tenant's master record (My Deals). BUY deals = one per matched buyer; sole source of buyer rows/counts.
- Tenancy: any authenticated email matching a person with ≥1 Sell-tagged deal (ANY stage). TENANT_OVERRIDES names, TENANT_BLOCKLIST denies.
- Disclosure gate: buyer identity/contact renders ONLY when Intro Status resolves past Matched, with exit statuses disclosing only given milestone/history evidence. Anonymized rows: 4-char HMAC(IDENTITY_SECRET, "{email}|{person_id}") codes, per-tenant. Never leak identity in HTML/attributes/comments on anonymous rows.
- Never client-facing: Partner Fee, internal summaries/descriptions/notes, won-deal amounts/counts/dates of ANY counterparty (only qualitative "Proven closer"/"Firm has closed"), other tenants' anything, other companies' interests of a buyer.
- Writes: Pipeline PUT first, abort all on failure; Dynamo (table syndicate-dash, tenant/sk) overrides newer-wins; audit items on every write; tenant rights are restricted server-side (403), never UI-only.
- Emails: SES from agent@agent.graciagroup.com to cgracia@rainmakersecurities.com ONLY. No other recipients ever.

## Public Bio (custom_label_3801446)
- Rules for writing bios (manual now, Bedrock later): docs/BIO_RULES.md.
- Write path (write_public_bio): Pipeline person PUT of ONLY this field first; on failure stop. Then Dynamo upsert tenant="__bios__", sk="bio#<person_id>", bio, updated_at (ISO UTC) + audit item.
- Display (buyer page): Dynamo __bios__ record first, else the person record's field. Headline under the location line; points in the "Before your call" card.
- Admin inline edit: buyer page with admin key + &edit=1 only (POST ?action=save_bio). Bulk: ?key=ADMIN_KEY&view=bios (JSON {"<person_id>": "<bio>"}, ≤25 entries, ≤1500 chars each, whole batch rejected on any invalid entry).

## Notes (buyer page ?buyer=<id>) — two types, both append-only
- Person note: Dynamo tenant=<viewing tenant's login email>, sk="buyer-note#<buyer person_id>". About the person across all deals. Write: POST ?action=update_buyer_note (tenant: own partition, only with a disclosed deal with that buyer; admin: must pass tenant_email). Visible to: that tenant + Gracia admin. NOT firm colleagues, NOT other tenants, never the counterparty. No Pipeline field.
- Deal note: Dynamo tenant=<deal's owning tenant email (_tenant_email_for_deal)>, sk="intro#<deal_id>". About one intro. Write: POST ?action=update_intro {notes} (tenant: disclosed row only; admin: any). Visible to: the owning tenant (buyer page, Active Intros Notes column, Company page) + Gracia admin. NOT firm colleagues (they read their own partition), never the counterparty. No Pipeline field (only status/deadline go to Pipeline).
- History: each item carries notes_history = [{id, type "person"|"deal", company, deal_id ("" for person), text, author_email ("admin" for Gracia), author_name ("Gracia Group" for admin), created_at ISO UTC}]. Every non-blank save appends one entry (unchanged latest text is skipped, for blur-autosave); legacy "note"/"notes" still mirror the latest text. Audit item per save.
- Migration: migrate_notes_history / GET ?key=ADMIN_KEY&migrate_notes=1 seeds legacy single-value notes as entry id "legacy-<sk>" (stored timestamp, else migration time). Idempotent: items with notes_history are skipped. Unmigrated items are also seeded on first append and shown on read.
- Buyer page layout: left = header, Before your call, Notes history (then About the firm / deal team); right = Track with you, person-note input, deal-note input(s). Narrow: header, Track with you, Before your call, inputs, Notes history. Tenant history = own partition only (disclosed deals); admin history = all tenants' entries.
