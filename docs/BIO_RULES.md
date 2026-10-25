# Public Bio rules (custom_label_3801446)

Where it lives: Pipeline person field "Public Bio" (custom_label_3801446) is the single source of truth. Edit it in Pipeline; syndicate-dash shows the change after the next hourly people.json refresh. Admin inline edit (&edit=1) and the bulk page (?view=bios) also write Pipeline first and show immediately.

Format: "<headline> • <point> • <point>" as plain text, split on "•", max 1500 characters. No HTML, no markdown. The headline renders next to the header card; each point is a line in the "Before your call" box.

1. Verify identity first: a source counts only if it matches at least 2 CRM facts (email domain, LinkedIn URL, company, city). If identity is unverified, write nothing and flag the person for Chad.
2. Never repeat anything the header card shows: name, firm/company, title, location, email, phone, LinkedIn, ID verified, Investor Level/QP, ticket size, Transactor Type.
3. Headline: a one-line characterization of the investor, under 100 characters — no firm name, title or tenure. Example: "Experienced and active investor in leading unicorns, with focus on AI, space, defense tech."
4. Points: 2–4, one sentence each, in this order:
   a. Investing record first — syndicator/SPV-raiser status when known (essential), notable portfolio names, and counts from PitchBook/Crunchbase. If nothing is found, say "No public investing record found."
   b. Prior roles — notable or relevant firms only. Skip details of companies the person founded unless they are famous or very successful.
   c. One closing judgment line, framed as an inference from background, e.g. "Background suggests a sophisticated investor and hard bargainer."
5. Sources: public sources (LinkedIn, company sites, press, SEC filings, PitchBook, Crunchbase) plus facts Chad supplies directly (e.g. "he told me he syndicates"). Never copy CRM notes, Summary, deal history, or anything from KYC/IQF/CEF.
6. Leave out: education, languages, call-prep about buying entity/domicile/jurisdiction (the IQF covers it), wealth or recent liquidity, net worth, age, ethnicity, nationality, mutual connections.
7. Fact points stay neutral; dates as "Oct 2013" or "2009–2014".
8. If fewer than 2 points can be verified, leave the field empty and do not pad.
9. Chad approves every draft before it is written; nothing auto-publishes.

Example (approved Oct 2026):
Experienced and active investor in leading unicorns, with focus on AI, space, defense tech. • Investing record: active syndicator who raises SPVs for his investors; PitchBook records 18 investments for his firm, and his LinkedIn shows late-stage positions including SpaceX, Groq, Figure, Perplexity and OpenAI. • Previously Partner at a VC firm (2017–2020), led AI investments on a fund's founding team, and ran angel investments for a family office; began in software and investment banking. • Background suggests a sophisticated investor and hard bargainer.
