# Create Council Land-Use Geography Repairs

Reads the reviewed geography repair ledger for Council land-use matters whose
affected districts were not clear from Legistar, ZAP, or deterministic
crosswalks.

For these rows, the unresolved land-use decisions were fed into ChatGPT with
available matter text, project identifiers, source URLs, and related geography
clues to infer the affected Council district or districts. The raw chat is not
the production input. The production source of truth is the accepted ledger in
`code/accepted_ai_geography_repair_ledger.csv`. It records one reviewed decision
per promoted repair: final district assignment, source of the repair, confidence,
promotion decision, evidence type, note, matter identifiers, and source URLs.

This task does not call ChatGPT. It normalizes the committed accepted ledger so
downstream vote-panel code reads a stable project file rather than an audit
workflow.

Creates `council_land_use_ai_geography_accepted_repairs.csv`.
