# Create Council Land-Use Geography Repairs

Reads the reviewed geography repair ledger for Council land-use matters whose
affected districts were not clear from Legistar, ZAP, or deterministic
crosswalks.

For these rows, the unresolved land-use decisions were fed into ChatGPT with
available matter text, project identifiers, source URLs, and related geography
clues to infer the affected Council district or districts. The raw chat is not
the production input. The source of truth is the accepted ledger in
`code/accepted_ai_geography_repair_ledger.csv`, which records the final district
assignment, confidence, evidence type, note, and source URLs for each promoted
repair.

Creates `council_land_use_ai_geography_accepted_repairs.csv`.
