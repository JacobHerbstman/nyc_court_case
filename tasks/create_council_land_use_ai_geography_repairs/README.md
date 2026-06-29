# Create Council Land-Use Geography Repairs

Reads the reviewed geography repair ledger for Council land-use matters whose affected districts were not clear from Legistar, ZAP, or deterministic crosswalks.

For these rows, available matter text, project identifiers, source URLs, and related geography clues were reviewed with ChatGPT as a first-pass aid to identify the affected Council district or districts. The production input is not the raw AI response; it is the accepted ledger in `code/accepted_ai_geography_repair_ledger.csv`, which records the final district assignment, confidence, evidence type, note, and source URLs for each promoted repair.

Creates `council_land_use_ai_geography_accepted_repairs.csv`.
