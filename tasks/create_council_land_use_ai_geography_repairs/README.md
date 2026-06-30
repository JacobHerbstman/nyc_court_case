# Create Council Land-Use Geography Repairs

Creates the accepted geography-repair file for Council land-use matters whose
affected districts were not clear from Legistar, ZAP, or deterministic
crosswalks.

For these rows, matter text, project identifiers, source URLs, and geography
clues were reviewed with ChatGPT to identify the likely affected Council
district or districts. The raw chat is not a production input. The production
source of truth is `code/accepted_ai_geography_repair_ledger.csv`, which records
the accepted district assignment, evidence, source notes, matter identifiers,
and source URLs for each promoted repair.
Field definitions and review caveats are documented in
`accepted_ai_geography_repair_ledger.md`.

This task does not call ChatGPT. It normalizes the committed ledger so
downstream vote-panel code reads a stable project file.

Creates `council_land_use_ai_geography_accepted_repairs.csv`.

Runtime: under 1 second.
