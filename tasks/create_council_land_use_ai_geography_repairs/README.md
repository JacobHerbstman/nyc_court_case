# Create Council Land-Use Geography Repairs

Creates the accepted geography-repair file for Council land-use matters whose
affected districts could not be assigned from Legistar, ZAP, or rule-based
crosswalks.

For these rows, matter text, project identifiers, source URLs, and geography
clues were reviewed in ChatGPT to identify the likely affected Council district
or districts. Raw review transcripts are not used by the production pipeline.
The authoritative input for this task is
`code/accepted_ai_geography_repair_ledger.csv`, which records the accepted
district assignment, evidence, source notes, matter identifiers, and source URLs
for each accepted repair.
Field definitions and review caveats are documented in
`accepted_ai_geography_repair_ledger.md`.

This task does not perform AI inference or external review. It normalizes the
accepted ledger into a stable CSV for downstream vote-panel code.

Output: `council_land_use_ai_geography_accepted_repairs.csv`.
