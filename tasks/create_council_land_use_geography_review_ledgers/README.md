# Create Council Land-Use Geography Review Ledgers

Stores the version-controlled geography review ledgers used by the Council
land-use decision pipeline.

For approval-side matters whose affected districts could not be assigned from
Legistar, ZAP, or rule-based crosswalks, matter text, project identifiers,
source URLs, and geography clues were reviewed in ChatGPT to identify likely
affected Council districts. The authoritative input is
`code/accepted_ai_geography_repair_ledger.csv`, which records the accepted
district assignment, evidence, source notes, matter identifiers, and source URLs
for each repair. Field definitions and review caveats are documented in
`accepted_ai_geography_repair_ledger.md`.

For nonapproval matters with unclear affected districts, this task also stores
the review queue and structured ChatGPT response ledger. Those responses are
geography leads, not final assignments. Downstream verification decides which
leads are supported by official records or BBL-to-district checks before they
enter the production decision panel.

This task does not perform AI inference or external review. It normalizes the
accepted repair ledger and exposes the committed review ledgers for downstream
tasks.

Outputs:

- `council_land_use_ai_geography_accepted_repairs.csv`
- `member_deference_nonapproval_geography_review_queue.csv`
- `member_deference_nonapproval_geography_chatgpt_review_responses.csv`
