# Create Council Land-Use Geography Review Ledgers

Stores the version-controlled geography review ledgers used by the Council
land-use decision pipeline.

Inputs are committed review ledgers. Some rows were reviewed in ChatGPT using
matter text, project identifiers, source URLs, and geography clues. Accepted
approval-side repairs are recorded in
`code/accepted_ai_geography_repair_ledger.csv`; field definitions and caveats
are in `accepted_ai_geography_repair_ledger.md`.

For nonapproval matters, the task stores the review queue and ChatGPT response
ledger as leads. Downstream verification decides which leads are supported by
official records or BBL-to-district checks.

This task does not run AI review. It normalizes the accepted repair ledger and
exposes the committed review ledgers for downstream tasks.

Outputs:

- `council_land_use_ai_geography_accepted_repairs.csv`
- `member_deference_nonapproval_geography_review_queue.csv`
- `member_deference_nonapproval_geography_chatgpt_review_responses.csv`
