# Create Council Land-Use Geography Review Ledgers

Stores the reviewed geography corrections used by the Council land-use decision
pipeline.

Inputs are committed review ledgers. Some rows were first reviewed with
ChatGPT using matter text, project identifiers, source URLs, and geography
clues. The accepted approval-side corrections are recorded in
`code/accepted_ai_geography_repair_ledger.csv`; field definitions and caveats
are in `accepted_ai_geography_repair_ledger.md`.

For nonapproval matters, the task stores the review list and ChatGPT response
ledger as leads. Downstream verification decides which leads are supported by
official records or BBL-to-district checks.

This task does not run AI review. It standardizes the accepted repair ledger and
makes the committed review files available to downstream tasks.

Outputs:

- `council_land_use_ai_geography_accepted_repairs.csv`
- `member_deference_nonapproval_geography_review_queue.csv`
- `member_deference_nonapproval_geography_chatgpt_review_responses.csv`
