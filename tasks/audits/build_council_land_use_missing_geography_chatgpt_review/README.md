# Council Land-Use Missing Geography Review

Builds review packets for Council land-use vote records whose affected Council
districts could not be read from the structured records.

The task prepares batches for ChatGPT, parses the responses, and writes review
files for source checking. ChatGPT output is not final data. A district enters
the analysis only after it is recorded in the manual verdict ledger or copied
into the accepted geography repair ledger used by the production workflow.

Main inputs:

- `council_land_use_missing_geography_roll_call_repair_queue.csv`
- ChatGPT response ledgers in `code/`
- `council_land_use_missing_geography_manual_verdicts.csv`

Main outputs:

- prompt batches in `output/batches/`
- parsed ChatGPT response files
- candidate repair files
- human verification lists
- `council_land_use_missing_geography_human_verified_repairs.csv`

Use this task to document the first-pass review, not to make analysis-ready
geography assignments by itself.
