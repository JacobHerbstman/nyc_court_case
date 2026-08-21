# Audit ULURP CPC Regex Labels

This audit compares the production deterministic CPC text measures with
Jacob's 200 completed report labels. It reports field-level agreement,
precision, recall, exact-count coverage, and exact agreement when a count is
parsed.

These are calibration statistics, not an out-of-sample validation. Tyler's
independent labels are intentionally absent and can be added later as the
validation sample.

Output:

- `ulurp_cpc_regex_training_agreement.csv`
