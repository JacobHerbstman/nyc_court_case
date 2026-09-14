# Audit ULURP CPC Regex Labels

This audit compares the production deterministic CPC text measures with three
manually coded samples. Jacob's 200 reports provide in-sample calibration. A
first 100-report Codex sample was used to revise the regex rules and is a
development sample. A second 100-report Codex sample was read only after those
rules were frozen and is the holdout test.

Both Codex samples draw randomly within decade, in proportion to the available
corpus, from reports whose own action code is `ZM`, `ZR`, or `ZS`. The holdout
also excludes the development sample. Fixed seeds and source-hash checks make
sample membership reproducible. The audit checks the extracted source hash
rather than a parser-derived narrative hash, so revised section or boundary
rules can be tested against the same hand coding. The disagreement outputs
preserve the manual evidence needed to diagnose missed context rather than
reporting agreement alone.
`unclear` manual values are excluded. Count agreement treats an unparsed regex
count as a miss and also reports agreement among counts that were parsed. The
project and development-direction fields are not compared because the
production regex task does not produce them.

Outputs:

- `ulurp_cpc_regex_training_agreement.csv`
- `ulurp_cpc_regex_validation_agreement.csv`
- `ulurp_cpc_regex_validation_disagreements.csv`
- `ulurp_cpc_regex_holdout_agreement.csv`
- `ulurp_cpc_regex_holdout_disagreements.csv`
- `ulurp_cpc_tyler_comparison_checks.md`

The Tyler comparison checks describe our current sample, reproduce two actual
report examples from his August 28, 2026 PDF, summarize the existing Codex
holdout, and run five controlled snippets through our actual counting functions.
They execute only our parser. Tyler's implementation and row-level extraction
output were not available, so these checks do not measure cross-parser agreement.
The substantive comparison is in `logbook/2026-09-14-regex-comparison.md`.
