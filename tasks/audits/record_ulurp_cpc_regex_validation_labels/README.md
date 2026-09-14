# Record ULURP CPC Regex Validation Labels

This record-only audit task preserves two rounds of Codex's blind manual coding
of CPC reports whose own action code is `ZM`, `ZR`, or `ZS`. Both are
fixed-seed, proportionally decade-stratified random samples.

`ulurp_cpc_regex_validation_labels_codex.csv` contains the first 100 reports,
which were not in Jacob's 200-report training sample. Those labels were used to
revise the regex rules and are therefore a development sample.

`ulurp_cpc_regex_holdout_labels_codex.csv` contains a second 100 reports that
exclude both the training and development samples. Its labels were recorded
only after the revised rules were frozen, so it provides an out-of-sample test.

The labels use the same definitions as the human training codebook, with three
additional fields needed to evaluate every production regex measure:
`cb_opposition`, `restrictive_declaration`, and `points_of_agreement`.

Both files feed `audit_ulurp_cpc_regex_labels`. Their source hashes, report
identifiers, and sample membership are checked there against the production
corpus. The Makefile only verifies that the committed decisions exist; it does
not recreate manual judgments.
