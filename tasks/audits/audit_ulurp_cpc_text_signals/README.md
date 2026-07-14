# ULURP CPC Text Signal Audit

This audit builds exploratory text signals from the corrected official CPC-report corpus for ULURP applications from 1975 through 2025. It measures only the comparable analysis narratives defined upstream: resolution boilerplate is excluded, DCP-designated lead groups are counted once, and exact duplicate narratives are counted once.

The task is deterministic. It does not call an LLM or an API. It parses report text into broad sections, removes high-frequency boilerplate sentences, applies hand-tuned keyword and context rules, and then runs a deterministic labeler over signal candidates before aggregating yearly rates.

`ulurp_cpc_text_signal_document.csv` preserves the same filtered rules as one binary indicator per comparable narrative. It is the document-level input for audit-only geographic splits; the other outputs aggregate those indicators over time.

The main judgment calls are in `code/label_ulurp_cpc_text_signal_kwic.py`. The calibration labels behind those rules are preserved separately in `tasks/audits/record_ulurp_cpc_text_analysis_decisions/output/ulurp_cpc_kwic_calibration_labels.csv` and enter this task through `input/manual_ulurp_cpc_text_signal_kwic_labels.csv`.

Important interpretation limits:

- The yearly outputs are exploratory measures, not final deference scores.
- The annual denominator is unique comparable narratives, not ZAP application rows or physical report files.
- The manual labels are a small adjudication set used to refine the rules; they are not a full hand-coded corpus.
- Council and Borough President references are deliberately filtered to avoid procedural filing, referral, and approval boilerplate.
- Revision and concession signals are deliberately conservative: requested zoning relief, special-permit modification language, date revisions, and generic plan amendments are not treated as negotiated concessions unless the surrounding text shows review-stage change, conditions, or applicant commitments.

Run `make` from `code/` to rebuild the audit outputs.
