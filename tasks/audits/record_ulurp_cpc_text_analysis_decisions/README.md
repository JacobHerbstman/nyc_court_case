# Record ULURP CPC Text Analysis Decisions

This record-only task preserves the reviewed decisions behind the exploratory
CPC text labels and homeowner-tercile plots.

- `ulurp_cpc_kwic_calibration_labels.csv` records sentence-window true-positive
  decisions used to refine the deterministic signal rules. It feeds the older
  signal audit and remains the review evidence for `build_ulurp_cpc_text_analysis`.
- `ulurp_cpc_initial_event_labels.csv` records the first full-report event-coding
  exercise used to define opposition, requests, changes, commitments, and local
  causal response. It is validation evidence for the deterministic labels and
  future supervised models.
- `ulurp_cpc_event_validation_labels.csv` records the later stratified
  60-report validation pass using the finalized event definitions. It is the
  held-out validation evidence for the text measures and future models.
- `ulurp_cpc_community_district_corrections.csv` fixes the one reviewed official
  community-district value that would otherwise misassign a report spatially.
  It feeds directly into the homeowner-tercile plot in
  `build_ulurp_cpc_text_analysis`.

The Makefile verifies that the decision files exist. It does not regenerate
research judgments or call an LLM.
