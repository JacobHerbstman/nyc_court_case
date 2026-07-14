# Record ULURP CPC Source Corrections

This record-only production task owns the two reviewed ledgers that modify the
official Department of City Planning CPC report index.

- `ulurp_cpc_source_corrections.csv` corrects verified identifier, URL, date,
  and source-availability errors in indexed rows. These corrections prevent the
  corpus builder from attaching the wrong document or treating a documented
  source failure as a download failure. It feeds directly into
  `build_ulurp_cpc_report_corpus`.
- `ulurp_cpc_index_additions.csv` records certified reports and related lead
  narratives verified to be absent from the official index. It is needed to
  preserve reviewed omissions in the corpus universe and also feeds directly
  into `build_ulurp_cpc_report_corpus`.

The Makefile only verifies that the committed decisions exist; it does not
regenerate them.
