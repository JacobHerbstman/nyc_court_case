# Record ULURP CPC Corrections

This record-only production task owns the reviewed corrections used to build
and summarize the Department of City Planning CPC report corpus.

- `ulurp_cpc_source_corrections.csv` corrects verified identifier, URL, date,
  and source-availability errors in indexed rows. These corrections prevent the
  corpus builder from attaching the wrong document or treating a documented
  source failure as a download failure. It feeds directly into
  `build_ulurp_cpc_report_corpus`.
- `ulurp_cpc_index_additions.csv` records certified reports and related lead
  narratives verified to be absent from the official index. It is needed to
  preserve reviewed omissions in the corpus universe and also feeds directly
  into `build_ulurp_cpc_report_corpus`.
- `ulurp_cpc_narrative_boundary_exceptions.csv` records hash-locked boundaries
  and exclusions for reports whose OCR or structure defeats the general
  narrative parser. It feeds `summarize_text_cpc_trends`.
- `ulurp_cpc_community_district_corrections.csv` fixes one reviewed official
  community-district value used by the homeowner-tercile summary. It feeds
  `summarize_text_cpc_trends`.

The Makefile only verifies that the committed decisions exist; it does not
regenerate them.
