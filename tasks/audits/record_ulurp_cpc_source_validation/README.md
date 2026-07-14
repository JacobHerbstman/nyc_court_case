# Record ULURP CPC Source Validation

This record-only audit task owns five reviewed ledgers used to establish that
the corpus contains the intended CPC reports and readable substantive text.

- `official_ulurp_cpc_document_validation_labels.csv` records document-identity
  and readability checks for the stable 100-per-decade certified-report sample.
  It is needed as preserved evidence that sampled PDFs and extracted text match
  the official index, but it does not override the production corpus.
- `official_ulurp_cpc_source_exception_labels.csv` resolves reports whose
  application header, grouped-report range, or extracted text required direct
  source review. It feeds into `audit_official_ulurp_cpc_corpus`.
- `official_ulurp_cpc_narrative_boundary_exceptions.csv` records hash-locked
  manual boundaries and exclusions where OCR or report structure defeats the
  general narrative parser. It feeds into `audit_official_ulurp_cpc_text_measurement`
  and `build_ulurp_cpc_text_analysis`.
- `official_ulurp_cpc_short_page_validation.csv` confirms that pages left with
  fewer than 50 words after OCR are genuinely sparse pages rather than missed
  prose. It feeds into `audit_official_ulurp_cpc_corpus`.
- `official_ulurp_cpc_external_reference_exclusions.csv` resolves apparent CPC
  omissions found in external Council and ZAP records as transcription errors,
  withdrawals, or actions outside this corpus. It feeds into
  `audit_official_ulurp_cpc_corpus`.

The Makefile only verifies that the committed decisions exist; it does not
regenerate them or call an LLM.
