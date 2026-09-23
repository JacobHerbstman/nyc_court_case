# Record ULURP CPC Source Validation

This record-only audit task owns reviewed ledgers used to establish that
the corpus contains the intended CPC reports and readable substantive text.

- `official_ulurp_cpc_document_validation_labels.csv` records document-identity
  and readability checks for the stable 100-per-decade certified-report sample.
  It is needed as preserved evidence that sampled PDFs and extracted text match
  the official index, but it does not override the production corpus.
- `official_ulurp_cpc_source_exception_labels.csv` resolves reports whose
  application header, grouped-report range, or extracted text required direct
  source review. It feeds into `audit_official_ulurp_cpc_corpus`.
- `official_ulurp_cpc_short_page_validation.csv` confirms that pages left with
  fewer than 50 words after OCR are genuinely sparse pages rather than missed
  prose. It feeds into `audit_official_ulurp_cpc_corpus`.
- `official_ulurp_cpc_external_reference_exclusions.csv` resolves apparent CPC
  omissions found in external Council and ZAP records as transcription errors,
  withdrawals, or actions outside this corpus. It feeds into
  `audit_official_ulurp_cpc_corpus`.

- `cpc_selection_reviews.csv` records the September 14, 2026 review of 22
  N-referral candidates and five duplicate-group concerns. Twenty passages
  explicitly refer readers to excluded N reports; two are examples of nearby
  language that does not establish such a referral. Decisions describe what the
  inspected passage establishes, not a blanket instruction to include a source
  or merge observations. The text-measurement audit consumes this ledger.
  After the September 22 attachment OCR repair, eight reviewed sources had new
  full-text fingerprints. Their cited pages and narratives are byte-identical
  before and after the repair, and C 890406/890407 PPQ still have identical
  text, so only `source_text_sha256` was updated; decisions are unchanged.

The Governors Island referral was additionally corroborated by downloading
`https://www.nyc.gov/assets/planning/download/pdf/about/cpc/130189a.pdf` on
September 14, 2026, converting it with Docling, and viewing both reports' first
pages. The indexed `www1.nyc.gov` URL returned HTTP 403; the current `www.nyc.gov`
URL succeeded. That review-only PDF is preserved unchanged at
`data_raw/cpc_selection_review_20260914/130189a.pdf`; its URL and SHA-256 are in
the ledger. It was manually acquired for adjudication. The production corpus
now acquires its own copy using the reviewed companion ledger; that ledger
promotes the 20 explicit referrals while retaining this audit's original evidence.
The certified source PDFs and texts used for all
other decisions remain in the existing corpus cache.

The Makefile only verifies that the committed decisions exist; it does not
regenerate them or call an LLM.
