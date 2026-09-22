# Build ULURP CPC Report Corpus

Builds the 1975-2025 CPC report corpus from the official Department of City
Planning report index. The task applies preserved source corrections, retains
certified ULURP reports, related narrative leads, and explicitly reviewed N
companions, downloads the canonical
PDFs, and extracts text with page-aware OCR where needed.

The manifest is the only tabular output. PDFs and extracted text are stored in
the two output subdirectories referenced by the manifest.

`record_ulurp_cpc_source_corrections/output/ulurp_cpc_companion_reports.csv`
supplies reviewed certified/N pairs and explicit download URLs. These indexed
N reports enter with `corpus_role=related_project_narrative_companion`, which
preserves their source records without making them independent analysis rows.
The current NYC domain is recorded explicitly for the newly downloaded reports;
the existing corpus's cached sources and URLs are retained. The label producer
checks the hash of the certified text that establishes each referral. A standard
data report accompanies the manifest.

This is a report-availability sample, not a complete project universe. Use
`build_zap_project_universe` for the project denominator, and retain projects
without a matched report. A missing report link or a blank bulk ULURP number
does not establish that a project never had an application number or report.

## Attachment OCR repair, September 22

The page OCR scan now continues through the end of each PDF. The earlier rule
stopped at the first CPC resolution, leaving scanned board recommendations and
other attachments without extracted text. The resolution page remains a
reported boundary; it no longer limits OCR. Pages with fewer than 50 embedded
words are examined, and remaining short pages are retained and flagged. This
screen also catches maps and blank sheets, so a short-page count is not a count
of substantive omissions.

Extraction writes task-local temporary text files. The build validates all
usable sources before publishing repaired text and the manifest. The original
PDF cache is reused. The pre-repair manifest, all extracted text and page-level
fingerprints are preserved in `data_raw/cpc_text_repair/20260922_before/`.
The existing text-measurement audit compares the two vintages and reports both
source-PDF and focal-narrative counts. Frozen model-reading trials remain
historical records; they do not silently acquire the new attachments.

`python3 -m unittest test_cpc_page_ocr.py` checks that an embedded or OCR-detected
resolution cannot stop attachment recovery and that OCR timeouts remain explicit.
