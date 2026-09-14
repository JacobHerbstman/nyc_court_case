# Build ULURP CPC Report Corpus

Builds the 1975-2025 CPC report corpus from the official Department of City
Planning report index. The task applies preserved source corrections, retains
certified ULURP reports and related narrative leads, downloads the canonical
PDFs, and extracts text with page-aware OCR where needed.

The manifest is the only tabular output. PDFs and extracted text are stored in
the two output subdirectories referenced by the manifest.

This is a report-availability sample, not a complete project universe. Use
`build_zap_project_universe` for the project denominator, and retain projects
without a matched report. A missing report link or a blank bulk ULURP number
does not establish that a project never had an application number or report.
