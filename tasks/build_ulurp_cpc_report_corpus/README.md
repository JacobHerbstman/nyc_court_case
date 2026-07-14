# Build ULURP CPC Report Corpus

Builds the 1975-2025 CPC report corpus from the official Department of City
Planning report index. The task applies preserved source corrections, retains
certified ULURP reports and related narrative leads, downloads the canonical
PDFs, and extracts text with page-aware OCR where needed.

The manifest is the only tabular output. PDFs and extracted text are stored in
the two output subdirectories referenced by the manifest.
