# Build ULURP CPC Report Usable Text Manifest

Builds a usable CPC report text manifest from the raw CPC report corpus.

Rows with directly extracted CPC report text are included as direct text rows.
Residual `C`-prefixed rows whose own CPC report URL was missing, but whose ZAP
project has sibling CPC report text that mentions the missing application stem,
are included through a `sibling_project_cpc_report` fallback. The raw download
manifest remains unchanged; this task only defines which text should be used for
analysis.
