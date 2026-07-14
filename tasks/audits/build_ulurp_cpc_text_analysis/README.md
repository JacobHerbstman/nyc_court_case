# Build ULURP CPC Text Analysis

This audit labels each comparable CPC-report narrative with deterministic,
section-aware text rules and produces the citywide and homeowner-tercile trend
plots. It reads the production CPC corpus directly and applies the committed
narrative-boundary and geography decisions from the source-validation and
text-analysis decision tasks.

The document-level label CSV is the only data output. One PDF contains the
all-report, non-PP, and ZM/ZR/ZS time series. The other contains the homeowner-
tercile series using the Figure 2 treatment and archived 2010 Council districts.
Both figures pool signal and document counts over the centered moving window
before calculating shares.

These labels remain exploratory measurements rather than a final deference
score. The record-only task supplies the narrative-boundary and geography
exceptions used here and separately preserves the calibration and validation
labels that document how the deterministic rules were reviewed.
