# Audit Official ULURP CPC Text Measurement

This task defines the document text that is comparable enough for substantive
measurement. The official CPC report remains the source unit, but the analysis
text ends at the report's first resolution heading. This retains the project
background, ULURP review, CB and BP recommendations, CPC hearing, and CPC
consideration while excluding resolution boilerplate and scanned appendices.

The narrative manifest preserves the source text path and exact character
boundary. It also defines three transparent universes:

- every distinct official certified CPC report;
- one comparable narrative per DCP-designated lead-report group, with exact
  duplicate narratives counted once;
- non-`PP` and `ZM`/`ZR`/`ZS` flags for planned robustness analyses.

No fuzzy matching or model judgment collapses reports. Related actions are
collapsed only when DCP marks one report as the lead for the same named project
and vote date, or when normalized narrative text is exactly identical.

Twenty-three page-rendered boundary exceptions are read from the record-only
validation task and locked to exact source-text hashes. They cover OCR-garbled
resolution cuts, visibly incomplete scans, a commissioner statement without
the majority report, and one report that explicitly delegates its analysis to
a companion. A changed extraction invalidates the recorded decision.

The deterministic decade sample exposes the narrative tail and the first text
excluded by the boundary. Its manual columns are intentionally blank pending
source review.
