---
title: "CPC OCR quality: is a rebuild worth restarting the run?"
date: "2026-09-27"
---

About three quarters of the reports completed in `full_sol_high_20260927` carry
reader notes about garbled or illegible OCR. Production text keeps each PDF's
embedded text layer and only runs Tesseract on nearly empty pages, so old city OCR
is never redone. Changing the reading text would change every packet and force a
new run. Jacob paused the run at about 18% to test this first.

`tasks/audits/audit_cpc_ocr_quality` sampled 34 completed reports from the pause
snapshot before any re-OCR was run: 29 flagged, 22 of them pre-1990, plus 5
controls, most with human-coded board votes or hearing speaker counts. That is 431
pages. It compared three texts:
- production text;
- Tesseract at 300 DPI with `--psm 3` on every page;
- blind Claude Opus 5.5 vision transcriptions of the 36 pages whose production text
  looked garbled or that readers named.

Tesseract at 300 DPI was no better than production text (dictionary share 0.872
against 0.882, and 0.654 against 0.664 on the garbled pages). Vision was much better
on those pages (0.827), but they are mostly maps, site plans, signature pages and
attachment forms.

All 11 human-coded board tallies, and the same 16 of 25 speaker tallies, were
findable in every version. The speaker misses are wording, not OCR. Where vision
recovered a tally from a degraded form, the same tally was already clean in the
report body. The readers' own measures on this flagged sample match or beat their
accuracy elsewhere.

Recommendation: resume the current run unchanged. A 300 DPI Tesseract rebuild would
take about 16 hours with no gain. Vision on flagged pages would cost roughly 200M
tokens of plan usage for text the readers do not need, and either would discard
1,662 completed readings.
