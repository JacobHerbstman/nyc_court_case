# Audit CPC OCR quality

Tests whether better OCR is worth rebuilding the CPC reading text and restarting
the statement run `full_sol_high_20260927`, paused September 27 at about 18%.

Production text (`build_ulurp_cpc_report_corpus`) takes each PDF's embedded text
layer with `pdftotext`. It runs Tesseract (200 DPI, `--psm 6`) only on pages with
fewer than 50 embedded words, so old city OCR layers are never redone.

## Steps

1. `select_ocr_sample.py`: fixed before any re-OCR.
   - Draws 34 completed reports from the pause snapshot of the run status table;
     the snapshot's hash is saved with the sample.
   - 29 flagged reports (reader notes mention garbled, illegible, unreadable or OCR
     text) and 5 unflagged controls, all but 5 with human-coded board votes or
     hearing speaker counts.
   - 22 of the 29 flagged reports are pre-1990. 431 pages in all.
   - The cells and seed are in the script and Makefile.
2. `reocr_sample_pages.py`: for every page the reader received, saves
   - (a) the production reading text;
   - (b) Tesseract 5.5.1 at 300 DPI, `--psm 3`, on every page.
   It picks pages for vision from (a) and the reader notes only: dictionary share
   below 0.80, or a segment the notes call garbled. That gives 36 pages.
3. Vision transcriptions (c) of those 36 pages, by Claude Opus 5.5 subagents on the
   Claude plan (no API billing).
   - Transcribers saw only the 300 DPI page images.
   - Transcriptions are in
     `data_raw/cpc_ocr_quality_vision/claude_opus_5_5_20260927/` with their own README.
   - They were acquired outside Make, and this task reads them as frozen input.
4. `compare_ocr_versions.py` compares four versions of each report:
   - `a`: production text;
   - `b_all`: (b) on every page;
   - `b_flagged`: (b) on the 36 pages, (a) elsewhere;
   - `c_flagged`: (c) on the 36 pages, (a) elsewhere.

   It checks three things for each version:
   - whether both numbers of each human-coded tally appear within 250 characters of
     a vote or speaker mention;
   - the dictionary share (alphabetic tokens found in the macOS word list, a rough
     garble rate);
   - the pages carrying board-recommendation, hearing or consideration sections.
   `cpc_ocr_excerpts.md` shows the five worst production pages side by side.

## Findings

- **Board votes.** All 11 human-coded community board vote tallies are findable in
  every version, including production text.
- **Speaker tallies.** 16 of 25 are findable, and the same 16 in every version.
  - The 9 misses are readable in production text but phrased without a number: "the
    applicant appeared in favor", a list of speaker types, or a count that differs
    from the human code.
- **Tesseract 300 DPI, `--psm 3`, is not better.** Mean dictionary share by report:
  - production 0.882;
  - `b_all` 0.872;
  - on the 36 garbled pages, production 0.664 against 0.654.
  It is better by 0.05 or more on 15 pages and worse on 19 of 431.
- **Vision is much better on garbled pages** (0.664 to 0.827), but those pages are
  mostly maps, site plans, signature pages and attachment forms.
  - Where it recovers a tally from a degraded form ("AGAINST 2.5" becomes 25), the
    same tally is already clean in the CPC report body the reader has.
- **Sections are already readable.** Of 144 pages with a recommendation, hearing or
  consideration section, only 7 have production share below 0.80, and Tesseract
  lifts none of them to 0.85.
- **Reader measures on this flagged sample are not worse than elsewhere.**
  Agreement with human codes:

  | Measure | This sample | Full run |
  |---|---|---|
  | Borough President | 0.97 | 0.84 |
  | Council member | 0.93 | 0.91 |
  | Community board | 0.90 | 0.83 |
  | Civic group | 0.83 | 0.82 |
  | Support speakers | 0.83 | 0.77 |
  | Opposition speakers | 0.89 | 0.90 |

## Recommendation

Resume the current run unchanged. The garble the readers note is real but sits on
pages that do not carry the measured content, and the content they do carry is
duplicated in the report body.

Cost of the alternatives:
- **Tesseract on every page:** about 16 hours on this Mac (129,271 PDF pages at
  2.7 s each, 6 workers), or about 6 hours for pre-1990 pages. It gives no gain.
- **Vision on flagged pages:** about 8% of pages, roughly 10,000 corpus-wide. At the
  measured 16,000–20,000 subagent tokens per page, that is on the order of 200M
  tokens of plan usage.
- **Either one** would also discard the 1,662 completed readings.

## Limits

- The tally check looks for the human-coded numbers near a keyword; it can miss
  reworded tallies and can match stray numbers, equally in every version.
- The dictionary share is crude: clean text full of names scores about 0.90.
- Vision transcriptions are one model's reading, not ground truth.
- The sample is 34 reports chosen to be hard; it is not a population estimate.
