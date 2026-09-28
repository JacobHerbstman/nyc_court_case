# Spot check of CPC statement measures

A human check of the report-level measures built from the full statement run
(`full_sol_high_20260927`), weighted toward reports where a council member or civic
group took part.

## Steps

1. **Sample** (`select_spot_check.py`): 45 completed reports of 40 pages or fewer,
   with no earlier human coding and not in the first 100.
   - Drawn once on September 27 and frozen in `code/cpc_spot_check_sample.csv`.
     The script refuses to redraw over it.
   - 15 have a derived council member position.
   - 15 more have a derived civic group position.
   - 5 mention a council member in the text but have no derived position.
   - 10 are otherwise random.
   - 512 pages in all; 19 of the reports are pre-1990.
2. **First pass**: five Claude Opus 5.5 subagents on the Claude plan coded 7
   measures for 9 reports each, from the reading text and `code/spot_check_codebook.md`.
   - The codebook holds the human codebook's definitions.
   - The coders were blind to the statement rows, the derived measures and the
     human coding.
   - Each value has an exact quote with its segment, and a note.
   - Saved unedited in `data_raw/cpc_spot_check_first_pass/claude_opus_5_5_20260927/`.
3. **Workbook** (`build_spot_check_workbook.py`): `output/cpc_spot_check_review.xlsx`
   and `output/cpc_spot_check_items.csv`.
   - One row per report and measure, with the first-pass value, quote, page and note.
   - The derived value sits beside it, with the statement rows behind it.
   - Every quote is checked against its segment.
4. **Jacob's review**: mark `jacob_agrees` Y or N for each first-pass value, and give
   the right value when N. Only the Review sheet is needed; the rest are on an optional
   Other items sheet. The Review sheet has 72 items:
   - all 47 where the first pass and derived value differ;
   - 25 seeded agreements (15 council member or civic group, 10 other), to check the
     two are not both wrong.
   - The answers are then saved as a committed table in `code/`. That table becomes
     the reference for scoring both the first pass and the derived measures.
   - The build refuses to overwrite a workbook that already has answers.

## First pass against derived values (before review)

The two agree on 268 of 315 items:

| Measure | Agree |
|---|---|
| Borough President | 44 of 45 |
| Council member | 43 of 45 |
| Community board | 42 of 45 |
| Substantial local opposition | 42 of 45 |
| Civic group | 36 of 45 |
| Opposition speakers | 36 of 45 |
| Support speakers | 25 of 45 |

Speaker disagreements mostly come from the derived counting rule:
- It counts a speaker's several hearing rows separately.
- It misses tallies summed over hearing dates or given in a companion report.

## Judgment calls the first-pass coders flagged

These are for Jacob's review:
- **Unnamed groups:** the coders were inconsistent. Some counted groups the report
  describes but does not name ("a block association") as civic groups; others did
  not, following "named" strictly.
- **Operators and sponsors:** facility operators, sponsor institutions and a labor
  union were counted as civic groups in some reports but not others.
- **Council member mentions:** a council member named only on a cc line, or second-hand
  ("the CB reports the Council Member supports"), was coded none in most cases.
- **Split tallies:** where hearing tallies are split, e.g. for the program vs for the
  site, or across companion reports, the coders' choices are explained in the notes.

## Review results, September 28

Jacob reviewed 70 of the 72 Review items. He agreed with the first pass on 67 and
disagreed on 3.

The other 2 are the landfill report's speaker counts. The tallies are printed in the
companion rezoning report (C 790065 ZMQ, p.1), which was heard on the same dates.
Claude checked them on the page image: 5+2 in favor and 11+11 against, so 7 and 22.
They are recorded as `claude_page_check` in `code/jacob_spot_check_review.csv`.

The workbook now has a `quote_pdf_url` column, because a quote can come from a
companion report in the same bundle. The build fills saved answers back in.

`output/cpc_spot_check_scores.csv` scores both methods against the reviewed values:
- **First pass:** right on 69 of 72 reviewed items. Its 3 misses are civic group calls.
  Estimated accuracy on all 315 items is 0.99.
- **Derived measures:** right on 3 of the 47 disagreements. All 25 sampled agreements
  were confirmed. Estimated accuracy on all 315 items is 0.86.
  - Speaker counts: wrong in all 29 of their disagreements (20 support, 9 opposition).
  - Civic groups: wrong in 6 of 9.
  - Community board, substantial opposition, council member, Borough President:
    wrong in the rest (3, 3, 2 and 1).

Jacob's civic group rulings, added to the codebook:
- Businesses are not civic groups.
- Facility operators, such as a senior center, are not civic groups.
- Community groups count when they clearly take a side, even if the report does not
  name them.

## After the September 28 rule changes

The civic group and speaker rules in `tasks/_lib/cpc_statement_measures.py` changed.
The reviewed items stay fixed as the scored set, and `review_group` records the
original strata.

`output/cpc_spot_check_scores.csv` scores every item against a reference: the
reviewed value where reviewed, otherwise the first pass. All 25 sampled agreements
were confirmed.

Derived measures are right on 276 of 315 items; the first pass on 312. By measure,
out of 45 reports:

| Measure | Derived correct |
|---|---|
| Borough President | 44 |
| Council member | 43 |
| Community board | 42 |
| Civic group | 42 (was 39) |
| Substantial local opposition | 42 |
| Opposition speakers | 38 (3 blank) |
| Support speakers | 25 (9 blank) |

Speaker counts stay weak because the reader usually does not record the hearing
tally ("There were 13 appearances in favor") as a statement row. No rule can
recover that from the rows.

Two answers to confirm with Jacob:
- He agreed that the Nuestros Niños operator supports (3a2361ba02feb156d070), but
  ruled that operators are not civic groups.
- He agreed with none for U Thant Park, where three representatives of unnamed
  neighborhood groups supported (5ccd0074693eefe3c7ba), but ruled that unnamed
  groups that clearly take a side count.

## Judging from Sol's rows, September 28

Can a model answer the codebook from Sol's statement rows instead of re-reading the
PDF? `write_judge_packets.py` writes two packets per report:
- `rows_only`: the statement rows;
- `rows_hearing`: the same rows plus the report's hearing pages.

Blind Claude subagents judged each packet with the codebook and Jacob's rulings. See
`data_raw/cpc_spot_check_row_judging/claude_opus_5_5_20260928/`.

Jacob then corrected two answers:
- Nuestros Niños is an operator, so none.
- The unnamed U Thant neighborhood groups obviously support, so support.

After those corrections, correct items out of 315, against the reviewed value or else
the first pass:

| Method | Correct |
|---|---|
| rows_hearing | 312 |
| First pass (direct reading of the full report) | 310 |
| Current rules | 278 |
| rows_only | 270 |

On the 72 items Jacob reviewed:

| Method | Correct |
|---|---|
| rows_hearing | 70 |
| First pass | 67 |
| rows_only | 59 |
| Current rules | 38 |

`rows_hearing` gets both speaker counts right in all 45 reports. `rows_only` cannot:
the tallies are usually not in the rows.

Its 2 remaining reviewed misses are Henry Street Settlement's civic group position and
a Borough President request reported only second-hand.

The workbook is `.PRECIOUS` in the Makefile, because the shared `.DELETE_ON_ERROR`
would otherwise delete it when the build refuses to overwrite unsaved answers.

Scale estimates, from the reports completed so far:
- The packets used here, with every row field plus hearing pages, would be about 85M
  tokens for the corpus.
- A slim packet (actor, roles, type, stance, response, stage and summary, plus hearing
  pages) would be about 28M tokens.
- Re-reading every report in full would be about 67M tokens.
- The slim packet is untested.

## Slim packets, September 28

`rows_hearing_slim` packets hold the hearing pages plus a compact table of the row
fields the measures use: statement_id, actor, roles, project team, type, stance,
component, votes, response and links, stage and summary. They drop quotes, notes and
repeated field names, so they are about half the size of `rows_hearing` (median
13,107 characters against 27,614).

Results, blind Claude subagents with the same instructions:

| Method | All 315 items | Jacob's 72 reviewed items |
|---|---|---|
| rows_hearing_slim | 310 | 68 |
| rows_hearing | 312 | 70 |
| First pass | 310 | 67 |

- The slim version is right on both speaker counts in all 45 reports.
- Its two extra reviewed misses are borderline judgment calls:
  - a council member's general request relayed by the community board;
  - whether five residents' objections count as substantial opposition.
- Subagent usage is about 7,400 tokens per report, against about 12,600 for
  `rows_hearing`: 333,000 against 567,000 tokens for the 45 reports. At that rate the
  full corpus would be roughly 67M subagent tokens.
