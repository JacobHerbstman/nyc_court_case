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
