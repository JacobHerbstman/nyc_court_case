---
title: "CPC measures: positive-case misses, rule fixes, and a spot check"
date: "2026-09-27"
---

The Codex session running `full_sol_high_20260927` flagged two problems:
- a council member opposition missing for 610 Lexington Avenue;
- council agreement with human codes inflated by the many reports with no council
  member at all.

On reports where a human coded an actual position, the derived measures were right
for 6 of 12 council members and 10 of 24 civic groups. Nearly every miss was a rule
problem, not a reading problem: the reader had recorded the member or group.

Two rules in `tasks/_lib/cpc_statement_measures.py` changed:
- **Council member and civic group positions** now follow the codebook's "opposes all
  or part". A concern counts as opposition.
- **Civic groups** now include institutions and officers of named organizations,
  whatever the reader marked as their speaking role.

With 112 human-coded reports, positive cases rose to 8 of 12 for council members and
17 of 24 for civic groups. Test-retest stayed at 0.99–1.00. The rules were fixed on
the same reports, so this gain is optimistic.

`tasks/audits/spot_check_cpc_statement_measures` froze 45 fresh completed reports,
weighted toward council member and civic group activity. Blind Claude Opus 5.5
subagents coded seven measures from the report text, with a quote for each value.
The review workbook sets each first-pass value beside the derived value.

Before Jacob's review, the first pass and the derived values agree on 268 of 315
items. They agree on 43 of 45 council positions, but only 36 of 45 civic groups and
25 of 45 support-speaker counts. The speaker gaps mostly come from the derived
counting rule, not the reading. Jacob's answers will be saved as a committed table
and used to score both.
