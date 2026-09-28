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

Review results, September 28:
- **Review.** Jacob reviewed 70 of the 72 priority items and agreed with the blind
  first pass on 67. The other 2, the landfill speaker counts, were confirmed on the
  page image of the companion rezoning report heard on the same dates.
- **First pass:** right on 69 of 72 reviewed items. The misses are all civic group
  calls. Estimated accuracy on all 315 items is 0.99.
- **Derived measures:** right on only 3 of the 47 disagreements. Estimated accuracy on
  all 315 items is 0.86.
  - Speaker counts are wrong in all 29 of their disagreements; civic groups in 6 of 9.
  - Council member positions were rarely at issue (2 items). Council members took
    part in few of these reports.
- **Rulings** (added to the spot-check codebook):
  - businesses and facility operators are not civic groups;
  - community groups that clearly take a side count even when the report does not
    name them;
  - hearing counts printed in a companion report heard on the same dates count for
    the whole bundle.
- **Conclusion.** The errors are in turning rows into measures, not in reading.
  Reading a report directly for these measures is close to right.
