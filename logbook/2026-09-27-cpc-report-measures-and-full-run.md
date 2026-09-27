---
title: "CPC statement rows: report-level measures, and the full run"
date: "2026-09-27"
---

The first-100 audit found row-level errors in actor attribution, stage,
component-specific support and request versus adoption. Before changing the
reader instructions, we checked whether those errors matter for the report-level
measures the research will use. `tasks/audits/summarize_cpc_statement_report_measures`
derives fifteen measures from the rows, following the human codebook
definitions: local opposition, local requests, revisions, explicit responses,
community board and Borough President requests, council member and civic group
positions, and five issue topics.

Applying the audit to the twenty audited reports changes 7 of 300 report-measure
values, or 10 if rows the audit left unclear are also dropped. The corrected
rows agree slightly less with the earlier human codes (0.80 against 0.84 on 116
overlapping cells). Four of the seven changes come from one report whose
community board was also the applicant, which is a definitional question.
Row-level errors of this size do not move report-level answers, so the audit's
instruction fixes were not made.

Derived measures agree with the human codes on 86% of 275 cells, and on 92% of
the 50 cells where both coders agreed. The first issue-topic rule counted any
mention outside the project description. Almost every report touches the
environment in its environmental review, so agreement was 39–58%. Counting a
topic only when a local actor opposes, objects to, or asks for something about
it raises agreement to 84–100% for four of five topics. That rule matches the
codebook's intent, but it was chosen after comparing rules on the same twenty
reports. Infrastructure and services remains weak (0.63).

Procedural response (0.53) could not be built from the rows: the schema had no
field for studies, monitoring, task forces or consultation, and only three of
seventeen compared reports were human-coded positive. Jacob approved adding a
`procedural_action` field and reading every report, including the first 100,
once under the new version. That avoids re-reading the corpus later.

The full run is `full_sol_high_20260927` (GPT-6 Sol, high reasoning).
`codex exec` with the installed CLI (0.144.6) is rejected for this model on the
ChatGPT plan, so the run uses the Codex app route, as the first hundred did.
Reading order is fixed in advance: the first hundred, then the other 8,872
single-packet reports in seeded random order, so a partial run is a random
sample, then the 91 split reports. A human spot check of request and adoption
rows is still planned, after much of the corpus is read.
