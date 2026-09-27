# Summarize CPC statement report measures

Derives report-level measures from the first-100 statement rows
(`extract_ulurp_cpc_statements`) and asks whether row-level errors found by the
source audit (`audit_ulurp_cpc_statements`) change them. The rules are in
`code/cpc_statement_measures.py`, shared by both scripts; measure names follow
the human codebook where one exists. `code/check_full_run_measures.py` applies
them to the full run's completed reports so far.

Outputs:
- `cpc_statement_report_measures.csv`: one row per report and measure (100 reports).
- `cpc_statement_measure_audit_sensitivity.csv`: the 20 audited reports, each measure
  from the original rows and after applying the audit. The audit version drops rows
  judged clear errors, then adds inventory statements the extraction missed or only
  partly captured, plus the inventory counterparts of the erroneous rows.
  A stricter version also drops rows the audit left unclear.
- `cpc_statement_measure_human_agreement.csv`: derived vs earlier human codes on the
  20 human-coded reports.
- `cpc_statement_full_run_human_agreement.csv`, `cpc_statement_full_run_retest.csv` and
  `cpc_statement_full_run_length.csv`: the full run so far. The files give agreement
  with human codes on every completed human-coded report and first-100 vs full-run
  values for reports read twice. They also give statement rows against report length.
  They are snapshots of a run in progress and rebuild as it grows.

Rules:
- Local actors are community boards, borough presidents and boards, elected
  officials, organizations, residents and unidentified hearing speakers, excluding the
  project team.
- Rows describing another project are ignored.
- An issue topic counts only when a local actor opposes, objects to, or asks for
  something about it. Mentions in the project description or in CPC's own findings
  do not count.
  The rules were compared on these same 20 reports before this one was chosen,
  so its agreement is somewhat optimistic.
- CPC hearing speakers, per side: the larger of any stated tally ("six speakers
  in favor") and the number of individually described speakers. A named group with
  a number ("five members of local art organizations") counts that many. Numbers of
  letters or petitions do not count. If the only tally row gives no number, the
  count is blank (unknown).
- Procedural response: a local request or concern answered by a row whose
  `procedural_action` is a study, monitoring/reporting, task force or
  outreach/consultation. Runs before September 27 have no such field and leave it
  blank.

Findings on the first 100, September 27:
- Applying the audit changes 7 of 300 report-measure values (10 in the strict
  version), and it does not raise agreement with the human codes (0.85 before,
  0.82 after, on the 109 overlapping cells).
- Agreement with the human codes, excluding speaker counts, is 0.88 (258 cells)
  and 0.92 where both coders agreed (48 cells). It is 0.82–1.00 for every measure
  except infrastructure/services (0.63).
- Speaker counts match exactly in 0.71 (support, 17 reports) and 0.89
  (opposition, 18 reports) of cases.
- The September 26 schema has no field for studies, monitoring or consultation, so
  procedural response is blank for that run.

Full run so far (1,662 completed reports, snapshot September 27):
- Agreement with human codes on 93 human-coded reports:
  - 0.91 council member position;
  - 0.84 Borough President request/opposition;
  - 0.82 civic group position;
  - 0.83 community board request/opposition;
  - 0.93 substantial local opposition.
- Speaker counts:
  - support: exact match 0.77, within one 0.93, correlation 0.97;
  - opposition: exact match 0.90, within one 0.95, correlation 0.82.
- Weakest measures:
  - environment/open space: 0.70;
  - infrastructure/services: 0.74;
  - approved over unresolved objection: 0.75.
- Procedural response with the new field is 0.80, the same as coding every report 0
  (0.81). It finds 11 of 17 human positives but also flags 12 others.
- Test-retest on the 98 first-100 reports read twice: 0.92–1.00 per measure,
  0.93 for support-speaker counts.
- Statement rows track report length closely: the correlation of their logarithms
  is 0.95, from about 7 rows for the shortest fifth of reports to 98 for the longest.
  No long report has a suspiciously low row count.
