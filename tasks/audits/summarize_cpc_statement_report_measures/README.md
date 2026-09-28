# Summarize CPC statement report measures

Derives report-level measures from the first-100 statement rows
(`extract_ulurp_cpc_statements`) and asks whether row-level errors found by the
source audit (`audit_ulurp_cpc_statements`) change them. The rules are in
`tasks/_lib/cpc_statement_measures.py`, shared with the spot check; measure names follow
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
  officials, organizations and institutions, residents and unidentified hearing
  speakers, excluding the project team.
- Council member and civic group positions follow the codebook's "opposes all or
  part": any oppose stance or concern is opposition; otherwise support, a request or
  a commitment is support.
- Civic groups follow Jacob's September 28 rulings:
  - They are civic, neighborhood, tenant and community organizations and unions,
    including their officers and unnamed community groups that clearly take a side.
  - Businesses count only as associations, such as a merchants association or a
    chamber of commerce.
  - Individual businesses, facility operators and institutions do not count.
- Rows describing another project are ignored.
- An issue topic counts only when a local actor opposes, objects to, or asks for
  something about it. Mentions in the project description or in CPC's own findings
  do not count.
  The rules were compared on these same 20 reports before this one was chosen,
  so its agreement is somewhat optimistic.
- CPC hearing speakers, per side, from hearing speakers' position rows only:
  - A stated tally ("six speakers in favor") wins; tallies on different hearing dates
    are added.
  - Otherwise, count the distinct named speakers, after collapsing name variants of
    one speaker. A named group with a number counts that many.
  - A tally with no number, or a plural group with no number, leaves the count blank.
  - Changed September 28. The earlier rule double counted speakers who had several
    rows.
  - The reader often does not record the hearing tally as a row, which limits any
    rule; see `tasks/audits/spot_check_cpc_statement_measures`.
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

Full run so far (2,285 completed reports, snapshot September 27, after the rule change):
- Agreement with human codes on 112 human-coded reports:
  - council member position 0.94, but the harder test is positive cases, the reports
    where a human coded an actual position: 8 of 12 right;
  - civic group position 0.84, positive cases 17 of 24;
  - Borough President request/opposition 0.85, positive cases 30 of 30;
  - community board request/opposition 0.84, positive cases 50 of 53;
  - substantial local opposition 0.94, positive cases 37 of 43.
- Speaker counts:
  - support: exact match 0.76, within one 0.90, correlation 0.96;
  - opposition: exact match 0.91, within one 0.96, correlation 0.83.
- Weakest measures:
  - environment/open space 0.70;
  - infrastructure/services 0.74;
  - revision/concession 0.75;
  - explicit local response 0.76;
  - approved over unresolved objection 0.77.
- Procedural response agrees 0.76, below the 0.77 from coding every report 0.
- Test-retest on the 98 first-100 reports read twice: 0.92–1.00 per measure.
- Statement rows track report length closely (log correlation 0.95).
