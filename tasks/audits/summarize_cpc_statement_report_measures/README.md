# Summarize CPC statement report measures

Derives report-level measures from the first-100 statement rows
(`extract_ulurp_cpc_statements`) and asks whether row-level errors found by the
source audit (`audit_ulurp_cpc_statements`) change them. All rules are in
`code/build_cpc_statement_report_measures.py`; measure names follow the human
codebook where one exists.

Outputs:
- `cpc_statement_report_measures.csv`: one row per report and measure (100 reports).
- `cpc_statement_measure_audit_sensitivity.csv`: the 20 audited reports, each measure
  from the original rows and after applying the audit. The audit version drops rows
  judged clear errors, then adds inventory statements the extraction missed or only
  partly captured, plus the inventory counterparts of the erroneous rows.
  A stricter version also drops rows the audit left unclear.
- `cpc_statement_measure_human_agreement.csv`: derived vs earlier human codes on the
  20 human-coded reports.

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

Findings, September 27:
- Applying the audit changes 7 of 300 report-measure values (10 in the strict
  version), and it does not raise agreement with the human codes (0.84 before,
  0.80 after, on the 116 overlapping cells).
- Agreement with the human codes is 0.86 overall (275 cells) and 0.92 where both
  coders agreed (50 cells). It is 0.82–1.00 for every measure except
  infrastructure/services (0.63) and procedural response (0.53).
- The schema has no field for studies, monitoring or consultation, so procedural
  response is poorly defined from these rows. Only 3 of the 17 compared reports
  are human-coded positive.
