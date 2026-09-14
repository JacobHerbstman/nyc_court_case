# Build ULURP Corpus Spine

This task creates one application-level ULURP spine from the ZAP project data
for 1975-2025. It records application identifiers, project metadata, milestones,
and geography without downloading documents.

The output covers projects with a parsed application number and a reference
date in the requested range. Projects without a parsed number disappear when
the number list is expanded, including withdrawn and terminated projects whose
numbers are absent from the bulk export. It is therefore not the denominator
for project survival or approval analysis. `build_zap_project_universe` retains
the full public project list, including unknown scope and missing numbers.
The separate `build_ulurp_cpc_report_corpus` task builds the CPC report corpus.
