all: output/2026-09-26-member-deference-bug-fixes.pdf output/2026-09-26-cd-homeownership-decade-bins-drivers.pdf output/2026-09-22-cpc-llm-pilots.pdf output/2026-09-22-cpc-attachment-repair.pdf output/2026-09-15-cpc-human-reconciliation.pdf output/2026-09-15-withdrawal-project-types.pdf output/2026-09-14-cpc-sample-restrictions.pdf output/2026-09-14-regex-strengthening.pdf output/2026-09-14-zap-universe.pdf output/2026-09-14-workspace-checkpoint.pdf output/2026-09-14-regex-comparison.pdf

output/2026-09-26-member-deference-bug-fixes.pdf: 2026-09-26-member-deference-bug-fixes.md Makefile | output
	pandoc $< --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-26-cd-homeownership-decade-bins-drivers.pdf: 2026-09-26-cd-homeownership-decade-bins-drivers.md input/cd_homeownership_long_units_event_coefficients_raw_units_decade_bins.pdf Makefile | output
	pandoc $< --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-22-cpc-llm-pilots.pdf: 2026-09-22-cpc-llm-pilots.md Makefile | output
	pandoc $< --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-22-cpc-attachment-repair.pdf: 2026-09-22-cpc-attachment-repair.md input/cpc_attachment_findings.md Makefile | output
	pandoc 2026-09-22-cpc-attachment-repair.md input/cpc_attachment_findings.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-15-cpc-human-reconciliation.pdf: 2026-09-15-cpc-human-reconciliation.md input/ulurp_cpc_reconciliation_findings.md Makefile | output
	pandoc 2026-09-15-cpc-human-reconciliation.md input/ulurp_cpc_reconciliation_findings.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-15-withdrawal-project-types.pdf: 2026-09-15-withdrawal-project-types.md \
	input/zap_withdrawal_action_findings.md input/zap_withdrawals_by_action.pdf \
	input/zap_withdrawals_by_action_homeowner_tercile.pdf \
	input/zap_withdrawals_by_action_homeowner_tercile_ma.pdf Makefile | output
	pandoc 2026-09-15-withdrawal-project-types.md input/zap_withdrawal_action_findings.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-14-cpc-sample-restrictions.pdf: 2026-09-14-cpc-sample-restrictions.md input/cpc_selection_findings.md Makefile | output
	pandoc 2026-09-14-cpc-sample-restrictions.md input/cpc_selection_findings.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-14-regex-strengthening.pdf: 2026-09-14-regex-strengthening.md input/ulurp_cpc_regex_coverage.md Makefile | output
	pandoc 2026-09-14-regex-strengthening.md input/ulurp_cpc_regex_coverage.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-14-regex-comparison.pdf: 2026-09-14-regex-comparison.md 2026-09-14-regex-comparison-checks.md Makefile | output
	pandoc 2026-09-14-regex-comparison.md 2026-09-14-regex-comparison-checks.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-14-workspace-checkpoint.pdf: 2026-09-14-workspace-checkpoint.md Makefile | output
	pandoc $< --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-14-zap-universe.pdf: 2026-09-14-zap-universe.md input/zap_universe_findings.md input/zap_withdrawals_over_time.pdf input/zap_withdrawals_by_homeowner_tercile.pdf Makefile | output
	pandoc 2026-09-14-zap-universe.md input/zap_universe_findings.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

input/cpc_attachment_findings.md: ../tasks/audits/audit_official_ulurp_cpc_text_measurement/output/cpc_attachment_findings.md | input
	ln -sf ../$< $@

input/ulurp_cpc_reconciliation_findings.md: ../tasks/audits/audit_ulurp_cpc_regex_labels/output/ulurp_cpc_reconciliation_findings.md | input
	ln -sf ../$< $@

input/cd_homeownership_long_units_event_coefficients_raw_units_decade_bins.pdf: ../tasks/estimate_cd_homeownership_long_units_event_study/output/cd_homeownership_long_units_event_coefficients_raw_units_decade_bins.pdf | input
	ln -sf "../$<" "$@"

input/zap_universe_findings.md: ../tasks/audits/audit_zap_universe_coverage/output/zap_universe_findings.md | input
	ln -sf "../$<" "$@"

input/zap_withdrawal_action_findings.md: ../tasks/audits/audit_zap_universe_coverage/output/zap_withdrawal_action_findings.md | input
	ln -sf "../$<" "$@"

input/zap_withdrawals_by_action.pdf: ../tasks/audits/audit_zap_universe_coverage/output/zap_withdrawals_by_action.pdf | input
	ln -sf "../$<" "$@"

input/zap_withdrawals_by_action_homeowner_tercile.pdf: ../tasks/audits/audit_zap_universe_coverage/output/zap_withdrawals_by_action_homeowner_tercile.pdf | input
	ln -sf "../$<" "$@"

input/zap_withdrawals_by_action_homeowner_tercile_ma.pdf: ../tasks/audits/audit_zap_universe_coverage/output/zap_withdrawals_by_action_homeowner_tercile_ma.pdf | input
	ln -sf "../$<" "$@"

input/cpc_selection_findings.md: ../tasks/audits/audit_official_ulurp_cpc_text_measurement/output/cpc_selection_findings.md | input
	ln -sf "../$<" "$@"

input/ulurp_cpc_regex_coverage.md: ../tasks/audits/audit_ulurp_cpc_regex_labels/output/ulurp_cpc_regex_coverage.md | input
	ln -sf "../$<" "$@"

input/zap_withdrawals_over_time.pdf: ../tasks/audits/audit_zap_universe_coverage/output/zap_withdrawals_over_time.pdf | input
	ln -sf "../$<" "$@"

input/zap_withdrawals_by_homeowner_tercile.pdf: ../tasks/audits/audit_zap_universe_coverage/output/zap_withdrawals_by_homeowner_tercile.pdf | input
	ln -sf "../$<" "$@"

link-inputs: input/cd_homeownership_long_units_event_coefficients_raw_units_decade_bins.pdf input/cpc_attachment_findings.md input/ulurp_cpc_reconciliation_findings.md input/zap_universe_findings.md input/ulurp_cpc_regex_coverage.md input/zap_withdrawals_over_time.pdf input/zap_withdrawals_by_homeowner_tercile.pdf input/cpc_selection_findings.md input/zap_withdrawal_action_findings.md input/zap_withdrawals_by_action.pdf input/zap_withdrawals_by_action_homeowner_tercile.pdf input/zap_withdrawals_by_action_homeowner_tercile_ma.pdf

input output:
	mkdir -p $@

include ../tasks/generic.make
