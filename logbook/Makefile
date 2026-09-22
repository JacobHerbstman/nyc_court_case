all: output/2026-09-22-cpc-attachment-repair.pdf output/2026-09-22-sol-expansion.pdf output/2026-09-22-sol-human-validation.pdf output/2026-09-22-luna-pilot.pdf output/2026-09-22-jev-rules-retry.pdf output/2026-09-22-jev-frozen-comparison.pdf output/2026-09-21-jev-repair-test.pdf output/2026-09-21-cpc-source-repair.pdf output/2026-09-21-jev-quality.pdf output/2026-09-19-jev-corpus.pdf output/2026-09-19-jev-council.pdf output/2026-09-19-jev-transfer.pdf output/2026-09-19-jev-acceptance.pdf output/2026-09-19-jev-simple.pdf output/2026-09-19-jev-checks.pdf output/2026-09-19-jev-topics.pdf output/2026-09-19-jev-evidence.pdf output/2026-09-19-jev-repeats.pdf output/2026-09-19-jev-pilot.pdf output/2026-09-19-jev-feasibility.pdf output/2026-09-15-cpc-human-reconciliation.pdf output/2026-09-15-sol-subagent-pilot.pdf output/2026-09-15-withdrawal-project-types.pdf output/2026-09-14-cpc-sample-restrictions.pdf output/2026-09-14-regex-strengthening.pdf output/2026-09-14-zap-universe.pdf output/2026-09-14-workspace-checkpoint.pdf output/2026-09-14-regex-comparison.pdf

output/2026-09-22-cpc-attachment-repair.pdf: 2026-09-22-cpc-attachment-repair.md input/cpc_attachment_findings.md Makefile | output
	pandoc 2026-09-22-cpc-attachment-repair.md input/cpc_attachment_findings.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-22-sol-expansion.pdf: 2026-09-22-sol-expansion.md input/cpc_sol_expansion_findings_v3.md input/cpc_sol_actor_findings_v3.md Makefile | output
	pandoc 2026-09-22-sol-expansion.md input/cpc_sol_expansion_findings_v3.md input/cpc_sol_actor_findings_v3.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-22-sol-human-validation.pdf: 2026-09-22-sol-human-validation.md input/cpc_sol_validation_findings_v2.md input/cpc_sol_actor_findings_v1.md input/cpc_sol_actor_findings_v2.md Makefile | output
	pandoc 2026-09-22-sol-human-validation.md input/cpc_sol_validation_findings_v2.md input/cpc_sol_actor_findings_v1.md input/cpc_sol_actor_findings_v2.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-22-luna-pilot.pdf: 2026-09-22-luna-pilot.md input/cpc_luna_findings_v1.md input/cpc_sol_luna_findings_v1.md Makefile | output
	pandoc 2026-09-22-luna-pilot.md input/cpc_luna_findings_v1.md input/cpc_sol_luna_findings_v1.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-22-jev-rules-retry.pdf: 2026-09-22-jev-rules-retry.md input/cpc_jev_findings_v16.md Makefile | output
	pandoc 2026-09-22-jev-rules-retry.md input/cpc_jev_findings_v16.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-22-jev-frozen-comparison.pdf: 2026-09-22-jev-frozen-comparison.md input/cpc_jev_findings_v13.md Makefile | output
	pandoc 2026-09-22-jev-frozen-comparison.md input/cpc_jev_findings_v13.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-21-jev-repair-test.pdf: 2026-09-21-jev-repair-test.md input/cpc_jev_findings_v12.md Makefile | output
	pandoc 2026-09-21-jev-repair-test.md input/cpc_jev_findings_v12.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-21-cpc-source-repair.pdf: 2026-09-21-cpc-source-repair.md input/cpc_jev_scope_findings_v3.md Makefile | output
	pandoc 2026-09-21-cpc-source-repair.md input/cpc_jev_scope_findings_v3.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-21-jev-quality.pdf: 2026-09-21-jev-quality.md input/cpc_jev_quality_findings_v1.md Makefile | output
	pandoc 2026-09-21-jev-quality.md input/cpc_jev_quality_findings_v1.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-19-jev-corpus.pdf: 2026-09-19-jev-corpus.md input/cpc_jev_corpus_findings_v2.md Makefile | output
	pandoc 2026-09-19-jev-corpus.md input/cpc_jev_corpus_findings_v2.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-19-jev-council.pdf: 2026-09-19-jev-council.md input/cpc_jev_findings_v11.md input/cpc_jev_findings_v10.md input/cpc_jev_findings_v9.md Makefile | output
	pandoc 2026-09-19-jev-council.md input/cpc_jev_findings_v11.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-19-jev-transfer.pdf: 2026-09-19-jev-transfer.md input/cpc_jev_findings_v8.md Makefile | output
	pandoc 2026-09-19-jev-transfer.md input/cpc_jev_findings_v8.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-19-jev-acceptance.pdf: 2026-09-19-jev-acceptance.md input/cpc_jev_findings_v7.md Makefile | output
	pandoc 2026-09-19-jev-acceptance.md input/cpc_jev_findings_v7.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-19-jev-simple.pdf: 2026-09-19-jev-simple.md input/cpc_jev_findings_v6.md Makefile | output
	pandoc 2026-09-19-jev-simple.md input/cpc_jev_findings_v6.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-19-jev-checks.pdf: 2026-09-19-jev-checks.md input/cpc_jev_findings_v5.md Makefile | output
	pandoc 2026-09-19-jev-checks.md input/cpc_jev_findings_v5.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-19-jev-topics.pdf: 2026-09-19-jev-topics.md input/cpc_jev_findings_v4.md Makefile | output
	pandoc 2026-09-19-jev-topics.md input/cpc_jev_findings_v4.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-19-jev-evidence.pdf: 2026-09-19-jev-evidence.md input/cpc_jev_findings_v3.md Makefile | output
	pandoc 2026-09-19-jev-evidence.md input/cpc_jev_findings_v3.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-19-jev-repeats.pdf: 2026-09-19-jev-repeats.md input/cpc_jev_findings_v2.md Makefile | output
	pandoc 2026-09-19-jev-repeats.md input/cpc_jev_findings_v2.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-19-jev-pilot.pdf: 2026-09-19-jev-pilot.md input/cpc_jev_findings_v1.md Makefile | output
	pandoc 2026-09-19-jev-pilot.md input/cpc_jev_findings_v1.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-19-jev-feasibility.pdf: 2026-09-19-jev-feasibility.md input/cpc_jev_preparation_v1.md Makefile | output
	pandoc 2026-09-19-jev-feasibility.md input/cpc_jev_preparation_v1.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-15-cpc-human-reconciliation.pdf: 2026-09-15-cpc-human-reconciliation.md input/ulurp_cpc_reconciliation_findings.md Makefile | output
	pandoc 2026-09-15-cpc-human-reconciliation.md input/ulurp_cpc_reconciliation_findings.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-15-sol-subagent-pilot.pdf: 2026-09-15-sol-subagent-pilot.md input/cpc_subagent_findings_v3.md Makefile | output
	pandoc 2026-09-15-sol-subagent-pilot.md input/cpc_subagent_findings_v3.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

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

input/cpc_sol_expansion_findings_v3.md: ../tasks/audits/pilot_ulurp_cpc_llm_labels/output/cpc_sol_expansion_findings_v3.md | input
	ln -sf ../$< $@

input/cpc_sol_actor_findings_v3.md: ../tasks/audits/pilot_ulurp_cpc_llm_labels/output/cpc_sol_actor_findings_v3.md | input
	ln -sf ../$< $@

input/cpc_sol_actor_findings_v2.md: ../tasks/audits/pilot_ulurp_cpc_llm_labels/output/cpc_sol_actor_findings_v2.md | input
	ln -sf ../$< $@

input/cpc_sol_actor_findings_v1.md: ../tasks/audits/pilot_ulurp_cpc_llm_labels/output/cpc_sol_actor_findings_v1.md | input
	ln -sf ../$< $@

input/cpc_sol_validation_findings_v2.md: ../tasks/audits/pilot_ulurp_cpc_llm_labels/output/cpc_sol_validation_findings_v2.md | input
	ln -sf ../$< $@

input/cpc_sol_luna_findings_v1.md: ../tasks/audits/pilot_ulurp_cpc_llm_labels/output/cpc_sol_luna_findings_v1.md | input
	ln -sf ../$< $@

input/cpc_luna_findings_v1.md: ../tasks/audits/pilot_ulurp_cpc_llm_labels/output/cpc_luna_findings_v1.md | input
	ln -sf ../$< $@

input/cpc_jev_findings_v16.md: ../tasks/audits/pilot_ulurp_cpc_llm_labels/output/cpc_jev_findings_v16.md | input
	ln -sf ../$< $@

input/cpc_jev_findings_v13.md: ../tasks/audits/pilot_ulurp_cpc_llm_labels/output/cpc_jev_findings_v13.md | input
	ln -sf ../$< $@

input/cpc_jev_findings_v12.md: ../tasks/audits/pilot_ulurp_cpc_llm_labels/output/cpc_jev_findings_v12.md | input
	ln -sf ../$< $@

input/cpc_jev_scope_findings_v3.md: ../tasks/audits/pilot_ulurp_cpc_llm_labels/output/cpc_jev_scope_findings_v3.md | input
	ln -sf ../$< $@

input/cpc_jev_quality_findings_v1.md: ../tasks/audits/pilot_ulurp_cpc_llm_labels/output/cpc_jev_quality_findings_v1.md | input
	ln -sf ../$< $@

input/cpc_jev_corpus_findings_v2.md: ../tasks/audits/pilot_ulurp_cpc_llm_labels/output/cpc_jev_corpus_findings_v2.md | input
	ln -sf ../$< $@

input/cpc_jev_findings_v11.md: ../tasks/audits/pilot_ulurp_cpc_llm_labels/output/cpc_jev_findings_v11.md | input
	ln -sf ../$< $@

input/cpc_jev_findings_v9.md: ../tasks/audits/pilot_ulurp_cpc_llm_labels/output/cpc_jev_findings_v9.md | input
	ln -sf ../$< $@

input/cpc_jev_findings_v10.md: ../tasks/audits/pilot_ulurp_cpc_llm_labels/output/cpc_jev_findings_v10.md | input
	ln -sf ../$< $@

input/cpc_jev_findings_v8.md: ../tasks/audits/pilot_ulurp_cpc_llm_labels/output/cpc_jev_findings_v8.md | input
	ln -sf ../$< $@

input/cpc_jev_findings_v7.md: ../tasks/audits/pilot_ulurp_cpc_llm_labels/output/cpc_jev_findings_v7.md | input
	ln -sf ../$< $@

input/cpc_jev_findings_v6.md: ../tasks/audits/pilot_ulurp_cpc_llm_labels/output/cpc_jev_findings_v6.md | input
	ln -sf ../$< $@

input/cpc_jev_findings_v5.md: ../tasks/audits/pilot_ulurp_cpc_llm_labels/output/cpc_jev_findings_v5.md | input
	ln -sf ../$< $@

input/cpc_jev_findings_v4.md: ../tasks/audits/pilot_ulurp_cpc_llm_labels/output/cpc_jev_findings_v4.md | input
	ln -sf ../$< $@

input/cpc_jev_findings_v3.md: ../tasks/audits/pilot_ulurp_cpc_llm_labels/output/cpc_jev_findings_v3.md | input
	ln -sf ../$< $@

input/cpc_jev_findings_v2.md: ../tasks/audits/pilot_ulurp_cpc_llm_labels/output/cpc_jev_findings_v2.md | input
	ln -sf ../$< $@

input/cpc_jev_findings_v1.md: ../tasks/audits/pilot_ulurp_cpc_llm_labels/output/cpc_jev_findings_v1.md | input
	ln -sf ../$< $@

input/cpc_jev_preparation_v1.md: ../tasks/audits/pilot_ulurp_cpc_llm_labels/output/cpc_jev_preparation_v1.md | input
	ln -sf ../$< $@

input/ulurp_cpc_reconciliation_findings.md: ../tasks/audits/audit_ulurp_cpc_regex_labels/output/ulurp_cpc_reconciliation_findings.md | input
	ln -sf ../$< $@

input/cpc_subagent_findings_v3.md: ../tasks/audits/pilot_ulurp_cpc_llm_labels/output/cpc_subagent_findings_v3.md | input
	ln -sf ../$< $@

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

link-inputs: input/cpc_attachment_findings.md input/cpc_sol_expansion_findings_v3.md input/cpc_sol_actor_findings_v3.md input/cpc_sol_actor_findings_v2.md input/cpc_sol_actor_findings_v1.md input/cpc_sol_validation_findings_v2.md input/cpc_sol_luna_findings_v1.md input/cpc_luna_findings_v1.md input/cpc_jev_findings_v16.md input/cpc_jev_findings_v13.md input/cpc_jev_findings_v12.md input/cpc_jev_scope_findings_v3.md input/cpc_jev_quality_findings_v1.md input/cpc_jev_corpus_findings_v2.md input/cpc_jev_findings_v11.md input/cpc_jev_findings_v9.md input/cpc_jev_findings_v10.md input/cpc_jev_findings_v8.md input/cpc_jev_findings_v7.md input/cpc_jev_findings_v6.md input/cpc_jev_findings_v5.md input/cpc_jev_findings_v4.md input/cpc_jev_findings_v3.md input/cpc_jev_findings_v2.md input/cpc_jev_findings_v1.md input/cpc_jev_preparation_v1.md input/ulurp_cpc_reconciliation_findings.md input/cpc_subagent_findings_v3.md input/zap_universe_findings.md input/ulurp_cpc_regex_coverage.md input/zap_withdrawals_over_time.pdf input/zap_withdrawals_by_homeowner_tercile.pdf input/cpc_selection_findings.md input/zap_withdrawal_action_findings.md input/zap_withdrawals_by_action.pdf input/zap_withdrawals_by_action_homeowner_tercile.pdf input/zap_withdrawals_by_action_homeowner_tercile_ma.pdf

input output:
	mkdir -p $@

include ../tasks/generic.make
