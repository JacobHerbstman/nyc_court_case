all: output/2026-09-14-zap-universe.pdf output/2026-09-14-workspace-checkpoint.pdf output/2026-09-14-regex-comparison.pdf

output/2026-09-14-regex-comparison.pdf: 2026-09-14-regex-comparison.md input/ulurp_cpc_tyler_comparison_checks.md Makefile | output
	pandoc 2026-09-14-regex-comparison.md input/ulurp_cpc_tyler_comparison_checks.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-14-workspace-checkpoint.pdf: 2026-09-14-workspace-checkpoint.md Makefile | output
	pandoc $< --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-14-zap-universe.pdf: 2026-09-14-zap-universe.md input/zap_universe_findings.md Makefile | output
	pandoc 2026-09-14-zap-universe.md input/zap_universe_findings.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

input/zap_universe_findings.md: ../tasks/audits/audit_zap_universe_coverage/output/zap_universe_findings.md | input
	@test "$$(readlink "$@")" = "../$<" || ln -sf "../$<" "$@"

input/ulurp_cpc_tyler_comparison_checks.md: ../tasks/audits/audit_ulurp_cpc_regex_labels/output/ulurp_cpc_tyler_comparison_checks.md | input
	@test "$$(readlink "$@")" = "../$<" || ln -sf "../$<" "$@"

../tasks/audits/audit_zap_universe_coverage/output/zap_universe_findings.md: FORCE_UPSTREAM_CHECK
	$(MAKE) -C ../tasks/audits/audit_zap_universe_coverage/code ../output/zap_universe_findings.md

../tasks/audits/audit_ulurp_cpc_regex_labels/output/ulurp_cpc_tyler_comparison_checks.md: FORCE_UPSTREAM_CHECK
	$(MAKE) -C ../tasks/audits/audit_ulurp_cpc_regex_labels/code ../output/ulurp_cpc_tyler_comparison_checks.md

link-inputs: input/zap_universe_findings.md input/ulurp_cpc_tyler_comparison_checks.md

input output:
	mkdir -p $@

.PHONY: FORCE_UPSTREAM_CHECK
FORCE_UPSTREAM_CHECK:
