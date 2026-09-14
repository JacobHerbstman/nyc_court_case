all: output/2026-09-14-zap-universe.pdf

output/2026-09-14-zap-universe.pdf: 2026-09-14-zap-universe.md input/zap_universe_findings.md Makefile | output
	pandoc 2026-09-14-zap-universe.md input/zap_universe_findings.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

input/zap_universe_findings.md: ../tasks/audits/audit_zap_universe_coverage/output/zap_universe_findings.md | input
	@test "$$(readlink "$@")" = "../$<" || ln -sf "../$<" "$@"

../tasks/audits/audit_zap_universe_coverage/output/zap_universe_findings.md: FORCE_UPSTREAM_CHECK
	$(MAKE) -C ../tasks/audits/audit_zap_universe_coverage/code ../output/zap_universe_findings.md

link-inputs: input/zap_universe_findings.md

input output:
	mkdir -p $@

.PHONY: FORCE_UPSTREAM_CHECK
FORCE_UPSTREAM_CHECK:
