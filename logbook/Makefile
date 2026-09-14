all: output/2026-09-14-regex-strengthening.pdf output/2026-09-14-zap-universe.pdf output/2026-09-14-workspace-checkpoint.pdf output/2026-09-14-regex-comparison.pdf

output/2026-09-14-regex-strengthening.pdf: 2026-09-14-regex-strengthening.md input/ulurp_cpc_regex_coverage.md Makefile | output
	pandoc 2026-09-14-regex-strengthening.md input/ulurp_cpc_regex_coverage.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-14-regex-comparison.pdf: 2026-09-14-regex-comparison.md 2026-09-14-regex-comparison-checks.md Makefile | output
	pandoc 2026-09-14-regex-comparison.md 2026-09-14-regex-comparison-checks.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-14-workspace-checkpoint.pdf: 2026-09-14-workspace-checkpoint.md Makefile | output
	pandoc $< --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

output/2026-09-14-zap-universe.pdf: 2026-09-14-zap-universe.md input/zap_universe_findings.md Makefile | output
	pandoc 2026-09-14-zap-universe.md input/zap_universe_findings.md --pdf-engine=pdflatex -V geometry:margin=0.65in -V fontsize=10pt -V colorlinks=true -o $@

input/zap_universe_findings.md: ../tasks/audits/audit_zap_universe_coverage/output/zap_universe_findings.md | input
	ln -sf "../$<" "$@"

input/ulurp_cpc_regex_coverage.md: ../tasks/audits/audit_ulurp_cpc_regex_labels/output/ulurp_cpc_regex_coverage.md | input
	ln -sf "../$<" "$@"

link-inputs: input/zap_universe_findings.md input/ulurp_cpc_regex_coverage.md

input output:
	mkdir -p $@

include ../tasks/generic.make
