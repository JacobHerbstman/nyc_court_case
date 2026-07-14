SHELL := bash

all: ../output/ulurp_cpc_report_c_prefix_missing_qc.csv \
	../output/ulurp_cpc_report_c_prefix_missing_summary.csv \
	../output/ulurp_cpc_report_c_prefix_missing_cases.csv \
	../output/ulurp_cpc_report_c_prefix_missing_sibling_reports.csv

../output/ulurp_cpc_report_c_prefix_missing_qc.csv: audit_ulurp_cpc_report_c_prefix_missing.R \
	../input/ulurp_cpc_report_manifest.csv \
	../input/ulurp_cpc_report_failure_recheck.csv | ../output
	Rscript --vanilla $<
	touch $@

../output/ulurp_cpc_report_c_prefix_missing_summary.csv: ../output/ulurp_cpc_report_c_prefix_missing_qc.csv
	@test -s "$@"
	touch $@

../output/ulurp_cpc_report_c_prefix_missing_cases.csv: ../output/ulurp_cpc_report_c_prefix_missing_qc.csv
	@test -s "$@"
	touch $@

../output/ulurp_cpc_report_c_prefix_missing_sibling_reports.csv: ../output/ulurp_cpc_report_c_prefix_missing_qc.csv
	@test -s "$@"
	touch $@

../input/ulurp_cpc_report_manifest.csv: ../../build_ulurp_cpc_report_corpus/output/ulurp_cpc_report_manifest.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_cpc_report_failure_recheck.csv: ../../verify_ulurp_cpc_report_failures/output/ulurp_cpc_report_failure_recheck.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/ulurp_cpc_report_manifest.csv \
	../input/ulurp_cpc_report_failure_recheck.csv

include ../../generic.make
