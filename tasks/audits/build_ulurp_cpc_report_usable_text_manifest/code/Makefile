SHELL := bash

all: ../output/ulurp_cpc_report_usable_text_manifest.csv \
	../output/ulurp_cpc_report_usable_text_qc.csv

../output/ulurp_cpc_report_usable_text_manifest.csv: build_ulurp_cpc_report_usable_text_manifest.R \
	../input/ulurp_cpc_report_manifest.csv \
	../input/ulurp_cpc_report_c_prefix_missing_sibling_reports.csv | ../output ../output/usable_cpc_report_text/
	Rscript --vanilla $<
	touch $@

../output/ulurp_cpc_report_usable_text_qc.csv: ../output/ulurp_cpc_report_usable_text_manifest.csv
	@test -s "$@"
	touch $@

../input/ulurp_cpc_report_manifest.csv: ../../build_ulurp_cpc_report_corpus/output/ulurp_cpc_report_manifest.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_cpc_report_c_prefix_missing_sibling_reports.csv: ../../audit_ulurp_cpc_report_c_prefix_missing/output/ulurp_cpc_report_c_prefix_missing_sibling_reports.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/ulurp_cpc_report_manifest.csv \
	../input/ulurp_cpc_report_c_prefix_missing_sibling_reports.csv

include ../../generic.make
