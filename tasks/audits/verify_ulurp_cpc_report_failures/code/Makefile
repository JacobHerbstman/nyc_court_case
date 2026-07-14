SHELL := bash

all: ../output/ulurp_cpc_report_failure_recheck.csv \
	../output/ulurp_cpc_report_failure_recheck_by_year.csv \
	../output/ulurp_cpc_report_failure_recheck_qc.csv

../output/ulurp_cpc_report_failure_recheck.csv: verify_ulurp_cpc_report_failures.py ../input/ulurp_cpc_report_fetch_failures.csv ../input/ulurp_cpc_report_manifest.csv ../input/ulurp_corpus_project_spine.csv | ../output ../output/recovered_cpc_report_pdfs/
	python3 $<
	touch $@

../output/ulurp_cpc_report_failure_recheck_by_year.csv: ../output/ulurp_cpc_report_failure_recheck.csv
	@test -s "$@"
	touch $@

../output/ulurp_cpc_report_failure_recheck_qc.csv: ../output/ulurp_cpc_report_failure_recheck.csv
	@test -s "$@"
	touch $@

../input/ulurp_cpc_report_fetch_failures.csv: ../../build_ulurp_cpc_report_corpus/output/ulurp_cpc_report_fetch_failures.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_cpc_report_manifest.csv: ../../build_ulurp_cpc_report_corpus/output/ulurp_cpc_report_manifest.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_corpus_project_spine.csv: ../../build_ulurp_corpus_spine/output/ulurp_corpus_project_spine.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/ulurp_cpc_report_fetch_failures.csv ../input/ulurp_cpc_report_manifest.csv ../input/ulurp_corpus_project_spine.csv

include ../../generic.make
