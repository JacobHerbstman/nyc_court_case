SHELL := bash

all: ../output/ulurp_cpc_report_missing_profile_qc.csv \
	../output/ulurp_cpc_report_missing_profile_dimension.csv \
	../output/ulurp_cpc_report_missing_profile_year.csv \
	../output/ulurp_cpc_report_missing_profile_examples.csv

../output/ulurp_cpc_report_missing_profile_qc.csv: summarize_ulurp_cpc_report_missing_profile.R \
	../input/ulurp_cpc_report_manifest.csv \
	../input/ulurp_cpc_report_failure_recheck.csv \
	../input/ulurp_corpus_project_spine.csv | ../output
	Rscript --vanilla $<
	touch $@

../output/ulurp_cpc_report_missing_profile_dimension.csv: ../output/ulurp_cpc_report_missing_profile_qc.csv
	@test -s "$@"
	touch $@

../output/ulurp_cpc_report_missing_profile_year.csv: ../output/ulurp_cpc_report_missing_profile_qc.csv
	@test -s "$@"
	touch $@

../output/ulurp_cpc_report_missing_profile_examples.csv: ../output/ulurp_cpc_report_missing_profile_qc.csv
	@test -s "$@"
	touch $@

../input/ulurp_cpc_report_manifest.csv: ../../build_ulurp_cpc_report_corpus/output/ulurp_cpc_report_manifest.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_cpc_report_failure_recheck.csv: ../../verify_ulurp_cpc_report_failures/output/ulurp_cpc_report_failure_recheck.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_corpus_project_spine.csv: ../../build_ulurp_corpus_spine/output/ulurp_corpus_project_spine.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/ulurp_cpc_report_manifest.csv \
	../input/ulurp_cpc_report_failure_recheck.csv \
	../input/ulurp_corpus_project_spine.csv

include ../../generic.make
