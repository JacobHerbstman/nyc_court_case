SHELL := bash

SAMPLE_SEED := 20260702
DOCUMENTS_PER_COHORT := 10

all: ../output/ulurp_zoning_cpc_report_sample_index.csv \
	../output/ulurp_zoning_cpc_report_sample_text.csv \
	../output/ulurp_zoning_cpc_report_sample_fetch_failures.csv \
	../output/ulurp_zoning_cpc_report_sample_qc.csv

../output/ulurp_zoning_cpc_report_sample_index.csv: sample_ulurp_zoning_cpc_reports.py ../input/ulurp_corpus_project_spine.csv | ../output
	python3 $< $(SAMPLE_SEED) $(DOCUMENTS_PER_COHORT)

../output/ulurp_zoning_cpc_report_sample_text.csv: ../output/ulurp_zoning_cpc_report_sample_index.csv
	@test -s "$@"

../output/ulurp_zoning_cpc_report_sample_fetch_failures.csv: ../output/ulurp_zoning_cpc_report_sample_index.csv
	@test -s "$@"

../output/ulurp_zoning_cpc_report_sample_qc.csv: ../output/ulurp_zoning_cpc_report_sample_index.csv
	@test -s "$@"

../input/ulurp_corpus_project_spine.csv: ../../build_ulurp_corpus_spine/output/ulurp_corpus_project_spine.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/ulurp_corpus_project_spine.csv

include ../../generic.make
