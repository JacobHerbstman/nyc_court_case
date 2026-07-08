SHELL := bash

START_YEAR := 1975
END_YEAR := 2026
REPORT_LIMIT := 0
WORKERS := 1

all: ../output/ulurp_cpc_report_manifest.csv \
	../output/ulurp_cpc_report_text_manifest.csv \
	../output/ulurp_cpc_report_fetch_failures.csv \
	../output/ulurp_cpc_report_corpus_qc.csv

../output/ulurp_cpc_report_manifest.csv: build_ulurp_cpc_report_corpus.py ../input/ulurp_corpus_project_spine.csv ../input/ulurp_corpus_application_spine.csv | ../output
	python3 $< $(START_YEAR) $(END_YEAR) $(REPORT_LIMIT) $(WORKERS)
	touch $@

../output/ulurp_cpc_report_text_manifest.csv: ../output/ulurp_cpc_report_manifest.csv
	@test -s "$@"
	touch $@

../output/ulurp_cpc_report_fetch_failures.csv: ../output/ulurp_cpc_report_manifest.csv
	@test -s "$@"
	touch $@

../output/ulurp_cpc_report_corpus_qc.csv: ../output/ulurp_cpc_report_manifest.csv
	@test -s "$@"
	touch $@

../input/ulurp_corpus_project_spine.csv: ../../build_ulurp_corpus_spine/output/ulurp_corpus_project_spine.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_corpus_application_spine.csv: ../../build_ulurp_corpus_spine/output/ulurp_corpus_application_spine.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/ulurp_corpus_project_spine.csv ../input/ulurp_corpus_application_spine.csv

include ../../generic.make
