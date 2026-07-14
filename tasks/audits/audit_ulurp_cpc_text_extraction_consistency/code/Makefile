SHELL := bash

SAMPLE_DOCUMENTS_PER_YEAR := 15

all: ../output/ulurp_cpc_text_extraction_sample_document_metrics.csv \
	../output/ulurp_cpc_text_extraction_by_year.csv \
	../output/ulurp_cpc_text_extraction_by_period.csv \
	../output/ulurp_cpc_text_extraction_flagged_examples.csv

../output/ulurp_cpc_text_extraction_sample_document_metrics.csv: audit_ulurp_cpc_text_extraction_consistency.py \
	../input/ulurp_cpc_report_manifest.csv | ../output
	PYTHONUNBUFFERED=1 python3 $< $(SAMPLE_DOCUMENTS_PER_YEAR)

../output/ulurp_cpc_text_extraction_by_year.csv: ../output/ulurp_cpc_text_extraction_sample_document_metrics.csv
	@test -s "$@"

../output/ulurp_cpc_text_extraction_by_period.csv: ../output/ulurp_cpc_text_extraction_sample_document_metrics.csv
	@test -s "$@"

../output/ulurp_cpc_text_extraction_flagged_examples.csv: ../output/ulurp_cpc_text_extraction_sample_document_metrics.csv
	@test -s "$@"

../input/ulurp_cpc_report_manifest.csv: ../../../build_ulurp_cpc_report_corpus/output/ulurp_cpc_report_manifest.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/ulurp_cpc_report_manifest.csv

include ../../generic.make
