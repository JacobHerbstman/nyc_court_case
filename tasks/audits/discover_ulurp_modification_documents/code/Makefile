SHELL := bash

SAMPLE_MODE := full
DISCOVERY_LIMIT := 0

all: ../output/ulurp_modification_zap_document_links.csv \
	../output/ulurp_modification_zap_project_summary.csv \
	../output/ulurp_modification_zap_docket_text.csv \
	../output/ulurp_modification_zap_document_fetch_failures.csv \
	../output/ulurp_modification_zap_report_text.csv \
	../output/ulurp_modification_zap_report_snippets.csv \
	../output/ulurp_modification_zap_report_fetch_failures.csv \
	../output/ulurp_modification_zap_document_discovery_qc.csv

../output/ulurp_modification_zap_document_links.csv: discover_ulurp_modification_documents.py ../input/ulurp_modification_project_spine.csv | ../output ../temp ../temp/zap_documents
	python3 $< $(SAMPLE_MODE) $(DISCOVERY_LIMIT)

../output/ulurp_modification_zap_project_summary.csv: ../output/ulurp_modification_zap_document_links.csv
	@test -s "$@"

../output/ulurp_modification_zap_docket_text.csv: ../output/ulurp_modification_zap_document_links.csv
	@test -s "$@"

../output/ulurp_modification_zap_document_fetch_failures.csv: ../output/ulurp_modification_zap_document_links.csv
	@test -s "$@"

../output/ulurp_modification_zap_report_text.csv: ../output/ulurp_modification_zap_document_links.csv
	@test -s "$@"

../output/ulurp_modification_zap_report_snippets.csv: ../output/ulurp_modification_zap_document_links.csv
	@test -s "$@"

../output/ulurp_modification_zap_report_fetch_failures.csv: ../output/ulurp_modification_zap_document_links.csv
	@test -s "$@"

../output/ulurp_modification_zap_document_discovery_qc.csv: ../output/ulurp_modification_zap_document_links.csv
	@test -s "$@"

../input/ulurp_modification_project_spine.csv: ../../../build_ulurp_modification_spine/output/ulurp_modification_project_spine.csv | ../input
	@target="$$(cd ../../../build_ulurp_modification_spine/output && pwd)/ulurp_modification_project_spine.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

link-inputs: ../input/ulurp_modification_project_spine.csv

../temp/zap_documents:
	mkdir -p $@

include ../../generic.make
