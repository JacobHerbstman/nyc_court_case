START_YEAR := 1975
END_YEAR := 2025
WORKERS := 6
OCR_DPI := 200
OCR_PAGE_TIMEOUT_SECONDS := 90
MINIMUM_EMBEDDED_PAGE_WORDS := 50

all: ../output/ulurp_cpc_report_manifest.csv

../output/ulurp_cpc_report_manifest.csv: build_ulurp_cpc_report_corpus.py \
	../input/official_cpc_report_index.csv \
	../input/zap_project_data.parquet \
	../input/ulurp_cpc_source_corrections.csv \
	../input/ulurp_cpc_index_additions.csv | ../output ../output/cpc_report_pdfs/ ../output/cpc_report_text/
	python3 $< $(START_YEAR) $(END_YEAR) $(WORKERS) $(OCR_DPI) $(OCR_PAGE_TIMEOUT_SECONDS) $(MINIMUM_EMBEDDED_PAGE_WORDS)

../input/official_cpc_report_index.csv: ../../fetch_ulurp_cpc_report_index/output/official_cpc_report_index.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_project_data.parquet: ../../build_zap_datasets/output/zap_project_data.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_cpc_source_corrections.csv: ../../record_ulurp_cpc_source_corrections/output/ulurp_cpc_source_corrections.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_cpc_index_additions.csv: ../../record_ulurp_cpc_source_corrections/output/ulurp_cpc_index_additions.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/official_cpc_report_index.csv \
	../input/zap_project_data.parquet \
	../input/ulurp_cpc_source_corrections.csv \
	../input/ulurp_cpc_index_additions.csv

include ../../generic.make
