START_YEAR := 1975
END_YEAR := 2025
DOCUMENTS_PER_DECADE := 100

all: ../output/official_ulurp_cpc_corpus_summary.csv \
	../output/official_ulurp_cpc_corpus_by_year.csv \
	../output/official_ulurp_cpc_corpus_duplicate_content.csv \
	../output/official_ulurp_cpc_corpus_document_sample.csv \
	../output/official_ulurp_cpc_corpus_source_exceptions.csv \
	../output/official_ulurp_cpc_council_benchmark.csv \
	../output/official_ulurp_cpc_zap_residual.csv \
	../output/official_ulurp_cpc_zap_milestones.csv \
	../output/official_ulurp_cpc_zap_disposition_matches.csv

../output/official_ulurp_cpc_corpus_source_exceptions.csv: audit_official_ulurp_cpc_corpus.py \
	../input/official_ulurp_cpc_report_manifest.csv \
	../input/official_cpc_report_index.csv \
	../input/official_ulurp_cpc_source_corrections.csv \
	../input/official_ulurp_cpc_index_additions.csv \
	../input/official_ulurp_cpc_external_reference_exclusions.csv \
	../input/official_ulurp_cpc_source_exception_labels.csv \
	../input/official_ulurp_cpc_short_page_validation.csv \
	../input/ulurp_corpus_application_spine.csv \
	../input/council_land_use_decision_panel.csv | ../output
	python3 $< $(START_YEAR) $(END_YEAR) $(DOCUMENTS_PER_DECADE)

../output/official_ulurp_cpc_corpus_summary.csv: ../output/official_ulurp_cpc_corpus_source_exceptions.csv
	@test -s "$@"

../output/official_ulurp_cpc_corpus_by_year.csv: ../output/official_ulurp_cpc_corpus_source_exceptions.csv
	@test -s "$@"

../output/official_ulurp_cpc_corpus_duplicate_content.csv: ../output/official_ulurp_cpc_corpus_source_exceptions.csv
	@test -s "$@"

../output/official_ulurp_cpc_corpus_document_sample.csv: ../output/official_ulurp_cpc_corpus_source_exceptions.csv
	@test -s "$@"

../output/official_ulurp_cpc_council_benchmark.csv: ../output/official_ulurp_cpc_corpus_source_exceptions.csv
	@test -s "$@"

../output/official_ulurp_cpc_zap_residual.csv: ../output/official_ulurp_cpc_corpus_source_exceptions.csv
	@test -s "$@"

../output/official_ulurp_cpc_zap_milestones.csv: fetch_zap_residual_milestones.py \
	../output/official_ulurp_cpc_zap_residual.csv
	python3 $<

../output/official_ulurp_cpc_zap_disposition_matches.csv: verify_zap_residual_dispositions.py \
	../output/official_ulurp_cpc_zap_milestones.csv | ../temp
	python3 $<

../input/official_ulurp_cpc_report_manifest.csv: ../../../build_ulurp_cpc_report_corpus/output/ulurp_cpc_report_manifest.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/official_cpc_report_index.csv: ../../../fetch_ulurp_cpc_report_index/output/official_cpc_report_index.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/official_ulurp_cpc_source_corrections.csv: ../../../record_ulurp_cpc_source_corrections/output/ulurp_cpc_source_corrections.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/official_ulurp_cpc_index_additions.csv: ../../../record_ulurp_cpc_source_corrections/output/ulurp_cpc_index_additions.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/official_ulurp_cpc_external_reference_exclusions.csv: ../../record_ulurp_cpc_source_validation/output/official_ulurp_cpc_external_reference_exclusions.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/official_ulurp_cpc_source_exception_labels.csv: ../../record_ulurp_cpc_source_validation/output/official_ulurp_cpc_source_exception_labels.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/official_ulurp_cpc_short_page_validation.csv: ../../record_ulurp_cpc_source_validation/output/official_ulurp_cpc_short_page_validation.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_corpus_application_spine.csv: ../../../build_ulurp_corpus_spine/output/ulurp_corpus_application_spine.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/council_land_use_decision_panel.csv: ../../../build_council_land_use_decision_panel/output/council_land_use_decision_panel.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/official_ulurp_cpc_report_manifest.csv \
	../input/official_cpc_report_index.csv \
	../input/official_ulurp_cpc_source_corrections.csv \
	../input/official_ulurp_cpc_index_additions.csv \
	../input/official_ulurp_cpc_external_reference_exclusions.csv \
	../input/official_ulurp_cpc_source_exception_labels.csv \
	../input/official_ulurp_cpc_short_page_validation.csv \
	../input/ulurp_corpus_application_spine.csv \
	../input/council_land_use_decision_panel.csv

include ../../generic.make
