SHELL := bash
include ../../../shell_functions.make

CONTENT_MODE := first_pass

all: ../output/ulurp_modification_project_versions.csv \
	../output/ulurp_modification_discrete_modifications.csv \
	../output/ulurp_modification_commitments.csv \
	../output/ulurp_modification_citywide_text_district_modifications.csv

../output/ulurp_modification_project_versions.csv: build_ulurp_modification_content.R \
	../input/ulurp_modification_project_spine.csv \
	../input/ulurp_modification_zap_document_links.csv \
	../input/ulurp_modification_zap_docket_text.csv \
	../input/ulurp_modification_council_document_links.csv \
	../input/ulurp_modification_council_document_snippets.csv \
	../../../_lib/source_pipeline_utils.R | ../output
	$(R) $< $(CONTENT_MODE)

../output/ulurp_modification_discrete_modifications.csv: ../output/ulurp_modification_project_versions.csv
	@test -s "$@"

../output/ulurp_modification_commitments.csv: ../output/ulurp_modification_project_versions.csv
	@test -s "$@"

../output/ulurp_modification_citywide_text_district_modifications.csv: ../output/ulurp_modification_project_versions.csv
	@test -s "$@"

../input/ulurp_modification_project_spine.csv: ../../build_ulurp_modification_spine/output/ulurp_modification_project_spine.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_modification_zap_document_links.csv: ../../stage_ulurp_modification_zap_documents/output/ulurp_modification_zap_document_links.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_modification_zap_docket_text.csv: ../../stage_ulurp_modification_zap_documents/output/ulurp_modification_zap_docket_text.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_modification_council_document_links.csv: ../../fetch_council_modification_documents/output/ulurp_modification_council_document_links.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_modification_council_document_snippets.csv: ../../fetch_council_modification_documents/output/ulurp_modification_council_document_snippets.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/ulurp_modification_project_spine.csv \
	../input/ulurp_modification_zap_document_links.csv \
	../input/ulurp_modification_zap_docket_text.csv \
	../input/ulurp_modification_council_document_links.csv \
	../input/ulurp_modification_council_document_snippets.csv

include ../../generic.make
