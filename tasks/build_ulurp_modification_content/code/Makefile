SHELL := bash
include ../../shell_functions.make

CONTENT_MODE := first_pass
VALIDATION_CASE_SET := known_cases

all: ../output/ulurp_modification_project_versions.csv \
	../output/ulurp_modification_discrete_modifications.csv \
	../output/ulurp_modification_commitments.csv \
	../output/ulurp_modification_citywide_text_district_modifications.csv \
	../output/ulurp_modification_manual_review_queue.csv \
	../output/ulurp_modification_content_qc.csv

../output/ulurp_modification_project_versions.csv: build_ulurp_modification_content.R \
	../input/ulurp_modification_project_spine.csv \
	../input/ulurp_modification_zap_document_links.csv \
	../input/ulurp_modification_zap_docket_text.csv \
	../input/ulurp_modification_council_document_links.csv \
	../input/ulurp_modification_council_document_snippets.csv \
	../../_lib/source_pipeline_utils.R | ../output
	$(R) $< $(CONTENT_MODE) $(VALIDATION_CASE_SET)

../output/ulurp_modification_discrete_modifications.csv: ../output/ulurp_modification_project_versions.csv
	@test -s "$@"

../output/ulurp_modification_commitments.csv: ../output/ulurp_modification_project_versions.csv
	@test -s "$@"

../output/ulurp_modification_citywide_text_district_modifications.csv: ../output/ulurp_modification_project_versions.csv
	@test -s "$@"

../output/ulurp_modification_manual_review_queue.csv: ../output/ulurp_modification_project_versions.csv
	@test -s "$@"

../output/ulurp_modification_content_qc.csv: ../output/ulurp_modification_project_versions.csv
	@test -s "$@"

../input/ulurp_modification_project_spine.csv: ../../build_ulurp_modification_spine/output/ulurp_modification_project_spine.csv | ../input
	@target="$$(cd ../../build_ulurp_modification_spine/output && pwd)/ulurp_modification_project_spine.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

../input/ulurp_modification_zap_document_links.csv: ../../audits/discover_ulurp_modification_documents/output/ulurp_modification_zap_document_links.csv | ../input
	@target="$$(cd ../../audits/discover_ulurp_modification_documents/output && pwd)/ulurp_modification_zap_document_links.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

../input/ulurp_modification_zap_docket_text.csv: ../../audits/discover_ulurp_modification_documents/output/ulurp_modification_zap_docket_text.csv | ../input
	@target="$$(cd ../../audits/discover_ulurp_modification_documents/output && pwd)/ulurp_modification_zap_docket_text.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

../input/ulurp_modification_council_document_links.csv: ../../fetch_council_modification_documents/output/ulurp_modification_council_document_links.csv | ../input
	@target="$$(cd ../../fetch_council_modification_documents/output && pwd)/ulurp_modification_council_document_links.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

../input/ulurp_modification_council_document_snippets.csv: ../../fetch_council_modification_documents/output/ulurp_modification_council_document_snippets.csv | ../input
	@target="$$(cd ../../fetch_council_modification_documents/output && pwd)/ulurp_modification_council_document_snippets.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

link-inputs: ../input/ulurp_modification_project_spine.csv \
	../input/ulurp_modification_zap_document_links.csv \
	../input/ulurp_modification_zap_docket_text.csv \
	../input/ulurp_modification_council_document_links.csv \
	../input/ulurp_modification_council_document_snippets.csv

include ../../generic.make
