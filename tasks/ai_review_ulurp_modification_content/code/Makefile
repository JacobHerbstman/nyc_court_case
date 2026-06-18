SHELL := bash

BATCH_SIZE := 5
MAX_REVIEW_PROJECTS := 0
RANDOM_AUDIT_PER_STRATUM := 0
PROMPT_VERSION := v2
SOURCE_PACKET_MODE := zap_cpc_reports_only

all: ../output/ulurp_modification_ai_review_frame.csv \
	../output/ulurp_modification_ai_review_prompt.md \
	../output/ulurp_modification_ai_review_batch_manifest.csv \
	../output/ulurp_modification_ai_review_batches.jsonl \
	../output/ulurp_modification_ai_review_next_batch.md \
	../output/ulurp_modification_ai_review_seed_labels.csv \
	../output/ulurp_modification_ai_review_response_compiled.csv \
	../output/ulurp_modification_ai_review_response_modifications.csv \
	../output/ulurp_modification_ai_review_response_quantities.csv \
	../output/ulurp_modification_ai_review_response_errors.csv \
	../output/ulurp_modification_ai_review_missing_responses.csv \
	../output/ulurp_modification_ai_review_ready_labels.csv \
	../output/ulurp_modification_ai_review_qc.csv

../output/ulurp_modification_ai_review_qc.csv: build_ulurp_modification_ai_review.py \
	llm_review_responses.jsonl \
	../input/ulurp_modification_project_spine.csv \
	../input/ulurp_modification_project_summary.csv \
	../input/ulurp_modification_discrete_modifications.csv \
	../input/ulurp_modification_commitments.csv \
	../input/ulurp_modification_project_versions.csv \
	../input/ulurp_modification_manual_review_queue.csv \
	../input/ulurp_modification_zap_document_links.csv \
	../input/ulurp_modification_zap_docket_text.csv \
	../input/ulurp_modification_zap_report_text.csv \
	../input/ulurp_modification_zap_report_snippets.csv \
	../input/ulurp_modification_council_document_links.csv \
	../input/ulurp_modification_council_document_text.csv \
	../input/ulurp_modification_council_document_snippets.csv | ../output ../output/batches
	python3 $< $(BATCH_SIZE) $(MAX_REVIEW_PROJECTS) $(RANDOM_AUDIT_PER_STRATUM) $(PROMPT_VERSION) $(SOURCE_PACKET_MODE)

../output/ulurp_modification_ai_review_frame.csv: ../output/ulurp_modification_ai_review_qc.csv
	@test -s "$@"

../output/ulurp_modification_ai_review_prompt.md: ../output/ulurp_modification_ai_review_qc.csv
	@test -s "$@"

../output/ulurp_modification_ai_review_batch_manifest.csv: ../output/ulurp_modification_ai_review_qc.csv
	@test -s "$@"

../output/ulurp_modification_ai_review_batches.jsonl: ../output/ulurp_modification_ai_review_qc.csv
	@test -s "$@"

../output/ulurp_modification_ai_review_next_batch.md: ../output/ulurp_modification_ai_review_qc.csv
	@test -s "$@"

../output/ulurp_modification_ai_review_seed_labels.csv: ../output/ulurp_modification_ai_review_qc.csv
	@test -s "$@"

../output/ulurp_modification_ai_review_response_compiled.csv: ../output/ulurp_modification_ai_review_qc.csv
	@test -s "$@"

../output/ulurp_modification_ai_review_response_modifications.csv: ../output/ulurp_modification_ai_review_qc.csv
	@test -s "$@"

../output/ulurp_modification_ai_review_response_quantities.csv: ../output/ulurp_modification_ai_review_qc.csv
	@test -s "$@"

../output/ulurp_modification_ai_review_response_errors.csv: ../output/ulurp_modification_ai_review_qc.csv
	@test -s "$@"

../output/ulurp_modification_ai_review_missing_responses.csv: ../output/ulurp_modification_ai_review_qc.csv
	@test -s "$@"

../output/ulurp_modification_ai_review_ready_labels.csv: ../output/ulurp_modification_ai_review_qc.csv
	@test -s "$@"

../input/ulurp_modification_project_spine.csv: ../../build_ulurp_modification_spine/output/ulurp_modification_project_spine.csv | ../input
	@target="$$(cd ../../build_ulurp_modification_spine/output && pwd)/ulurp_modification_project_spine.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

../input/ulurp_modification_project_summary.csv: ../../summarize_ulurp_modification_content/output/ulurp_modification_project_summary.csv | ../input
	@target="$$(cd ../../summarize_ulurp_modification_content/output && pwd)/ulurp_modification_project_summary.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

../input/ulurp_modification_discrete_modifications.csv: ../../build_ulurp_modification_content/output/ulurp_modification_discrete_modifications.csv | ../input
	@target="$$(cd ../../build_ulurp_modification_content/output && pwd)/ulurp_modification_discrete_modifications.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

../input/ulurp_modification_commitments.csv: ../../build_ulurp_modification_content/output/ulurp_modification_commitments.csv | ../input
	@target="$$(cd ../../build_ulurp_modification_content/output && pwd)/ulurp_modification_commitments.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

../input/ulurp_modification_project_versions.csv: ../../build_ulurp_modification_content/output/ulurp_modification_project_versions.csv | ../input
	@target="$$(cd ../../build_ulurp_modification_content/output && pwd)/ulurp_modification_project_versions.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

../input/ulurp_modification_manual_review_queue.csv: ../../build_ulurp_modification_content/output/ulurp_modification_manual_review_queue.csv | ../input
	@target="$$(cd ../../build_ulurp_modification_content/output && pwd)/ulurp_modification_manual_review_queue.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

../input/ulurp_modification_zap_document_links.csv: ../../discover_ulurp_modification_documents/output/ulurp_modification_zap_document_links.csv | ../input
	@target="$$(cd ../../discover_ulurp_modification_documents/output && pwd)/ulurp_modification_zap_document_links.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

../input/ulurp_modification_zap_docket_text.csv: ../../discover_ulurp_modification_documents/output/ulurp_modification_zap_docket_text.csv | ../input
	@target="$$(cd ../../discover_ulurp_modification_documents/output && pwd)/ulurp_modification_zap_docket_text.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

../input/ulurp_modification_zap_report_text.csv: ../../discover_ulurp_modification_documents/output/ulurp_modification_zap_report_text.csv | ../input
	@target="$$(cd ../../discover_ulurp_modification_documents/output && pwd)/ulurp_modification_zap_report_text.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

../input/ulurp_modification_zap_report_snippets.csv: ../../discover_ulurp_modification_documents/output/ulurp_modification_zap_report_snippets.csv | ../input
	@target="$$(cd ../../discover_ulurp_modification_documents/output && pwd)/ulurp_modification_zap_report_snippets.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

../input/ulurp_modification_council_document_links.csv: ../../fetch_council_modification_documents/output/ulurp_modification_council_document_links.csv | ../input
	@target="$$(cd ../../fetch_council_modification_documents/output && pwd)/ulurp_modification_council_document_links.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

../input/ulurp_modification_council_document_text.csv: ../../fetch_council_modification_documents/output/ulurp_modification_council_document_text.csv | ../input
	@target="$$(cd ../../fetch_council_modification_documents/output && pwd)/ulurp_modification_council_document_text.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

../input/ulurp_modification_council_document_snippets.csv: ../../fetch_council_modification_documents/output/ulurp_modification_council_document_snippets.csv | ../input
	@target="$$(cd ../../fetch_council_modification_documents/output && pwd)/ulurp_modification_council_document_snippets.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

../output/batches:
	mkdir -p $@

link-inputs: ../input/ulurp_modification_project_spine.csv \
	../input/ulurp_modification_project_summary.csv \
	../input/ulurp_modification_discrete_modifications.csv \
	../input/ulurp_modification_commitments.csv \
	../input/ulurp_modification_project_versions.csv \
	../input/ulurp_modification_manual_review_queue.csv \
	../input/ulurp_modification_zap_document_links.csv \
	../input/ulurp_modification_zap_docket_text.csv \
	../input/ulurp_modification_zap_report_text.csv \
	../input/ulurp_modification_zap_report_snippets.csv \
	../input/ulurp_modification_council_document_links.csv \
	../input/ulurp_modification_council_document_text.csv \
	../input/ulurp_modification_council_document_snippets.csv

include ../../generic.make
