BATCH_SIZE = 6
MAX_REVIEW_PROJECTS = 0
RANDOM_AUDIT_PER_STRATUM = 1
PROMPT_VERSION = v1

all: ../output/zap_rezoning_llm_review_frame.csv \
	../output/zap_rezoning_llm_review_prompt.md \
	../output/zap_rezoning_llm_review_batch_manifest.csv \
	../output/zap_rezoning_llm_review_batches.jsonl \
	../output/zap_rezoning_llm_review_next_batch.md \
	../output/zap_rezoning_llm_review_response_compiled.csv \
	../output/zap_rezoning_llm_review_response_errors.csv \
	../output/zap_rezoning_llm_review_missing_responses.csv \
	../output/zap_rezoning_llm_review_qc.csv

../output/zap_rezoning_llm_review_qc.csv: build_zap_rezoning_llm_review.py llm_review_responses.jsonl ../input/zap_rezoning_direction_review_ledger.csv ../input/zap_rezoning_direction_project_classification.csv ../input/zap_rezoning_source_text_snippets.csv ../input/zap_rezoning_source_document_index.csv | ../output ../output/batches
	python3 $< $(BATCH_SIZE) $(MAX_REVIEW_PROJECTS) $(RANDOM_AUDIT_PER_STRATUM) $(PROMPT_VERSION)

../output/zap_rezoning_llm_review_frame.csv \
../output/zap_rezoning_llm_review_prompt.md \
../output/zap_rezoning_llm_review_batch_manifest.csv \
../output/zap_rezoning_llm_review_batches.jsonl \
../output/zap_rezoning_llm_review_next_batch.md \
../output/zap_rezoning_llm_review_response_compiled.csv \
../output/zap_rezoning_llm_review_response_errors.csv \
../output/zap_rezoning_llm_review_missing_responses.csv: ../output/zap_rezoning_llm_review_qc.csv
	@test -s "$@"

../input/zap_rezoning_direction_review_ledger.csv: ../../summarize_zap_rezoning_direction_scope/output/zap_rezoning_direction_review_ledger.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_rezoning_direction_project_classification.csv: ../../summarize_zap_rezoning_direction_scope/output/zap_rezoning_direction_project_classification.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_rezoning_source_text_snippets.csv: ../../extract_zap_rezoning_source_text/output/zap_rezoning_source_text_snippets.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_rezoning_source_document_index.csv: ../../extract_zap_rezoning_source_text/output/zap_rezoning_source_document_index.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../output/batches:
	mkdir -p $@

link-inputs: ../input/zap_rezoning_direction_review_ledger.csv ../input/zap_rezoning_direction_project_classification.csv ../input/zap_rezoning_source_text_snippets.csv ../input/zap_rezoning_source_document_index.csv

include ../../generic.make
