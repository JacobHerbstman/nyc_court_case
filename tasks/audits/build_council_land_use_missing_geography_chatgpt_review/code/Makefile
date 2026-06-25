SHELL := bash

BATCH_SIZE := 4
MAX_SIGNATURES := 0
PROMPT_VERSION := v1

all: ../output/council_land_use_missing_geography_chatgpt_review_frame.csv \
	../output/council_land_use_missing_geography_chatgpt_review_prompt.md \
	../output/council_land_use_missing_geography_chatgpt_review_batch_manifest.csv \
	../output/council_land_use_missing_geography_chatgpt_review_batches.jsonl \
	../output/council_land_use_missing_geography_chatgpt_review_next_batch.md \
	../output/council_land_use_missing_geography_chatgpt_review_responses_combined.csv \
	../output/council_land_use_missing_geography_chatgpt_review_missing_responses.csv \
	../output/council_land_use_missing_geography_chatgpt_review_response_errors.csv \
	../output/council_land_use_missing_geography_chatgpt_manual_review_queue.csv \
	../output/council_land_use_missing_geography_ai_repair_candidates.csv \
	../output/council_land_use_missing_geography_human_verification_queue.csv \
	../output/council_land_use_missing_geography_second_pass_frame.csv \
	../output/council_land_use_missing_geography_second_pass_prompt.md \
	../output/council_land_use_missing_geography_second_pass_batch_manifest.csv \
	../output/council_land_use_missing_geography_second_pass_batches.jsonl \
	../output/council_land_use_missing_geography_second_pass_next_batch.md \
	../output/council_land_use_missing_geography_second_pass_responses_combined.csv \
	../output/council_land_use_missing_geography_second_pass_missing_responses.csv \
	../output/council_land_use_missing_geography_second_pass_response_errors.csv \
	../output/council_land_use_missing_geography_second_pass_ai_repair_candidates.csv \
	../output/council_land_use_missing_geography_second_pass_human_verification_queue.csv \
	../output/council_land_use_missing_geography_adjudication_frame.csv \
	../output/council_land_use_missing_geography_adjudication_prompt.md \
	../output/council_land_use_missing_geography_adjudication_batch_manifest.csv \
	../output/council_land_use_missing_geography_adjudication_batches.jsonl \
	../output/council_land_use_missing_geography_adjudication_next_batch.md \
	../output/council_land_use_missing_geography_adjudication_responses_combined.csv \
	../output/council_land_use_missing_geography_adjudication_missing_responses.csv \
	../output/council_land_use_missing_geography_adjudication_response_errors.csv \
	../output/council_land_use_missing_geography_adjudication_ai_repair_candidates.csv \
	../output/council_land_use_missing_geography_adjudication_spot_check_queue.csv \
	../output/council_land_use_missing_geography_human_verified_repairs.csv \
	../output/council_land_use_missing_geography_manual_verdict_errors.csv \
	../output/council_land_use_missing_geography_browser_chatgpt_workflow.md \
	../output/council_land_use_missing_geography_chatgpt_review_qc.csv

../output/council_land_use_missing_geography_chatgpt_review_qc.csv: build_council_land_use_missing_geography_chatgpt_review.py \
	chatgpt_geography_review_responses.jsonl \
	chatgpt_geography_second_pass_responses.jsonl \
	chatgpt_geography_adjudication_responses.jsonl \
	council_land_use_missing_geography_manual_verdicts.csv \
	../input/council_land_use_missing_geography_roll_call_repair_queue.csv | ../output ../output/batches
	python3 $< $(BATCH_SIZE) $(MAX_SIGNATURES) $(PROMPT_VERSION)

../output/council_land_use_missing_geography_chatgpt_review_frame.csv \
../output/council_land_use_missing_geography_chatgpt_review_prompt.md \
../output/council_land_use_missing_geography_chatgpt_review_batch_manifest.csv \
../output/council_land_use_missing_geography_chatgpt_review_batches.jsonl \
../output/council_land_use_missing_geography_chatgpt_review_next_batch.md \
../output/council_land_use_missing_geography_chatgpt_review_responses_combined.csv \
../output/council_land_use_missing_geography_chatgpt_review_missing_responses.csv \
../output/council_land_use_missing_geography_chatgpt_review_response_errors.csv \
../output/council_land_use_missing_geography_chatgpt_manual_review_queue.csv \
../output/council_land_use_missing_geography_ai_repair_candidates.csv \
../output/council_land_use_missing_geography_human_verification_queue.csv \
../output/council_land_use_missing_geography_second_pass_frame.csv \
../output/council_land_use_missing_geography_second_pass_prompt.md \
../output/council_land_use_missing_geography_second_pass_batch_manifest.csv \
../output/council_land_use_missing_geography_second_pass_batches.jsonl \
../output/council_land_use_missing_geography_second_pass_next_batch.md \
../output/council_land_use_missing_geography_second_pass_responses_combined.csv \
../output/council_land_use_missing_geography_second_pass_missing_responses.csv \
../output/council_land_use_missing_geography_second_pass_response_errors.csv \
../output/council_land_use_missing_geography_second_pass_ai_repair_candidates.csv \
../output/council_land_use_missing_geography_second_pass_human_verification_queue.csv \
../output/council_land_use_missing_geography_adjudication_frame.csv \
../output/council_land_use_missing_geography_adjudication_prompt.md \
../output/council_land_use_missing_geography_adjudication_batch_manifest.csv \
../output/council_land_use_missing_geography_adjudication_batches.jsonl \
../output/council_land_use_missing_geography_adjudication_next_batch.md \
../output/council_land_use_missing_geography_adjudication_responses_combined.csv \
../output/council_land_use_missing_geography_adjudication_missing_responses.csv \
../output/council_land_use_missing_geography_adjudication_response_errors.csv \
../output/council_land_use_missing_geography_adjudication_ai_repair_candidates.csv \
../output/council_land_use_missing_geography_adjudication_spot_check_queue.csv \
../output/council_land_use_missing_geography_human_verified_repairs.csv \
../output/council_land_use_missing_geography_manual_verdict_errors.csv \
../output/council_land_use_missing_geography_browser_chatgpt_workflow.md: ../output/council_land_use_missing_geography_chatgpt_review_qc.csv
	@test -s "$@"

../input/council_land_use_missing_geography_roll_call_repair_queue.csv: ../../investigate_council_land_use_split_votes/output/council_land_use_missing_geography_roll_call_repair_queue.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

chatgpt_geography_adjudication_responses.jsonl:
	touch "$@"

../output/batches:
	mkdir -p $@

link-inputs: ../input/council_land_use_missing_geography_roll_call_repair_queue.csv

include ../../generic.make
