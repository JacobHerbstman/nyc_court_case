SHELL := bash

BATCH_SIZE := 5
MAX_BUNDLES := 0
PROMPT_VERSION := v1

all: ../output/council_land_use_remaining_split_geography_matter_rows.csv \
	../output/council_land_use_remaining_split_geography_bundle_queue.csv \
	../output/council_land_use_remaining_split_geography_triage_summary.csv \
	../output/council_land_use_remaining_split_geography_prompt.md \
	../output/council_land_use_remaining_split_geography_batch_manifest.csv \
	../output/council_land_use_remaining_split_geography_batches.jsonl \
	../output/council_land_use_remaining_split_geography_next_batch.md \
	../output/council_land_use_remaining_split_geography_responses_combined.csv \
	../output/council_land_use_remaining_split_geography_missing_responses.csv \
	../output/council_land_use_remaining_split_geography_response_errors.csv \
	../output/council_land_use_remaining_split_geography_ai_candidates.csv \
	../output/council_land_use_remaining_split_geography_workflow.md \
	../output/research_understanding_checklist.md \
	../output/council_land_use_remaining_split_geography_qc.csv

../output/council_land_use_remaining_split_geography_qc.csv: build_remaining_council_land_use_split_geography_review.py \
	chatgpt_remaining_split_geography_responses.jsonl \
	../input/council_land_use_decision_panel.csv \
	../input/council_land_use_split_vote_nonaffirmative_member_rows.csv | ../output ../output/batches
	python3 $< $(BATCH_SIZE) $(MAX_BUNDLES) $(PROMPT_VERSION)

../output/council_land_use_remaining_split_geography_matter_rows.csv \
../output/council_land_use_remaining_split_geography_bundle_queue.csv \
../output/council_land_use_remaining_split_geography_triage_summary.csv \
../output/council_land_use_remaining_split_geography_prompt.md \
../output/council_land_use_remaining_split_geography_batch_manifest.csv \
../output/council_land_use_remaining_split_geography_batches.jsonl \
../output/council_land_use_remaining_split_geography_next_batch.md \
../output/council_land_use_remaining_split_geography_responses_combined.csv \
../output/council_land_use_remaining_split_geography_missing_responses.csv \
../output/council_land_use_remaining_split_geography_response_errors.csv \
../output/council_land_use_remaining_split_geography_ai_candidates.csv \
../output/council_land_use_remaining_split_geography_workflow.md \
../output/research_understanding_checklist.md: ../output/council_land_use_remaining_split_geography_qc.csv
	@test -s "$@"

../input/council_land_use_decision_panel.csv: ../../../build_council_land_use_decision_panel/output/council_land_use_decision_panel.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/council_land_use_split_vote_nonaffirmative_member_rows.csv: ../../investigate_council_land_use_split_votes/output/council_land_use_split_vote_nonaffirmative_member_rows.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

chatgpt_remaining_split_geography_responses.jsonl:
	touch "$@"

../output/batches:
	mkdir -p "$@"

link-inputs: ../input/council_land_use_decision_panel.csv \
	../input/council_land_use_split_vote_nonaffirmative_member_rows.csv

include ../../../generic.make
