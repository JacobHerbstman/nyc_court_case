SOL_MODEL := gpt-5.6-sol
SOL_REASONING_EFFORT := medium
SOL_MAX_OUTPUT_TOKENS := 6000
PILOT_DOCUMENTS := 20
SAMPLE_SEED := ulurp-cpc-llm-pilot-v1

all: ../output/ulurp_cpc_llm_pilot_requests_sol_medium_v2.jsonl

../output/ulurp_cpc_llm_pilot_requests_sol_medium_v2.jsonl: build_ulurp_cpc_llm_pilot_requests.py \
	cpc_llm_prompt.txt cpc_llm_schema.json \
	../input/official_ulurp_cpc_narrative_manifest.csv \
	../input/ulurp_cpc_training_labels_jacob.csv | ../output
	python3 $< sol_medium_v2 $(SOL_MODEL) $(SOL_REASONING_EFFORT) $(SOL_MAX_OUTPUT_TOKENS) $(PILOT_DOCUMENTS) $(SAMPLE_SEED)

../output/ulurp_cpc_llm_pilot_responses_sol_medium_v2.jsonl: run_ulurp_cpc_llm_pilot.py \
	../output/ulurp_cpc_llm_pilot_requests_sol_medium_v2.jsonl \
	../input/ulurp_cpc_training_labels_jacob.csv | ../output ../temp
	python3 $< sol_medium_v2

../output/ulurp_cpc_llm_pilot_comparison_sol_medium_v2.csv: ../output/ulurp_cpc_llm_pilot_responses_sol_medium_v2.jsonl
	@test -s "$@"

../input/official_ulurp_cpc_narrative_manifest.csv: ../../audit_official_ulurp_cpc_text_measurement/output/official_ulurp_cpc_narrative_manifest.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_cpc_training_labels_jacob.csv: ../../../record_ulurp_cpc_training_labels/output/ulurp_cpc_training_labels_jacob.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/official_ulurp_cpc_narrative_manifest.csv \
	../input/ulurp_cpc_training_labels_jacob.csv

include ../../generic.make
