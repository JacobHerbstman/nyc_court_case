SHELL := bash
PYTHON := /Users/jacobherbstman/miniconda3/bin/python3

all: ../output/council_land_use_ai_geography_deterministic_verification.csv \
	../output/council_land_use_ai_geography_deterministic_repairs.csv \
	../output/council_land_use_ai_geography_deterministic_manual_queue.csv \
	../output/council_land_use_ai_geography_conflict_chatgpt_review.csv \
	../output/council_land_use_ai_geography_conflict_chatgpt_review_errors.csv \
	../output/council_land_use_ai_geography_deterministic_qc.csv \
	../output/full_document_conflict_prompts/conflict_indexed_web_prompt_manifest.csv \
	../output/full_document_conflict_prompts/conflict_indexed_web_search_queries.csv \
	../output/full_document_conflict_prompts/clu_geo_010_indexed_web_prompt.md \
	../output/full_document_conflict_prompts/clu_geo_119_indexed_web_prompt.md \
	../output/full_document_conflict_prompts/clu_geo_122_indexed_web_prompt.md \
	../output/full_document_conflict_prompts/clu_geo_124_indexed_web_prompt.md \
	../output/full_document_conflict_prompts/clu_geo_142_indexed_web_prompt.md \
	../output/manual_queue_web_review_batches/manual_queue_web_review_case_manifest.csv \
	../output/manual_queue_web_review_batches/manual_queue_web_review_batch_manifest.csv \
	../output/manual_queue_web_review_batches/manual_queue_web_review_search_queries.csv \
	../output/manual_queue_web_review_batches/manual_queue_web_review_next_batch.md \
	../output/manual_queue_web_review_batches/manual_queue_web_review_summary.csv \
	../output/manual_queue_web_review_batches/manual_queue_web_review_run_log.csv \
	../output/remaining_queue_web_review_batches/remaining_queue_web_review_case_manifest.csv \
	../output/remaining_queue_web_review_batches/remaining_queue_web_review_batch_manifest.csv \
	../output/remaining_queue_web_review_batches/remaining_queue_web_review_search_queries.csv \
	../output/remaining_queue_web_review_batches/remaining_queue_web_review_next_batch.md \
	../output/remaining_queue_web_review_batches/remaining_queue_web_review_summary.csv \
	../output/remaining_queue_web_review_batches/remaining_queue_web_review_run_log.csv \
	../output/remaining_queue_web_review_batches/remaining_queue_human_review_needed_materials.csv \
	../output/council_land_use_ai_geography_accepted_repairs.csv \
	../output/council_land_use_ai_geography_accepted_repairs_excluded_by_current_queue.csv

../output/council_land_use_ai_geography_deterministic_qc.csv: verify_council_land_use_ai_geography.py \
	chatgpt_conflict_review_responses.jsonl \
	../input/council_land_use_missing_geography_adjudication_ai_repair_candidates.csv \
	../input/zap_bbl.csv \
	../input/nyc_mappluto_25v4_shp.zip \
	../input/dcp_council_boundary_archive_index.csv \
	../input/dcp_council_boundary_archive_output | ../output
	$(PYTHON) $<

../output/council_land_use_ai_geography_deterministic_verification.csv \
../output/council_land_use_ai_geography_deterministic_repairs.csv \
../output/council_land_use_ai_geography_deterministic_manual_queue.csv \
../output/council_land_use_ai_geography_conflict_chatgpt_review.csv \
../output/council_land_use_ai_geography_conflict_chatgpt_review_errors.csv: ../output/council_land_use_ai_geography_deterministic_qc.csv
	@test -s "$@"

../output/full_document_conflict_prompts/conflict_indexed_web_prompt_manifest.csv: build_conflict_review_prompt_packets.py | ../output/full_document_conflict_prompts
	$(PYTHON) $<

../output/full_document_conflict_prompts/conflict_indexed_web_search_queries.csv \
../output/full_document_conflict_prompts/clu_geo_010_full_document_prompt.md \
../output/full_document_conflict_prompts/clu_geo_119_full_document_prompt.md \
../output/full_document_conflict_prompts/clu_geo_122_full_document_prompt.md \
../output/full_document_conflict_prompts/clu_geo_124_full_document_prompt.md \
../output/full_document_conflict_prompts/clu_geo_142_full_document_prompt.md \
../output/full_document_conflict_prompts/clu_geo_010_indexed_web_prompt.md \
../output/full_document_conflict_prompts/clu_geo_119_indexed_web_prompt.md \
../output/full_document_conflict_prompts/clu_geo_122_indexed_web_prompt.md \
../output/full_document_conflict_prompts/clu_geo_124_indexed_web_prompt.md \
../output/full_document_conflict_prompts/clu_geo_142_indexed_web_prompt.md: ../output/full_document_conflict_prompts/conflict_indexed_web_prompt_manifest.csv
	@test -s "$@"

../output/manual_queue_web_review_batches/manual_queue_web_review_case_manifest.csv: build_manual_queue_web_review_packets.py \
	../output/council_land_use_ai_geography_deterministic_manual_queue.csv | ../output/manual_queue_web_review_batches
	$(PYTHON) $<

../output/manual_queue_web_review_batches/manual_queue_web_review_batch_manifest.csv \
../output/manual_queue_web_review_batches/manual_queue_web_review_batches.jsonl \
../output/manual_queue_web_review_batches/manual_queue_web_review_search_queries.csv \
../output/manual_queue_web_review_batches/manual_queue_web_review_next_batch.md: ../output/manual_queue_web_review_batches/manual_queue_web_review_case_manifest.csv
	@test -s "$@"

../output/manual_queue_web_review_batches/manual_queue_web_review_summary.csv: summarize_manual_queue_web_review_responses.py \
	manual_queue_human_spot_checks.csv \
	manual_queue_researcher_batch_acceptance.csv \
	../output/manual_queue_web_review_batches/manual_queue_web_review_responses.jsonl | ../output/manual_queue_web_review_batches
	$(PYTHON) $<

../output/manual_queue_web_review_batches/manual_queue_web_review_run_log.csv: ../output/manual_queue_web_review_batches/manual_queue_web_review_summary.csv
	@test -s "$@"

../output/remaining_queue_web_review_batches/remaining_queue_web_review_case_manifest.csv: build_remaining_queue_web_review_packets.py \
	../input/council_land_use_missing_geography_adjudication_ai_repair_candidates.csv \
	../output/manual_queue_web_review_batches/manual_queue_web_review_case_manifest.csv | ../output/remaining_queue_web_review_batches
	$(PYTHON) $<

../output/remaining_queue_web_review_batches/remaining_queue_web_review_batch_manifest.csv \
../output/remaining_queue_web_review_batches/remaining_queue_web_review_batches.jsonl \
../output/remaining_queue_web_review_batches/remaining_queue_web_review_search_queries.csv \
../output/remaining_queue_web_review_batches/remaining_queue_web_review_next_batch.md: ../output/remaining_queue_web_review_batches/remaining_queue_web_review_case_manifest.csv
	@test -s "$@"

../output/remaining_queue_web_review_batches/remaining_queue_web_review_summary.csv: summarize_remaining_queue_web_review_responses.py \
	remaining_queue_researcher_adjudication.csv \
	../output/remaining_queue_web_review_batches/remaining_queue_web_review_responses.jsonl | ../output/remaining_queue_web_review_batches
	$(PYTHON) $<

../output/remaining_queue_web_review_batches/remaining_queue_web_review_run_log.csv: ../output/remaining_queue_web_review_batches/remaining_queue_web_review_summary.csv
	@test -s "$@"

../output/remaining_queue_web_review_batches/remaining_queue_human_review_needed_materials.csv: ../output/remaining_queue_web_review_batches/remaining_queue_web_review_summary.csv
	@test -s "$@"

../output/council_land_use_ai_geography_accepted_repairs.csv: build_accepted_geography_repairs.py \
	../input/council_land_use_missing_geography_adjudication_ai_repair_candidates.csv \
	../output/council_land_use_ai_geography_deterministic_repairs.csv \
	../output/manual_queue_web_review_batches/manual_queue_web_review_summary.csv \
	../output/remaining_queue_web_review_batches/remaining_queue_web_review_summary.csv | ../output
	$(PYTHON) $<

../output/council_land_use_ai_geography_accepted_repairs_excluded_by_current_queue.csv: ../output/council_land_use_ai_geography_accepted_repairs.csv
	@test -s "$@"

../input/council_land_use_missing_geography_adjudication_ai_repair_candidates.csv: ../../build_council_land_use_missing_geography_chatgpt_review/output/council_land_use_missing_geography_adjudication_ai_repair_candidates.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_bbl.csv: ../../../../data_raw/dcp_zap_bbl/20260501/zap_bbl.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/nyc_mappluto_25v4_shp.zip: ../../../../data_raw/dcp_mappluto_current/25v4/nyc_mappluto_25v4_shp.zip | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/dcp_council_boundary_archive_index.csv: ../../../archive/stage_dcp_council_boundary_archive/output/dcp_council_boundary_archive_index.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/dcp_council_boundary_archive_output: ../../../archive/stage_dcp_council_boundary_archive/output | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sfn "$<" "$@"

../output/full_document_conflict_prompts:
	mkdir -p "$@"

../output/manual_queue_web_review_batches:
	mkdir -p "$@"

../output/remaining_queue_web_review_batches:
	mkdir -p "$@"

link-inputs: ../input/council_land_use_missing_geography_adjudication_ai_repair_candidates.csv \
	../input/zap_bbl.csv \
	../input/nyc_mappluto_25v4_shp.zip \
	../input/dcp_council_boundary_archive_index.csv \
	../input/dcp_council_boundary_archive_output

include ../../generic.make
