SHELL := bash

all: ../output/member_deference_nonapproval_geography_recovery.csv \
	../output/member_deference_nonapproval_geography_recovery_summary.csv \
	../output/member_deference_nonapproval_geography_recovery_unresolved.csv \
	../output/member_deference_nonapproval_application_crosswalk.csv \
	../output/member_deference_nonapproval_title_location_candidates.csv \
	../output/member_deference_nonapproval_geography_review_queue.csv \
	../output/member_deference_nonapproval_geography_review_batches.md \
	../output/member_deference_nonapproval_geography_recovery_qc.csv

../output/member_deference_nonapproval_geography_recovery.csv: recover_member_deference_nonapproval_geography.py \
	../input/member_deference_final_action_vote_queue.csv \
	../input/zap_project_data.parquet \
	../input/zap_project_bbl.parquet \
	../input/dcp_mappluto_current_25v4.parquet | ../output
	python3 $<

../output/member_deference_nonapproval_geography_recovery_summary.csv: ../output/member_deference_nonapproval_geography_recovery.csv
	@test -s "$@"

../output/member_deference_nonapproval_geography_recovery_unresolved.csv: ../output/member_deference_nonapproval_geography_recovery.csv
	@test -s "$@"

../output/member_deference_nonapproval_application_crosswalk.csv: ../output/member_deference_nonapproval_geography_recovery.csv
	@test -s "$@"

../output/member_deference_nonapproval_title_location_candidates.csv: ../output/member_deference_nonapproval_geography_recovery.csv
	@test -s "$@"

../output/member_deference_nonapproval_geography_review_queue.csv: ../output/member_deference_nonapproval_geography_recovery.csv
	@test -s "$@"

../output/member_deference_nonapproval_geography_review_batches.md: ../output/member_deference_nonapproval_geography_recovery.csv
	@test -s "$@"

../output/member_deference_nonapproval_geography_recovery_qc.csv: ../output/member_deference_nonapproval_geography_recovery.csv
	@test -s "$@"

../output/member_deference_nonapproval_geography_chatgpt_review_responses.csv:
	@test -s "$@"

../input/member_deference_final_action_vote_queue.csv: ../../build_member_deference_vote_panel/output/member_deference_final_action_vote_queue.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_project_data.parquet: ../../../stage_zap_datasets/output/zap_project_data.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_project_bbl.parquet: ../../../stage_zap_datasets/output/zap_project_bbl.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/dcp_mappluto_current_25v4.parquet: ../../../stage_mappluto_lots/output/dcp_mappluto_current_25v4.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/member_deference_final_action_vote_queue.csv \
	../input/zap_project_data.parquet \
	../input/zap_project_bbl.parquet \
	../input/dcp_mappluto_current_25v4.parquet

include ../../../generic.make
