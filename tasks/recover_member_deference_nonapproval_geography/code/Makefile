all: ../output/member_deference_nonapproval_geography_recovery.csv

../output/member_deference_nonapproval_geography_recovery.csv: recover_member_deference_nonapproval_geography.py \
	../../_lib/member_deference_utils.py \
	../input/member_deference_final_action_vote_queue.csv \
	../input/zap_project_data.parquet \
	../input/zap_project_bbl.parquet \
	../input/mappluto_current_lot_lookup.parquet | ../output
	python3 $<

../input/member_deference_final_action_vote_queue.csv: ../../build_member_deference_vote_panel/output/member_deference_final_action_vote_queue.csv | ../input
	ln -sf "$<" "$@"

../input/zap_project_data.parquet: ../../build_zap_datasets/output/zap_project_data.parquet | ../input
	ln -sf "$<" "$@"

../input/zap_project_bbl.parquet: ../../build_zap_datasets/output/zap_project_bbl.parquet | ../input
	ln -sf "$<" "$@"

../input/mappluto_current_lot_lookup.parquet: ../../build_mappluto_current_lookup/output/mappluto_current_lot_lookup.parquet | ../input
	ln -sf "$<" "$@"

link-inputs: ../input/member_deference_final_action_vote_queue.csv \
	../input/zap_project_data.parquet \
	../input/zap_project_bbl.parquet \
	../input/mappluto_current_lot_lookup.parquet

include ../../generic.make
