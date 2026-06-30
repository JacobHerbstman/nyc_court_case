all: ../output/member_deference_nonapproval_geography_conservative_queue.csv

../output/member_deference_nonapproval_geography_conservative_queue.csv: verify_member_deference_nonapproval_geography.py \
	../../_lib/member_deference_utils.py \
	../input/member_deference_final_action_vote_queue.csv \
	../input/member_deference_nonapproval_geography_recovery.csv \
	../input/member_deference_nonapproval_geography_review_queue.csv \
	../input/member_deference_nonapproval_geography_chatgpt_review_responses.csv \
	../input/member_deference_matter_universe.csv \
	../input/mappluto_current_lot_lookup.parquet \
	../input/council_member_roster_master.csv | ../output
	python3 $<

../input/member_deference_final_action_vote_queue.csv: ../../build_member_deference_vote_panel/output/member_deference_final_action_vote_queue.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/member_deference_nonapproval_geography_recovery.csv: ../../recover_member_deference_nonapproval_geography/output/member_deference_nonapproval_geography_recovery.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/member_deference_nonapproval_geography_review_queue.csv: ../../create_council_land_use_geography_review_ledgers/output/member_deference_nonapproval_geography_review_queue.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/member_deference_nonapproval_geography_chatgpt_review_responses.csv: ../../create_council_land_use_geography_review_ledgers/output/member_deference_nonapproval_geography_chatgpt_review_responses.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/member_deference_matter_universe.csv: ../../build_member_deference_vote_panel/output/member_deference_matter_universe.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/mappluto_current_lot_lookup.parquet: ../../build_mappluto_current_lookup/output/mappluto_current_lot_lookup.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/council_member_roster_master.csv: ../../build_council_member_roster/output/council_member_roster_master.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/member_deference_final_action_vote_queue.csv \
	../input/member_deference_nonapproval_geography_recovery.csv \
	../input/member_deference_nonapproval_geography_review_queue.csv \
	../input/member_deference_nonapproval_geography_chatgpt_review_responses.csv \
	../input/member_deference_matter_universe.csv \
	../input/mappluto_current_lot_lookup.parquet \
	../input/council_member_roster_master.csv

include ../../generic.make
