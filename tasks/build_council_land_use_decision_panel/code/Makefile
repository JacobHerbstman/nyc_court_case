all: ../output/council_land_use_decision_panel.csv

../output/council_land_use_decision_panel.csv: build_council_land_use_decision_panel.py \
	../../_lib/member_deference_utils.py \
	../input/member_deference_matter_universe.csv \
	../input/member_deference_vote_panel.csv \
	../input/member_deference_nonapproval_geography_conservative_queue.csv \
	../input/member_deference_nonapproval_action_details.csv \
	../input/member_deference_nonapproval_local_member_vote_status.csv | ../output
	python3 $<

../input/member_deference_matter_universe.csv: ../../build_member_deference_vote_panel/output/member_deference_matter_universe.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/member_deference_vote_panel.csv: ../../build_member_deference_vote_panel/output/member_deference_vote_panel.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/member_deference_nonapproval_geography_conservative_queue.csv: ../../verify_member_deference_nonapproval_geography/output/member_deference_nonapproval_geography_conservative_queue.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/member_deference_nonapproval_action_details.csv: ../../fetch_council_land_use_nonapproval_votes/output/member_deference_nonapproval_action_details.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/member_deference_nonapproval_local_member_vote_status.csv: ../../fetch_council_land_use_nonapproval_votes/output/member_deference_nonapproval_local_member_vote_status.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/member_deference_matter_universe.csv \
	../input/member_deference_vote_panel.csv \
	../input/member_deference_nonapproval_geography_conservative_queue.csv \
	../input/member_deference_nonapproval_action_details.csv \
	../input/member_deference_nonapproval_local_member_vote_status.csv

include ../../generic.make
