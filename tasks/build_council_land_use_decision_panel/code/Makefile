SHELL := bash

all: ../output/council_land_use_decision_panel.csv \
	../output/council_land_use_local_member_votes.csv

../output/council_land_use_decision_panel.csv: build_council_land_use_decision_panel.py \
	../input/member_deference_matter_universe.csv \
	../input/member_deference_vote_panel.csv \
	../input/member_deference_nonapproval_geography_conservative_queue.csv \
	../input/member_deference_nonapproval_action_details.csv \
	../input/member_deference_nonapproval_local_member_matter_audit.csv \
	../input/member_deference_nonapproval_local_member_votes.csv | ../output
	python3 $<

../output/council_land_use_local_member_votes.csv: ../output/council_land_use_decision_panel.csv
	@test -s "$@"

../input/member_deference_matter_universe.csv: ../../audits/build_member_deference_vote_panel/output/member_deference_matter_universe.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/member_deference_vote_panel.csv: ../../audits/build_member_deference_vote_panel/output/member_deference_vote_panel.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/member_deference_nonapproval_geography_conservative_queue.csv: ../../audits/verify_member_deference_nonapproval_geography/output/member_deference_nonapproval_geography_conservative_queue.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/member_deference_nonapproval_action_details.csv: ../../audits/fetch_member_deference_nonapproval_action_votes/output/member_deference_nonapproval_action_details.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/member_deference_nonapproval_local_member_matter_audit.csv: ../../audits/fetch_member_deference_nonapproval_action_votes/output/member_deference_nonapproval_local_member_matter_audit.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/member_deference_nonapproval_local_member_votes.csv: ../../audits/fetch_member_deference_nonapproval_action_votes/output/member_deference_nonapproval_local_member_votes.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/member_deference_matter_universe.csv \
	../input/member_deference_vote_panel.csv \
	../input/member_deference_nonapproval_geography_conservative_queue.csv \
	../input/member_deference_nonapproval_action_details.csv \
	../input/member_deference_nonapproval_local_member_matter_audit.csv \
	../input/member_deference_nonapproval_local_member_votes.csv

include ../../generic.make
