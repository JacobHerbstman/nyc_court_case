SHELL := bash

all: ../output/member_deference_nonapproval_action_details.csv \
	../output/member_deference_nonapproval_local_member_votes.csv \
	../output/member_deference_nonapproval_local_member_vote_status.csv

../output/member_deference_nonapproval_action_details.csv: fetch_member_deference_nonapproval_action_votes.py \
	../input/member_deference_final_action_vote_queue.csv | ../output
	python3 $<

../output/member_deference_nonapproval_member_votes.csv: ../output/member_deference_nonapproval_action_details.csv
	@test -s "$@"

../output/member_deference_nonapproval_local_member_votes.csv: ../output/member_deference_nonapproval_action_details.csv
	@test -s "$@"

../output/member_deference_nonapproval_local_member_vote_status.csv: ../output/member_deference_nonapproval_action_details.csv
	@test -s "$@"

../input/member_deference_final_action_vote_queue.csv: ../../verify_member_deference_nonapproval_geography/output/member_deference_nonapproval_geography_conservative_queue.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/member_deference_final_action_vote_queue.csv

include ../../generic.make
