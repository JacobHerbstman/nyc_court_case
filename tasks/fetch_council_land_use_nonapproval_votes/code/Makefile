all: ../output/member_deference_nonapproval_action_details.csv \
	../output/member_deference_nonapproval_local_member_vote_status.csv

../%/member_deference_nonapproval_action_details.csv ../%/member_deference_nonapproval_local_member_vote_status.csv: fetch_council_land_use_nonapproval_votes.py \
	../../_lib/legistar_utils.py \
	../../_lib/member_deference_utils.py \
	../input/member_deference_nonapproval_geography_conservative_queue.csv | ../output
	python3 $<

../input/member_deference_nonapproval_geography_conservative_queue.csv: ../../verify_member_deference_nonapproval_geography/output/member_deference_nonapproval_geography_conservative_queue.csv | ../input
	ln -sf "$<" "$@"

link-inputs: ../input/member_deference_nonapproval_geography_conservative_queue.csv

include ../../generic.make
