all: ../output/council_land_use_ai_geography_accepted_repairs.csv \
	../output/member_deference_nonapproval_geography_review_queue.csv \
	../output/member_deference_nonapproval_geography_chatgpt_review_responses.csv

../output/council_land_use_ai_geography_accepted_repairs.csv: normalize_accepted_ai_geography_repairs.py \
	accepted_ai_geography_repair_ledger.csv | ../output
	python3 $<

../output/member_deference_nonapproval_geography_review_queue.csv:
	@test -s "$@"

../output/member_deference_nonapproval_geography_chatgpt_review_responses.csv:
	@test -s "$@"

link-inputs:

include ../../generic.make
