SHELL := bash

all: ../output/council_land_use_ai_geography_accepted_repairs.csv

../output/council_land_use_ai_geography_accepted_repairs.csv: normalize_accepted_ai_geography_repairs.py \
	accepted_ai_geography_repair_ledger.csv \
	../input/council_land_use_remaining_split_geography_bundle_queue.csv \
	../input/council_land_use_remaining_split_geography_responses_combined.csv | ../output
	python3 $<

../input/council_land_use_remaining_split_geography_bundle_queue.csv: ../../audits/build_remaining_council_land_use_split_geography_review/output/council_land_use_remaining_split_geography_bundle_queue.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/council_land_use_remaining_split_geography_responses_combined.csv: ../../audits/build_remaining_council_land_use_split_geography_review/output/council_land_use_remaining_split_geography_responses_combined.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/council_land_use_remaining_split_geography_bundle_queue.csv \
	../input/council_land_use_remaining_split_geography_responses_combined.csv

include ../../generic.make
