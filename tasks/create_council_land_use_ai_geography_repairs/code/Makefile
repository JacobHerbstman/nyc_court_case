all: ../output/council_land_use_ai_geography_accepted_repairs.csv

../output/council_land_use_ai_geography_accepted_repairs.csv: normalize_accepted_ai_geography_repairs.py \
	accepted_ai_geography_repair_ledger.csv | ../output
	python3 $<

link-inputs:

include ../../generic.make
