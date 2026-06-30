all: ../output/council_member_roster_master.csv

../output/council_member_roster_master.csv: build_council_member_roster.py \
	../../_lib/legistar_utils.py \
	../temp/council_member_roster_source_files.csv | ../output
	python3 $<

../temp/council_member_roster_source_files.csv: fetch_council_member_roster_sources.py \
	../../_lib/legistar_utils.py | ../temp
	python3 $<

link-inputs:

include ../../generic.make
