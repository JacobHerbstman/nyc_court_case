all: ../output/council_member_roster_source_files.csv

../output/council_member_roster_source_files.csv: fetch_council_member_roster_sources.py ../../_lib/legistar_utils.py | ../output
	python3 $<

link-inputs:

include ../../generic.make
