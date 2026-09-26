all: ../output/council_member_roster_master.csv

../output/council_member_roster_master.csv: build_council_member_roster.py \
	official_roster_corrections.csv \
	../../_lib/legistar_utils.py \
	../../_lib/member_deference_utils.py \
	../temp/council_member_roster_source_files.csv | ../output ../report
	python3 $<

../temp/council_member_roster_source_files.csv: fetch_council_member_roster_sources.py \
	../../_lib/legistar_utils.py | ../temp
	python3 $<

link-inputs:

include ../../generic.make
