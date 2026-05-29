SHELL := bash

all: ../output/council_member_roster_master.csv

../output/council_member_roster_master.csv: stage_council_member_roster.py ../input/council_member_roster_source_files.csv | ../output
	python3 $<

../input/council_member_roster_source_files.csv: ../../fetch_council_member_roster_sources/output/council_member_roster_source_files.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/council_member_roster_source_files.csv

include ../../generic.make
