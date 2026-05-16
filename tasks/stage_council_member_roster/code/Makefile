SHELL := bash

all: ../output/council_member_roster_master.csv \
	../output/council_member_roster_terms.csv \
	../output/council_member_roster_source_files.csv \
	../output/council_member_roster_coverage.csv \
	../output/council_member_roster_known_date_checks.csv \
	../output/council_member_roster_manual_review.csv \
	../output/council_member_roster_overlap_qc.csv \
	../output/council_member_roster_stage_qc.csv

../output/council_member_roster_master.csv: stage_council_member_roster.py ../input/council_member_roster_source_files.csv | ../output
	python3 $<

../output/council_member_roster_terms.csv: ../output/council_member_roster_master.csv
	@test -s "$@"

../output/council_member_roster_source_files.csv: ../output/council_member_roster_master.csv
	@test -s "$@"

../output/council_member_roster_coverage.csv: ../output/council_member_roster_master.csv
	@test -s "$@"

../output/council_member_roster_known_date_checks.csv: ../output/council_member_roster_master.csv
	@test -s "$@"

../output/council_member_roster_manual_review.csv: ../output/council_member_roster_master.csv
	@test -s "$@"

../output/council_member_roster_overlap_qc.csv: ../output/council_member_roster_master.csv
	@test -s "$@"

../output/council_member_roster_stage_qc.csv: ../output/council_member_roster_master.csv
	@test -s "$@"

../input/council_member_roster_source_files.csv: ../../fetch_council_member_roster_sources/output/council_member_roster_source_files.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/council_member_roster_source_files.csv

include ../../generic.make
