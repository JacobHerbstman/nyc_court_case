SHELL := bash

all: ../output/member_deference_vote_panel_summary.csv \
	../output/member_deference_vote_panel_qc.csv \
	../output/member_deference_vote_panel_exception_candidates.csv \
	../output/member_deference_vote_panel_unresolved.csv \
	../output/member_deference_matter_universe_summary.csv \
	../output/member_deference_filed_matter_audit.csv \
	../output/member_deference_final_action_vote_queue_summary.csv

../output/member_deference_vote_panel_summary.csv: audit_member_deference_vote_panel.py \
	../input/member_deference_vote_panel.csv \
	../input/member_deference_matter_universe.csv \
	../input/member_deference_final_action_vote_queue.csv \
	../input/council_land_use_ai_geography_accepted_repairs.csv | ../output
	python3 $<

../output/member_deference_vote_panel_qc.csv: ../output/member_deference_vote_panel_summary.csv
	@test -s "$@"

../output/member_deference_vote_panel_exception_candidates.csv: ../output/member_deference_vote_panel_summary.csv
	@test -s "$@"

../output/member_deference_vote_panel_unresolved.csv: ../output/member_deference_vote_panel_summary.csv
	@test -s "$@"

../output/member_deference_matter_universe_summary.csv: ../output/member_deference_vote_panel_summary.csv
	@test -s "$@"

../output/member_deference_filed_matter_audit.csv: ../output/member_deference_vote_panel_summary.csv
	@test -s "$@"

../output/member_deference_final_action_vote_queue_summary.csv: ../output/member_deference_vote_panel_summary.csv
	@test -s "$@"

../input/member_deference_vote_panel.csv: ../../../build_member_deference_vote_panel/output/member_deference_vote_panel.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/member_deference_matter_universe.csv: ../../../build_member_deference_vote_panel/output/member_deference_matter_universe.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/member_deference_final_action_vote_queue.csv: ../../../build_member_deference_vote_panel/output/member_deference_final_action_vote_queue.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/council_land_use_ai_geography_accepted_repairs.csv: ../../../create_council_land_use_ai_geography_repairs/output/council_land_use_ai_geography_accepted_repairs.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/member_deference_vote_panel.csv \
	../input/member_deference_matter_universe.csv \
	../input/member_deference_final_action_vote_queue.csv \
	../input/council_land_use_ai_geography_accepted_repairs.csv

include ../../generic.make
