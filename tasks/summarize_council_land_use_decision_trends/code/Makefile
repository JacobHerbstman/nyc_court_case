SHELL := bash

all: ../output/council_land_use_adoption_over_local_member_rollcall_opposition_rolling5_with_raw_clean.pdf

../output/council_land_use_adoption_over_local_member_rollcall_opposition_rolling5_with_raw_clean.pdf: summarize_council_land_use_decision_trends.R \
	../input/council_land_use_decision_panel.csv \
	../input/council_land_use_decision_universe_validation_summary.csv | ../output
	Rscript $<

../input/council_land_use_decision_panel.csv: ../../build_council_land_use_decision_panel/output/council_land_use_decision_panel.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/council_land_use_decision_universe_validation_summary.csv: ../../validate_council_land_use_decision_universe/output/council_land_use_decision_universe_validation_summary.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/council_land_use_decision_panel.csv \
	../input/council_land_use_decision_universe_validation_summary.csv

include ../../generic.make
