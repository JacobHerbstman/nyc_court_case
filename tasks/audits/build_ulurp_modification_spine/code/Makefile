SHELL := bash
include ../../../shell_functions.make

START_YEAR := 2002
END_YEAR := 2025
SAMPLE_MODE := core

all: ../output/ulurp_modification_project_spine.csv \
	../output/ulurp_modification_project_matter_crosswalk.csv

../output/ulurp_modification_project_spine.csv: build_ulurp_modification_spine.R ../../../_lib/source_pipeline_utils.R ../input/zap_ulurp_project_base.csv ../input/council_land_use_decision_panel.csv ../input/member_deference_matter_universe.csv | ../output
	$(R) $< $(START_YEAR) $(END_YEAR) $(SAMPLE_MODE)

../output/ulurp_modification_project_matter_crosswalk.csv: ../output/ulurp_modification_project_spine.csv
	@test -s "$@"

../input/zap_ulurp_project_base.csv: ../../build_zap_ulurp_project_base/output/zap_ulurp_project_base.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/council_land_use_decision_panel.csv: ../../../build_council_land_use_decision_panel/output/council_land_use_decision_panel.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/member_deference_matter_universe.csv: ../../../build_member_deference_vote_panel/output/member_deference_matter_universe.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_ulurp_project_base.csv ../input/council_land_use_decision_panel.csv ../input/member_deference_matter_universe.csv

include ../../generic.make
