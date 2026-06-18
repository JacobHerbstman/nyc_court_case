SHELL := bash
include ../../shell_functions.make

START_YEAR := 2002
END_YEAR := 2025
SAMPLE_MODE := core
VALIDATION_CASE_SET := known_cases

all: ../output/ulurp_modification_project_spine.csv \
	../output/ulurp_modification_project_matter_crosswalk.csv \
	../output/ulurp_modification_ambiguous_application_keys.csv \
	../output/ulurp_modification_validation_cases.csv \
	../output/ulurp_modification_spine_qc.csv

../output/ulurp_modification_project_spine.csv: build_ulurp_modification_spine.R ../../_lib/source_pipeline_utils.R ../input/zap_ulurp_redev_project_base.csv ../input/council_land_use_decision_panel.csv ../input/member_deference_matter_universe.csv | ../output
	$(R) $< $(START_YEAR) $(END_YEAR) $(SAMPLE_MODE) $(VALIDATION_CASE_SET)

../output/ulurp_modification_project_matter_crosswalk.csv: ../output/ulurp_modification_project_spine.csv
	@test -s "$@"

../output/ulurp_modification_ambiguous_application_keys.csv: ../output/ulurp_modification_project_spine.csv
	@test -s "$@"

../output/ulurp_modification_validation_cases.csv: ../output/ulurp_modification_project_spine.csv
	@test -s "$@"

../output/ulurp_modification_spine_qc.csv: ../output/ulurp_modification_project_spine.csv
	@test -s "$@"

../input/zap_ulurp_redev_project_base.csv: ../../build_zap_ulurp_redev_base/output/zap_ulurp_redev_project_base.csv | ../input
	@target="$$(cd ../../build_zap_ulurp_redev_base/output && pwd)/zap_ulurp_redev_project_base.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

../input/council_land_use_decision_panel.csv: ../../build_council_land_use_decision_panel/output/council_land_use_decision_panel.csv | ../input
	@target="$$(cd ../../build_council_land_use_decision_panel/output && pwd)/council_land_use_decision_panel.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

../input/member_deference_matter_universe.csv: ../../build_member_deference_vote_panel/output/member_deference_matter_universe.csv | ../input
	@target="$$(cd ../../build_member_deference_vote_panel/output && pwd)/member_deference_matter_universe.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

link-inputs: ../input/zap_ulurp_redev_project_base.csv ../input/council_land_use_decision_panel.csv ../input/member_deference_matter_universe.csv

include ../../generic.make
