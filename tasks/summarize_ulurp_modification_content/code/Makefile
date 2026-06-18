SHELL := bash
include ../../shell_functions.make

SUMMARY_MODE := first_pass
VALIDATION_CASE_SET := known_cases

all: ../output/ulurp_modification_project_summary.csv \
	../output/ulurp_modification_category_summary.csv \
	../output/ulurp_modification_stratum_year_summary.csv \
	../output/ulurp_modification_summary_qc.csv

../output/ulurp_modification_project_summary.csv: summarize_ulurp_modification_content.R \
	../input/ulurp_modification_project_spine.csv \
	../input/ulurp_modification_project_versions.csv \
	../input/ulurp_modification_discrete_modifications.csv \
	../input/ulurp_modification_commitments.csv \
	../input/ulurp_modification_citywide_text_district_modifications.csv \
	../../_lib/source_pipeline_utils.R | ../output
	$(R) $< $(SUMMARY_MODE) $(VALIDATION_CASE_SET)

../output/ulurp_modification_category_summary.csv: ../output/ulurp_modification_project_summary.csv
	@test -s "$@"

../output/ulurp_modification_stratum_year_summary.csv: ../output/ulurp_modification_project_summary.csv
	@test -s "$@"

../output/ulurp_modification_summary_qc.csv: ../output/ulurp_modification_project_summary.csv
	@test -s "$@"

../input/ulurp_modification_project_spine.csv: ../../build_ulurp_modification_spine/output/ulurp_modification_project_spine.csv | ../input
	@target="$$(cd ../../build_ulurp_modification_spine/output && pwd)/ulurp_modification_project_spine.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

../input/ulurp_modification_project_versions.csv: ../../build_ulurp_modification_content/output/ulurp_modification_project_versions.csv | ../input
	@target="$$(cd ../../build_ulurp_modification_content/output && pwd)/ulurp_modification_project_versions.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

../input/ulurp_modification_discrete_modifications.csv: ../../build_ulurp_modification_content/output/ulurp_modification_discrete_modifications.csv | ../input
	@target="$$(cd ../../build_ulurp_modification_content/output && pwd)/ulurp_modification_discrete_modifications.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

../input/ulurp_modification_commitments.csv: ../../build_ulurp_modification_content/output/ulurp_modification_commitments.csv | ../input
	@target="$$(cd ../../build_ulurp_modification_content/output && pwd)/ulurp_modification_commitments.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

../input/ulurp_modification_citywide_text_district_modifications.csv: ../../build_ulurp_modification_content/output/ulurp_modification_citywide_text_district_modifications.csv | ../input
	@target="$$(cd ../../build_ulurp_modification_content/output && pwd)/ulurp_modification_citywide_text_district_modifications.csv"; \
	if [ "$$(readlink "$@")" != "$$target" ]; then ln -sf "$$target" "$@"; fi

link-inputs: ../input/ulurp_modification_project_spine.csv \
	../input/ulurp_modification_project_versions.csv \
	../input/ulurp_modification_discrete_modifications.csv \
	../input/ulurp_modification_commitments.csv \
	../input/ulurp_modification_citywide_text_district_modifications.csv

include ../../generic.make
