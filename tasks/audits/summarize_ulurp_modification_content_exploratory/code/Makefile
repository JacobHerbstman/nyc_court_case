SHELL := bash

all: ../output/ulurp_modification_category_summary.csv \
	../output/ulurp_modification_stratum_year_summary.csv

../output/ulurp_modification_category_summary.csv: summarize_ulurp_modification_content_exploratory.R \
	../input/ulurp_modification_project_spine.csv \
	../input/ulurp_modification_project_summary.csv \
	../input/ulurp_modification_discrete_modifications.csv \
	../../../_lib/source_pipeline_utils.R | ../output
	$(R) $<

../output/ulurp_modification_stratum_year_summary.csv: ../output/ulurp_modification_category_summary.csv
	@test -s "$@"

../input/ulurp_modification_project_spine.csv: ../../../build_ulurp_modification_spine/output/ulurp_modification_project_spine.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_modification_project_summary.csv: ../../../summarize_ulurp_modification_content/output/ulurp_modification_project_summary.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_modification_discrete_modifications.csv: ../../../build_ulurp_modification_content/output/ulurp_modification_discrete_modifications.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/ulurp_modification_project_spine.csv \
	../input/ulurp_modification_project_summary.csv \
	../input/ulurp_modification_discrete_modifications.csv

include ../../generic.make
