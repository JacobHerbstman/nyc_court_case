SHELL := bash
include ../../../shell_functions.make

all: ../output/zap_ulurp_project_base.csv

../output/zap_ulurp_project_base.csv: build_zap_ulurp_project_base.R ../../../_lib/source_pipeline_utils.R ../input/zap_project_data.parquet ../input/zap_housing_cohort_base.csv ../input/zap_housing_hdb_project_summary.csv | ../output
	$(R) $<

../input/zap_project_data.parquet: ../../../build_zap_datasets/output/zap_project_data.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_housing_cohort_base.csv: ../../build_zap_housing_cohort_base/output/zap_housing_cohort_base.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_housing_hdb_project_summary.csv: ../../build_zap_housing_hdb_link/output/zap_housing_hdb_project_summary.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_project_data.parquet ../input/zap_housing_cohort_base.csv ../input/zap_housing_hdb_project_summary.csv

include ../../generic.make
