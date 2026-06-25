SHELL := bash
include ../../../shell_functions.make

all: ../output/zap_ulurp_redev_project_base.csv ../output/zap_ulurp_redev_project_base_qc.csv

../output/zap_ulurp_redev_project_base.csv: build_zap_ulurp_redev_base.R ../../../_lib/source_pipeline_utils.R ../input/zap_project_data.parquet ../input/zap_housing_cohort_base.csv ../input/zap_housing_hdb_project_summary.csv ../input/cd_redevelopment_potential_baseline.csv | ../output
	$(R) $<

../output/zap_ulurp_redev_project_base_qc.csv: ../output/zap_ulurp_redev_project_base.csv

../input/zap_project_data.parquet: ../../../stage_zap_datasets/output/zap_project_data.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_housing_cohort_base.csv: ../../../build_zap_housing_cohort_base/output/zap_housing_cohort_base.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_housing_hdb_project_summary.csv: ../../../build_zap_housing_hdb_link/output/zap_housing_hdb_project_summary.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/cd_redevelopment_potential_baseline.csv: ../../redevelopment_potential_first_pass/output/cd_redevelopment_potential_baseline.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_project_data.parquet ../input/zap_housing_cohort_base.csv ../input/zap_housing_hdb_project_summary.csv ../input/cd_redevelopment_potential_baseline.csv

include ../../generic.make
