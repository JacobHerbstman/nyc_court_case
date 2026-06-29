SHELL := bash
include ../../../shell_functions.make

all: ../output/zap_housing_cohort_base.csv

../output/zap_housing_cohort_base.csv: build_zap_housing_cohort_base.R ../../../_lib/source_pipeline_utils.R ../input/zap_project_data.parquet ../input/zap_project_bbl.parquet ../input/cd_homeownership_1990_measure.csv ../input/cd_baseline_1990_controls.csv | ../output
	$(R) $<

../input/zap_project_data.parquet: ../../../build_zap_datasets/output/zap_project_data.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_project_bbl.parquet: ../../../build_zap_datasets/output/zap_project_bbl.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/cd_homeownership_1990_measure.csv: ../../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/cd_baseline_1990_controls.csv: ../../../build_cd_baseline_1990_controls/output/cd_baseline_1990_controls.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_project_data.parquet ../input/zap_project_bbl.parquet ../input/cd_homeownership_1990_measure.csv ../input/cd_baseline_1990_controls.csv

include ../../generic.make
