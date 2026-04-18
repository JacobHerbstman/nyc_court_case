SHELL := bash
include ../../shell_functions.make

all: ../output/zap_housing_cohort_base.csv ../output/zap_housing_cohort_base_qc.csv

../input/zap_project_data.parquet: ../../stage_zap_datasets/output/zap_project_data.parquet | ../input
	ln -sf "$<" "$@"

../input/zap_project_bbl.parquet: ../../stage_zap_datasets/output/zap_project_bbl.parquet | ../input
	ln -sf "$<" "$@"

../input/cd_homeownership_1990_measure.csv: ../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	ln -sf "$<" "$@"

../input/cd_baseline_1990_controls.csv: ../../build_cd_baseline_1990_controls/output/cd_baseline_1990_controls.csv | ../input
	ln -sf "$<" "$@"

../output/zap_housing_cohort_base.csv: build_zap_housing_cohort_base.R ../input/zap_project_data.parquet ../input/zap_project_bbl.parquet ../input/cd_homeownership_1990_measure.csv ../input/cd_baseline_1990_controls.csv | ../output
	$(R) $< ../input/zap_project_data.parquet ../input/zap_project_bbl.parquet ../input/cd_homeownership_1990_measure.csv ../input/cd_baseline_1990_controls.csv ../output/zap_housing_cohort_base.csv ../output/zap_housing_cohort_base_qc.csv

../output/zap_housing_cohort_base_qc.csv: ../output/zap_housing_cohort_base.csv

link-inputs: ../input/zap_project_data.parquet ../input/zap_project_bbl.parquet ../input/cd_homeownership_1990_measure.csv ../input/cd_baseline_1990_controls.csv

include ../../generic.make
