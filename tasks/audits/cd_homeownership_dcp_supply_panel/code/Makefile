SHELL := bash
include ../../../shell_functions.make

all: ../output/cd_homeownership_dcp_supply_panel.csv

../output/cd_homeownership_dcp_supply_panel.csv: build_cd_homeownership_dcp_supply_panel.R ../../../_lib/source_pipeline_utils.R ../input/cd_homeownership_1990_measure.csv ../input/cd_baseline_1990_controls.csv ../input/dcp_housing_database_project_level_25q4.parquet | ../output
	$(R) $<

../input/cd_homeownership_1990_measure.csv: ../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/cd_baseline_1990_controls.csv: ../../build_cd_baseline_1990_controls/output/cd_baseline_1990_controls.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/dcp_housing_database_project_level_25q4.parquet: ../../build_dcp_housing_database/output/dcp_housing_database_project_level_25q4.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/cd_homeownership_1990_measure.csv ../input/cd_baseline_1990_controls.csv ../input/dcp_housing_database_project_level_25q4.parquet

include ../../generic.make
