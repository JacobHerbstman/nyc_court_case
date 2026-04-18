SHELL := bash
include ../../shell_functions.make

all: ../output/cd_homeownership_dcp_supply_panel.csv ../output/cd_homeownership_dcp_supply_panel_qc.csv

../input/cd_homeownership_1990_measure.csv: ../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	ln -sf "$<" "$@"

../input/cd_baseline_1990_controls.csv: ../../build_cd_baseline_1990_controls/output/cd_baseline_1990_controls.csv | ../input
	ln -sf "$<" "$@"

../input/dcp_housing_database_project_level_25q4.parquet: ../../stage_dcp_housing_database/output/dcp_housing_database_project_level_25q4.parquet | ../input
	ln -sf "$<" "$@"

../output/cd_homeownership_dcp_supply_panel.csv: build_cd_homeownership_dcp_supply_panel.R ../input/cd_homeownership_1990_measure.csv ../input/cd_baseline_1990_controls.csv ../input/dcp_housing_database_project_level_25q4.parquet | ../output
	$(R) $< ../input/cd_homeownership_1990_measure.csv ../input/cd_baseline_1990_controls.csv ../input/dcp_housing_database_project_level_25q4.parquet ../output/cd_homeownership_dcp_supply_panel.csv ../output/cd_homeownership_dcp_supply_panel_qc.csv

../output/cd_homeownership_dcp_supply_panel_qc.csv: ../output/cd_homeownership_dcp_supply_panel.csv

link-inputs: ../input/cd_homeownership_1990_measure.csv ../input/cd_baseline_1990_controls.csv ../input/dcp_housing_database_project_level_25q4.parquet

include ../../generic.make
