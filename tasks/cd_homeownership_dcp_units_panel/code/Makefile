SHELL := bash
include ../../shell_functions.make

all: ../output/cd_homeownership_dcp_units_panel.csv ../output/cd_homeownership_dcp_units_panel_qc.csv

../input/cd_homeownership_1990_measure.csv: ../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	ln -sf "$<" "$@"

../input/dcp_housing_database_project_level_25q4.parquet: ../../stage_dcp_housing_database/output/dcp_housing_database_project_level_25q4.parquet | ../input
	ln -sf "$<" "$@"

../output/cd_homeownership_dcp_units_panel.csv: build_cd_homeownership_dcp_units_panel.R ../input/cd_homeownership_1990_measure.csv ../input/dcp_housing_database_project_level_25q4.parquet | ../output
	$(R) $< ../input/cd_homeownership_1990_measure.csv ../input/dcp_housing_database_project_level_25q4.parquet ../output/cd_homeownership_dcp_units_panel.csv ../output/cd_homeownership_dcp_units_panel_qc.csv

../output/cd_homeownership_dcp_units_panel_qc.csv: ../output/cd_homeownership_dcp_units_panel.csv

link-inputs: ../input/cd_homeownership_1990_measure.csv ../input/dcp_housing_database_project_level_25q4.parquet

include ../../generic.make
