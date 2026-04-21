SHELL := bash
include ../../shell_functions.make

all: ../output/cd_homeownership_long_units_level_year.csv ../output/cd_homeownership_long_units_level_era.csv ../output/cd_homeownership_long_units_built_form_controls.csv ../output/cd_homeownership_long_units_developability_residuals.csv ../output/cd_homeownership_long_units_levels_developability_plots.pdf ../output/cd_homeownership_long_units_levels_developability_qc.csv

../input/cd_homeownership_long_units_series.csv: ../../build_cd_homeownership_long_units_series/output/cd_homeownership_long_units_series.csv | ../input
	ln -sf "$<" "$@"

../input/dcp_housing_database_project_level_25q4.parquet: ../../stage_dcp_housing_database/output/dcp_housing_database_project_level_25q4.parquet | ../input
	ln -sf "$<" "$@"

../input/mappluto_construction_proxy_cd_year.csv: ../../build_mappluto_construction_proxy/output/mappluto_construction_proxy_cd_year.csv | ../input
	ln -sf "$<" "$@"

../input/dcp_mappluto_current_25v4.parquet: ../../stage_mappluto_lots/output/dcp_mappluto_current_25v4.parquet | ../input
	ln -sf "$<" "$@"

../input/cd_baseline_1990_controls.csv: ../../build_cd_baseline_1990_controls/output/cd_baseline_1990_controls.csv | ../input
	ln -sf "$<" "$@"

../output/cd_homeownership_long_units_level_year.csv: summarize_cd_homeownership_long_units_levels_developability.R ../input/cd_homeownership_long_units_series.csv ../input/dcp_housing_database_project_level_25q4.parquet ../input/mappluto_construction_proxy_cd_year.csv ../input/dcp_mappluto_current_25v4.parquet ../input/cd_baseline_1990_controls.csv | ../output
	$(R) $< ../input/cd_homeownership_long_units_series.csv ../input/dcp_housing_database_project_level_25q4.parquet ../input/mappluto_construction_proxy_cd_year.csv ../input/dcp_mappluto_current_25v4.parquet ../input/cd_baseline_1990_controls.csv ../output/cd_homeownership_long_units_level_year.csv ../output/cd_homeownership_long_units_level_era.csv ../output/cd_homeownership_long_units_built_form_controls.csv ../output/cd_homeownership_long_units_developability_residuals.csv ../output/cd_homeownership_long_units_levels_developability_plots.pdf ../output/cd_homeownership_long_units_levels_developability_qc.csv

../output/cd_homeownership_long_units_level_era.csv: ../output/cd_homeownership_long_units_level_year.csv

../output/cd_homeownership_long_units_built_form_controls.csv: ../output/cd_homeownership_long_units_level_year.csv

../output/cd_homeownership_long_units_developability_residuals.csv: ../output/cd_homeownership_long_units_level_year.csv

../output/cd_homeownership_long_units_levels_developability_plots.pdf: ../output/cd_homeownership_long_units_level_year.csv

../output/cd_homeownership_long_units_levels_developability_qc.csv: ../output/cd_homeownership_long_units_level_year.csv

link-inputs: ../input/cd_homeownership_long_units_series.csv ../input/dcp_housing_database_project_level_25q4.parquet ../input/mappluto_construction_proxy_cd_year.csv ../input/dcp_mappluto_current_25v4.parquet ../input/cd_baseline_1990_controls.csv

include ../../generic.make
