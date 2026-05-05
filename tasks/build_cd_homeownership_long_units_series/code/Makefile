SHELL := bash
include ../../shell_functions.make

all: ../output/cd_homeownership_long_units_series.csv ../output/cd_homeownership_long_units_series_qc.csv

../output/cd_homeownership_long_units_series.csv: build_cd_homeownership_long_units_series.R ../input/cd_homeownership_1990_measure.csv ../input/cd_baseline_1990_controls.csv ../input/mappluto_construction_proxy_cd_year.csv ../input/dcp_housing_database_files.csv | ../output
	$(R) $<

../output/cd_homeownership_long_units_series_qc.csv: ../output/cd_homeownership_long_units_series.csv

../input/cd_homeownership_1990_measure.csv: ../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/cd_baseline_1990_controls.csv: ../../build_cd_baseline_1990_controls/output/cd_baseline_1990_controls.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/mappluto_construction_proxy_cd_year.csv: ../../build_mappluto_construction_proxy/output/mappluto_construction_proxy_cd_year.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/dcp_housing_database_files.csv: ../../stage_dcp_housing_database/output/dcp_housing_database_files.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/cd_homeownership_1990_measure.csv ../input/cd_baseline_1990_controls.csv ../input/mappluto_construction_proxy_cd_year.csv ../input/dcp_housing_database_files.csv

include ../../generic.make
