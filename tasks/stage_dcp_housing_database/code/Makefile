SHELL := bash
include ../../shell_functions.make

all: ../output/dcp_housing_database_files.csv ../output/dcp_housing_database_qc.csv ../output/dcp_housing_database_city_year.parquet ../output/dcp_housing_database_borough_year.parquet

../output/dcp_housing_database_files.csv: stage_dcp_housing_database.R ../input/dcp_housing_database_raw_files.csv | ../output
	$(R) $<

../output/dcp_housing_database_qc.csv: ../output/dcp_housing_database_files.csv
	@test -s "$@"

../output/dcp_housing_database_city_year.parquet: ../output/dcp_housing_database_files.csv
	@test -s "$@"

../output/dcp_housing_database_borough_year.parquet: ../output/dcp_housing_database_files.csv
	@test -s "$@"

../output/dcp_housing_database_project_level_25q4.parquet: ../output/dcp_housing_database_files.csv
	@test -s "$@"

../input/dcp_housing_database_raw_files.csv: ../../load_dcp_housing_database_raw/output/dcp_housing_database_raw_files.csv | ../input
	@test "$$(readlink "$@")" = "../../load_dcp_housing_database_raw/output/dcp_housing_database_raw_files.csv" || ln -sf "../../load_dcp_housing_database_raw/output/dcp_housing_database_raw_files.csv" "$@"

link-inputs: ../input/dcp_housing_database_raw_files.csv

include ../../generic.make
