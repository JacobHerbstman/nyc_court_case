SHELL := bash
include ../../shell_functions.make

all: ../output/dcp_housing_database_files.csv ../output/dcp_housing_database_qc.csv ../output/dcp_housing_database_city_year.parquet ../output/dcp_housing_database_borough_year.parquet

../input/dcp_housing_database_raw_files.csv: ../../load_dcp_housing_database_raw/output/dcp_housing_database_raw_files.csv | ../input
	ln -sf "../../load_dcp_housing_database_raw/output/dcp_housing_database_raw_files.csv" "$@"

../output/dcp_housing_database_files.csv: stage_dcp_housing_database.R ../input/dcp_housing_database_raw_files.csv | ../output
	$(R) $< ../input/dcp_housing_database_raw_files.csv ../output/dcp_housing_database_files.csv ../output/dcp_housing_database_qc.csv ../output/dcp_housing_database_city_year.parquet ../output/dcp_housing_database_borough_year.parquet

../output/dcp_housing_database_qc.csv: ../output/dcp_housing_database_files.csv

../output/dcp_housing_database_city_year.parquet: ../output/dcp_housing_database_files.csv

../output/dcp_housing_database_borough_year.parquet: ../output/dcp_housing_database_files.csv

link-inputs: ../input/dcp_housing_database_raw_files.csv

include ../../generic.make
