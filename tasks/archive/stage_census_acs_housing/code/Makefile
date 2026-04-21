SHELL := bash
include ../../shell_functions.make

all: ../output/census_acs_housing_files.csv ../output/census_acs_housing_qc.csv ../output/census_acs_housing_tract_year.parquet ../output/census_acs_housing_borough_year.parquet ../output/census_acs_housing_city_year.parquet

../input/census_acs_housing_raw_files.csv: ../../load_census_acs_housing_raw/output/census_acs_housing_raw_files.csv | ../input
	ln -sf "../../load_census_acs_housing_raw/output/census_acs_housing_raw_files.csv" "$@"

../output/census_acs_housing_files.csv: stage_census_acs_housing.R ../input/census_acs_housing_raw_files.csv | ../output
	$(R) $< ../input/census_acs_housing_raw_files.csv ../output/census_acs_housing_files.csv ../output/census_acs_housing_qc.csv ../output/census_acs_housing_tract_year.parquet ../output/census_acs_housing_borough_year.parquet ../output/census_acs_housing_city_year.parquet

../output/census_acs_housing_qc.csv: ../output/census_acs_housing_files.csv

../output/census_acs_housing_tract_year.parquet: ../output/census_acs_housing_files.csv

../output/census_acs_housing_borough_year.parquet: ../output/census_acs_housing_files.csv

../output/census_acs_housing_city_year.parquet: ../output/census_acs_housing_files.csv

link-inputs: ../input/census_acs_housing_raw_files.csv

include ../../generic.make
