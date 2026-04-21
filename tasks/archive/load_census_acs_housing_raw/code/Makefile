SHELL := bash
include ../../shell_functions.make

all: ../output/census_acs_housing_raw_files.csv ../output/census_acs_housing_raw_qc.csv

../input/census_acs_housing_files.csv: ../../fetch_census_acs_housing/output/census_acs_housing_files.csv | ../input
	ln -sf "../../fetch_census_acs_housing/output/census_acs_housing_files.csv" "$@"

../output/census_acs_housing_raw_files.csv: load_census_acs_housing_raw.R ../input/census_acs_housing_files.csv | ../output
	$(R) $< ../input/census_acs_housing_files.csv ../output/census_acs_housing_raw_files.csv ../output/census_acs_housing_raw_qc.csv

../output/census_acs_housing_raw_qc.csv: ../output/census_acs_housing_raw_files.csv

link-inputs: ../input/census_acs_housing_files.csv

include ../../generic.make
