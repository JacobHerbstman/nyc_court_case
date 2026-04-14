SHELL := bash
include ../../shell_functions.make

all: ../output/dcp_housing_database_raw_files.csv ../output/dcp_housing_database_raw_qc.csv

../input/dcp_housing_database_files.csv: ../../fetch_dcp_housing_database/output/dcp_housing_database_files.csv | ../input
	ln -sf "../../fetch_dcp_housing_database/output/dcp_housing_database_files.csv" "$@"

../output/dcp_housing_database_raw_files.csv: load_dcp_housing_database_raw.R ../input/dcp_housing_database_files.csv | ../output
	$(R) $< ../input/dcp_housing_database_files.csv ../output/dcp_housing_database_raw_files.csv ../output/dcp_housing_database_raw_qc.csv

../output/dcp_housing_database_raw_qc.csv: ../output/dcp_housing_database_raw_files.csv

link-inputs: ../input/dcp_housing_database_files.csv

include ../../generic.make
