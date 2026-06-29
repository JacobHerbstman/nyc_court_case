SHELL := bash
include ../../../shell_functions.make

all: ../output/dcp_housing_database_files.csv ../output/dcp_housing_database_project_level_25q4.parquet

../output/dcp_housing_database_files.csv: build_dcp_housing_database.R ../output/dcp_housing_database_raw_files.csv | ../output
	$(R) $<

../output/dcp_housing_database_project_level_25q4.parquet: ../output/dcp_housing_database_files.csv
	@test -s "$@"

../output/dcp_housing_database_raw_files.csv: load_dcp_housing_database_raw.R ../output/dcp_housing_database_source_files.csv | ../output
	$(R) $<

../output/dcp_housing_database_source_files.csv: fetch_dcp_housing_database.R ../input/source_catalog.csv | ../output
	$(R) $<

../input/source_catalog.csv: ../../../source_registry/code/source_catalog.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/source_catalog.csv

include ../../generic.make
