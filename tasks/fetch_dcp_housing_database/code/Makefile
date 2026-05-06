SHELL := bash
include ../../shell_functions.make

all: ../output/dcp_housing_database_files.csv ../output/dcp_housing_database_checksums.csv ../output/dcp_housing_database_provenance.csv

../output/dcp_housing_database_files.csv: fetch_dcp_housing_database.R ../input/source_catalog.csv | ../output
	$(R) $<

../output/dcp_housing_database_checksums.csv: ../output/dcp_housing_database_files.csv
	@test -s "$@"

../output/dcp_housing_database_provenance.csv: ../output/dcp_housing_database_files.csv
	@test -s "$@"

../input/source_catalog.csv: ../../source_registry/code/source_catalog.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/source_catalog.csv

include ../../generic.make
