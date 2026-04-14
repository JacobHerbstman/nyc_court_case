SHELL := bash
include ../../shell_functions.make

all: ../output/census_acs_housing_files.csv ../output/census_acs_housing_checksums.csv ../output/census_acs_housing_provenance.csv

../input/source_catalog.csv: ../../source_registry/code/source_catalog.csv | ../input
	ln -sf "$<" "$@"

../output/census_acs_housing_files.csv: fetch_census_acs_housing.R ../input/source_catalog.csv | ../output
	$(R) $< ../input/source_catalog.csv ../output/census_acs_housing_files.csv ../output/census_acs_housing_checksums.csv ../output/census_acs_housing_provenance.csv

../output/census_acs_housing_checksums.csv: ../output/census_acs_housing_files.csv

../output/census_acs_housing_provenance.csv: ../output/census_acs_housing_files.csv

link-inputs: ../input/source_catalog.csv

include ../../generic.make
