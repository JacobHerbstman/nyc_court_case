SHELL := bash
include ../../shell_functions.make

all: ../output/dcp_cd_profiles_1990_2000_files.csv ../output/dcp_cd_profiles_1990_2000_checksums.csv ../output/dcp_cd_profiles_1990_2000_provenance.csv

../input/source_catalog.csv: ../../source_registry/code/source_catalog.csv | ../input
	ln -sf "$<" "$@"

../output/dcp_cd_profiles_1990_2000_files.csv: fetch_dcp_cd_profiles_1990_2000.R ../input/source_catalog.csv | ../output
	$(R) $< ../input/source_catalog.csv ../output/dcp_cd_profiles_1990_2000_files.csv ../output/dcp_cd_profiles_1990_2000_checksums.csv ../output/dcp_cd_profiles_1990_2000_provenance.csv

../output/dcp_cd_profiles_1990_2000_checksums.csv: ../output/dcp_cd_profiles_1990_2000_files.csv

../output/dcp_cd_profiles_1990_2000_provenance.csv: ../output/dcp_cd_profiles_1990_2000_files.csv

link-inputs: ../input/source_catalog.csv

include ../../generic.make
