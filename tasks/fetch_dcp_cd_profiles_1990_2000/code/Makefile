SHELL := bash
include ../../shell_functions.make

all: ../output/dcp_cd_profiles_1990_2000_files.csv ../output/dcp_cd_profiles_1990_2000_checksums.csv ../output/dcp_cd_profiles_1990_2000_provenance.csv

../output/dcp_cd_profiles_1990_2000_files.csv: fetch_dcp_cd_profiles_1990_2000.R ../input/source_catalog.csv | ../output
	$(R) $<

../output/dcp_cd_profiles_1990_2000_checksums.csv: ../output/dcp_cd_profiles_1990_2000_files.csv
	@test -s "$@"

../output/dcp_cd_profiles_1990_2000_provenance.csv: ../output/dcp_cd_profiles_1990_2000_files.csv
	@test -s "$@"

../input/source_catalog.csv: ../../source_registry/code/source_catalog.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/source_catalog.csv

include ../../generic.make
