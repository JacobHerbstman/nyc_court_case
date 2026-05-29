SHELL := bash
include ../../shell_functions.make

all: ../output/census_bps_files.csv

../output/census_bps_files.csv: fetch_census_bps.R ../input/source_catalog.csv | ../output
	$(R) $<

../input/source_catalog.csv: ../../source_registry/code/source_catalog.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/source_catalog.csv

include ../../generic.make
