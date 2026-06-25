SHELL := bash
include ../../../shell_functions.make

all: ../output/census_bps_raw_files.csv

../output/census_bps_raw_files.csv: load_census_bps_raw.R ../input/census_bps_files.csv | ../output
	$(R) $<

../input/census_bps_files.csv: ../../fetch_census_bps/output/census_bps_files.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/census_bps_files.csv

include ../../generic.make
