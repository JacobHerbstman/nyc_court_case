SHELL := bash
include ../../shell_functions.make

all: ../output/census_bps_city_year.parquet

../output/census_bps_city_year.parquet: stage_census_bps.R ../input/census_bps_raw_files.csv | ../output
	$(R) $<

../input/census_bps_raw_files.csv: ../../load_census_bps_raw/output/census_bps_raw_files.csv | ../input
	@test "$$(readlink "$@")" = "../../load_census_bps_raw/output/census_bps_raw_files.csv" || ln -sf "../../load_census_bps_raw/output/census_bps_raw_files.csv" "$@"

link-inputs: ../input/census_bps_raw_files.csv

include ../../generic.make
