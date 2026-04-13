SHELL := bash
include ../../shell_functions.make

all: ../output/census_bps_index.csv ../output/census_bps_qc.csv

../input/census_bps_raw_files.csv: ../../load_census_bps_raw/output/census_bps_raw_files.csv | ../input
	ln -sf "../../load_census_bps_raw/output/census_bps_raw_files.csv" "$@"

../output/census_bps_index.csv: stage_census_bps.R ../input/census_bps_raw_files.csv | ../output
	$(R) $< ../input/census_bps_raw_files.csv ../output/census_bps_index.csv ../output/census_bps_qc.csv

../output/census_bps_qc.csv: ../output/census_bps_index.csv

link-inputs: ../input/census_bps_raw_files.csv

include ../../generic.make
