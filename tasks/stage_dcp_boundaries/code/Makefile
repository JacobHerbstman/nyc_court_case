SHELL := bash
include ../../shell_functions.make

all: ../output/dcp_boundary_index.csv ../output/dcp_boundary_qc.csv

../input/dcp_boundary_raw_files.csv: ../../load_dcp_boundaries_raw/output/dcp_boundary_raw_files.csv | ../input
	@test "$$(readlink "$@")" = "../../load_dcp_boundaries_raw/output/dcp_boundary_raw_files.csv" || ln -sf "../../load_dcp_boundaries_raw/output/dcp_boundary_raw_files.csv" "$@"

../output/dcp_boundary_index.csv ../output/dcp_boundary_qc.csv ../output/dcp_boundary_city_council_districts_20260501.parquet ../output/dcp_boundary_community_districts_20260501.parquet: stage_dcp_boundaries.R ../input/dcp_boundary_raw_files.csv | ../output
	$(R) $< ../input/dcp_boundary_raw_files.csv ../output/dcp_boundary_index.csv ../output/dcp_boundary_qc.csv

link-inputs: ../input/dcp_boundary_raw_files.csv

include ../../generic.make
