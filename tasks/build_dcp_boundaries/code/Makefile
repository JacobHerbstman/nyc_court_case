SHELL := bash
include ../../shell_functions.make

all: ../output/dcp_boundary_index.csv ../output/dcp_boundary_community_districts_20260501.parquet

../output/dcp_boundary_index.csv: build_dcp_boundaries.R ../input/dcp_boundary_raw_files.csv | ../output
	$(R) $<

../output/dcp_boundary_community_districts_20260501.parquet: ../output/dcp_boundary_index.csv
	@test -s "$@"

../input/dcp_boundary_raw_files.csv: ../../load_dcp_boundaries_raw/output/dcp_boundary_raw_files.csv | ../input
	@test "$$(readlink "$@")" = "../../load_dcp_boundaries_raw/output/dcp_boundary_raw_files.csv" || ln -sf "../../load_dcp_boundaries_raw/output/dcp_boundary_raw_files.csv" "$@"

link-inputs: ../input/dcp_boundary_raw_files.csv

include ../../generic.make
