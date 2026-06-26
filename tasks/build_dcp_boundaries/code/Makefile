SHELL := bash
include ../../shell_functions.make

all: ../output/dcp_boundary_index.csv ../output/dcp_boundary_community_districts_20260501.parquet

../output/dcp_boundary_index.csv: build_dcp_boundaries.R ../output/dcp_boundary_raw_files.csv | ../output
	$(R) $<

../output/dcp_boundary_community_districts_20260501.parquet: ../output/dcp_boundary_index.csv
	@test -s "$@"

../output/dcp_boundary_raw_files.csv: load_dcp_boundaries_raw.R ../output/dcp_boundary_files.csv | ../output
	$(R) $<

../output/dcp_boundary_files.csv: fetch_dcp_boundaries.R ../input/source_catalog.csv | ../output
	$(R) $<

../input/source_catalog.csv: ../../source_registry/code/source_catalog.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/source_catalog.csv

include ../../generic.make
