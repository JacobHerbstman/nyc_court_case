all: ../output/dcp_boundary_index.csv ../output/dcp_boundary_community_districts_20260501.parquet

../%/dcp_boundary_index.csv ../%/dcp_boundary_community_districts_20260501.parquet: build_dcp_boundaries.R Makefile ../output/dcp_boundary_raw_files.csv | ../output
	Rscript --vanilla $<

../output/dcp_boundary_raw_files.csv: load_dcp_boundaries_raw.R ../output/dcp_boundary_files.csv | ../output
	Rscript --vanilla $<

../output/dcp_boundary_files.csv: fetch_dcp_boundaries.R ../input/source_catalog.csv | ../output
	Rscript --vanilla $<

../input/source_catalog.csv: ../../source_registry/code/source_catalog.csv | ../input
	ln -sf "$<" "$@"

link-inputs: ../input/source_catalog.csv

include ../../generic.make
