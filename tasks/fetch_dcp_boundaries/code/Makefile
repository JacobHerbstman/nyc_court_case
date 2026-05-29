SHELL := bash
include ../../shell_functions.make

all: ../output/dcp_boundary_files.csv

../output/dcp_boundary_files.csv: fetch_dcp_boundaries.R ../input/source_catalog.csv | ../output
	$(R) $<

../input/source_catalog.csv: ../../source_registry/code/source_catalog.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/source_catalog.csv

include ../../generic.make
