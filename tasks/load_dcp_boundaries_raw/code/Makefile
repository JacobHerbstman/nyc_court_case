SHELL := bash
include ../../shell_functions.make

all: ../output/dcp_boundary_raw_files.csv

../output/dcp_boundary_raw_files.csv: load_dcp_boundaries_raw.R ../input/dcp_boundary_files.csv | ../output
	$(R) $<

../input/dcp_boundary_files.csv: ../../fetch_dcp_boundaries/output/dcp_boundary_files.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/dcp_boundary_files.csv

include ../../generic.make
