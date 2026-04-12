SHELL := bash
include ../../shell_functions.make

all: ../output/dcp_boundary_index.csv ../output/dcp_boundary_qc.csv

../input/dcp_boundary_files.csv: ../../fetch_dcp_boundaries/output/dcp_boundary_files.csv | ../input
	ln -sf "$<" "$@"

../output/dcp_boundary_index.csv: stage_dcp_boundaries.R ../input/dcp_boundary_files.csv | ../output
	$(R) $< ../input/dcp_boundary_files.csv ../output/dcp_boundary_index.csv ../output/dcp_boundary_qc.csv

../output/dcp_boundary_qc.csv: ../output/dcp_boundary_index.csv

link-inputs: ../input/dcp_boundary_files.csv

include ../../generic.make
