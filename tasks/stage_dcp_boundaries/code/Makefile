SHELL := bash
include ../../shell_functions.make

all: ../output/dcp_boundary_index.csv ../output/dcp_boundary_qc.csv

../input/dcp_boundary_raw_files.csv: ../../load_dcp_boundaries_raw/output/dcp_boundary_raw_files.csv | ../input
	ln -sf "../../load_dcp_boundaries_raw/output/dcp_boundary_raw_files.csv" "$@"

../output/dcp_boundary_index.csv: stage_dcp_boundaries.R ../input/dcp_boundary_raw_files.csv | ../output
	$(R) $< ../input/dcp_boundary_raw_files.csv ../output/dcp_boundary_index.csv ../output/dcp_boundary_qc.csv

../output/dcp_boundary_qc.csv: ../output/dcp_boundary_index.csv

link-inputs: ../input/dcp_boundary_raw_files.csv

include ../../generic.make
