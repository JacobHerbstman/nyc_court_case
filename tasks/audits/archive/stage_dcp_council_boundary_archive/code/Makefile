SHELL := bash
include ../../shell_functions.make

all: ../output/dcp_council_boundary_archive_index.csv ../output/dcp_council_boundary_archive_qc.csv

../input/dcp_council_boundary_archive_raw_files.csv: ../../load_dcp_council_boundary_archive_raw/output/dcp_council_boundary_archive_raw_files.csv | ../input
	ln -sf "$<" "$@"

../output/dcp_council_boundary_archive_index.csv: stage_dcp_council_boundary_archive.R ../input/dcp_council_boundary_archive_raw_files.csv | ../output
	$(R) $< ../input/dcp_council_boundary_archive_raw_files.csv ../output/dcp_council_boundary_archive_index.csv ../output/dcp_council_boundary_archive_qc.csv

../output/dcp_council_boundary_archive_qc.csv: ../output/dcp_council_boundary_archive_index.csv

link-inputs: ../input/dcp_council_boundary_archive_raw_files.csv

include ../../generic.make
