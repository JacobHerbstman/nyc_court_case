SHELL := bash
include ../../shell_functions.make

all: ../output/dcp_council_boundary_archive_raw_files.csv ../output/dcp_council_boundary_archive_raw_qc.csv

../input/dcp_council_boundary_archive_files.csv: ../../fetch_dcp_council_boundary_archive/output/dcp_council_boundary_archive_files.csv | ../input
	ln -sf "$<" "$@"

../output/dcp_council_boundary_archive_raw_files.csv: load_dcp_council_boundary_archive_raw.R ../input/dcp_council_boundary_archive_files.csv | ../output
	$(R) $< ../input/dcp_council_boundary_archive_files.csv ../output/dcp_council_boundary_archive_raw_files.csv ../output/dcp_council_boundary_archive_raw_qc.csv

../output/dcp_council_boundary_archive_raw_qc.csv: ../output/dcp_council_boundary_archive_raw_files.csv

link-inputs: ../input/dcp_council_boundary_archive_files.csv

include ../../generic.make
