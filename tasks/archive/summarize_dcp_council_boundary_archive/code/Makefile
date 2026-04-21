SHELL := bash
include ../../shell_functions.make

all: ../output/dcp_council_boundary_archive_inventory.csv ../output/dcp_council_boundary_archive_canonical_regimes.csv ../output/dcp_council_boundary_archive_audit_summary.csv ../output/dcp_council_boundary_archive_feasibility_gate.csv

../input/dcp_council_boundary_archive_files.csv: ../../fetch_dcp_council_boundary_archive/output/dcp_council_boundary_archive_files.csv | ../input
	ln -sf "$<" "$@"

../input/dcp_council_boundary_archive_index.csv: ../../stage_dcp_council_boundary_archive/output/dcp_council_boundary_archive_index.csv | ../input
	ln -sf "$<" "$@"

../input/dcp_council_boundary_archive_qc.csv: ../../stage_dcp_council_boundary_archive/output/dcp_council_boundary_archive_qc.csv | ../input
	ln -sf "$<" "$@"

../input/dcp_boundary_qc.csv: ../../stage_dcp_boundaries/output/dcp_boundary_qc.csv | ../input
	ln -sf "$<" "$@"

../output/dcp_council_boundary_archive_inventory.csv: summarize_dcp_council_boundary_archive.R ../input/dcp_council_boundary_archive_files.csv ../input/dcp_council_boundary_archive_index.csv ../input/dcp_council_boundary_archive_qc.csv ../input/dcp_boundary_qc.csv | ../output
	$(R) $< ../input/dcp_council_boundary_archive_files.csv ../input/dcp_council_boundary_archive_index.csv ../input/dcp_council_boundary_archive_qc.csv ../input/dcp_boundary_qc.csv ../output/dcp_council_boundary_archive_inventory.csv ../output/dcp_council_boundary_archive_signature_groups.csv ../output/dcp_council_boundary_archive_canonical_regimes.csv ../output/dcp_council_boundary_archive_audit_summary.csv ../output/dcp_council_boundary_archive_feasibility_gate.csv

../output/dcp_council_boundary_archive_signature_groups.csv: ../output/dcp_council_boundary_archive_inventory.csv

../output/dcp_council_boundary_archive_canonical_regimes.csv: ../output/dcp_council_boundary_archive_inventory.csv

../output/dcp_council_boundary_archive_audit_summary.csv: ../output/dcp_council_boundary_archive_inventory.csv

../output/dcp_council_boundary_archive_feasibility_gate.csv: ../output/dcp_council_boundary_archive_inventory.csv

link-inputs: ../input/dcp_council_boundary_archive_files.csv ../input/dcp_council_boundary_archive_index.csv ../input/dcp_council_boundary_archive_qc.csv ../input/dcp_boundary_qc.csv

include ../../generic.make
