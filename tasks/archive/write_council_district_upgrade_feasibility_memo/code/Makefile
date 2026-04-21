SHELL := bash
include ../../shell_functions.make

all: ../output/council_district_upgrade_feasibility_memo.md ../output/council_district_upgrade_feasibility_decision_table.csv

../input/dcp_council_boundary_archive_audit_summary.csv: ../../summarize_dcp_council_boundary_archive/output/dcp_council_boundary_archive_audit_summary.csv | ../input
	ln -sf "$<" "$@"

../input/dcp_council_boundary_archive_canonical_regimes.csv: ../../summarize_dcp_council_boundary_archive/output/dcp_council_boundary_archive_canonical_regimes.csv | ../input
	ln -sf "$<" "$@"

../input/dcp_council_boundary_archive_feasibility_gate.csv: ../../summarize_dcp_council_boundary_archive/output/dcp_council_boundary_archive_feasibility_gate.csv | ../input
	ln -sf "$<" "$@"

../output/council_district_upgrade_feasibility_memo.md: write_council_district_upgrade_feasibility_memo.R ../input/dcp_council_boundary_archive_audit_summary.csv ../input/dcp_council_boundary_archive_canonical_regimes.csv ../input/dcp_council_boundary_archive_feasibility_gate.csv | ../output
	$(R) $< ../input/dcp_council_boundary_archive_audit_summary.csv ../input/dcp_council_boundary_archive_canonical_regimes.csv ../input/dcp_council_boundary_archive_feasibility_gate.csv ../output/council_district_upgrade_feasibility_memo.md ../output/council_district_upgrade_feasibility_decision_table.csv

../output/council_district_upgrade_feasibility_decision_table.csv: ../output/council_district_upgrade_feasibility_memo.md

link-inputs: ../input/dcp_council_boundary_archive_audit_summary.csv ../input/dcp_council_boundary_archive_canonical_regimes.csv ../input/dcp_council_boundary_archive_feasibility_gate.csv

include ../../generic.make
