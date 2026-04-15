SHELL := bash
include ../../shell_functions.make

all: ../output/dcp_boundary_audit_summary.csv ../output/dcp_boundary_variable_quality.csv ../output/dcp_boundary_anomalies.csv ../output/dcp_boundary_figures.pdf ../output/dcp_boundary_maps.pdf

../input/dcp_boundary_index.csv: ../../stage_dcp_boundaries/output/dcp_boundary_index.csv | ../input
	ln -sf "../../stage_dcp_boundaries/output/dcp_boundary_index.csv" "$@"

../input/dcp_boundary_qc.csv: ../../stage_dcp_boundaries/output/dcp_boundary_qc.csv | ../input
	ln -sf "../../stage_dcp_boundaries/output/dcp_boundary_qc.csv" "$@"

../output/dcp_boundary_audit_summary.csv: summarize_dcp_boundaries.R ../input/dcp_boundary_index.csv ../input/dcp_boundary_qc.csv | ../output
	$(R) $< ../input/dcp_boundary_index.csv ../input/dcp_boundary_qc.csv ../output/dcp_boundary_audit_summary.csv ../output/dcp_boundary_variable_quality.csv ../output/dcp_boundary_anomalies.csv ../output/dcp_boundary_figures.pdf ../output/dcp_boundary_maps.pdf

../output/dcp_boundary_variable_quality.csv: ../output/dcp_boundary_audit_summary.csv

../output/dcp_boundary_anomalies.csv: ../output/dcp_boundary_audit_summary.csv

../output/dcp_boundary_figures.pdf: ../output/dcp_boundary_audit_summary.csv

../output/dcp_boundary_maps.pdf: ../output/dcp_boundary_audit_summary.csv

link-inputs: ../input/dcp_boundary_index.csv ../input/dcp_boundary_qc.csv

include ../../generic.make
