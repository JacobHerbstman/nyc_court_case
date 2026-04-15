SHELL := bash
include ../../shell_functions.make

all: ../output/dob_audit_summary.csv ../output/dob_variable_quality.csv ../output/dob_anomalies.csv ../output/dob_figures.pdf ../output/dob_maps.pdf

../input/dob_open_data_files.csv: ../../stage_dob_open_data/output/dob_open_data_files.csv | ../input
	ln -sf "../../stage_dob_open_data/output/dob_open_data_files.csv" "$@"

../input/dob_open_data_qc.csv: ../../stage_dob_open_data/output/dob_open_data_qc.csv | ../input
	ln -sf "../../stage_dob_open_data/output/dob_open_data_qc.csv" "$@"

../input/dob_field_dictionary.csv: ../../stage_dob_open_data/output/dob_field_dictionary.csv | ../input
	ln -sf "../../stage_dob_open_data/output/dob_field_dictionary.csv" "$@"

../input/dcp_boundary_index.csv: ../../stage_dcp_boundaries/output/dcp_boundary_index.csv | ../input
	ln -sf "../../stage_dcp_boundaries/output/dcp_boundary_index.csv" "$@"

../output/dob_audit_summary.csv: summarize_dob_open_data.R ../input/dob_open_data_files.csv ../input/dob_open_data_qc.csv ../input/dob_field_dictionary.csv ../input/dcp_boundary_index.csv | ../output
	$(R) $< ../input/dob_open_data_files.csv ../input/dob_open_data_qc.csv ../input/dob_field_dictionary.csv ../input/dcp_boundary_index.csv ../output/dob_audit_summary.csv ../output/dob_variable_quality.csv ../output/dob_anomalies.csv ../output/dob_figures.pdf ../output/dob_maps.pdf

../output/dob_variable_quality.csv: ../output/dob_audit_summary.csv

../output/dob_anomalies.csv: ../output/dob_audit_summary.csv

../output/dob_figures.pdf: ../output/dob_audit_summary.csv

../output/dob_maps.pdf: ../output/dob_audit_summary.csv

link-inputs: ../input/dob_open_data_files.csv ../input/dob_open_data_qc.csv ../input/dob_field_dictionary.csv ../input/dcp_boundary_index.csv

include ../../generic.make
