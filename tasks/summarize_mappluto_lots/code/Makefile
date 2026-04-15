SHELL := bash
include ../../shell_functions.make

all: ../output/mappluto_audit_summary.csv ../output/mappluto_variable_quality.csv ../output/mappluto_anomalies.csv ../output/mappluto_cd_totals.csv ../output/mappluto_borough_totals.csv ../output/mappluto_figures.pdf ../output/mappluto_maps.pdf

../input/mappluto_lot_files.csv: ../../stage_mappluto_lots/output/mappluto_lot_files.csv | ../input
	ln -sf "../../stage_mappluto_lots/output/mappluto_lot_files.csv" "$@"

../input/mappluto_lot_qc.csv: ../../stage_mappluto_lots/output/mappluto_lot_qc.csv | ../input
	ln -sf "../../stage_mappluto_lots/output/mappluto_lot_qc.csv" "$@"

../input/mappluto_checksums.csv: ../../fetch_mappluto_archive/output/mappluto_checksums.csv | ../input
	ln -sf "../../fetch_mappluto_archive/output/mappluto_checksums.csv" "$@"

../input/dcp_boundary_index.csv: ../../stage_dcp_boundaries/output/dcp_boundary_index.csv | ../input
	ln -sf "../../stage_dcp_boundaries/output/dcp_boundary_index.csv" "$@"

../output/mappluto_audit_summary.csv: summarize_mappluto_lots.R ../input/mappluto_lot_files.csv ../input/mappluto_lot_qc.csv ../input/mappluto_checksums.csv ../input/dcp_boundary_index.csv | ../output
	$(R) $< ../input/mappluto_lot_files.csv ../input/mappluto_lot_qc.csv ../input/mappluto_checksums.csv ../input/dcp_boundary_index.csv ../output/mappluto_audit_summary.csv ../output/mappluto_variable_quality.csv ../output/mappluto_anomalies.csv ../output/mappluto_cd_totals.csv ../output/mappluto_borough_totals.csv ../output/mappluto_figures.pdf ../output/mappluto_maps.pdf

../output/mappluto_variable_quality.csv: ../output/mappluto_audit_summary.csv

../output/mappluto_anomalies.csv: ../output/mappluto_audit_summary.csv

../output/mappluto_cd_totals.csv: ../output/mappluto_audit_summary.csv

../output/mappluto_borough_totals.csv: ../output/mappluto_audit_summary.csv

../output/mappluto_figures.pdf: ../output/mappluto_audit_summary.csv

../output/mappluto_maps.pdf: ../output/mappluto_audit_summary.csv

link-inputs: ../input/mappluto_lot_files.csv ../input/mappluto_lot_qc.csv ../input/mappluto_checksums.csv ../input/dcp_boundary_index.csv

include ../../generic.make
