SHELL := bash
include ../../shell_functions.make

all: ../output/source_coverage_audit.csv ../output/human_qa_gate_status.csv

../input/source_catalog.csv: ../../source_registry/code/source_catalog.csv | ../input
	ln -sf "../../source_registry/code/source_catalog.csv" "$@"

../input/mappluto_lot_qc.csv: ../../stage_mappluto_lots/output/mappluto_lot_qc.csv | ../input
	ln -sf "../../stage_mappluto_lots/output/mappluto_lot_qc.csv" "$@"

../input/dob_open_data_qc.csv: ../../stage_dob_open_data/output/dob_open_data_qc.csv | ../input
	ln -sf "../../stage_dob_open_data/output/dob_open_data_qc.csv" "$@"

../input/nhgis_qc.csv: ../../stage_nhgis/output/nhgis_qc.csv | ../input
	ln -sf "../../stage_nhgis/output/nhgis_qc.csv" "$@"

../input/dcp_boundary_qc.csv: ../../stage_dcp_boundaries/output/dcp_boundary_qc.csv | ../input
	ln -sf "../../stage_dcp_boundaries/output/dcp_boundary_qc.csv" "$@"

../input/census_bps_qc.csv: ../../stage_census_bps/output/census_bps_qc.csv | ../input
	ln -sf "../../stage_census_bps/output/census_bps_qc.csv" "$@"

../input/dcp_housing_database_qc.csv: ../../stage_dcp_housing_database/output/dcp_housing_database_qc.csv | ../input
	ln -sf "../../stage_dcp_housing_database/output/dcp_housing_database_qc.csv" "$@"

../input/census_acs_housing_qc.csv: ../../stage_census_acs_housing/output/census_acs_housing_qc.csv | ../input
	ln -sf "../../stage_census_acs_housing/output/census_acs_housing_qc.csv" "$@"

../input/furman_coredata_files.csv: ../../stage_furman_coredata/output/furman_coredata_files.csv | ../input
	ln -sf "../../stage_furman_coredata/output/furman_coredata_files.csv" "$@"

../input/archival_record_inventory.csv: ../../stage_archival_records/output/archival_record_inventory.csv | ../input
	ln -sf "../../stage_archival_records/output/archival_record_inventory.csv" "$@"

../input/lot_identifier_crosswalk_qc.csv: ../../build_lot_crosswalks/output/lot_identifier_crosswalk_qc.csv | ../input
	ln -sf "../../build_lot_crosswalks/output/lot_identifier_crosswalk_qc.csv" "$@"

../input/mappluto_release_diff_summary.csv: ../../audit_mappluto_release_diffs/output/mappluto_release_diff_summary.csv | ../input
	ln -sf "../../audit_mappluto_release_diffs/output/mappluto_release_diff_summary.csv" "$@"

../input/benchmark_results.csv: ../../reconcile_source_benchmarks/output/benchmark_results.csv | ../input
	ln -sf "../../reconcile_source_benchmarks/output/benchmark_results.csv" "$@"

../output/source_coverage_audit.csv: build_source_coverage_audit.R ../input/source_catalog.csv ../input/mappluto_lot_qc.csv ../input/dob_open_data_qc.csv ../input/nhgis_qc.csv ../input/dcp_boundary_qc.csv ../input/census_bps_qc.csv ../input/dcp_housing_database_qc.csv ../input/census_acs_housing_qc.csv ../input/furman_coredata_files.csv ../input/archival_record_inventory.csv ../input/lot_identifier_crosswalk_qc.csv ../input/mappluto_release_diff_summary.csv ../input/benchmark_results.csv human_qa_spot_check.csv | ../output
	$(R) $< ../input/source_catalog.csv ../input/mappluto_lot_qc.csv ../input/dob_open_data_qc.csv ../input/nhgis_qc.csv ../input/dcp_boundary_qc.csv ../input/census_bps_qc.csv ../input/dcp_housing_database_qc.csv ../input/census_acs_housing_qc.csv ../input/furman_coredata_files.csv ../input/archival_record_inventory.csv ../input/lot_identifier_crosswalk_qc.csv ../input/mappluto_release_diff_summary.csv ../input/benchmark_results.csv human_qa_spot_check.csv ../output/source_coverage_audit.csv ../output/human_qa_gate_status.csv

../output/human_qa_gate_status.csv: ../output/source_coverage_audit.csv

link-inputs: ../input/source_catalog.csv ../input/mappluto_lot_qc.csv ../input/dob_open_data_qc.csv ../input/nhgis_qc.csv ../input/dcp_boundary_qc.csv ../input/census_bps_qc.csv ../input/dcp_housing_database_qc.csv ../input/census_acs_housing_qc.csv ../input/furman_coredata_files.csv ../input/archival_record_inventory.csv ../input/lot_identifier_crosswalk_qc.csv ../input/mappluto_release_diff_summary.csv ../input/benchmark_results.csv

include ../../generic.make
