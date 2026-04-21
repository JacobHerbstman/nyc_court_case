SHELL := bash
include ../../shell_functions.make

all: ../output/dob_permit_issuance_harmonized_audit_summary.csv ../output/dob_permit_issuance_harmonized_anomalies.csv ../output/dob_permit_issuance_harmonized_city_year.csv ../output/dob_permit_issuance_harmonized_borough_year.csv ../output/dob_permit_issuance_harmonized_community_district.csv ../output/dob_permit_issuance_harmonized_bps_city_year_compare.csv ../output/dob_permit_issuance_harmonized_figures.pdf ../output/dob_permit_issuance_harmonized_maps.pdf

../input/dob_permit_issuance_harmonized.parquet: ../../build_dob_permit_issuance_harmonized/output/dob_permit_issuance_harmonized.parquet | ../input
	ln -sf "$<" "$@"

../input/dob_permit_issuance_harmonized_qc.csv: ../../build_dob_permit_issuance_harmonized/output/dob_permit_issuance_harmonized_qc.csv | ../input
	ln -sf "$<" "$@"

../input/current_source_decision_1989_2013.csv: ../../build_dob_permit_issuance_harmonized/code/current_source_decision_1989_2013.csv | ../input
	ln -sf "$<" "$@"

../input/dcp_boundary_index.csv: ../../stage_dcp_boundaries/output/dcp_boundary_index.csv | ../input
	ln -sf "$<" "$@"

../input/census_bps_city_year.parquet: ../../stage_census_bps/output/census_bps_city_year.parquet | ../input
	ln -sf "$<" "$@"

../output/dob_permit_issuance_harmonized_audit_summary.csv: summarize_dob_permit_issuance.R ../input/dob_permit_issuance_harmonized.parquet ../input/dob_permit_issuance_harmonized_qc.csv ../input/current_source_decision_1989_2013.csv ../input/dcp_boundary_index.csv ../input/census_bps_city_year.parquet | ../output
	$(R) $< ../input/dob_permit_issuance_harmonized.parquet ../input/dob_permit_issuance_harmonized_qc.csv ../input/current_source_decision_1989_2013.csv ../input/dcp_boundary_index.csv ../input/census_bps_city_year.parquet ../output/dob_permit_issuance_harmonized_audit_summary.csv ../output/dob_permit_issuance_harmonized_anomalies.csv ../output/dob_permit_issuance_harmonized_figures.pdf ../output/dob_permit_issuance_harmonized_maps.pdf

../output/dob_permit_issuance_harmonized_anomalies.csv: ../output/dob_permit_issuance_harmonized_audit_summary.csv

../output/dob_permit_issuance_harmonized_city_year.csv: ../output/dob_permit_issuance_harmonized_audit_summary.csv

../output/dob_permit_issuance_harmonized_borough_year.csv: ../output/dob_permit_issuance_harmonized_audit_summary.csv

../output/dob_permit_issuance_harmonized_community_district.csv: ../output/dob_permit_issuance_harmonized_audit_summary.csv

../output/dob_permit_issuance_harmonized_bps_city_year_compare.csv: ../output/dob_permit_issuance_harmonized_audit_summary.csv

../output/dob_permit_issuance_harmonized_figures.pdf: ../output/dob_permit_issuance_harmonized_audit_summary.csv

../output/dob_permit_issuance_harmonized_maps.pdf: ../output/dob_permit_issuance_harmonized_audit_summary.csv

link-inputs: ../input/dob_permit_issuance_harmonized.parquet ../input/dob_permit_issuance_harmonized_qc.csv ../input/current_source_decision_1989_2013.csv ../input/dcp_boundary_index.csv ../input/census_bps_city_year.parquet

include ../../generic.make
