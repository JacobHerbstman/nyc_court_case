SHELL := bash
include ../../shell_functions.make

all: ../output/brooklyn_homeownership_case_study_controls.csv ../output/brooklyn_homeownership_case_study_controls_qc.csv

../input/cd_baseline_1990_controls.csv: ../../build_cd_baseline_1990_controls/output/cd_baseline_1990_controls.csv | ../input
	ln -sf "$<" "$@"

../input/nhgis_files.csv: ../../stage_nhgis/output/nhgis_files.csv | ../input
	ln -sf "$<" "$@"

../input/nhgis_1990_tract_extract.parquet: ../../stage_nhgis/output/nhgis_1990_tract_extract.parquet | ../input
	ln -sf "$<" "$@"

../input/dcp_boundary_index.csv: ../../stage_dcp_boundaries/output/dcp_boundary_index.csv | ../input
	ln -sf "$<" "$@"

../input/dcp_boundary_community_districts_20260412.parquet: ../../stage_dcp_boundaries/output/dcp_boundary_community_districts_20260412.parquet | ../input
	ln -sf "$<" "$@"

../input/cd_redevelopment_potential_baseline.csv: ../../redevelopment_potential_first_pass/output/cd_redevelopment_potential_baseline.csv | ../input
	ln -sf "$<" "$@"

../output/brooklyn_homeownership_case_study_controls.csv: build_brooklyn_homeownership_case_study_controls.R ../input/cd_baseline_1990_controls.csv ../input/nhgis_files.csv ../input/nhgis_1990_tract_extract.parquet ../input/dcp_boundary_index.csv ../input/dcp_boundary_community_districts_20260412.parquet ../input/cd_redevelopment_potential_baseline.csv | ../output
	$(R) $< ../input/cd_baseline_1990_controls.csv ../input/nhgis_files.csv ../input/nhgis_1990_tract_extract.parquet ../input/dcp_boundary_index.csv ../input/dcp_boundary_community_districts_20260412.parquet ../input/cd_redevelopment_potential_baseline.csv ../output/brooklyn_homeownership_case_study_controls.csv ../output/brooklyn_homeownership_case_study_controls_qc.csv

../output/brooklyn_homeownership_case_study_controls_qc.csv: ../output/brooklyn_homeownership_case_study_controls.csv

link-inputs: ../input/cd_baseline_1990_controls.csv ../input/nhgis_files.csv ../input/nhgis_1990_tract_extract.parquet ../input/dcp_boundary_index.csv ../input/dcp_boundary_community_districts_20260412.parquet ../input/cd_redevelopment_potential_baseline.csv

include ../../generic.make
