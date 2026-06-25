SHELL := bash
include ../../../shell_functions.make

all: ../output/brooklyn_homeownership_case_study_controls.csv ../output/brooklyn_homeownership_case_study_controls_qc.csv

../output/brooklyn_homeownership_case_study_controls.csv: build_brooklyn_homeownership_case_study_controls.R ../../../_lib/source_pipeline_utils.R ../input/cd_baseline_1990_controls.csv ../input/nhgis_files.csv ../input/nhgis_1990_tract_extract.parquet ../input/dcp_boundary_index.csv ../input/dcp_boundary_community_districts_20260501.parquet ../input/cd_redevelopment_potential_baseline.csv | ../output
	$(R) $<

../output/brooklyn_homeownership_case_study_controls_qc.csv: ../output/brooklyn_homeownership_case_study_controls.csv

../input/cd_baseline_1990_controls.csv: ../../../build_cd_baseline_1990_controls/output/cd_baseline_1990_controls.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/nhgis_files.csv: ../../../build_nhgis_extracts/output/nhgis_files.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/nhgis_1990_tract_extract.parquet: ../../../build_nhgis_extracts/output/nhgis_1990_tract_extract.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/dcp_boundary_index.csv: ../../../build_dcp_boundaries/output/dcp_boundary_index.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/dcp_boundary_community_districts_20260501.parquet: ../../../build_dcp_boundaries/output/dcp_boundary_community_districts_20260501.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/cd_redevelopment_potential_baseline.csv: ../../redevelopment_potential_first_pass/output/cd_redevelopment_potential_baseline.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/cd_baseline_1990_controls.csv ../input/nhgis_files.csv ../input/nhgis_1990_tract_extract.parquet ../input/dcp_boundary_index.csv ../input/dcp_boundary_community_districts_20260501.parquet ../input/cd_redevelopment_potential_baseline.csv

include ../../generic.make
