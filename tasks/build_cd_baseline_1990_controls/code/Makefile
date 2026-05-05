SHELL := bash
include ../../shell_functions.make

all: ../output/cd_baseline_1990_controls.csv ../output/cd_baseline_1990_controls_qc.csv ../output/cd_baseline_1990_controls_overlay_qc.csv

../output/cd_baseline_1990_controls.csv: build_cd_baseline_1990_controls.R ../../_lib/source_pipeline_utils.R ../input/cd_homeownership_1990_measure.csv ../input/dcp_cd_profiles_1990_2000_files.csv ../input/nhgis_files.csv ../input/nhgis_1980_tract_extract.parquet ../input/nhgis_1990_tract_extract.parquet ../input/dcp_boundary_index.csv | ../output
	$(R) $<

../output/cd_baseline_1990_controls_qc.csv: ../output/cd_baseline_1990_controls.csv

../output/cd_baseline_1990_controls_overlay_qc.csv: ../output/cd_baseline_1990_controls.csv

../input/cd_homeownership_1990_measure.csv: ../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/dcp_cd_profiles_1990_2000_files.csv: ../../stage_dcp_cd_profiles_1990_2000/output/dcp_cd_profiles_1990_2000_files.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/nhgis_files.csv: ../../stage_nhgis/output/nhgis_files.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/nhgis_1980_tract_extract.parquet: ../../stage_nhgis/output/nhgis_1980_tract_extract.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/nhgis_1990_tract_extract.parquet: ../../stage_nhgis/output/nhgis_1990_tract_extract.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/dcp_boundary_index.csv: ../../stage_dcp_boundaries/output/dcp_boundary_index.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/cd_homeownership_1990_measure.csv ../input/dcp_cd_profiles_1990_2000_files.csv ../input/nhgis_files.csv ../input/nhgis_1980_tract_extract.parquet ../input/nhgis_1990_tract_extract.parquet ../input/dcp_boundary_index.csv

include ../../generic.make
