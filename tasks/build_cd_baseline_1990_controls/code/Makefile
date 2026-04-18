SHELL := bash
include ../../shell_functions.make

all: ../output/cd_baseline_1990_controls.csv ../output/cd_baseline_1990_controls_qc.csv ../output/cd_baseline_1990_controls_overlay_qc.csv

../input/cd_homeownership_1990_measure.csv: ../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	ln -sf "$<" "$@"

../input/dcp_cd_profiles_1990_2000_20260415.parquet: ../../stage_dcp_cd_profiles_1990_2000/output/dcp_cd_profiles_1990_2000_20260415.parquet | ../input
	ln -sf "$<" "$@"

../input/nhgis_files.csv: ../../stage_nhgis/output/nhgis_files.csv | ../input
	ln -sf "$<" "$@"

../input/nhgis_1980_tract_extract.parquet: ../../stage_nhgis/output/nhgis_1980_tract_extract.parquet | ../input
	ln -sf "$<" "$@"

../input/nhgis_1990_tract_extract.parquet: ../../stage_nhgis/output/nhgis_1990_tract_extract.parquet | ../input
	ln -sf "$<" "$@"

../input/dcp_boundary_index.csv: ../../stage_dcp_boundaries/output/dcp_boundary_index.csv | ../input
	ln -sf "$<" "$@"

../output/cd_baseline_1990_controls.csv: build_cd_baseline_1990_controls.R ../input/cd_homeownership_1990_measure.csv ../input/dcp_cd_profiles_1990_2000_20260415.parquet ../input/nhgis_files.csv ../input/nhgis_1980_tract_extract.parquet ../input/nhgis_1990_tract_extract.parquet ../input/dcp_boundary_index.csv | ../output
	$(R) $< ../input/cd_homeownership_1990_measure.csv ../input/dcp_cd_profiles_1990_2000_20260415.parquet ../input/nhgis_files.csv ../input/nhgis_1980_tract_extract.parquet ../input/nhgis_1990_tract_extract.parquet ../input/dcp_boundary_index.csv ../output/cd_baseline_1990_controls.csv ../output/cd_baseline_1990_controls_qc.csv ../output/cd_baseline_1990_controls_overlay_qc.csv

../output/cd_baseline_1990_controls_qc.csv: ../output/cd_baseline_1990_controls.csv

../output/cd_baseline_1990_controls_overlay_qc.csv: ../output/cd_baseline_1990_controls.csv

link-inputs: ../input/cd_homeownership_1990_measure.csv ../input/dcp_cd_profiles_1990_2000_20260415.parquet ../input/nhgis_files.csv ../input/nhgis_1980_tract_extract.parquet ../input/nhgis_1990_tract_extract.parquet ../input/dcp_boundary_index.csv

include ../../generic.make
