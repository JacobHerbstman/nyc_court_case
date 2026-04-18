SHELL := bash
include ../../shell_functions.make

all: ../output/nhgis_cd_homeownership_1990.csv ../output/nhgis_cd_homeownership_1990_borough_summary.csv ../output/nhgis_cd_homeownership_1990_assignment_qc.csv ../output/nhgis_cd_homeownership_1990_figures.pdf

../input/nhgis_files.csv: ../../stage_nhgis/output/nhgis_files.csv | ../input
	ln -sf "$<" "$@"

../input/nhgis_1990_tract_extract.parquet: ../../stage_nhgis/output/nhgis_1990_tract_extract.parquet | ../input
	ln -sf "$<" "$@"

../input/dcp_boundary_index.csv: ../../stage_dcp_boundaries/output/dcp_boundary_index.csv | ../input
	ln -sf "$<" "$@"

../output/nhgis_cd_homeownership_1990.csv: summarize_cd_homeownership_1990.R ../input/nhgis_files.csv ../input/nhgis_1990_tract_extract.parquet ../input/dcp_boundary_index.csv | ../output
	$(R) $< ../input/nhgis_files.csv ../input/nhgis_1990_tract_extract.parquet ../input/dcp_boundary_index.csv ../output/nhgis_cd_homeownership_1990.csv ../output/nhgis_cd_homeownership_1990_borough_summary.csv ../output/nhgis_cd_homeownership_1990_assignment_qc.csv ../output/nhgis_cd_homeownership_1990_figures.pdf

../output/nhgis_cd_homeownership_1990_borough_summary.csv: ../output/nhgis_cd_homeownership_1990.csv

../output/nhgis_cd_homeownership_1990_assignment_qc.csv: ../output/nhgis_cd_homeownership_1990.csv

../output/nhgis_cd_homeownership_1990_figures.pdf: ../output/nhgis_cd_homeownership_1990.csv

link-inputs: ../input/nhgis_files.csv ../input/nhgis_1990_tract_extract.parquet ../input/dcp_boundary_index.csv

include ../../generic.make
