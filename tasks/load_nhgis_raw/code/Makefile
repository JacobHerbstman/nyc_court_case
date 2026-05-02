SHELL := bash
include ../../shell_functions.make

all: ../output/nhgis_raw_files.csv ../output/nhgis_raw_qc.csv

../input/nhgis_extract_downloads.csv: ../../fetch_nhgis_extracts/output/nhgis_extract_downloads.csv | ../input
	@test "$$(readlink "$@")" = "../../fetch_nhgis_extracts/output/nhgis_extract_downloads.csv" || ln -sf "../../fetch_nhgis_extracts/output/nhgis_extract_downloads.csv" "$@"

../input/nhgis_table_map.csv: ../../fetch_nhgis_extracts/code/nhgis_table_map.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../output/nhgis_raw_files.csv ../output/nhgis_raw_qc.csv ../output/nhgis_1980_tract_extract_raw.parquet ../output/nhgis_1990_tract_extract_raw.parquet: load_nhgis_raw.R ../input/nhgis_extract_downloads.csv ../input/nhgis_table_map.csv | ../output
	$(R) $< ../input/nhgis_extract_downloads.csv ../input/nhgis_table_map.csv ../output/nhgis_raw_files.csv ../output/nhgis_raw_qc.csv

link-inputs: ../input/nhgis_extract_downloads.csv ../input/nhgis_table_map.csv

include ../../generic.make
