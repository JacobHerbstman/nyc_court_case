SHELL := bash
include ../../shell_functions.make

all: ../output/nhgis_raw_files.csv ../output/nhgis_raw_qc.csv

../input/nhgis_extract_downloads.csv: ../../fetch_nhgis_extracts/output/nhgis_extract_downloads.csv | ../input
	ln -sf "../../fetch_nhgis_extracts/output/nhgis_extract_downloads.csv" "$@"

../input/nhgis_table_map.csv: ../../fetch_nhgis_extracts/code/nhgis_table_map.csv | ../input
	ln -sf "$<" "$@"

../output/nhgis_raw_files.csv: load_nhgis_raw.R ../input/nhgis_extract_downloads.csv ../input/nhgis_table_map.csv | ../output
	$(R) $< ../input/nhgis_extract_downloads.csv ../input/nhgis_table_map.csv ../output/nhgis_raw_files.csv ../output/nhgis_raw_qc.csv

../output/nhgis_raw_qc.csv: ../output/nhgis_raw_files.csv

link-inputs: ../input/nhgis_extract_downloads.csv ../input/nhgis_table_map.csv

include ../../generic.make
