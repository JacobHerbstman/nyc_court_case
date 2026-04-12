SHELL := bash
include ../../shell_functions.make

all: ../output/nhgis_files.csv ../output/nhgis_qc.csv

../input/source_catalog.csv: ../../source_registry/code/source_catalog.csv | ../input
	ln -sf "$<" "$@"

../input/nhgis_extract_downloads.csv: | ../input
	ln -sf "../../fetch_nhgis_extracts/output/nhgis_extract_downloads.csv" "$@"

../input/nhgis_table_map.csv: ../../fetch_nhgis_extracts/code/nhgis_table_map.csv | ../input
	ln -sf "$<" "$@"

../output/nhgis_files.csv: stage_nhgis.R ../input/source_catalog.csv ../input/nhgis_extract_downloads.csv ../input/nhgis_table_map.csv | ../output
	$(R) $< ../input/source_catalog.csv ../input/nhgis_extract_downloads.csv ../input/nhgis_table_map.csv ../output/nhgis_files.csv ../output/nhgis_qc.csv

../output/nhgis_qc.csv: ../output/nhgis_files.csv

link-inputs: ../input/source_catalog.csv ../input/nhgis_extract_downloads.csv ../input/nhgis_table_map.csv

include ../../generic.make
