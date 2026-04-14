SHELL := bash
include ../../shell_functions.make

all: ../output/nhgis_files.csv ../output/nhgis_qc.csv ../output/nhgis_1980_reconciliation.csv

../input/nhgis_raw_files.csv: ../../load_nhgis_raw/output/nhgis_raw_files.csv | ../input
	ln -sf "../../load_nhgis_raw/output/nhgis_raw_files.csv" "$@"

../input/nhgis_table_map.csv: ../../fetch_nhgis_extracts/code/nhgis_table_map.csv | ../input
	ln -sf "$<" "$@"

../output/nhgis_files.csv: stage_nhgis.R ../input/nhgis_raw_files.csv ../input/nhgis_table_map.csv | ../output
	$(R) $< ../input/nhgis_raw_files.csv ../input/nhgis_table_map.csv ../output/nhgis_files.csv ../output/nhgis_qc.csv ../output/nhgis_1980_reconciliation.csv

../output/nhgis_qc.csv: ../output/nhgis_files.csv

../output/nhgis_1980_reconciliation.csv: ../output/nhgis_files.csv

link-inputs: ../input/nhgis_raw_files.csv ../input/nhgis_table_map.csv

include ../../generic.make
