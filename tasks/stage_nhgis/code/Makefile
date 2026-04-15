SHELL := bash
include ../../shell_functions.make

all: ../output/nhgis_files.csv ../output/nhgis_qc.csv ../output/nhgis_1980_reconciliation.csv

../input/nhgis_raw_files.csv: ../../load_nhgis_raw/output/nhgis_raw_files.csv | ../input
	ln -sf "../../load_nhgis_raw/output/nhgis_raw_files.csv" "$@"

../input/nhgis_table_map.csv: ../../fetch_nhgis_extracts/code/nhgis_table_map.csv | ../input
	ln -sf "$<" "$@"

../input/nhgis_1980_tract_extract_raw.parquet: ../../load_nhgis_raw/output/nhgis_1980_tract_extract_raw.parquet | ../input
	ln -sf "$<" "$@"

../input/nhgis_1990_tract_extract_raw.parquet: ../../load_nhgis_raw/output/nhgis_1990_tract_extract_raw.parquet | ../input
	ln -sf "$<" "$@"

../output/nhgis_files.csv: stage_nhgis.R ../input/nhgis_raw_files.csv ../input/nhgis_table_map.csv ../input/nhgis_1980_tract_extract_raw.parquet ../input/nhgis_1990_tract_extract_raw.parquet nhgis_income_overrides.csv | ../output
	$(R) $< ../input/nhgis_raw_files.csv ../input/nhgis_table_map.csv nhgis_income_overrides.csv ../output/nhgis_files.csv ../output/nhgis_qc.csv ../output/nhgis_1980_reconciliation.csv

../output/nhgis_qc.csv: ../output/nhgis_files.csv

../output/nhgis_1980_reconciliation.csv: ../output/nhgis_files.csv

link-inputs: ../input/nhgis_raw_files.csv ../input/nhgis_table_map.csv ../input/nhgis_1980_tract_extract_raw.parquet ../input/nhgis_1990_tract_extract_raw.parquet

include ../../generic.make
