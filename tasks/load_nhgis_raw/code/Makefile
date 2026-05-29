SHELL := bash
include ../../shell_functions.make

all: ../output/nhgis_raw_files.csv \
	../output/nhgis_1980_tract_extract_raw.parquet \
	../output/nhgis_1990_tract_extract_raw.parquet

../output/nhgis_raw_files.csv: load_nhgis_raw.R ../input/nhgis_extract_downloads.csv ../input/nhgis_table_map.csv | ../output
	$(R) $<

../output/nhgis_1980_tract_extract_raw.parquet: ../output/nhgis_raw_files.csv
	@test -s "$@"

../output/nhgis_1990_tract_extract_raw.parquet: ../output/nhgis_raw_files.csv
	@test -s "$@"

../input/nhgis_extract_downloads.csv: ../../fetch_nhgis_extracts/output/nhgis_extract_downloads.csv | ../input
	@test "$$(readlink "$@")" = "../../fetch_nhgis_extracts/output/nhgis_extract_downloads.csv" || ln -sf "../../fetch_nhgis_extracts/output/nhgis_extract_downloads.csv" "$@"

../input/nhgis_table_map.csv: ../../fetch_nhgis_extracts/code/nhgis_table_map.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/nhgis_extract_downloads.csv ../input/nhgis_table_map.csv

include ../../generic.make
