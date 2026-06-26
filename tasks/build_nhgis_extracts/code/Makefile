SHELL := bash
include ../../shell_functions.make

all: ../output/nhgis_files.csv \
	../output/nhgis_1980_tract_extract.parquet \
	../output/nhgis_1990_tract_extract.parquet

../output/nhgis_files.csv: build_nhgis_extracts.R ../output/nhgis_raw_files.csv nhgis_table_map.csv ../output/nhgis_1980_tract_extract_raw.parquet ../output/nhgis_1990_tract_extract_raw.parquet nhgis_income_overrides.csv | ../output
	$(R) $<

../output/nhgis_1980_tract_extract.parquet: ../output/nhgis_files.csv
	@test -s "$@"

../output/nhgis_1990_tract_extract.parquet: ../output/nhgis_files.csv
	@test -s "$@"

../output/nhgis_raw_files.csv: load_nhgis_raw.R ../output/nhgis_extract_downloads.csv nhgis_table_map.csv | ../output
	$(R) $<

../output/nhgis_1980_tract_extract_raw.parquet: ../output/nhgis_raw_files.csv
	@test -s "$@"

../output/nhgis_1990_tract_extract_raw.parquet: ../output/nhgis_raw_files.csv
	@test -s "$@"

../output/nhgis_extract_downloads.csv: fetch_nhgis_extracts.R ../input/source_catalog.csv nhgis_1980_extract.json nhgis_1990_extract.json nhgis_table_map.csv | ../output
	$(R) $<

../input/source_catalog.csv: ../../source_registry/code/source_catalog.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/source_catalog.csv

include ../../generic.make
