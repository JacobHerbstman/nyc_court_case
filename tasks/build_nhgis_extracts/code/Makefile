SHELL := bash
include ../../shell_functions.make

all: ../output/nhgis_1990_tract_extract.parquet \
	../output/nhgis_1990_tract_gis_zip.csv

../output/nhgis_1990_tract_extract.parquet: build_nhgis_extracts.R ../temp/nhgis_raw_files.csv nhgis_table_map.csv ../temp/nhgis_1990_tract_extract_raw.parquet nhgis_income_overrides.csv | ../output
	$(R) $<

../output/nhgis_1990_tract_gis_zip.csv: ../output/nhgis_1990_tract_extract.parquet
	@test -s "$@"

../temp/nhgis_raw_files.csv: load_nhgis_raw.R ../temp/nhgis_extract_downloads.csv nhgis_table_map.csv | ../temp
	$(R) $<

../temp/nhgis_1990_tract_extract_raw.parquet: ../temp/nhgis_raw_files.csv
	@test -s "$@"

../temp/nhgis_extract_downloads.csv: fetch_nhgis_extracts.R ../input/source_catalog.csv nhgis_1990_extract.json nhgis_table_map.csv | ../temp
	$(R) $<

../input/source_catalog.csv: ../../source_registry/code/source_catalog.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/source_catalog.csv

include ../../generic.make
