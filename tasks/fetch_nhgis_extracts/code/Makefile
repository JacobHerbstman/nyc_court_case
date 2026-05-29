SHELL := bash
include ../../shell_functions.make

all: ../output/nhgis_extract_downloads.csv

../output/nhgis_extract_downloads.csv: fetch_nhgis_extracts.R ../input/source_catalog.csv nhgis_1980_extract.json nhgis_1990_extract.json nhgis_table_map.csv | ../output
	$(R) $<

../input/source_catalog.csv: ../../source_registry/code/source_catalog.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/source_catalog.csv

include ../../generic.make
