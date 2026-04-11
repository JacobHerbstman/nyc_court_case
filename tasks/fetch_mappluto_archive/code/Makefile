SHELL := bash
include ../../shell_functions.make

all: ../output/mappluto_download_audit.csv

../input/source_catalog.csv: ../../source_registry/code/source_catalog.csv | ../input
	ln -sf "$<" "$@"

../input/manual_manifest.csv: ../../source_registry/code/manual_manifest.csv | ../input
	ln -sf "$<" "$@"

../output/mappluto_download_audit.csv: fetch_mappluto_archive.R ../input/source_catalog.csv ../input/manual_manifest.csv | ../output
	$(R) $< ../input/source_catalog.csv ../input/manual_manifest.csv $@

link-inputs: ../input/source_catalog.csv ../input/manual_manifest.csv

include ../../generic.make
