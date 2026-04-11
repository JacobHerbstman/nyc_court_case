SHELL := bash
include ../../shell_functions.make

all: ../output/nhgis_files.csv ../output/nhgis_qc.csv

../input/source_catalog.csv: ../../source_registry/code/source_catalog.csv | ../input
	ln -sf "$<" "$@"

../input/manual_manifest.csv: ../../source_registry/code/manual_manifest.csv | ../input
	ln -sf "$<" "$@"

../output/nhgis_files.csv: stage_nhgis.R ../input/source_catalog.csv ../input/manual_manifest.csv | ../output
	$(R) $< ../input/source_catalog.csv ../input/manual_manifest.csv ../output/nhgis_files.csv ../output/nhgis_qc.csv

../output/nhgis_qc.csv: ../output/nhgis_files.csv

link-inputs: ../input/source_catalog.csv ../input/manual_manifest.csv

include ../../generic.make
