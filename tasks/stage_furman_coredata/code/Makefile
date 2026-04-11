SHELL := bash
include ../../shell_functions.make

all: ../output/furman_coredata_files.csv

../input/source_catalog.csv: ../../source_registry/code/source_catalog.csv | ../input
	ln -sf "$<" "$@"

../input/manual_manifest.csv: ../../source_registry/code/manual_manifest.csv | ../input
	ln -sf "$<" "$@"

../output/furman_coredata_files.csv: stage_furman_coredata.R ../input/source_catalog.csv ../input/manual_manifest.csv | ../output
	$(R) $< ../input/source_catalog.csv ../input/manual_manifest.csv $@

link-inputs: ../input/source_catalog.csv ../input/manual_manifest.csv

include ../../generic.make
