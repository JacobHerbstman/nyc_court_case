SHELL := bash
include ../../shell_functions.make

all: ../output/furman_coredata_raw_files.csv ../output/furman_coredata_raw_qc.csv

../input/source_catalog.csv: ../../source_registry/code/source_catalog.csv | ../input
	ln -sf "$<" "$@"

../input/manual_manifest.csv: ../../source_registry/code/manual_manifest.csv | ../input
	ln -sf "$<" "$@"

../output/furman_coredata_raw_files.csv: load_furman_coredata_raw.R ../input/source_catalog.csv ../input/manual_manifest.csv | ../output
	$(R) $< ../input/source_catalog.csv ../input/manual_manifest.csv ../output/furman_coredata_raw_files.csv ../output/furman_coredata_raw_qc.csv

../output/furman_coredata_raw_qc.csv: ../output/furman_coredata_raw_files.csv

link-inputs: ../input/source_catalog.csv ../input/manual_manifest.csv

include ../../generic.make
