SHELL := bash
include ../../shell_functions.make

all: ../output/zap_files.csv ../output/zap_checksums.csv ../output/zap_provenance.csv

../input/source_catalog.csv: ../../source_registry/code/source_catalog.csv | ../input
	ln -sf "$<" "$@"

../output/zap_files.csv: fetch_zap_datasets.R ../input/source_catalog.csv | ../output
	$(R) $< ../input/source_catalog.csv ../output/zap_files.csv ../output/zap_checksums.csv ../output/zap_provenance.csv

../output/zap_checksums.csv: ../output/zap_files.csv

../output/zap_provenance.csv: ../output/zap_files.csv

link-inputs: ../input/source_catalog.csv

include ../../generic.make
