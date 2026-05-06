SHELL := bash
include ../../shell_functions.make

all: ../output/zap_files.csv ../output/zap_checksums.csv ../output/zap_provenance.csv

../output/zap_files.csv: fetch_zap_datasets.R ../input/source_catalog.csv | ../output
	$(R) $<

../output/zap_checksums.csv: ../output/zap_files.csv
	@test -s "$@"

../output/zap_provenance.csv: ../output/zap_files.csv
	@test -s "$@"

../input/source_catalog.csv: ../../source_registry/code/source_catalog.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/source_catalog.csv

include ../../generic.make
