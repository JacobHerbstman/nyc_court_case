SHELL := bash
include ../../shell_functions.make

all: ../output/zap_project_data.parquet ../output/zap_project_bbl.parquet

../output/zap_project_data.parquet: build_zap_datasets.R ../output/zap_raw_files.csv | ../output
	$(R) $<

../output/zap_project_bbl.parquet: ../output/zap_project_data.parquet
	@test -s "$@"

../output/zap_raw_files.csv: load_zap_raw.R ../output/zap_files.csv | ../output
	$(R) $<

../output/zap_files.csv: fetch_zap_datasets.R ../input/source_catalog.csv | ../output
	$(R) $<

../input/source_catalog.csv: ../../source_registry/code/source_catalog.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/source_catalog.csv

include ../../generic.make
