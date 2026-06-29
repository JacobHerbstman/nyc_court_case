SHELL := bash
include ../../shell_functions.make

all: ../output/zap_project_data.parquet ../output/zap_project_bbl.parquet

../output/zap_project_data.parquet: build_zap_datasets.R ../../_lib/source_pipeline_utils.R ../input/source_catalog.csv | ../output
	$(R) $<

../output/zap_project_bbl.parquet: ../output/zap_project_data.parquet
	@test -s "$@"

../input/source_catalog.csv: ../../source_registry/code/source_catalog.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/source_catalog.csv

include ../../generic.make
