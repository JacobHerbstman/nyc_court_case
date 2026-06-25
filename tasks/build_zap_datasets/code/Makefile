SHELL := bash
include ../../shell_functions.make

all: ../output/zap_project_data.parquet ../output/zap_project_bbl.parquet

../output/zap_project_data.parquet: build_zap_datasets.R ../input/zap_raw_files.csv | ../output
	$(R) $<

../output/zap_project_bbl.parquet: ../output/zap_project_data.parquet
	@test -s "$@"

../output/zap_stage_qc.csv: ../output/zap_project_data.parquet
	@test -s "$@"

../input/zap_raw_files.csv: ../../load_zap_raw/output/zap_raw_files.csv | ../input
	@test "$$(readlink "$@")" = "../../load_zap_raw/output/zap_raw_files.csv" || ln -sf "../../load_zap_raw/output/zap_raw_files.csv" "$@"

link-inputs: ../input/zap_raw_files.csv

include ../../generic.make
