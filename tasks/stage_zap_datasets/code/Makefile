SHELL := bash
include ../../shell_functions.make

all: ../output/zap_stage_qc.csv ../output/zap_project_data.parquet ../output/zap_project_bbl.parquet

../input/zap_raw_files.csv: ../../load_zap_raw/output/zap_raw_files.csv | ../input
	ln -sf "../../load_zap_raw/output/zap_raw_files.csv" "$@"

../output/zap_stage_qc.csv: stage_zap_datasets.R ../input/zap_raw_files.csv | ../output
	$(R) $< ../input/zap_raw_files.csv ../output/zap_stage_qc.csv ../output/zap_project_data.parquet ../output/zap_project_bbl.parquet

../output/zap_project_data.parquet: ../output/zap_stage_qc.csv

../output/zap_project_bbl.parquet: ../output/zap_stage_qc.csv

link-inputs: ../input/zap_raw_files.csv

include ../../generic.make
