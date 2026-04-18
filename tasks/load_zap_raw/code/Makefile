SHELL := bash
include ../../shell_functions.make

all: ../output/zap_raw_files.csv ../output/zap_raw_qc.csv ../output/zap_columns_metadata.csv

../input/zap_files.csv: ../../fetch_zap_datasets/output/zap_files.csv | ../input
	ln -sf "../../fetch_zap_datasets/output/zap_files.csv" "$@"

../output/zap_raw_files.csv: load_zap_raw.R ../input/zap_files.csv | ../output
	$(R) $< ../input/zap_files.csv ../output/zap_raw_files.csv ../output/zap_raw_qc.csv ../output/zap_columns_metadata.csv

../output/zap_raw_qc.csv: ../output/zap_raw_files.csv

../output/zap_columns_metadata.csv: ../output/zap_raw_files.csv

link-inputs: ../input/zap_files.csv

include ../../generic.make
