SHELL := bash
include ../../shell_functions.make

all: ../output/zap_raw_files.csv ../output/zap_raw_qc.csv ../output/zap_columns_metadata.csv

../output/zap_raw_files.csv: load_zap_raw.R ../input/zap_files.csv | ../output
	$(R) $<

../output/zap_raw_qc.csv: ../output/zap_raw_files.csv
	@test -s "$@"

../output/zap_columns_metadata.csv: ../output/zap_raw_files.csv
	@test -s "$@"

../input/zap_files.csv: ../../fetch_zap_datasets/output/zap_files.csv | ../input
	@test "$$(readlink "$@")" = "../../fetch_zap_datasets/output/zap_files.csv" || ln -sf "../../fetch_zap_datasets/output/zap_files.csv" "$@"

link-inputs: ../input/zap_files.csv

include ../../generic.make
