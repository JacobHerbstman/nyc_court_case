SHELL := bash
include ../../shell_functions.make

all: ../output/furman_coredata_files.csv ../output/furman_coredata_qc.csv

../input/furman_coredata_raw_files.csv: ../../load_furman_coredata_raw/output/furman_coredata_raw_files.csv | ../input
	ln -sf "../../load_furman_coredata_raw/output/furman_coredata_raw_files.csv" "$@"

../output/furman_coredata_files.csv: stage_furman_coredata.R ../input/furman_coredata_raw_files.csv | ../output
	$(R) $< ../input/furman_coredata_raw_files.csv ../output/furman_coredata_files.csv ../output/furman_coredata_qc.csv

../output/furman_coredata_qc.csv: ../output/furman_coredata_files.csv

link-inputs: ../input/furman_coredata_raw_files.csv

include ../../generic.make
