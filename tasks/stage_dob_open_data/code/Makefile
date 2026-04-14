SHELL := bash
include ../../shell_functions.make

all: ../output/dob_open_data_files.csv ../output/dob_open_data_qc.csv ../output/dob_field_dictionary.csv

../input/dob_open_data_raw_files.csv: ../../load_dob_open_data_raw/output/dob_open_data_raw_files.csv | ../input
	ln -sf "../../load_dob_open_data_raw/output/dob_open_data_raw_files.csv" "$@"

../output/dob_open_data_files.csv: stage_dob_open_data.R ../input/dob_open_data_raw_files.csv | ../output
	$(R) $< ../input/dob_open_data_raw_files.csv ../output/dob_open_data_files.csv ../output/dob_open_data_qc.csv ../output/dob_field_dictionary.csv

../output/dob_open_data_qc.csv: ../output/dob_open_data_files.csv

../output/dob_field_dictionary.csv: ../output/dob_open_data_files.csv

link-inputs: ../input/dob_open_data_raw_files.csv

include ../../generic.make
