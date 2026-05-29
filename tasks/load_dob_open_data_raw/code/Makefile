SHELL := bash
include ../../shell_functions.make

all: ../output/dob_open_data_raw_files.csv

../output/dob_open_data_raw_files.csv: load_dob_open_data_raw.R ../input/dob_open_data_files.csv | ../output
	$(R) $<

../input/dob_open_data_files.csv: ../../fetch_dob_open_data/output/dob_open_data_files.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/dob_open_data_files.csv

include ../../generic.make
