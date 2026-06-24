SHELL := bash
include ../../../shell_functions.make

all: ../output/dob_open_data_files.csv

../output/dob_open_data_files.csv: fetch_dob_open_data.R ../input/source_catalog.csv | ../output
	$(R) $<

../input/source_catalog.csv: ../../../source_registry/code/source_catalog.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/source_catalog.csv

include ../../generic.make
