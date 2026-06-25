SHELL := bash
include ../../../shell_functions.make

all: ../output/dof_421a_exempt_bbl_year.csv

../output/dof_421a_exempt_bbl_year.csv: stage_dof_421a_exempt_properties.R ../input/dof_421a_raw_files.csv | ../output
	$(R) $<

../input/dof_421a_raw_files.csv: ../../fetch_dof_421a_exempt_properties/output/dof_421a_raw_files.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/dof_421a_raw_files.csv

include ../../generic.make
