SHELL := bash
include ../../shell_functions.make

all: ../output/dof_421a_exempt_property_rows.csv ../output/dof_421a_exempt_bbl_year.csv ../output/dof_421a_stage_qc.csv

../output/dof_421a_exempt_property_rows.csv: stage_dof_421a_exempt_properties.R ../input/dof_421a_raw_files.csv | ../output
	$(R) $<

../output/dof_421a_exempt_bbl_year.csv: ../output/dof_421a_exempt_property_rows.csv
	@test -s "$@"

../output/dof_421a_stage_qc.csv: ../output/dof_421a_exempt_property_rows.csv
	@test -s "$@"

../input/dof_421a_raw_files.csv: ../../fetch_dof_421a_exempt_properties/output/dof_421a_raw_files.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/dof_421a_raw_files.csv

include ../../generic.make
