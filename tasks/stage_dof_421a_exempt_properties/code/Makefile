SHELL := bash
include ../../shell_functions.make

all: ../output/dof_421a_exempt_property_rows.csv ../output/dof_421a_exempt_bbl_year.csv ../output/dof_421a_stage_qc.csv

../input/dof_421a_raw_files.csv: ../../fetch_dof_421a_exempt_properties/output/dof_421a_raw_files.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../output/dof_421a_exempt_property_rows.csv: stage_dof_421a_exempt_properties.R ../input/dof_421a_raw_files.csv | ../output
	$(R) $< ../input/dof_421a_raw_files.csv ../output/dof_421a_exempt_property_rows.csv ../output/dof_421a_exempt_bbl_year.csv ../output/dof_421a_stage_qc.csv

../output/dof_421a_exempt_bbl_year.csv: ../output/dof_421a_exempt_property_rows.csv
../output/dof_421a_stage_qc.csv: ../output/dof_421a_exempt_property_rows.csv

link-inputs: ../input/dof_421a_raw_files.csv

include ../../generic.make
