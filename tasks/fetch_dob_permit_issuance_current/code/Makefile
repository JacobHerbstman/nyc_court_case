SHELL := bash
include ../../shell_functions.make

all: ../output/dob_permit_issuance_current_files.csv ../output/dob_permit_issuance_current_qc.csv

../input/source_catalog.csv: ../../source_registry/code/source_catalog.csv | ../input
	ln -sf "$<" "$@"

../output/dob_permit_issuance_current_files.csv: fetch_dob_permit_issuance_current.R ../input/source_catalog.csv | ../output
	$(R) $< ../input/source_catalog.csv ../output/dob_permit_issuance_current_files.csv ../output/dob_permit_issuance_current_qc.csv

../output/dob_permit_issuance_current_qc.csv: ../output/dob_permit_issuance_current_files.csv

link-inputs: ../input/source_catalog.csv

include ../../generic.make
