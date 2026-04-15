SHELL := bash
include ../../shell_functions.make

all: ../output/dob_permit_issuance_current_raw_files.csv ../output/dob_permit_issuance_current_raw_qc.csv

../input/dob_permit_issuance_current_files.csv: ../../fetch_dob_permit_issuance_current/output/dob_permit_issuance_current_files.csv | ../input
	ln -sf "../../fetch_dob_permit_issuance_current/output/dob_permit_issuance_current_files.csv" "$@"

../output/dob_permit_issuance_current_raw_files.csv: load_dob_permit_issuance_current_raw.R ../input/dob_permit_issuance_current_files.csv | ../output
	$(R) $< ../input/dob_permit_issuance_current_files.csv ../output/dob_permit_issuance_current_raw_files.csv ../output/dob_permit_issuance_current_raw_qc.csv

../output/dob_permit_issuance_current_raw_qc.csv: ../output/dob_permit_issuance_current_raw_files.csv

link-inputs: ../input/dob_permit_issuance_current_files.csv

include ../../generic.make
