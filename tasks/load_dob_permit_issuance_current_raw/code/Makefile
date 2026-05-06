SHELL := bash
include ../../shell_functions.make

all: ../output/dob_permit_issuance_current_raw_files.csv ../output/dob_permit_issuance_current_raw_qc.csv

../output/dob_permit_issuance_current_raw_files.csv: load_dob_permit_issuance_current_raw.R ../input/dob_permit_issuance_current_files.csv | ../output
	$(R) $<

../output/dob_permit_issuance_current_raw_qc.csv: ../output/dob_permit_issuance_current_raw_files.csv
	@test -s "$@"

../output/dob_permit_issuance_current_raw.parquet: ../output/dob_permit_issuance_current_raw_files.csv
	@test -s "$@"

../input/dob_permit_issuance_current_files.csv: ../../fetch_dob_permit_issuance_current/output/dob_permit_issuance_current_files.csv | ../input
	@test "$$(readlink "$@")" = "../../fetch_dob_permit_issuance_current/output/dob_permit_issuance_current_files.csv" || ln -sf "../../fetch_dob_permit_issuance_current/output/dob_permit_issuance_current_files.csv" "$@"

link-inputs: ../input/dob_permit_issuance_current_files.csv

include ../../generic.make
