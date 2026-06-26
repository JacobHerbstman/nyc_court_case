SHELL := bash
include ../../shell_functions.make

all: ../output/dob_permit_issuance_harmonized.parquet

../output/dob_permit_issuance_harmonized.parquet: build_dob_permit_issuance_harmonized.R ../../_lib/source_pipeline_utils.R ../../_lib/dob_permit_issuance_utils.R current_source_decision_1989_2013.csv ../output/dob_permit_issuance_current_raw_files.csv ../output/dob_permit_issuance_current_raw.parquet | ../output
	$(R) $<

../output/dob_permit_issuance_current_raw_files.csv: load_dob_permit_issuance_current_raw.R ../output/dob_permit_issuance_current_files.csv | ../output
	$(R) $<

../output/dob_permit_issuance_current_raw.parquet: ../output/dob_permit_issuance_current_raw_files.csv
	@test -s "$@"

../output/dob_permit_issuance_current_files.csv: fetch_dob_permit_issuance_current.R ../input/source_catalog.csv | ../output
	$(R) $<

../input/source_catalog.csv: ../../source_registry/code/source_catalog.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/source_catalog.csv

include ../../generic.make
