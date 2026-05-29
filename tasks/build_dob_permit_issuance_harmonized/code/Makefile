SHELL := bash
include ../../shell_functions.make

all: ../output/dob_permit_issuance_harmonized.parquet

../output/dob_permit_issuance_harmonized.parquet: build_dob_permit_issuance_harmonized.R ../../_lib/source_pipeline_utils.R ../../_lib/dob_permit_issuance_utils.R current_source_decision_1989_2013.csv ../input/dob_permit_issuance_current_raw_files.csv ../input/dob_permit_issuance_current_raw.parquet ../input/census_bps_city_year.parquet | ../output
	$(R) $<

../input/dob_permit_issuance_current_raw_files.csv: ../../load_dob_permit_issuance_current_raw/output/dob_permit_issuance_current_raw_files.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/dob_permit_issuance_current_raw.parquet: ../../load_dob_permit_issuance_current_raw/output/dob_permit_issuance_current_raw.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/census_bps_city_year.parquet: ../../stage_census_bps/output/census_bps_city_year.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/dob_permit_issuance_current_raw_files.csv ../input/dob_permit_issuance_current_raw.parquet ../input/census_bps_city_year.parquet

include ../../generic.make
