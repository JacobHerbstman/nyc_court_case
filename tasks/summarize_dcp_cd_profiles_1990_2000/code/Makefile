SHELL := bash
include ../../shell_functions.make

all: ../output/dcp_cd_profiles_1990_2000_audit_summary.csv ../output/dcp_cd_profiles_1990_2000_parse_coverage.csv ../output/dcp_cd_profiles_1990_2000_unresolved_review.csv ../output/dcp_cd_profiles_1990_2000_known_value_checks.csv

../input/dcp_cd_profiles_1990_2000_files.csv: ../../stage_dcp_cd_profiles_1990_2000/output/dcp_cd_profiles_1990_2000_files.csv | ../input
	ln -sf "$<" "$@"

../input/dcp_cd_profiles_1990_2000_qc.csv: ../../stage_dcp_cd_profiles_1990_2000/output/dcp_cd_profiles_1990_2000_qc.csv | ../input
	ln -sf "$<" "$@"

../output/dcp_cd_profiles_1990_2000_audit_summary.csv: summarize_dcp_cd_profiles_1990_2000.R ../input/dcp_cd_profiles_1990_2000_files.csv ../input/dcp_cd_profiles_1990_2000_qc.csv | ../output
	$(R) $< ../input/dcp_cd_profiles_1990_2000_files.csv ../input/dcp_cd_profiles_1990_2000_qc.csv ../output/dcp_cd_profiles_1990_2000_audit_summary.csv ../output/dcp_cd_profiles_1990_2000_parse_coverage.csv ../output/dcp_cd_profiles_1990_2000_unresolved_review.csv ../output/dcp_cd_profiles_1990_2000_known_value_checks.csv

../output/dcp_cd_profiles_1990_2000_parse_coverage.csv: ../output/dcp_cd_profiles_1990_2000_audit_summary.csv

../output/dcp_cd_profiles_1990_2000_unresolved_review.csv: ../output/dcp_cd_profiles_1990_2000_audit_summary.csv

../output/dcp_cd_profiles_1990_2000_known_value_checks.csv: ../output/dcp_cd_profiles_1990_2000_audit_summary.csv

link-inputs: ../input/dcp_cd_profiles_1990_2000_files.csv ../input/dcp_cd_profiles_1990_2000_qc.csv

include ../../generic.make
