SHELL := bash
include ../../shell_functions.make

all: ../output/dcp_cd_homeownership_1990.csv ../output/dcp_cd_homeownership_1990_borough_summary.csv ../output/dcp_cd_homeownership_1990_qc.csv

../input/dcp_cd_profiles_1990_2000_files.csv: ../../stage_dcp_cd_profiles_1990_2000/output/dcp_cd_profiles_1990_2000_files.csv | ../input
	ln -sf "$<" "$@"

../output/dcp_cd_homeownership_1990.csv: summarize_dcp_cd_homeownership_1990.R ../input/dcp_cd_profiles_1990_2000_files.csv | ../output
	$(R) $< ../input/dcp_cd_profiles_1990_2000_files.csv ../output/dcp_cd_homeownership_1990.csv ../output/dcp_cd_homeownership_1990_borough_summary.csv ../output/dcp_cd_homeownership_1990_qc.csv

../output/dcp_cd_homeownership_1990_borough_summary.csv: ../output/dcp_cd_homeownership_1990.csv

../output/dcp_cd_homeownership_1990_qc.csv: ../output/dcp_cd_homeownership_1990.csv

link-inputs: ../input/dcp_cd_profiles_1990_2000_files.csv

include ../../generic.make
