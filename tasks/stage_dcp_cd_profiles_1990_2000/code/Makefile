SHELL := bash
include ../../shell_functions.make

all: ../output/dcp_cd_profiles_1990_2000_files.csv ../output/dcp_cd_profiles_1990_2000_qc.csv

../input/dcp_cd_profiles_1990_2000_raw_files.csv: ../../load_dcp_cd_profiles_1990_2000_raw/output/dcp_cd_profiles_1990_2000_raw_files.csv | ../input
	ln -sf "$<" "$@"

../output/dcp_cd_profiles_1990_2000_files.csv: stage_dcp_cd_profiles_1990_2000.R ../input/dcp_cd_profiles_1990_2000_raw_files.csv | ../output
	$(R) $< ../input/dcp_cd_profiles_1990_2000_raw_files.csv ../output/dcp_cd_profiles_1990_2000_files.csv ../output/dcp_cd_profiles_1990_2000_qc.csv

../output/dcp_cd_profiles_1990_2000_qc.csv: ../output/dcp_cd_profiles_1990_2000_files.csv

link-inputs: ../input/dcp_cd_profiles_1990_2000_raw_files.csv

include ../../generic.make
