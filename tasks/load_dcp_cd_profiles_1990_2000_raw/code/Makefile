SHELL := bash
include ../../shell_functions.make

PYTHON = python3

all: ../output/dcp_cd_profiles_1990_2000_raw_files.csv ../output/dcp_cd_profiles_1990_2000_raw_qc.csv

../input/dcp_cd_profiles_1990_2000_files.csv: ../../fetch_dcp_cd_profiles_1990_2000/output/dcp_cd_profiles_1990_2000_files.csv | ../input
	ln -sf "$<" "$@"

../output/dcp_cd_profiles_1990_2000_raw_files.csv: load_dcp_cd_profiles_1990_2000_raw.py ../input/dcp_cd_profiles_1990_2000_files.csv | ../output
	$(PYTHON) $< ../input/dcp_cd_profiles_1990_2000_files.csv ../output/dcp_cd_profiles_1990_2000_raw_files.csv ../output/dcp_cd_profiles_1990_2000_raw_qc.csv

../output/dcp_cd_profiles_1990_2000_raw_qc.csv: ../output/dcp_cd_profiles_1990_2000_raw_files.csv

link-inputs: ../input/dcp_cd_profiles_1990_2000_files.csv

include ../../generic.make
