SHELL := bash
include ../../shell_functions.make

all: ../output/cd_homeownership_1990_measure.csv ../output/cd_homeownership_1990_measure_qc.csv

../input/dcp_cd_profiles_1990_2000_files.csv: ../../stage_dcp_cd_profiles_1990_2000/output/dcp_cd_profiles_1990_2000_files.csv | ../input
	ln -sf "$<" "$@"

../output/cd_homeownership_1990_measure.csv: build_cd_homeownership_1990_measure.R ../input/dcp_cd_profiles_1990_2000_files.csv | ../output
	$(R) $< ../input/dcp_cd_profiles_1990_2000_files.csv ../output/cd_homeownership_1990_measure.csv ../output/cd_homeownership_1990_measure_qc.csv

../output/cd_homeownership_1990_measure_qc.csv: ../output/cd_homeownership_1990_measure.csv

link-inputs: ../input/dcp_cd_profiles_1990_2000_files.csv

include ../../generic.make
