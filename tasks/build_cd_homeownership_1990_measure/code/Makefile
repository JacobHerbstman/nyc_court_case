SHELL := bash
include ../../shell_functions.make

all: ../output/cd_homeownership_1990_measure.csv ../output/cd_homeownership_1990_measure_qc.csv

../input/dcp_cd_homeownership_1990.csv: ../../summarize_dcp_cd_homeownership_1990/output/dcp_cd_homeownership_1990.csv | ../input
	ln -sf "$<" "$@"

../output/cd_homeownership_1990_measure.csv: build_cd_homeownership_1990_measure.R ../input/dcp_cd_homeownership_1990.csv | ../output
	$(R) $< ../input/dcp_cd_homeownership_1990.csv ../output/cd_homeownership_1990_measure.csv ../output/cd_homeownership_1990_measure_qc.csv

../output/cd_homeownership_1990_measure_qc.csv: ../output/cd_homeownership_1990_measure.csv

link-inputs: ../input/dcp_cd_homeownership_1990.csv

include ../../generic.make
