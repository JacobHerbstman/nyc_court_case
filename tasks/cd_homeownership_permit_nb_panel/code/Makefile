SHELL := bash
include ../../shell_functions.make

all: ../output/cd_homeownership_permit_nb_panel.csv ../output/cd_homeownership_permit_nb_panel_qc.csv ../output/cd_homeownership_permit_nb_job_conflicts.csv

../input/cd_homeownership_1990_measure.csv: ../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	ln -sf "$<" "$@"

../input/dob_permit_issuance_harmonized.parquet: ../../build_dob_permit_issuance_harmonized/output/dob_permit_issuance_harmonized.parquet | ../input
	ln -sf "$<" "$@"

../output/cd_homeownership_permit_nb_panel.csv: build_cd_homeownership_permit_nb_panel.R ../input/cd_homeownership_1990_measure.csv ../input/dob_permit_issuance_harmonized.parquet | ../output
	$(R) $< ../input/cd_homeownership_1990_measure.csv ../input/dob_permit_issuance_harmonized.parquet ../output/cd_homeownership_permit_nb_panel.csv ../output/cd_homeownership_permit_nb_panel_qc.csv ../output/cd_homeownership_permit_nb_job_conflicts.csv

../output/cd_homeownership_permit_nb_panel_qc.csv: ../output/cd_homeownership_permit_nb_panel.csv

../output/cd_homeownership_permit_nb_job_conflicts.csv: ../output/cd_homeownership_permit_nb_panel.csv

link-inputs: ../input/cd_homeownership_1990_measure.csv ../input/dob_permit_issuance_harmonized.parquet

include ../../generic.make
