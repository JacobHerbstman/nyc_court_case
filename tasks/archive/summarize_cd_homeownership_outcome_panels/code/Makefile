SHELL := bash
include ../../shell_functions.make

all: ../output/cd_homeownership_outcome_tercile_membership.csv ../output/cd_homeownership_outcome_tercile_year.csv ../output/cd_homeownership_outcome_tercile_five_year.csv ../output/cd_homeownership_outcome_panels_qc.csv ../output/cd_homeownership_outcome_tercile_plots.pdf

../input/cd_homeownership_permit_nb_panel.csv: ../../cd_homeownership_permit_nb_panel/output/cd_homeownership_permit_nb_panel.csv | ../input
	ln -sf "$<" "$@"

../input/cd_homeownership_dcp_units_panel.csv: ../../cd_homeownership_dcp_units_panel/output/cd_homeownership_dcp_units_panel.csv | ../input
	ln -sf "$<" "$@"

../output/cd_homeownership_outcome_tercile_membership.csv: summarize_cd_homeownership_outcome_panels.R ../input/cd_homeownership_permit_nb_panel.csv ../input/cd_homeownership_dcp_units_panel.csv | ../output
	$(R) $< ../input/cd_homeownership_permit_nb_panel.csv ../input/cd_homeownership_dcp_units_panel.csv ../output/cd_homeownership_outcome_tercile_membership.csv ../output/cd_homeownership_outcome_tercile_year.csv ../output/cd_homeownership_outcome_tercile_five_year.csv ../output/cd_homeownership_outcome_panels_qc.csv ../output/cd_homeownership_outcome_tercile_plots.pdf

../output/cd_homeownership_outcome_tercile_year.csv: ../output/cd_homeownership_outcome_tercile_membership.csv

../output/cd_homeownership_outcome_tercile_five_year.csv: ../output/cd_homeownership_outcome_tercile_membership.csv

../output/cd_homeownership_outcome_panels_qc.csv: ../output/cd_homeownership_outcome_tercile_membership.csv

../output/cd_homeownership_outcome_tercile_plots.pdf: ../output/cd_homeownership_outcome_tercile_membership.csv

link-inputs: ../input/cd_homeownership_permit_nb_panel.csv ../input/cd_homeownership_dcp_units_panel.csv

include ../../generic.make
