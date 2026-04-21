SHELL := bash
include ../../shell_functions.make

all: ../output/cd_homeownership_outcome_era_interactions.csv ../output/cd_homeownership_outcome_model_summary.csv

../input/cd_homeownership_permit_nb_panel.csv: ../../cd_homeownership_permit_nb_panel/output/cd_homeownership_permit_nb_panel.csv | ../input
	ln -sf "$<" "$@"

../input/cd_homeownership_dcp_units_panel.csv: ../../cd_homeownership_dcp_units_panel/output/cd_homeownership_dcp_units_panel.csv | ../input
	ln -sf "$<" "$@"

../output/cd_homeownership_outcome_era_interactions.csv: estimate_cd_homeownership_outcome_panels.R ../input/cd_homeownership_permit_nb_panel.csv ../input/cd_homeownership_dcp_units_panel.csv | ../output
	$(R) $< ../input/cd_homeownership_permit_nb_panel.csv ../input/cd_homeownership_dcp_units_panel.csv ../output/cd_homeownership_outcome_era_interactions.csv ../output/cd_homeownership_outcome_model_summary.csv

../output/cd_homeownership_outcome_model_summary.csv: ../output/cd_homeownership_outcome_era_interactions.csv

link-inputs: ../input/cd_homeownership_permit_nb_panel.csv ../input/cd_homeownership_dcp_units_panel.csv

include ../../generic.make
