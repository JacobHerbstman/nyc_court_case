SHELL := bash
include ../../shell_functions.make

all: ../output/cd_homeownership_dcp_supply_era_interactions.csv ../output/cd_homeownership_dcp_supply_model_summary.csv

../input/cd_homeownership_dcp_supply_panel.csv: ../../cd_homeownership_dcp_supply_panel/output/cd_homeownership_dcp_supply_panel.csv | ../input
	ln -sf "$<" "$@"

../input/cd_homeownership_permit_nb_panel.csv: ../../cd_homeownership_permit_nb_panel/output/cd_homeownership_permit_nb_panel.csv | ../input
	ln -sf "$<" "$@"

../output/cd_homeownership_dcp_supply_era_interactions.csv: estimate_cd_homeownership_dcp_supply.R ../input/cd_homeownership_dcp_supply_panel.csv ../input/cd_homeownership_permit_nb_panel.csv | ../output
	$(R) $< ../input/cd_homeownership_dcp_supply_panel.csv ../input/cd_homeownership_permit_nb_panel.csv ../output/cd_homeownership_dcp_supply_era_interactions.csv ../output/cd_homeownership_dcp_supply_model_summary.csv

../output/cd_homeownership_dcp_supply_model_summary.csv: ../output/cd_homeownership_dcp_supply_era_interactions.csv

link-inputs: ../input/cd_homeownership_dcp_supply_panel.csv ../input/cd_homeownership_permit_nb_panel.csv

include ../../generic.make
