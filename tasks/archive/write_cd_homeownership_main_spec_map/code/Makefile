R := Rscript

all: ../output/cd_homeownership_main_spec_map.csv ../output/cd_homeownership_main_spec_map.md

../input/cd_homeownership_dcp_supply_era_interactions.csv: ../../estimate_cd_homeownership_dcp_supply/output/cd_homeownership_dcp_supply_era_interactions.csv | ../input
	ln -sf "../../estimate_cd_homeownership_dcp_supply/output/cd_homeownership_dcp_supply_era_interactions.csv" "$@"

../input/cd_homeownership_outcome_era_interactions.csv: ../../estimate_cd_homeownership_outcome_panels/output/cd_homeownership_outcome_era_interactions.csv | ../input
	ln -sf "../../estimate_cd_homeownership_outcome_panels/output/cd_homeownership_outcome_era_interactions.csv" "$@"

../output/cd_homeownership_main_spec_map.csv: write_cd_homeownership_main_spec_map.R ../input/cd_homeownership_dcp_supply_era_interactions.csv ../input/cd_homeownership_outcome_era_interactions.csv | ../output
	$(R) $< ../input/cd_homeownership_dcp_supply_era_interactions.csv ../input/cd_homeownership_outcome_era_interactions.csv ../output/cd_homeownership_main_spec_map.csv ../output/cd_homeownership_main_spec_map.md

../output/cd_homeownership_main_spec_map.md: ../output/cd_homeownership_main_spec_map.csv

link-inputs: ../input/cd_homeownership_dcp_supply_era_interactions.csv ../input/cd_homeownership_outcome_era_interactions.csv
