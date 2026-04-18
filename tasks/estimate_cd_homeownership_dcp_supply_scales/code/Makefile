R := Rscript

all: ../output/cd_homeownership_dcp_supply_scale_results.csv ../output/cd_homeownership_dcp_supply_scale_model_summary.csv ../output/cd_homeownership_dcp_supply_transform_eligibility.csv ../output/cd_homeownership_dcp_supply_scale_summary.md

../input/cd_homeownership_dcp_supply_panel.csv: ../../cd_homeownership_dcp_supply_panel/output/cd_homeownership_dcp_supply_panel.csv | ../input
	ln -sf "../../cd_homeownership_dcp_supply_panel/output/cd_homeownership_dcp_supply_panel.csv" "$@"

../input/cd_baseline_1990_controls.csv: ../../build_cd_baseline_1990_controls/output/cd_baseline_1990_controls.csv | ../input
	ln -sf "../../build_cd_baseline_1990_controls/output/cd_baseline_1990_controls.csv" "$@"

../output/cd_homeownership_dcp_supply_scale_results.csv: estimate_cd_homeownership_dcp_supply_scales.R ../input/cd_homeownership_dcp_supply_panel.csv ../input/cd_baseline_1990_controls.csv | ../output
	$(R) $< ../input/cd_homeownership_dcp_supply_panel.csv ../input/cd_baseline_1990_controls.csv ../output/cd_homeownership_dcp_supply_scale_results.csv ../output/cd_homeownership_dcp_supply_scale_model_summary.csv ../output/cd_homeownership_dcp_supply_transform_eligibility.csv ../output/cd_homeownership_dcp_supply_scale_summary.md

../output/cd_homeownership_dcp_supply_scale_model_summary.csv: ../output/cd_homeownership_dcp_supply_scale_results.csv

../output/cd_homeownership_dcp_supply_transform_eligibility.csv: ../output/cd_homeownership_dcp_supply_scale_results.csv

../output/cd_homeownership_dcp_supply_scale_summary.md: ../output/cd_homeownership_dcp_supply_scale_results.csv

link-inputs: ../input/cd_homeownership_dcp_supply_panel.csv ../input/cd_baseline_1990_controls.csv
