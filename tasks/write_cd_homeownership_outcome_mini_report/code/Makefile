SHELL := bash
include ../../shell_functions.make

all: ../output/cd_homeownership_outcome_mini_report.md

../input/cd_homeownership_1990_measure_qc.csv: ../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure_qc.csv | ../input
	ln -sf "$<" "$@"

../input/cd_homeownership_permit_nb_panel_qc.csv: ../../cd_homeownership_permit_nb_panel/output/cd_homeownership_permit_nb_panel_qc.csv | ../input
	ln -sf "$<" "$@"

../input/cd_homeownership_dcp_units_panel_qc.csv: ../../cd_homeownership_dcp_units_panel/output/cd_homeownership_dcp_units_panel_qc.csv | ../input
	ln -sf "$<" "$@"

../input/cd_homeownership_outcome_panels_qc.csv: ../../summarize_cd_homeownership_outcome_panels/output/cd_homeownership_outcome_panels_qc.csv | ../input
	ln -sf "$<" "$@"

../input/cd_homeownership_outcome_tercile_five_year.csv: ../../summarize_cd_homeownership_outcome_panels/output/cd_homeownership_outcome_tercile_five_year.csv | ../input
	ln -sf "$<" "$@"

../input/cd_homeownership_outcome_model_summary.csv: ../../estimate_cd_homeownership_outcome_panels/output/cd_homeownership_outcome_model_summary.csv | ../input
	ln -sf "$<" "$@"

../input/cd_homeownership_outcome_era_interactions.csv: ../../estimate_cd_homeownership_outcome_panels/output/cd_homeownership_outcome_era_interactions.csv | ../input
	ln -sf "$<" "$@"

../input/cd_homeownership_outcome_tercile_plots.pdf: ../../summarize_cd_homeownership_outcome_panels/output/cd_homeownership_outcome_tercile_plots.pdf | ../input
	ln -sf "$<" "$@"

../output/cd_homeownership_outcome_mini_report.md: write_cd_homeownership_outcome_mini_report.R ../input/cd_homeownership_1990_measure_qc.csv ../input/cd_homeownership_permit_nb_panel_qc.csv ../input/cd_homeownership_dcp_units_panel_qc.csv ../input/cd_homeownership_outcome_panels_qc.csv ../input/cd_homeownership_outcome_tercile_five_year.csv ../input/cd_homeownership_outcome_model_summary.csv ../input/cd_homeownership_outcome_era_interactions.csv ../input/cd_homeownership_outcome_tercile_plots.pdf | ../output
	$(R) $< ../input/cd_homeownership_1990_measure_qc.csv ../input/cd_homeownership_permit_nb_panel_qc.csv ../input/cd_homeownership_dcp_units_panel_qc.csv ../input/cd_homeownership_outcome_panels_qc.csv ../input/cd_homeownership_outcome_tercile_five_year.csv ../input/cd_homeownership_outcome_model_summary.csv ../input/cd_homeownership_outcome_era_interactions.csv ../input/cd_homeownership_outcome_tercile_plots.pdf ../output/cd_homeownership_outcome_mini_report.md

link-inputs: ../input/cd_homeownership_1990_measure_qc.csv ../input/cd_homeownership_permit_nb_panel_qc.csv ../input/cd_homeownership_dcp_units_panel_qc.csv ../input/cd_homeownership_outcome_panels_qc.csv ../input/cd_homeownership_outcome_tercile_five_year.csv ../input/cd_homeownership_outcome_model_summary.csv ../input/cd_homeownership_outcome_era_interactions.csv ../input/cd_homeownership_outcome_tercile_plots.pdf

include ../../generic.make
