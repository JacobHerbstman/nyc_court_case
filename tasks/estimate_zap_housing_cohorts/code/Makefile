SHELL := bash
include ../../shell_functions.make

all: ../output/zap_housing_era_interactions.csv ../output/zap_housing_model_summary.csv

../input/zap_housing_initial_panel.csv: ../../summarize_zap_housing_cohorts/output/zap_housing_initial_panel.csv | ../input
	ln -sf "$<" "$@"

../input/zap_housing_mature_cohort_panel.csv: ../../summarize_zap_housing_cohorts/output/zap_housing_mature_cohort_panel.csv | ../input
	ln -sf "$<" "$@"

../output/zap_housing_era_interactions.csv: estimate_zap_housing_cohorts.R ../input/zap_housing_initial_panel.csv ../input/zap_housing_mature_cohort_panel.csv | ../output
	$(R) $< ../input/zap_housing_initial_panel.csv ../input/zap_housing_mature_cohort_panel.csv ../output/zap_housing_era_interactions.csv ../output/zap_housing_model_summary.csv

../output/zap_housing_model_summary.csv: ../output/zap_housing_era_interactions.csv

link-inputs: ../input/zap_housing_initial_panel.csv ../input/zap_housing_mature_cohort_panel.csv

include ../../generic.make
