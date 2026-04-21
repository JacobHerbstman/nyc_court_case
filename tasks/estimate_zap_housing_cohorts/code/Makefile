SHELL := bash
include ../../shell_functions.make

all: ../output/zap_housing_era_interactions.csv ../output/zap_housing_model_summary.csv ../output/zap_housing_era_interactions_ref_1985_1989.csv ../output/zap_housing_model_summary_ref_1985_1989.csv

../input/zap_housing_initial_panel.csv: ../../summarize_zap_housing_cohorts/output/zap_housing_initial_panel.csv | ../input
	ln -sf "$<" "$@"

../input/zap_housing_mature_cohort_panel.csv: ../../summarize_zap_housing_cohorts/output/zap_housing_mature_cohort_panel.csv | ../input
	ln -sf "$<" "$@"

../output/zap_housing_era_interactions.csv: estimate_zap_housing_cohorts.R ../input/zap_housing_initial_panel.csv ../input/zap_housing_mature_cohort_panel.csv | ../output
	$(R) $< ../input/zap_housing_initial_panel.csv ../input/zap_housing_mature_cohort_panel.csv 1980-1984 ../output/zap_housing_era_interactions.csv ../output/zap_housing_model_summary.csv

../output/zap_housing_model_summary.csv: ../output/zap_housing_era_interactions.csv

../output/zap_housing_era_interactions_ref_1985_1989.csv: estimate_zap_housing_cohorts.R ../input/zap_housing_initial_panel.csv ../input/zap_housing_mature_cohort_panel.csv | ../output
	$(R) $< ../input/zap_housing_initial_panel.csv ../input/zap_housing_mature_cohort_panel.csv 1985-1989 ../output/zap_housing_era_interactions_ref_1985_1989.csv ../output/zap_housing_model_summary_ref_1985_1989.csv

../output/zap_housing_model_summary_ref_1985_1989.csv: ../output/zap_housing_era_interactions_ref_1985_1989.csv

link-inputs: ../input/zap_housing_initial_panel.csv ../input/zap_housing_mature_cohort_panel.csv

include ../../generic.make
