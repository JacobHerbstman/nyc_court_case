SHELL := bash
include ../../shell_functions.make

all: ../output/zap_housing_initial_panel.csv ../output/zap_housing_mature_cohort_panel.csv ../output/zap_housing_tercile_year_summary.csv ../output/zap_housing_tercile_era_summary.csv ../output/zap_housing_bbl_link_completeness.csv ../output/zap_housing_summary_qc.csv ../output/zap_housing_plots.pdf

../input/zap_housing_cohort_base.csv: ../../build_zap_housing_cohort_base/output/zap_housing_cohort_base.csv | ../input
	ln -sf "$<" "$@"

../output/zap_housing_initial_panel.csv: summarize_zap_housing_cohorts.R ../input/zap_housing_cohort_base.csv | ../output
	$(R) $< ../input/zap_housing_cohort_base.csv ../output/zap_housing_initial_panel.csv ../output/zap_housing_mature_cohort_panel.csv ../output/zap_housing_tercile_year_summary.csv ../output/zap_housing_tercile_era_summary.csv ../output/zap_housing_bbl_link_completeness.csv ../output/zap_housing_summary_qc.csv ../output/zap_housing_plots.pdf

../output/zap_housing_mature_cohort_panel.csv: ../output/zap_housing_initial_panel.csv

../output/zap_housing_tercile_year_summary.csv: ../output/zap_housing_initial_panel.csv

../output/zap_housing_tercile_era_summary.csv: ../output/zap_housing_initial_panel.csv

../output/zap_housing_bbl_link_completeness.csv: ../output/zap_housing_initial_panel.csv

../output/zap_housing_summary_qc.csv: ../output/zap_housing_initial_panel.csv

../output/zap_housing_plots.pdf: ../output/zap_housing_initial_panel.csv

link-inputs: ../input/zap_housing_cohort_base.csv

include ../../generic.make
