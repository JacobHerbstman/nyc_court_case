SHELL := bash
include ../../shell_functions.make

all: ../output/zap_housing_hdb_cohort_panel.csv ../output/zap_housing_hdb_tercile_era_summary.csv ../output/zap_housing_hdb_summary_qc.csv ../output/zap_housing_hdb_plots.pdf

../input/zap_housing_hdb_project_summary.csv: ../../build_zap_housing_hdb_link/output/zap_housing_hdb_project_summary.csv | ../input
	ln -sf "$<" "$@"

../output/zap_housing_hdb_cohort_panel.csv: summarize_zap_housing_hdb_link.R ../input/zap_housing_hdb_project_summary.csv | ../output
	$(R) $< ../input/zap_housing_hdb_project_summary.csv ../output/zap_housing_hdb_cohort_panel.csv ../output/zap_housing_hdb_tercile_era_summary.csv ../output/zap_housing_hdb_summary_qc.csv ../output/zap_housing_hdb_plots.pdf

../output/zap_housing_hdb_tercile_era_summary.csv: ../output/zap_housing_hdb_cohort_panel.csv

../output/zap_housing_hdb_summary_qc.csv: ../output/zap_housing_hdb_cohort_panel.csv

../output/zap_housing_hdb_plots.pdf: ../output/zap_housing_hdb_cohort_panel.csv

link-inputs: ../input/zap_housing_hdb_project_summary.csv

include ../../generic.make
