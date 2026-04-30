SHELL := bash
include ../../shell_functions.make

all: ../output/zap_ulurp_redev_model_summary.csv ../output/zap_ulurp_redev_nested_diagnostics.csv ../output/zap_ulurp_redev_coefficients.pdf

../input/zap_ulurp_redev_cd_year_panel.csv: ../../summarize_zap_ulurp_redev_pipeline/output/zap_ulurp_redev_cd_year_panel.csv | ../input
	ln -sf "$<" "$@"

../input/zap_ulurp_redev_mature_cohort_panel.csv: ../../summarize_zap_ulurp_redev_pipeline/output/zap_ulurp_redev_mature_cohort_panel.csv | ../input
	ln -sf "$<" "$@"

../input/zap_ulurp_redev_yield_panel.csv: ../../summarize_zap_ulurp_redev_pipeline/output/zap_ulurp_redev_yield_panel.csv | ../input
	ln -sf "$<" "$@"

../output/zap_ulurp_redev_model_summary.csv ../output/zap_ulurp_redev_nested_diagnostics.csv ../output/zap_ulurp_redev_coefficients.pdf: estimate_zap_ulurp_redev_pipeline.R ../../_lib/source_pipeline_utils.R ../input/zap_ulurp_redev_cd_year_panel.csv ../input/zap_ulurp_redev_mature_cohort_panel.csv ../input/zap_ulurp_redev_yield_panel.csv | ../output
	$(R) $< ../input/zap_ulurp_redev_cd_year_panel.csv ../input/zap_ulurp_redev_mature_cohort_panel.csv ../input/zap_ulurp_redev_yield_panel.csv ../output/zap_ulurp_redev_model_summary.csv ../output/zap_ulurp_redev_nested_diagnostics.csv ../output/zap_ulurp_redev_coefficients.pdf

link-inputs: ../input/zap_ulurp_redev_cd_year_panel.csv ../input/zap_ulurp_redev_mature_cohort_panel.csv ../input/zap_ulurp_redev_yield_panel.csv

include ../../generic.make
