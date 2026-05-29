all: ../output/zap_housing_initial_panel.csv ../output/zap_housing_mature_cohort_panel.csv ../output/zap_housing_tercile_year_summary.csv ../output/zap_housing_tercile_era_summary.csv ../output/zap_housing_plots.pdf

../output/zap_housing_initial_panel.csv: summarize_zap_housing_cohorts.R ../../../_lib/source_pipeline_utils.R ../input/zap_housing_cohort_base.csv | ../output
	Rscript --vanilla $<

../output/zap_housing_mature_cohort_panel.csv ../output/zap_housing_tercile_year_summary.csv ../output/zap_housing_tercile_era_summary.csv ../output/zap_housing_plots.pdf: ../output/zap_housing_initial_panel.csv
	@test -s "$@"

../input/zap_housing_cohort_base.csv: ../../../build_zap_housing_cohort_base/output/zap_housing_cohort_base.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_housing_cohort_base.csv

include ../../generic.make
