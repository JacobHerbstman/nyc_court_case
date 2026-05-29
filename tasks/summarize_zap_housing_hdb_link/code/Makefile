all: ../output/zap_housing_hdb_cohort_panel.csv ../output/zap_housing_hdb_tercile_era_summary.csv ../output/zap_housing_hdb_plots.pdf

../output/zap_housing_hdb_cohort_panel.csv: summarize_zap_housing_hdb_link.R ../../_lib/source_pipeline_utils.R ../input/zap_housing_hdb_project_summary.csv | ../output
	Rscript --vanilla $<

../output/zap_housing_hdb_tercile_era_summary.csv: ../output/zap_housing_hdb_cohort_panel.csv
	@test -s "$@"

../output/zap_housing_hdb_plots.pdf: ../output/zap_housing_hdb_cohort_panel.csv
	@test -s "$@"

../input/zap_housing_hdb_project_summary.csv: ../../build_zap_housing_hdb_link/output/zap_housing_hdb_project_summary.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_housing_hdb_project_summary.csv

include ../../generic.make
