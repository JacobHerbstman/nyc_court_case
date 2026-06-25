SHELL := bash
include ../../../shell_functions.make

all: ../output/zap_ulurp_redev_cd_year_panel.csv ../output/zap_ulurp_redev_mature_cohort_panel.csv ../output/zap_ulurp_redev_yield_panel.csv ../output/zap_ulurp_redev_2x2_era_summary.csv ../output/zap_ulurp_redev_plots.pdf

../output/zap_ulurp_redev_cd_year_panel.csv: summarize_zap_ulurp_redev_pipeline.R ../../../_lib/source_pipeline_utils.R ../input/zap_ulurp_redev_project_base.csv ../input/zap_housing_hdb_link_candidates.csv | ../output
	Rscript --vanilla $<

../output/zap_ulurp_redev_mature_cohort_panel.csv: ../output/zap_ulurp_redev_cd_year_panel.csv
	@test -s "$@"

../output/zap_ulurp_redev_yield_panel.csv: ../output/zap_ulurp_redev_cd_year_panel.csv
	@test -s "$@"

../output/zap_ulurp_redev_2x2_era_summary.csv: ../output/zap_ulurp_redev_cd_year_panel.csv
	@test -s "$@"

../output/zap_ulurp_redev_plots.pdf: ../output/zap_ulurp_redev_cd_year_panel.csv
	@test -s "$@"

../input/zap_ulurp_redev_project_base.csv: ../../build_zap_ulurp_redev_base/output/zap_ulurp_redev_project_base.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_housing_hdb_link_candidates.csv: ../../../build_zap_housing_hdb_link/output/zap_housing_hdb_link_candidates.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_ulurp_redev_project_base.csv ../input/zap_housing_hdb_link_candidates.csv

include ../../generic.make
