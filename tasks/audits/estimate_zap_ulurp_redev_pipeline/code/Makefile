all: ../output/zap_ulurp_redev_model_summary.csv ../output/zap_ulurp_redev_nested_diagnostics.csv ../output/zap_ulurp_redev_coefficients.pdf

../output/zap_ulurp_redev_model_summary.csv: estimate_zap_ulurp_redev_pipeline.R ../../../_lib/source_pipeline_utils.R ../input/zap_ulurp_redev_cd_year_panel.csv ../input/zap_ulurp_redev_mature_cohort_panel.csv ../input/zap_ulurp_redev_yield_panel.csv | ../output
	Rscript --vanilla $<

../output/zap_ulurp_redev_nested_diagnostics.csv: ../output/zap_ulurp_redev_model_summary.csv
	@test -s "$@"

../output/zap_ulurp_redev_coefficients.pdf: ../output/zap_ulurp_redev_model_summary.csv
	@test -s "$@"

../input/zap_ulurp_redev_cd_year_panel.csv: ../../../summarize_zap_ulurp_redev_pipeline/output/zap_ulurp_redev_cd_year_panel.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_ulurp_redev_mature_cohort_panel.csv: ../../../summarize_zap_ulurp_redev_pipeline/output/zap_ulurp_redev_mature_cohort_panel.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_ulurp_redev_yield_panel.csv: ../../../summarize_zap_ulurp_redev_pipeline/output/zap_ulurp_redev_yield_panel.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_ulurp_redev_cd_year_panel.csv ../input/zap_ulurp_redev_mature_cohort_panel.csv ../input/zap_ulurp_redev_yield_panel.csv

include ../../generic.make
