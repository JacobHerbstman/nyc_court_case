all: ../output/cd_homeownership_pipeline_event_coefficients.csv \
	../output/cd_homeownership_pipeline_event_coefficients.pdf \
	../output/cd_homeownership_pipeline_timing_delay_estimates.csv \
	../output/cd_homeownership_pipeline_status_estimates.csv \
	../output/cd_homeownership_pipeline_permit_coefficients.csv \
	../output/cd_homeownership_pipeline_permit_coefficients.pdf \
	../output/cd_homeownership_pipeline_model_summary.csv

../output/cd_homeownership_pipeline_event_coefficients.csv: estimate_cd_homeownership_pipeline_mechanisms.R ../../../_lib/source_pipeline_utils.R ../input/zap_ulurp_redev_project_base.csv ../input/zap_ulurp_redev_cd_year_panel.csv ../input/zap_ulurp_redev_mature_cohort_panel.csv ../input/zap_ulurp_redev_yield_panel.csv ../input/cd_homeownership_permit_nb_panel.csv ../input/cd_redevelopment_potential_baseline.csv | ../output
	Rscript --vanilla $<

../output/cd_homeownership_pipeline_event_coefficients.pdf: ../output/cd_homeownership_pipeline_event_coefficients.csv
	@test -s "$@"

../output/cd_homeownership_pipeline_timing_delay_estimates.csv: ../output/cd_homeownership_pipeline_event_coefficients.csv
	@test -s "$@"

../output/cd_homeownership_pipeline_status_estimates.csv: ../output/cd_homeownership_pipeline_event_coefficients.csv
	@test -s "$@"

../output/cd_homeownership_pipeline_permit_coefficients.csv: ../output/cd_homeownership_pipeline_event_coefficients.csv
	@test -s "$@"

../output/cd_homeownership_pipeline_permit_coefficients.pdf: ../output/cd_homeownership_pipeline_event_coefficients.csv
	@test -s "$@"

../output/cd_homeownership_pipeline_model_summary.csv: ../output/cd_homeownership_pipeline_event_coefficients.csv
	@test -s "$@"

../input/zap_ulurp_redev_project_base.csv: ../../build_zap_ulurp_redev_base/output/zap_ulurp_redev_project_base.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_ulurp_redev_cd_year_panel.csv: ../../summarize_zap_ulurp_redev_pipeline/output/zap_ulurp_redev_cd_year_panel.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_ulurp_redev_mature_cohort_panel.csv: ../../summarize_zap_ulurp_redev_pipeline/output/zap_ulurp_redev_mature_cohort_panel.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_ulurp_redev_yield_panel.csv: ../../summarize_zap_ulurp_redev_pipeline/output/zap_ulurp_redev_yield_panel.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/cd_homeownership_permit_nb_panel.csv: ../../../cd_homeownership_permit_nb_panel/output/cd_homeownership_permit_nb_panel.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/cd_redevelopment_potential_baseline.csv: ../../redevelopment_potential_first_pass/output/cd_redevelopment_potential_baseline.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_ulurp_redev_project_base.csv ../input/zap_ulurp_redev_cd_year_panel.csv ../input/zap_ulurp_redev_mature_cohort_panel.csv ../input/zap_ulurp_redev_yield_panel.csv ../input/cd_homeownership_permit_nb_panel.csv ../input/cd_redevelopment_potential_baseline.csv

include ../../generic.make
