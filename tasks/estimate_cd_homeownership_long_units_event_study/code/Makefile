SHELL := bash
include ../../shell_functions.make

all: ../output/cd_homeownership_long_units_event_coefficients.csv \
	../output/cd_homeownership_long_units_event_coefficients.pdf \
	../output/cd_homeownership_long_units_event_coefficients_fe_only.pdf \
	../output/cd_homeownership_long_units_event_coefficients_light_mix.pdf \
	../output/cd_homeownership_long_units_event_coefficients_5_plus_light_mix.pdf \
	../output/cd_homeownership_long_units_event_coefficients_5_plus_four_controls.pdf \
	../output/cd_homeownership_long_units_event_coefficients_5yr_bins.csv \
	../output/cd_homeownership_long_units_event_coefficients_5_plus_four_controls_5yr_bins.pdf \
	../output/cd_homeownership_long_units_event_coefficients_light_no_preprod.pdf \
	../output/cd_homeownership_long_units_event_coefficients_light_controls.pdf \
	../output/cd_homeownership_long_units_event_poisson_coefficients.csv \
	../output/cd_homeownership_long_units_event_poisson_coefficients_fe_only.pdf \
	../output/cd_homeownership_long_units_event_poisson_coefficients_5_plus_four_controls.pdf \
	../output/cd_homeownership_long_units_event_poisson_coefficients_5yr_bins.csv \
	../output/cd_homeownership_long_units_event_poisson_coefficients_5_plus_four_controls_5yr_bins.pdf \
	../output/cd_homeownership_long_units_long_difference_estimates.csv \
	../output/cd_homeownership_long_units_long_difference_5_plus_four_controls.tex \
	../output/cd_homeownership_long_units_event_5_plus_four_controls_macros.tex \
	../output/cd_homeownership_long_units_triple_diff_coefficients.csv \
	../output/cd_homeownership_long_units_triple_diff_coefficients.pdf \
	../output/cd_homeownership_long_units_randomization_inference.csv \
	../output/cd_homeownership_long_units_model_summary.csv \
	../output/cd_homeownership_long_units_design_qc.csv

../output/cd_homeownership_long_units_design_qc.csv: estimate_cd_homeownership_long_units_event_study.R ../../_lib/source_pipeline_utils.R ../input/cd_homeownership_long_units_series.csv ../input/cd_baseline_1990_controls.csv ../input/cd_redevelopment_potential_baseline.csv | ../output
	$(R) $<

../output/cd_homeownership_long_units_event_coefficients.csv ../output/cd_homeownership_long_units_event_coefficients.pdf ../output/cd_homeownership_long_units_event_coefficients_fe_only.pdf ../output/cd_homeownership_long_units_event_coefficients_light_mix.pdf ../output/cd_homeownership_long_units_event_coefficients_5_plus_light_mix.pdf ../output/cd_homeownership_long_units_event_coefficients_5_plus_four_controls.pdf ../output/cd_homeownership_long_units_event_coefficients_5yr_bins.csv ../output/cd_homeownership_long_units_event_coefficients_5_plus_four_controls_5yr_bins.pdf ../output/cd_homeownership_long_units_event_coefficients_light_no_preprod.pdf ../output/cd_homeownership_long_units_event_coefficients_light_controls.pdf ../output/cd_homeownership_long_units_event_poisson_coefficients.csv ../output/cd_homeownership_long_units_event_poisson_coefficients_fe_only.pdf ../output/cd_homeownership_long_units_event_poisson_coefficients_5_plus_four_controls.pdf ../output/cd_homeownership_long_units_event_poisson_coefficients_5yr_bins.csv ../output/cd_homeownership_long_units_event_poisson_coefficients_5_plus_four_controls_5yr_bins.pdf ../output/cd_homeownership_long_units_long_difference_estimates.csv ../output/cd_homeownership_long_units_long_difference_5_plus_four_controls.tex ../output/cd_homeownership_long_units_event_5_plus_four_controls_macros.tex ../output/cd_homeownership_long_units_triple_diff_coefficients.csv ../output/cd_homeownership_long_units_triple_diff_coefficients.pdf ../output/cd_homeownership_long_units_randomization_inference.csv ../output/cd_homeownership_long_units_model_summary.csv: ../output/cd_homeownership_long_units_design_qc.csv
	@test -s "$@"

../input/cd_homeownership_long_units_series.csv: ../../build_cd_homeownership_long_units_series/output/cd_homeownership_long_units_series.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/cd_baseline_1990_controls.csv: ../../build_cd_baseline_1990_controls/output/cd_baseline_1990_controls.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/cd_redevelopment_potential_baseline.csv: ../../redevelopment_potential_first_pass/output/cd_redevelopment_potential_baseline.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/cd_homeownership_long_units_series.csv ../input/cd_baseline_1990_controls.csv ../input/cd_redevelopment_potential_baseline.csv

include ../../generic.make
