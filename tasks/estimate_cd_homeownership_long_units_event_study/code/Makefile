all: ../output/cd_homeownership_long_units_event_coefficients_raw_units_5yr_bins.csv \
	../output/cd_homeownership_long_units_event_coefficients_raw_units_5yr_bins.pdf \
	../output/cd_homeownership_long_units_long_difference_raw_units_estimates.csv \
	../output/cd_homeownership_long_units_long_difference_raw_units.tex

../output/cd_homeownership_long_units_event_coefficients_raw_units_5yr_bins.csv: estimate_cd_homeownership_long_units_event_study.R ../../_lib/source_pipeline_utils.R ../input/cd_homeownership_long_units_series.csv | ../output
	Rscript --vanilla $<

../output/cd_homeownership_long_units_event_coefficients_raw_units_5yr_bins.pdf: ../output/cd_homeownership_long_units_event_coefficients_raw_units_5yr_bins.csv
	@test -s "$@"

../output/cd_homeownership_long_units_long_difference_raw_units_estimates.csv: ../output/cd_homeownership_long_units_event_coefficients_raw_units_5yr_bins.csv
	@test -s "$@"

../output/cd_homeownership_long_units_long_difference_raw_units.tex: ../output/cd_homeownership_long_units_event_coefficients_raw_units_5yr_bins.csv
	@test -s "$@"

../input/cd_homeownership_long_units_series.csv: ../../build_cd_homeownership_long_units_series/output/cd_homeownership_long_units_series.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/cd_homeownership_long_units_series.csv

include ../../generic.make
