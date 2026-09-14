all: ../output/cd_homeownership_long_units_event_coefficients_raw_units_5yr_bins.csv \
	../output/cd_homeownership_long_units_event_coefficients_raw_units_5yr_bins.pdf \
	../output/cd_homeownership_long_units_long_difference_raw_units_estimates.csv \
	../output/cd_homeownership_long_units_long_difference_raw_units.tex

../%/cd_homeownership_long_units_event_coefficients_raw_units_5yr_bins.csv ../%/cd_homeownership_long_units_event_coefficients_raw_units_5yr_bins.pdf ../%/cd_homeownership_long_units_long_difference_raw_units_estimates.csv ../%/cd_homeownership_long_units_long_difference_raw_units.tex: estimate_cd_homeownership_long_units_event_study.R Makefile ../../_lib/data_reports.R ../input/cd_homeownership_long_units_series.csv | ../output ../report
	Rscript --vanilla $<

../input/cd_homeownership_long_units_series.csv: ../../build_cd_homeownership_long_units_series/output/cd_homeownership_long_units_series.csv | ../input
	ln -sf "$<" "$@"

link-inputs: ../input/cd_homeownership_long_units_series.csv

include ../../generic.make
