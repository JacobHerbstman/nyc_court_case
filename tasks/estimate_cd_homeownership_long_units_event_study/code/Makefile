BIN_SCHEMES = 5yr_bins decade_bins decade_pre_bins

all: $(foreach scheme,$(BIN_SCHEMES),../output/cd_homeownership_long_units_event_coefficients_raw_units_$(scheme).csv ../output/cd_homeownership_long_units_event_coefficients_raw_units_$(scheme).pdf) \
	../output/cd_homeownership_long_units_long_difference_raw_units_estimates.csv \
	../output/cd_homeownership_long_units_long_difference_raw_units.tex

../output/cd_homeownership_long_units_event_coefficients_raw_units_%.csv ../output/cd_homeownership_long_units_event_coefficients_raw_units_%.pdf: estimate_cd_homeownership_long_units_event_study.R Makefile ../../_lib/data_reports.R ../input/cd_homeownership_long_units_series.csv | ../output ../report
	Rscript --vanilla $< $*

../%/cd_homeownership_long_units_long_difference_raw_units_estimates.csv ../%/cd_homeownership_long_units_long_difference_raw_units.tex: estimate_cd_homeownership_long_units_long_difference.R Makefile ../../_lib/data_reports.R ../input/cd_homeownership_long_units_series.csv | ../output ../report
	Rscript --vanilla $<

../input/cd_homeownership_long_units_series.csv: ../../build_cd_homeownership_long_units_series/output/cd_homeownership_long_units_series.csv | ../input
	ln -sf "$<" "$@"

link-inputs: ../input/cd_homeownership_long_units_series.csv

include ../../generic.make
