SHELL := bash
include ../../shell_functions.make

all: ../output/ccdist2010_homeownership_long_units_event_coefficients_5yr_bins.csv \
	../output/ccdist2010_homeownership_long_units_event_coefficients_5yr_bins.pdf \
	../output/ccdist2010_homeownership_long_units_long_difference_estimates.csv \
	../output/ccdist2010_homeownership_long_units_long_difference.tex

../output/ccdist2010_homeownership_long_units_event_coefficients_5yr_bins.csv: estimate_ccd2010_homeownership_long_units_event_study.R ../../_lib/source_pipeline_utils.R ../input/ccdist2010_homeownership_long_units_series.csv | ../output
	$(R) $<

../output/ccdist2010_homeownership_long_units_event_coefficients_5yr_bins.pdf \
../output/ccdist2010_homeownership_long_units_long_difference_estimates.csv \
../output/ccdist2010_homeownership_long_units_long_difference.tex: ../output/ccdist2010_homeownership_long_units_event_coefficients_5yr_bins.csv
	@test -s "$@"

../input/ccdist2010_homeownership_long_units_series.csv: ../../build_ccd2010_homeownership_long_units_series/output/ccdist2010_homeownership_long_units_series.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/ccdist2010_homeownership_long_units_series.csv

include ../../generic.make
