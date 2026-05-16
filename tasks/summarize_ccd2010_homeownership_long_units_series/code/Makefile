SHELL := bash
include ../../shell_functions.make

all: ../output/ccdist2010_homeownership_long_units_city_year.csv \
	../output/ccdist2010_homeownership_long_units_tercile_year.csv \
	../output/ccdist2010_homeownership_long_units_tercile_year_ma3.csv \
	../output/ccdist2010_homeownership_long_units_tercile_era.csv \
	../output/ccdist2010_homeownership_long_units_brooklyn_rank.csv \
	../output/ccdist2010_homeownership_long_units_summary_qc.csv \
	../output/ccdist2010_homeownership_long_units_plots.pdf \
	../output/ccdist2010_homeownership_long_units_count_plots.pdf

../output/ccdist2010_homeownership_long_units_city_year.csv: summarize_ccd2010_homeownership_long_units_series.R ../input/ccdist2010_homeownership_long_units_series.csv | ../output
	$(R) $<

../output/ccdist2010_homeownership_long_units_tercile_year.csv \
../output/ccdist2010_homeownership_long_units_tercile_year_ma3.csv \
../output/ccdist2010_homeownership_long_units_tercile_era.csv \
../output/ccdist2010_homeownership_long_units_brooklyn_rank.csv \
../output/ccdist2010_homeownership_long_units_summary_qc.csv \
../output/ccdist2010_homeownership_long_units_plots.pdf \
../output/ccdist2010_homeownership_long_units_count_plots.pdf: ../output/ccdist2010_homeownership_long_units_city_year.csv
	@test -s "$@"

../input/ccdist2010_homeownership_long_units_series.csv: ../../build_ccd2010_homeownership_long_units_series/output/ccdist2010_homeownership_long_units_series.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/ccdist2010_homeownership_long_units_series.csv

include ../../generic.make
