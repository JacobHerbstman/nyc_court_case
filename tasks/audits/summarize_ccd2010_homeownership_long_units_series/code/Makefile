all: ../output/ccdist2010_homeownership_long_units_raw_units_plots.pdf \
	../output/ccdist2010_homeownership_historical_decade_units_plot.pdf

../output/ccdist2010_homeownership_long_units_raw_units_plots.pdf: summarize_ccd2010_homeownership_long_units_series.R ../input/ccdist2010_homeownership_long_units_series.csv | ../output
	Rscript --vanilla $<

../output/ccdist2010_homeownership_historical_decade_units_plot.pdf: plot_ccd2010_homeownership_historical_decades.R ../input/ccdist2010_homeownership_long_units_series.csv | ../output
	Rscript --vanilla $<

../input/ccdist2010_homeownership_long_units_series.csv: ../../build_ccd2010_homeownership_long_units_series/output/ccdist2010_homeownership_long_units_series.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/ccdist2010_homeownership_long_units_series.csv

include ../../generic.make
