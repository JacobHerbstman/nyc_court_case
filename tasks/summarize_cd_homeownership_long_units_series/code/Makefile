all: ../output/cd_homeownership_long_units_raw_units_plots.pdf \
	../output/cd_homeownership_historical_decade_units_plot.pdf

../output/cd_homeownership_long_units_raw_units_plots.pdf: summarize_cd_homeownership_long_units_series.R ../input/cd_homeownership_long_units_series.csv | ../output
	Rscript --vanilla $<

../output/cd_homeownership_historical_decade_units_plot.pdf: plot_cd_homeownership_historical_decades.R ../input/cd_homeownership_long_units_series.csv | ../output
	Rscript --vanilla $<

../input/cd_homeownership_long_units_series.csv: ../../build_cd_homeownership_long_units_series/output/cd_homeownership_long_units_series.csv | ../input
	ln -sf "$<" "$@"

link-inputs: ../input/cd_homeownership_long_units_series.csv

include ../../generic.make
