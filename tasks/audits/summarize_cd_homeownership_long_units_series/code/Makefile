all: \
	../output/cd_homeownership_long_units_city_year.csv \
	../output/cd_homeownership_long_units_tercile_year.csv \
	../output/cd_homeownership_long_units_tercile_year_ma3.csv \
	../output/cd_homeownership_long_units_tercile_year_ma5.csv \
	../output/cd_homeownership_long_units_pluto_full_tercile_year.csv \
	../output/cd_homeownership_long_units_tercile_era.csv \
	../output/cd_homeownership_long_units_summary_qc.csv \
	../output/cd_homeownership_long_units_plots.pdf \
	../output/cd_homeownership_long_units_pluto_full_plots.pdf \
	../output/cd_homeownership_long_units_tercile_plot_ma3.pdf \
	../output/cd_homeownership_long_units_tercile_plot_ma5.pdf \
	../output/cd_homeownership_long_units_projects_50_plus_counts.pdf \
	../output/cd_homeownership_long_units_projects_50_plus_counts_ma3.pdf \
	../output/cd_homeownership_long_units_projects_50_plus_counts_ma5.pdf \
	../output/cd_homeownership_long_units_total_unit_counts.pdf \
	../output/cd_homeownership_long_units_total_unit_counts_ma3.pdf \
	../output/cd_homeownership_long_units_total_unit_counts_ma5.pdf

../output/cd_homeownership_long_units_city_year.csv: summarize_cd_homeownership_long_units_series.R ../input/cd_homeownership_long_units_series.csv ../input/mappluto_construction_proxy_cd_year.csv | ../output
	Rscript --vanilla $<

../output/cd_homeownership_long_units_tercile_year.csv \
../output/cd_homeownership_long_units_tercile_year_ma3.csv \
../output/cd_homeownership_long_units_tercile_year_ma5.csv \
../output/cd_homeownership_long_units_pluto_full_tercile_year.csv \
../output/cd_homeownership_long_units_tercile_era.csv \
../output/cd_homeownership_long_units_summary_qc.csv \
../output/cd_homeownership_long_units_plots.pdf \
../output/cd_homeownership_long_units_pluto_full_plots.pdf \
../output/cd_homeownership_long_units_tercile_plot_ma3.pdf \
../output/cd_homeownership_long_units_tercile_plot_ma5.pdf \
../output/cd_homeownership_long_units_projects_50_plus_counts.pdf \
../output/cd_homeownership_long_units_projects_50_plus_counts_ma3.pdf \
../output/cd_homeownership_long_units_projects_50_plus_counts_ma5.pdf \
../output/cd_homeownership_long_units_total_unit_counts.pdf \
../output/cd_homeownership_long_units_total_unit_counts_ma3.pdf \
../output/cd_homeownership_long_units_total_unit_counts_ma5.pdf: ../output/cd_homeownership_long_units_city_year.csv
	@test -s "$@"

../input/cd_homeownership_long_units_series.csv: ../../build_cd_homeownership_long_units_series/output/cd_homeownership_long_units_series.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/mappluto_construction_proxy_cd_year.csv: ../../build_mappluto_construction_proxy/output/mappluto_construction_proxy_cd_year.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/cd_homeownership_long_units_series.csv ../input/mappluto_construction_proxy_cd_year.csv

include ../../generic.make
