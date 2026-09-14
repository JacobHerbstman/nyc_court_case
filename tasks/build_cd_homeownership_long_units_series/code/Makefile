all: ../output/cd_homeownership_long_units_series.csv

../output/cd_homeownership_long_units_series.csv: build_cd_homeownership_long_units_series.R ../../_lib/data_reports.R ../input/cd_homeownership_1990_measure.csv ../input/mappluto_construction_proxy_cd_year.csv | ../output ../report
	Rscript --vanilla $<

../input/cd_homeownership_1990_measure.csv: ../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	ln -sf "$<" "$@"

../input/mappluto_construction_proxy_cd_year.csv: ../../build_mappluto_construction_proxy/output/mappluto_construction_proxy_cd_year.csv | ../input
	ln -sf "$<" "$@"

link-inputs: ../input/cd_homeownership_1990_measure.csv ../input/mappluto_construction_proxy_cd_year.csv

include ../../generic.make
