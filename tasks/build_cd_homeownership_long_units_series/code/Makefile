all: ../output/cd_homeownership_long_units_series.csv

../output/cd_homeownership_long_units_series.csv: build_cd_homeownership_long_units_series.R ../../_lib/source_pipeline_utils.R ../input/cd_homeownership_1990_measure.csv ../input/mappluto_construction_proxy_cd_year.csv | ../output
	Rscript --vanilla $<

../input/cd_homeownership_1990_measure.csv: ../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/mappluto_construction_proxy_cd_year.csv: ../../build_mappluto_construction_proxy/output/mappluto_construction_proxy_cd_year.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/cd_homeownership_1990_measure.csv ../input/mappluto_construction_proxy_cd_year.csv

include ../../generic.make
