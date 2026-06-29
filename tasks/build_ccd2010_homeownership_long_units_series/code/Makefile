all: ../output/ccdist2010_homeownership_long_units_series.csv

../output/ccdist2010_homeownership_long_units_series.csv: build_ccd2010_homeownership_long_units_series.R \
	../../_lib/source_pipeline_utils.R \
	../input/ccdist2010_homeownership_1990_measure.csv \
	../input/ccdist2010_mappluto_construction_proxy_district_year.csv | ../output
	Rscript --vanilla $<

../input/ccdist2010_homeownership_1990_measure.csv: ../../build_ccd2010_homeownership_1990_measure/output/ccdist2010_homeownership_1990_measure.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ccdist2010_mappluto_construction_proxy_district_year.csv: ../../build_ccd2010_mappluto_construction_proxy/output/ccdist2010_mappluto_construction_proxy_district_year.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/ccdist2010_homeownership_1990_measure.csv ../input/ccdist2010_mappluto_construction_proxy_district_year.csv

include ../../generic.make
