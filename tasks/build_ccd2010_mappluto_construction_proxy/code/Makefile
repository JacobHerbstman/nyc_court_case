all: ../output/ccdist2010_mappluto_construction_proxy_district_year.csv

../output/ccdist2010_mappluto_construction_proxy_district_year.csv: build_ccd2010_mappluto_construction_proxy.R \
	../../_lib/source_pipeline_utils.R \
	../../_lib/mappluto_utils.R \
	../input/mappluto_files.csv \
	../input/ccdist2010_homeownership_1990_measure.csv | ../output
	Rscript --vanilla $<

../input/mappluto_files.csv: ../../fetch_mappluto_archive/output/mappluto_files.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ccdist2010_homeownership_1990_measure.csv: ../../build_ccd2010_homeownership_1990_measure/output/ccdist2010_homeownership_1990_measure.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/mappluto_files.csv ../input/ccdist2010_homeownership_1990_measure.csv

include ../../generic.make
