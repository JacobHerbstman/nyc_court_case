SHELL := bash
include ../../shell_functions.make

all: ../output/ccdist2010_mappluto_construction_proxy_lot_level.parquet \
	../output/ccdist2010_mappluto_bbl_lookup.parquet \
	../output/ccdist2010_mappluto_construction_proxy_district_year.csv \
	../output/ccdist2010_mappluto_construction_proxy_qc.csv

../output/ccdist2010_mappluto_construction_proxy_lot_level.parquet: build_ccd2010_mappluto_construction_proxy.R ../../_lib/source_pipeline_utils.R ../input/mappluto_lot_files.csv ../input/ccdist2010_homeownership_1990_measure.csv | ../output
	$(R) $<

../output/ccdist2010_mappluto_bbl_lookup.parquet: ../output/ccdist2010_mappluto_construction_proxy_lot_level.parquet
	@test -s "$@"

../output/ccdist2010_mappluto_construction_proxy_district_year.csv: ../output/ccdist2010_mappluto_construction_proxy_lot_level.parquet
	@test -s "$@"

../output/ccdist2010_mappluto_construction_proxy_qc.csv: ../output/ccdist2010_mappluto_construction_proxy_lot_level.parquet
	@test -s "$@"

../input/mappluto_lot_files.csv: ../../stage_mappluto_lots/output/mappluto_lot_files.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ccdist2010_homeownership_1990_measure.csv: ../../build_ccd2010_homeownership_1990_measure/output/ccdist2010_homeownership_1990_measure.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/mappluto_lot_files.csv ../input/ccdist2010_homeownership_1990_measure.csv

include ../../generic.make
