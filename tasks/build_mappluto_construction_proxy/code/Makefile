SHELL := bash
include ../../shell_functions.make

all: ../output/mappluto_construction_proxy_lot_level.parquet ../output/mappluto_construction_proxy_cd_year.csv

../output/mappluto_construction_proxy_lot_level.parquet: build_mappluto_construction_proxy.R ../../_lib/source_pipeline_utils.R ../input/mappluto_current_lot_lookup.parquet ../input/cd_homeownership_1990_measure.csv | ../output
	$(R) $<

../output/mappluto_construction_proxy_cd_year.csv: ../output/mappluto_construction_proxy_lot_level.parquet
	@test -s "$@"

../input/mappluto_current_lot_lookup.parquet: ../../build_mappluto_current_lookup/output/mappluto_current_lot_lookup.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/cd_homeownership_1990_measure.csv: ../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/mappluto_current_lot_lookup.parquet ../input/cd_homeownership_1990_measure.csv

include ../../generic.make
