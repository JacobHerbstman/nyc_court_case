SHELL := bash
include ../../../shell_functions.make

all: ../output/mappluto_construction_proxy_cd_year.csv

../output/mappluto_construction_proxy_lot_level.parquet: build_mappluto_construction_proxy.R ../../../_lib/source_pipeline_utils.R ../input/mappluto_current_lot_lookup.parquet | ../output
	$(R) $<

../output/mappluto_construction_proxy_cd_year.csv: ../output/mappluto_construction_proxy_lot_level.parquet
	@test -s "$@"

../input/mappluto_current_lot_lookup.parquet: ../../../build_mappluto_current_lookup/output/mappluto_current_lot_lookup.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/mappluto_current_lot_lookup.parquet

include ../../generic.make
