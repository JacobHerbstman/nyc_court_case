all: ../output/mappluto_construction_proxy_cd_year.csv

../output/mappluto_construction_proxy_cd_year.csv: build_mappluto_construction_proxy.R ../../_lib/data_reports.R ../input/mappluto_current_lot_lookup.parquet | ../output ../report
	Rscript --vanilla $<

../input/mappluto_current_lot_lookup.parquet: ../../build_mappluto_current_lookup/output/mappluto_current_lot_lookup.parquet | ../input
	ln -sf "$<" "$@"

link-inputs: ../input/mappluto_current_lot_lookup.parquet

include ../../generic.make
