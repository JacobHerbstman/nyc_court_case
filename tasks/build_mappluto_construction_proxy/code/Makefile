R := Rscript --vanilla

all: ../output/mappluto_construction_proxy_lot_level.parquet ../output/mappluto_construction_proxy_cd_year.csv ../output/mappluto_construction_proxy_qc.csv

../input/mappluto_lot_files.csv: ../../stage_mappluto_lots/output/mappluto_lot_files.csv | ../input
	ln -sf "../../stage_mappluto_lots/output/mappluto_lot_files.csv" "$@"

../input/cd_homeownership_1990_measure.csv: ../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	ln -sf "../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv" "$@"

../output/mappluto_construction_proxy_lot_level.parquet: build_mappluto_construction_proxy.R ../input/mappluto_lot_files.csv ../input/cd_homeownership_1990_measure.csv | ../output
	$(R) $< ../input/mappluto_lot_files.csv ../input/cd_homeownership_1990_measure.csv ../output/mappluto_construction_proxy_lot_level.parquet ../output/mappluto_construction_proxy_cd_year.csv ../output/mappluto_construction_proxy_qc.csv

../output/mappluto_construction_proxy_cd_year.csv: ../output/mappluto_construction_proxy_lot_level.parquet

../output/mappluto_construction_proxy_qc.csv: ../output/mappluto_construction_proxy_lot_level.parquet

link-inputs: ../input/mappluto_lot_files.csv ../input/cd_homeownership_1990_measure.csv
