R := Rscript --vanilla

all: ../output/cd_homeownership_long_units_series.csv ../output/cd_homeownership_long_units_series_qc.csv

../input/cd_homeownership_1990_measure.csv: ../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	ln -sf "../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv" "$@"

../input/cd_baseline_1990_controls.csv: ../../build_cd_baseline_1990_controls/output/cd_baseline_1990_controls.csv | ../input
	ln -sf "../../build_cd_baseline_1990_controls/output/cd_baseline_1990_controls.csv" "$@"

../input/mappluto_construction_proxy_cd_year.csv: ../../build_mappluto_construction_proxy/output/mappluto_construction_proxy_cd_year.csv | ../input
	ln -sf "../../build_mappluto_construction_proxy/output/mappluto_construction_proxy_cd_year.csv" "$@"

../input/dcp_housing_database_project_level_25q4.parquet: ../../stage_dcp_housing_database/output/dcp_housing_database_project_level_25q4.parquet | ../input
	ln -sf "../../stage_dcp_housing_database/output/dcp_housing_database_project_level_25q4.parquet" "$@"

../output/cd_homeownership_long_units_series.csv: build_cd_homeownership_long_units_series.R ../input/cd_homeownership_1990_measure.csv ../input/cd_baseline_1990_controls.csv ../input/mappluto_construction_proxy_cd_year.csv ../input/dcp_housing_database_project_level_25q4.parquet | ../output
	$(R) $< ../input/cd_homeownership_1990_measure.csv ../input/cd_baseline_1990_controls.csv ../input/mappluto_construction_proxy_cd_year.csv ../input/dcp_housing_database_project_level_25q4.parquet ../output/cd_homeownership_long_units_series.csv ../output/cd_homeownership_long_units_series_qc.csv

../output/cd_homeownership_long_units_series_qc.csv: ../output/cd_homeownership_long_units_series.csv

link-inputs: ../input/cd_homeownership_1990_measure.csv ../input/cd_baseline_1990_controls.csv ../input/mappluto_construction_proxy_cd_year.csv ../input/dcp_housing_database_project_level_25q4.parquet
