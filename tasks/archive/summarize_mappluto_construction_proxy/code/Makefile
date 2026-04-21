R := Rscript --vanilla

all: ../output/mappluto_construction_proxy_city_year.csv ../output/mappluto_construction_proxy_borough_year.csv ../output/mappluto_construction_proxy_vs_hdb_city_year.csv ../output/mappluto_construction_proxy_vs_hdb_borough_year.csv ../output/mappluto_construction_proxy_vs_decadal.csv ../output/mappluto_construction_proxy_validation_summary.csv

../input/mappluto_construction_proxy_cd_year.csv: ../../build_mappluto_construction_proxy/output/mappluto_construction_proxy_cd_year.csv | ../input
	ln -sf "../../build_mappluto_construction_proxy/output/mappluto_construction_proxy_cd_year.csv" "$@"

../input/cd_homeownership_1990_measure.csv: ../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	ln -sf "../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv" "$@"

../input/dcp_housing_database_project_level_25q4.parquet: ../../stage_dcp_housing_database/output/dcp_housing_database_project_level_25q4.parquet | ../input
	ln -sf "../../stage_dcp_housing_database/output/dcp_housing_database_project_level_25q4.parquet" "$@"

../input/dcp_cd_profiles_1990_2000_20260415.parquet: ../../stage_dcp_cd_profiles_1990_2000/output/dcp_cd_profiles_1990_2000_20260415.parquet | ../input
	ln -sf "../../stage_dcp_cd_profiles_1990_2000/output/dcp_cd_profiles_1990_2000_20260415.parquet" "$@"

../input/cd_baseline_1990_controls.csv: ../../build_cd_baseline_1990_controls/output/cd_baseline_1990_controls.csv | ../input
	ln -sf "../../build_cd_baseline_1990_controls/output/cd_baseline_1990_controls.csv" "$@"

../output/mappluto_construction_proxy_city_year.csv: summarize_mappluto_construction_proxy.R ../input/mappluto_construction_proxy_cd_year.csv ../input/cd_homeownership_1990_measure.csv ../input/dcp_housing_database_project_level_25q4.parquet ../input/dcp_cd_profiles_1990_2000_20260415.parquet ../input/cd_baseline_1990_controls.csv | ../output
	$(R) $< ../input/mappluto_construction_proxy_cd_year.csv ../input/cd_homeownership_1990_measure.csv ../input/dcp_housing_database_project_level_25q4.parquet ../input/dcp_cd_profiles_1990_2000_20260415.parquet ../input/cd_baseline_1990_controls.csv ../output/mappluto_construction_proxy_city_year.csv ../output/mappluto_construction_proxy_borough_year.csv ../output/mappluto_construction_proxy_vs_hdb_city_year.csv ../output/mappluto_construction_proxy_vs_hdb_borough_year.csv ../output/mappluto_construction_proxy_vs_decadal.csv ../output/mappluto_construction_proxy_validation_summary.csv

../output/mappluto_construction_proxy_borough_year.csv: ../output/mappluto_construction_proxy_city_year.csv

../output/mappluto_construction_proxy_vs_hdb_city_year.csv: ../output/mappluto_construction_proxy_city_year.csv

../output/mappluto_construction_proxy_vs_hdb_borough_year.csv: ../output/mappluto_construction_proxy_city_year.csv

../output/mappluto_construction_proxy_vs_decadal.csv: ../output/mappluto_construction_proxy_city_year.csv

../output/mappluto_construction_proxy_validation_summary.csv: ../output/mappluto_construction_proxy_city_year.csv

link-inputs: ../input/mappluto_construction_proxy_cd_year.csv ../input/cd_homeownership_1990_measure.csv ../input/dcp_housing_database_project_level_25q4.parquet ../input/dcp_cd_profiles_1990_2000_20260415.parquet ../input/cd_baseline_1990_controls.csv
