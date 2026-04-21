R := Rscript

all: ../output/cd_homeownership_long_units_sensitivity_pooled.csv ../output/cd_homeownership_long_units_sensitivity_borough.csv ../output/cd_homeownership_long_units_sensitivity_leave_one_out.csv ../output/cd_homeownership_long_units_sensitivity_top5_out.csv ../output/cd_homeownership_long_units_sensitivity_levels.csv ../output/cd_homeownership_long_units_sensitivity_extensive.csv ../output/cd_homeownership_long_units_sensitivity_top5_cd_year.csv ../output/cd_homeownership_long_units_sensitivity_summary.csv ../output/cd_homeownership_long_units_sensitivity_qc.csv ../output/cd_homeownership_long_units_sensitivity_plots.pdf

../input/cd_homeownership_long_units_series.csv: ../../build_cd_homeownership_long_units_series/output/cd_homeownership_long_units_series.csv | ../input
	ln -sf "../../build_cd_homeownership_long_units_series/output/cd_homeownership_long_units_series.csv" "$@"

../input/mappluto_construction_proxy_cd_year.csv: ../../build_mappluto_construction_proxy/output/mappluto_construction_proxy_cd_year.csv | ../input
	ln -sf "../../build_mappluto_construction_proxy/output/mappluto_construction_proxy_cd_year.csv" "$@"

../input/dcp_housing_database_project_level_25q4.parquet: ../../stage_dcp_housing_database/output/dcp_housing_database_project_level_25q4.parquet | ../input
	ln -sf "../../stage_dcp_housing_database/output/dcp_housing_database_project_level_25q4.parquet" "$@"

../output/cd_homeownership_long_units_sensitivity_pooled.csv: summarize_cd_homeownership_long_units_sensitivity.R ../input/cd_homeownership_long_units_series.csv ../input/mappluto_construction_proxy_cd_year.csv ../input/dcp_housing_database_project_level_25q4.parquet | ../output
	$(R) $< ../input/cd_homeownership_long_units_series.csv ../input/mappluto_construction_proxy_cd_year.csv ../input/dcp_housing_database_project_level_25q4.parquet ../output/cd_homeownership_long_units_sensitivity_pooled.csv ../output/cd_homeownership_long_units_sensitivity_borough.csv ../output/cd_homeownership_long_units_sensitivity_leave_one_out.csv ../output/cd_homeownership_long_units_sensitivity_top5_out.csv ../output/cd_homeownership_long_units_sensitivity_levels.csv ../output/cd_homeownership_long_units_sensitivity_extensive.csv ../output/cd_homeownership_long_units_sensitivity_top5_cd_year.csv ../output/cd_homeownership_long_units_sensitivity_summary.csv ../output/cd_homeownership_long_units_sensitivity_qc.csv ../output/cd_homeownership_long_units_sensitivity_plots.pdf

../output/cd_homeownership_long_units_sensitivity_borough.csv: ../output/cd_homeownership_long_units_sensitivity_pooled.csv

../output/cd_homeownership_long_units_sensitivity_leave_one_out.csv: ../output/cd_homeownership_long_units_sensitivity_pooled.csv

../output/cd_homeownership_long_units_sensitivity_top5_out.csv: ../output/cd_homeownership_long_units_sensitivity_pooled.csv

../output/cd_homeownership_long_units_sensitivity_levels.csv: ../output/cd_homeownership_long_units_sensitivity_pooled.csv

../output/cd_homeownership_long_units_sensitivity_extensive.csv: ../output/cd_homeownership_long_units_sensitivity_pooled.csv

../output/cd_homeownership_long_units_sensitivity_top5_cd_year.csv: ../output/cd_homeownership_long_units_sensitivity_pooled.csv

../output/cd_homeownership_long_units_sensitivity_summary.csv: ../output/cd_homeownership_long_units_sensitivity_pooled.csv

../output/cd_homeownership_long_units_sensitivity_qc.csv: ../output/cd_homeownership_long_units_sensitivity_pooled.csv

../output/cd_homeownership_long_units_sensitivity_plots.pdf: ../output/cd_homeownership_long_units_sensitivity_pooled.csv

../input ../output:
	mkdir -p $@

link-inputs: ../input/cd_homeownership_long_units_series.csv ../input/mappluto_construction_proxy_cd_year.csv ../input/dcp_housing_database_project_level_25q4.parquet
