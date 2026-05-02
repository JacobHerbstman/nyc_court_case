R := Rscript

all: ../output/cd_homeownership_long_units_sensitivity_pooled.csv ../output/cd_homeownership_long_units_sensitivity_borough.csv ../output/cd_homeownership_long_units_sensitivity_leave_one_out.csv ../output/cd_homeownership_long_units_sensitivity_top5_out.csv ../output/cd_homeownership_long_units_sensitivity_levels.csv ../output/cd_homeownership_long_units_sensitivity_extensive.csv ../output/cd_homeownership_long_units_sensitivity_top5_cd_year.csv ../output/cd_homeownership_long_units_sensitivity_summary.csv ../output/cd_homeownership_long_units_sensitivity_qc.csv ../output/cd_homeownership_long_units_sensitivity_plots.pdf

../input/cd_homeownership_long_units_series.csv: ../../build_cd_homeownership_long_units_series/output/cd_homeownership_long_units_series.csv | ../input
	@test "$$(readlink "$@")" = "../../build_cd_homeownership_long_units_series/output/cd_homeownership_long_units_series.csv" || ln -sf "../../build_cd_homeownership_long_units_series/output/cd_homeownership_long_units_series.csv" "$@"

../input/mappluto_construction_proxy_cd_year.csv: ../../build_mappluto_construction_proxy/output/mappluto_construction_proxy_cd_year.csv | ../input
	@test "$$(readlink "$@")" = "../../build_mappluto_construction_proxy/output/mappluto_construction_proxy_cd_year.csv" || ln -sf "../../build_mappluto_construction_proxy/output/mappluto_construction_proxy_cd_year.csv" "$@"

../input/dcp_housing_database_files.csv: ../../stage_dcp_housing_database/output/dcp_housing_database_files.csv | ../input
	@test "$$(readlink "$@")" = "../../stage_dcp_housing_database/output/dcp_housing_database_files.csv" || ln -sf "../../stage_dcp_housing_database/output/dcp_housing_database_files.csv" "$@"

../output/cd_homeownership_long_units_sensitivity_pooled.csv ../output/cd_homeownership_long_units_sensitivity_borough.csv ../output/cd_homeownership_long_units_sensitivity_leave_one_out.csv ../output/cd_homeownership_long_units_sensitivity_top5_out.csv ../output/cd_homeownership_long_units_sensitivity_levels.csv ../output/cd_homeownership_long_units_sensitivity_extensive.csv ../output/cd_homeownership_long_units_sensitivity_top5_cd_year.csv ../output/cd_homeownership_long_units_sensitivity_summary.csv ../output/cd_homeownership_long_units_sensitivity_qc.csv ../output/cd_homeownership_long_units_sensitivity_plots.pdf: summarize_cd_homeownership_long_units_sensitivity.R ../input/cd_homeownership_long_units_series.csv ../input/mappluto_construction_proxy_cd_year.csv ../input/dcp_housing_database_files.csv | ../output
	$(R) $< ../input/cd_homeownership_long_units_series.csv ../input/mappluto_construction_proxy_cd_year.csv ../input/dcp_housing_database_files.csv ../output/cd_homeownership_long_units_sensitivity_pooled.csv ../output/cd_homeownership_long_units_sensitivity_borough.csv ../output/cd_homeownership_long_units_sensitivity_leave_one_out.csv ../output/cd_homeownership_long_units_sensitivity_top5_out.csv ../output/cd_homeownership_long_units_sensitivity_levels.csv ../output/cd_homeownership_long_units_sensitivity_extensive.csv ../output/cd_homeownership_long_units_sensitivity_top5_cd_year.csv ../output/cd_homeownership_long_units_sensitivity_summary.csv ../output/cd_homeownership_long_units_sensitivity_qc.csv ../output/cd_homeownership_long_units_sensitivity_plots.pdf

link-inputs: ../input/cd_homeownership_long_units_series.csv ../input/mappluto_construction_proxy_cd_year.csv ../input/dcp_housing_database_files.csv

include ../../generic.make
