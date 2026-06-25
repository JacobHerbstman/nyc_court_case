all: ../output/cd_homeownership_long_units_level_year.csv ../output/cd_homeownership_long_units_level_era.csv ../output/cd_homeownership_long_units_built_form_controls.csv ../output/cd_homeownership_long_units_developability_residuals.csv ../output/cd_homeownership_long_units_levels_developability_plots.pdf ../output/cd_homeownership_long_units_levels_developability_qc.csv

../output/cd_homeownership_long_units_level_year.csv: summarize_cd_homeownership_long_units_levels_developability.R ../../../_lib/source_pipeline_utils.R ../input/cd_homeownership_long_units_series.csv ../input/dcp_housing_database_files.csv ../input/mappluto_construction_proxy_cd_year.csv ../input/mappluto_lot_files.csv ../input/cd_baseline_1990_controls.csv | ../output
	Rscript --vanilla $<

../output/cd_homeownership_long_units_level_era.csv ../output/cd_homeownership_long_units_built_form_controls.csv ../output/cd_homeownership_long_units_developability_residuals.csv ../output/cd_homeownership_long_units_levels_developability_plots.pdf ../output/cd_homeownership_long_units_levels_developability_qc.csv: ../output/cd_homeownership_long_units_level_year.csv
	@test -s "$@"

../input/cd_homeownership_long_units_series.csv: ../../../build_cd_homeownership_long_units_series/output/cd_homeownership_long_units_series.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/dcp_housing_database_files.csv: ../../../build_dcp_housing_database/output/dcp_housing_database_files.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/mappluto_construction_proxy_cd_year.csv: ../../../build_mappluto_construction_proxy/output/mappluto_construction_proxy_cd_year.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/mappluto_lot_files.csv: ../../../build_mappluto_lots/output/mappluto_lot_files.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/cd_baseline_1990_controls.csv: ../../../build_cd_baseline_1990_controls/output/cd_baseline_1990_controls.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/cd_homeownership_long_units_series.csv ../input/dcp_housing_database_files.csv ../input/mappluto_construction_proxy_cd_year.csv ../input/mappluto_lot_files.csv ../input/cd_baseline_1990_controls.csv

include ../../generic.make
