all: ../output/pluto_exploration_report.md \
	../output/pluto_exploration_rebuilt_lot_level.csv \
	../output/pluto_exploration_rebuilt_district_year.csv \
	../output/pluto_exploration_rebuilt_bbl_lookup.csv \
	../output/pluto_exploration_key_columns_dictionary.csv \
	../output/pluto_exploration_column_profile.csv \
	../output/pluto_exploration_numeric_quantiles.csv \
	../output/pluto_exploration_categorical_values.csv \
	../output/pluto_exploration_proxy_filter_steps.csv \
	../output/pluto_exploration_assumption_checks.csv \
	../output/pluto_exploration_condo_profile.csv \
	../output/pluto_exploration_condo_suspect_groups.csv \
	../output/pluto_exploration_year_built_profile.csv \
	../output/pluto_exploration_pluto_mappluto_overlap.csv \
	../output/pluto_exploration_pluto_only_residential_lots.csv \
	../output/pluto_exploration_district_year_sanity.csv \
	../output/pluto_exploration_outlier_lots.csv

../output/pluto_exploration_report.md: explore_pluto.R ../../../_lib/source_pipeline_utils.R ../input/ccdist2010_homeownership_1990_measure.csv ../input/nyc_mappluto_25v4_shp.zip ../input/nyc_pluto_25v4_csv.zip | ../output
	Rscript --vanilla $<

../output/pluto_exploration_rebuilt_lot_level.csv \
../output/pluto_exploration_rebuilt_district_year.csv \
../output/pluto_exploration_rebuilt_bbl_lookup.csv \
../output/pluto_exploration_key_columns_dictionary.csv \
../output/pluto_exploration_column_profile.csv \
../output/pluto_exploration_numeric_quantiles.csv \
../output/pluto_exploration_categorical_values.csv \
../output/pluto_exploration_proxy_filter_steps.csv \
../output/pluto_exploration_assumption_checks.csv \
../output/pluto_exploration_condo_profile.csv \
../output/pluto_exploration_condo_suspect_groups.csv \
../output/pluto_exploration_year_built_profile.csv \
../output/pluto_exploration_pluto_mappluto_overlap.csv \
../output/pluto_exploration_pluto_only_residential_lots.csv \
../output/pluto_exploration_district_year_sanity.csv \
../output/pluto_exploration_outlier_lots.csv: ../output/pluto_exploration_report.md
	@test -s "$@"

../input/ccdist2010_homeownership_1990_measure.csv: ../../../build_ccd2010_homeownership_1990_measure/output/ccdist2010_homeownership_1990_measure.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/nyc_mappluto_25v4_shp.zip: ../../../../data_raw/dcp_mappluto_current/25v4/nyc_mappluto_25v4_shp.zip | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/nyc_pluto_25v4_csv.zip: ../../../../data_raw/dcp_pluto_current/25v4/nyc_pluto_25v4_csv.zip | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/ccdist2010_homeownership_1990_measure.csv ../input/nyc_mappluto_25v4_shp.zip ../input/nyc_pluto_25v4_csv.zip

include ../../generic.make
