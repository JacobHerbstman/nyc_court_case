all: ../output/cd_homeownership_long_units_borough_era_shares.csv ../output/cd_homeownership_long_units_borough_overlap_error.csv ../output/cd_homeownership_long_units_exact_era_splice_total_units.csv ../output/cd_homeownership_long_units_borough_plots.pdf ../output/cd_homeownership_long_units_borough_details_qc.csv

../output/cd_homeownership_long_units_borough_era_shares.csv: summarize_cd_homeownership_long_units_borough_details.R ../input/cd_homeownership_long_units_series.csv ../input/mappluto_construction_proxy_cd_year.csv ../input/dcp_housing_database_files.csv ../input/cd_homeownership_exact_decadal_validation_tercile.csv ../input/cd_homeownership_proxy_overlap_borough_year.csv | ../output
	Rscript --vanilla $<

../output/cd_homeownership_long_units_borough_overlap_error.csv ../output/cd_homeownership_long_units_exact_era_splice_total_units.csv ../output/cd_homeownership_long_units_borough_plots.pdf ../output/cd_homeownership_long_units_borough_details_qc.csv: ../output/cd_homeownership_long_units_borough_era_shares.csv
	@test -s "$@"

../input/cd_homeownership_long_units_series.csv: ../../build_cd_homeownership_long_units_series/output/cd_homeownership_long_units_series.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/mappluto_construction_proxy_cd_year.csv: ../../build_mappluto_construction_proxy/output/mappluto_construction_proxy_cd_year.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/dcp_housing_database_files.csv: ../../build_dcp_housing_database/output/dcp_housing_database_files.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/cd_homeownership_exact_decadal_validation_tercile.csv: ../../build_cd_homeownership_exact_decadal_validation/output/cd_homeownership_exact_decadal_validation_tercile.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/cd_homeownership_proxy_overlap_borough_year.csv: ../../summarize_cd_homeownership_proxy_overlap_validation/output/cd_homeownership_proxy_overlap_borough_year.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/cd_homeownership_long_units_series.csv ../input/mappluto_construction_proxy_cd_year.csv ../input/dcp_housing_database_files.csv ../input/cd_homeownership_exact_decadal_validation_tercile.csv ../input/cd_homeownership_proxy_overlap_borough_year.csv

include ../../generic.make
