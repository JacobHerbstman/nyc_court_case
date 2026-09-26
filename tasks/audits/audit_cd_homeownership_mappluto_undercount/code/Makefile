ACS_TABLES := B25034_005 B25034_047 B25034_061 B25034_081 B25034_085 \
	B25127_005 B25127_047 B25127_061 B25127_081 B25127_085

all: ../output/census_year_built_cd.csv ../output/mappluto_vintage_cd_year.csv \
	../output/dob_permit_cd_year.csv ../output/hdb_cd_year.csv \
	../output/undercount_cd_windows.csv ../output/undercount_tercile_summary.csv \
	../output/undercount_borough_summary.csv ../output/undercount_gap_correlates.csv \
	../output/undercount_ratio_by_tercile.pdf ../output/undercount_event_study_coefficients.csv \
	../output/undercount_long_differences.csv ../output/undercount_event_study_coefficients.pdf

# Fetches read IPUMS_API_KEY and CENSUS_API_KEY from ~/.Renviron, so they run without --vanilla.
../output/nhgis_1990_stf3_year_built_tract_csv.zip: fetch_nhgis_1990_year_built.R nhgis_1990_year_built_extract.json | ../output ../temp
	Rscript $<

../output/acs5_2024_%.json: fetch_acs5_2024_tract_table.R | ../output ../temp
	Rscript $< $*

../output/census_year_built_cd.csv: build_census_year_built_cd.R ../../../_lib/data_reports.R \
	../output/nhgis_1990_stf3_year_built_tract_csv.zip $(patsubst %,../output/acs5_2024_%.json,$(ACS_TABLES)) \
	../input/nhgis0005_shape.zip ../input/tl_2020_36_tract.zip ../input/nyc_mappluto_25v4_arc_shp.zip \
	../input/dcp_cd_profiles_1990_2000_20260501.parquet | ../output ../report
	Rscript --vanilla $<

../output/mappluto_vintage_cd_year.csv: build_mappluto_vintage_cd_year.R ../../../_lib/data_reports.R \
	../input/nyc_mappluto_25v4_arc_shp.zip ../input/mappluto_02b.zip ../input/mappluto_construction_proxy_cd_year.csv | ../output ../report
	Rscript --vanilla $<

../output/dob_permit_cd_year.csv: build_dob_permit_cd_year.R ../../../_lib/data_reports.R ../input/dob_permit_issuance_current.csv | ../output ../report
	Rscript --vanilla $<

../output/hdb_cd_year.csv: build_hdb_cd_year.R ../../../_lib/data_reports.R ../input/nychdb_25q4_csv.zip | ../output ../report
	Rscript --vanilla $<

../%/undercount_cd_windows.csv ../%/undercount_tercile_summary.csv ../%/undercount_borough_summary.csv \
../%/undercount_gap_correlates.csv ../%/undercount_ratio_by_tercile.pdf: diagnose_mappluto_undercount.R ../../../_lib/data_reports.R \
	../output/mappluto_vintage_cd_year.csv ../output/census_year_built_cd.csv ../output/hdb_cd_year.csv ../output/dob_permit_cd_year.csv \
	../input/cd_homeownership_1990_measure.csv ../input/census_bps_borough_year.parquet | ../output ../report
	Rscript --vanilla $<

../%/undercount_event_study_coefficients.csv ../%/undercount_long_differences.csv ../%/undercount_event_study_coefficients.pdf: \
	estimate_undercount_alternatives.R ../../../_lib/data_reports.R ../output/census_year_built_cd.csv ../output/hdb_cd_year.csv \
	../input/cd_homeownership_long_units_series.csv | ../output ../report
	Rscript --vanilla $<

../input/nhgis0005_shape.zip: ../../../../data_raw/nhgis_1990_tract_extract/1990/nhgis0005_shape.zip | ../input
	ln -sf "$<" "$@"

../input/tl_2020_36_tract.zip: ../../../../data_raw/census_tiger_2020_ny_tracts/2020/tl_2020_36_tract.zip | ../input
	ln -sf "$<" "$@"

../input/nyc_mappluto_25v4_arc_shp.zip: ../../../../data_raw/dcp_mappluto_archive/25v4/nyc_mappluto_25v4_arc_shp.zip | ../input
	ln -sf "$<" "$@"

../input/mappluto_02b.zip: ../../../../data_raw/dcp_mappluto_archive/02b/mappluto_02b.zip | ../input
	ln -sf "$<" "$@"

../input/dob_permit_issuance_current.csv: ../../../../data_raw/dob_permit_issuance_current/20260501/dob_permit_issuance_current.csv | ../input
	ln -sf "$<" "$@"

../input/nychdb_25q4_csv.zip: ../../../../data_raw/dcp_housing_database_project_level/25Q4/nychdb_25q4_csv.zip | ../input
	ln -sf "$<" "$@"

../input/dcp_cd_profiles_1990_2000_20260501.parquet: ../../../build_dcp_cd_profiles_1990_2000/output/dcp_cd_profiles_1990_2000_20260501.parquet | ../input
	ln -sf "$<" "$@"

../input/mappluto_construction_proxy_cd_year.csv: ../../../build_mappluto_construction_proxy/output/mappluto_construction_proxy_cd_year.csv | ../input
	ln -sf "$<" "$@"

../input/cd_homeownership_1990_measure.csv: ../../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	ln -sf "$<" "$@"

../input/cd_homeownership_long_units_series.csv: ../../../build_cd_homeownership_long_units_series/output/cd_homeownership_long_units_series.csv | ../input
	ln -sf "$<" "$@"

../input/census_bps_borough_year.parquet: ../../stage_census_bps/output/census_bps_borough_year.parquet | ../input
	ln -sf "$<" "$@"

link-inputs: ../input/nhgis0005_shape.zip ../input/tl_2020_36_tract.zip ../input/nyc_mappluto_25v4_arc_shp.zip \
	../input/mappluto_02b.zip ../input/dob_permit_issuance_current.csv ../input/nychdb_25q4_csv.zip \
	../input/dcp_cd_profiles_1990_2000_20260501.parquet ../input/mappluto_construction_proxy_cd_year.csv \
	../input/cd_homeownership_1990_measure.csv ../input/cd_homeownership_long_units_series.csv \
	../input/census_bps_borough_year.parquet

include ../../generic.make
