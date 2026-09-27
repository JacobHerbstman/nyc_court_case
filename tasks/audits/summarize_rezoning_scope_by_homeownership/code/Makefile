all: ../output/rezonings.csv ../output/rezoning_cd_term_summary.csv ../output/rezoning_homeownership_gradients.csv \
	../output/rezoning_local_control_link.csv \
	../output/rezoning_land_area_by_tercile_term.pdf ../output/rezoning_capacity_by_tercile_term.pdf \
	../output/rezoning_counts_by_tercile_term.pdf ../output/rezoning_homeownership_gradients.pdf

../temp/lot_zoning_panel.parquet: build_lot_zoning_panel.R \
	../input/mappluto_02b.zip \
	../input/nyc_pluto_03c.zip \
	../input/nyc_pluto_04c.zip \
	../input/nyc_pluto_05d.zip \
	../input/nyc_pluto_06c.zip \
	../input/nyc_pluto_07c.zip \
	../input/nyc_pluto_09v1.zip \
	../input/nyc_pluto_10v1.zip \
	../input/nyc_pluto_11v1.zip \
	../input/nyc_pluto_12v1.zip \
	../input/nyc_pluto_13v1.zip \
	../input/nyc_pluto_14v1.zip \
	../input/nyc_pluto_15v1.zip \
	../input/nyc_pluto_16v1.zip \
	../input/nyc_pluto_17v1.zip \
	../input/nyc_mappluto_18v1_1_arc_shp.zip \
	../input/nyc_mappluto_19v1_arc_shp.zip \
	../input/nyc_mappluto_20v1_arc_shp.zip \
	../input/nyc_mappluto_21v1_arc_shp.zip \
	../input/nyc_mappluto_22v1_arc_shp.zip \
	../input/nyc_mappluto_23v1_arc_shp.zip \
	../input/nyc_mappluto_24v1_arc_shp.zip \
	../input/nyc_mappluto_25v1_arc_shp.zip \
	../input/nyc_mappluto_25v4_arc_shp.zip | ../temp
	Rscript --vanilla $<

../%/residential_far_by_district.csv ../%/lot_capacity_changes.parquet ../%/zma_footprints.csv ../%/cd_baseline_2002.csv: measure_lot_capacity_changes.R \
	../temp/lot_zoning_panel.parquet ../input/nycgiszoningfeatures_202608shp.zip ../input/mappluto_02b.zip \
	../input/nyc_mappluto_18v1_1_arc_shp.zip ../input/nyc_mappluto_25v4_arc_shp.zip | ../temp
	Rscript --vanilla $<

../output/rezonings.csv: build_rezoning_table.R cpc_request_hand_check.csv ../temp/lot_capacity_changes.parquet ../temp/zma_footprints.csv \
	../input/zap_project_universe.csv ../input/council_land_use_decision_panel.csv ../input/ulurp_cpc_report_manifest.csv | ../output ../report
	Rscript --vanilla $<

../%/rezoning_cd_term_summary.csv ../%/rezoning_homeownership_gradients.csv ../%/rezoning_local_control_link.csv ../%/rezoning_land_area_by_tercile_term.pdf ../%/rezoning_capacity_by_tercile_term.pdf ../%/rezoning_counts_by_tercile_term.pdf ../%/rezoning_homeownership_gradients.pdf: \
	summarize_rezonings_by_homeownership.R ../output/rezonings.csv ../temp/lot_capacity_changes.parquet \
	../temp/cd_baseline_2002.csv ../input/cd_homeownership_1990_measure.csv | ../output ../report
	Rscript --vanilla $<

../input/mappluto_02b.zip: ../../../../data_raw/dcp_mappluto_archive/02b/mappluto_02b.zip | ../input
	ln -sf "$<" "$@"

../input/nyc_pluto_03c.zip: ../../../../data_raw/dcp_mappluto_archive/03c/nyc_pluto_03c.zip | ../input
	ln -sf "$<" "$@"

../input/nyc_pluto_04c.zip: ../../../../data_raw/dcp_mappluto_archive/04c/nyc_pluto_04c.zip | ../input
	ln -sf "$<" "$@"

../input/nyc_pluto_05d.zip: ../../../../data_raw/dcp_mappluto_archive/05d/nyc_pluto_05d.zip | ../input
	ln -sf "$<" "$@"

../input/nyc_pluto_06c.zip: ../../../../data_raw/dcp_mappluto_archive/06c/nyc_pluto_06c.zip | ../input
	ln -sf "$<" "$@"

../input/nyc_pluto_07c.zip: ../../../../data_raw/dcp_mappluto_archive/07c/nyc_pluto_07c.zip | ../input
	ln -sf "$<" "$@"

../input/nyc_pluto_09v1.zip: ../../../../data_raw/dcp_mappluto_archive/09v1/nyc_pluto_09v1.zip | ../input
	ln -sf "$<" "$@"

../input/nyc_pluto_10v1.zip: ../../../../data_raw/dcp_mappluto_archive/10v1/nyc_pluto_10v1.zip | ../input
	ln -sf "$<" "$@"

../input/nyc_pluto_11v1.zip: ../../../../data_raw/dcp_mappluto_archive/11v1/nyc_pluto_11v1.zip | ../input
	ln -sf "$<" "$@"

../input/nyc_pluto_12v1.zip: ../../../../data_raw/dcp_mappluto_archive/12v1/nyc_pluto_12v1.zip | ../input
	ln -sf "$<" "$@"

../input/nyc_pluto_13v1.zip: ../../../../data_raw/dcp_mappluto_archive/13v1/nyc_pluto_13v1.zip | ../input
	ln -sf "$<" "$@"

../input/nyc_pluto_14v1.zip: ../../../../data_raw/dcp_mappluto_archive/14v1/nyc_pluto_14v1.zip | ../input
	ln -sf "$<" "$@"

../input/nyc_pluto_15v1.zip: ../../../../data_raw/dcp_mappluto_archive/15v1/nyc_pluto_15v1.zip | ../input
	ln -sf "$<" "$@"

../input/nyc_pluto_16v1.zip: ../../../../data_raw/dcp_mappluto_archive/16v1/nyc_pluto_16v1.zip | ../input
	ln -sf "$<" "$@"

../input/nyc_pluto_17v1.zip: ../../../../data_raw/dcp_mappluto_archive/17v1/nyc_pluto_17v1.zip | ../input
	ln -sf "$<" "$@"

../input/nyc_mappluto_18v1_1_arc_shp.zip: ../../../../data_raw/dcp_mappluto_archive/18v1.1/nyc_mappluto_18v1_1_arc_shp.zip | ../input
	ln -sf "$<" "$@"

../input/nyc_mappluto_19v1_arc_shp.zip: ../../../../data_raw/dcp_mappluto_archive/19v1/nyc_mappluto_19v1_arc_shp.zip | ../input
	ln -sf "$<" "$@"

../input/nyc_mappluto_20v1_arc_shp.zip: ../../../../data_raw/dcp_mappluto_archive/20v1/nyc_mappluto_20v1_arc_shp.zip | ../input
	ln -sf "$<" "$@"

../input/nyc_mappluto_21v1_arc_shp.zip: ../../../../data_raw/dcp_mappluto_archive/21v1/nyc_mappluto_21v1_arc_shp.zip | ../input
	ln -sf "$<" "$@"

../input/nyc_mappluto_22v1_arc_shp.zip: ../../../../data_raw/dcp_mappluto_archive/22v1/nyc_mappluto_22v1_arc_shp.zip | ../input
	ln -sf "$<" "$@"

../input/nyc_mappluto_23v1_arc_shp.zip: ../../../../data_raw/dcp_mappluto_archive/23v1/nyc_mappluto_23v1_arc_shp.zip | ../input
	ln -sf "$<" "$@"

../input/nyc_mappluto_24v1_arc_shp.zip: ../../../../data_raw/dcp_mappluto_archive/24v1/nyc_mappluto_24v1_arc_shp.zip | ../input
	ln -sf "$<" "$@"

../input/nyc_mappluto_25v1_arc_shp.zip: ../../../../data_raw/dcp_mappluto_archive/25v1/nyc_mappluto_25v1_arc_shp.zip | ../input
	ln -sf "$<" "$@"

../input/nyc_mappluto_25v4_arc_shp.zip: ../../../../data_raw/dcp_mappluto_archive/25v4/nyc_mappluto_25v4_arc_shp.zip | ../input
	ln -sf "$<" "$@"

../input/nycgiszoningfeatures_202608shp.zip: ../../../../data_raw/dcp_gis_zoning_features/202608/nycgiszoningfeatures_202608shp.zip | ../input
	ln -sf "$<" "$@"

../input/zap_project_universe.csv: ../../../build_zap_project_universe/output/zap_project_universe.csv | ../input
	ln -sf "$<" "$@"

../input/council_land_use_decision_panel.csv: ../../../build_council_land_use_decision_panel/output/council_land_use_decision_panel.csv | ../input
	ln -sf "$<" "$@"

../input/ulurp_cpc_report_manifest.csv: ../../../build_ulurp_cpc_report_corpus/output/ulurp_cpc_report_manifest.csv | ../input
	ln -sf "$<" "$@"

../input/cd_homeownership_1990_measure.csv: ../../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	ln -sf "$<" "$@"

include ../../generic.make
