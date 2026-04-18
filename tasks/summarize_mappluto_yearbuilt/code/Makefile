SHELL := bash
include ../../shell_functions.make

all: ../output/mappluto_yearbuilt_city_summary.csv ../output/mappluto_yearbuilt_borough_summary.csv ../output/mappluto_yearbuilt_cd_summary.csv ../output/mappluto_adjacent_release_audit.csv ../output/mappluto_yearbuilt_support_summary.csv

../input/mappluto_lot_files.csv: ../../stage_mappluto_lots/output/mappluto_lot_files.csv | ../input
	ln -sf "../../stage_mappluto_lots/output/mappluto_lot_files.csv" "$@"

../output/mappluto_yearbuilt_city_summary.csv: summarize_mappluto_yearbuilt.R ../input/mappluto_lot_files.csv | ../output
	$(R) $< ../input/mappluto_lot_files.csv ../output/mappluto_yearbuilt_city_summary.csv ../output/mappluto_yearbuilt_borough_summary.csv ../output/mappluto_yearbuilt_cd_summary.csv ../output/mappluto_adjacent_release_audit.csv ../output/mappluto_yearbuilt_support_summary.csv

../output/mappluto_yearbuilt_borough_summary.csv: ../output/mappluto_yearbuilt_city_summary.csv

../output/mappluto_yearbuilt_cd_summary.csv: ../output/mappluto_yearbuilt_city_summary.csv

../output/mappluto_adjacent_release_audit.csv: ../output/mappluto_yearbuilt_city_summary.csv

../output/mappluto_yearbuilt_support_summary.csv: ../output/mappluto_yearbuilt_city_summary.csv

link-inputs: ../input/mappluto_lot_files.csv

include ../../generic.make
