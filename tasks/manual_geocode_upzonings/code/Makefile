SHELL := bash
include ../../shell_functions.make

all: ../output/manual_geocode_upzoning_queue.csv \
	../output/manual_geocode_upzoning_google_queries.csv \
	../output/manual_geocode_upzoning_census_batch_template.csv \
	../output/manual_geocode_upzoning_chatgpt_prompt.md \
	../output/manual_geocode_upzoning_qc.csv

../output/manual_geocode_upzoning_queue.csv: build_manual_geocode_upzonings.R manual_geocode_upzonings.csv ../../_lib/source_pipeline_utils.R ../input/zap_zoning_map_special_permit_project_classification.csv ../input/zap_zoning_map_special_permit_project_ccd2010_fractional.csv ../input/zap_project_data.parquet ../input/zap_project_bbl.parquet ../input/ccdist2010_mappluto_bbl_lookup.parquet | ../output
	$(R) $<

../output/manual_geocode_upzoning_google_queries.csv \
../output/manual_geocode_upzoning_census_batch_template.csv \
../output/manual_geocode_upzoning_chatgpt_prompt.md \
../output/manual_geocode_upzoning_qc.csv: ../output/manual_geocode_upzoning_queue.csv
	@test -s "$@"

../input/zap_zoning_map_special_permit_project_classification.csv: ../../summarize_zap_zoning_actions_by_decade/output/zap_zoning_map_special_permit_project_classification.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_zoning_map_special_permit_project_ccd2010_fractional.csv: ../../summarize_zap_zoning_actions_by_decade/output/zap_zoning_map_special_permit_project_ccd2010_fractional.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_project_data.parquet: ../../stage_zap_datasets/output/zap_project_data.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_project_bbl.parquet: ../../stage_zap_datasets/output/zap_project_bbl.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ccdist2010_mappluto_bbl_lookup.parquet: ../../build_ccd2010_mappluto_construction_proxy/output/ccdist2010_mappluto_bbl_lookup.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_zoning_map_special_permit_project_classification.csv ../input/zap_zoning_map_special_permit_project_ccd2010_fractional.csv ../input/zap_project_data.parquet ../input/zap_project_bbl.parquet ../input/ccdist2010_mappluto_bbl_lookup.parquet

include ../../generic.make
