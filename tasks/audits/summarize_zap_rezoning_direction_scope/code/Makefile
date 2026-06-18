all: ../output/zap_rezoning_direction_project_classification.csv \
	../output/zap_rezoning_direction_parse_pairs.csv \
	../output/zap_rezoning_direction_city_year.csv \
	../output/zap_rezoning_direction_period.csv \
	../output/zap_rezoning_direction_diagnostic_year.csv \
	../output/zap_rezoning_direction_tercile_year.csv \
	../output/zap_rezoning_direction_tercile_diagnostic_year.csv \
	../output/zap_rezoning_direction_ccd_year_panel.csv \
	../output/zap_rezoning_direction_event_design_panel.csv \
	../output/zap_rezoning_direction_event_coefficients_5yr_bins.csv \
	../output/zap_rezoning_direction_event_coefficients_5yr_bins.pdf \
	../output/zap_rezoning_direction_long_difference_estimates.csv \
	../output/zap_rezoning_direction_long_difference.tex \
	../output/zap_rezoning_direction_parse_coverage_by_period.csv \
	../output/zap_rezoning_direction_coverage_by_period_group.csv \
	../output/zap_rezoning_direction_scope_match_quality.csv \
	../output/zap_rezoning_direction_unrecognized_zoning_codes.csv \
	../output/zap_rezoning_direction_manual_review.csv \
	../output/zap_rezoning_direction_manual_classification_queue.csv \
	../output/zap_rezoning_direction_manual_classification_chatgpt_prompt.md \
	../output/zap_rezoning_direction_manual_classification_summary.csv \
	../output/zap_rezoning_direction_text_candidate_queue.csv \
	../output/zap_rezoning_direction_text_candidate_summary.csv \
	../output/zap_rezoning_direction_unknown_audit.csv \
	../output/zap_rezoning_direction_unknown_audit_summary.csv \
	../output/zap_rezoning_direction_resolution_waterfall.csv \
	../output/zap_rezoning_direction_source_reviewed_cases.csv \
	../output/zap_rezoning_direction_block_expanded_scope_audit.csv \
	../output/zap_rezoning_direction_review_ledger.csv \
	../output/zap_rezoning_direction_review_ledger_summary.csv \
	../output/zap_rezoning_direction_reviewed_city_year.csv \
	../output/zap_rezoning_direction_reviewed_period.csv \
	../output/zap_rezoning_direction_top_abs_far_acres.csv \
	../output/zap_rezoning_direction_zoning_district_lookup.csv \
	../output/zap_rezoning_direction_city_trends.pdf \
	../output/zap_rezoning_direction_tercile_trends.pdf \
	../output/zap_rezoning_direction_qc.csv

../output/zap_rezoning_direction_qc.csv: summarize_zap_rezoning_direction_scope.R manual_rezoning_direction_classification.csv source_reviewed_rezoning_scope.csv nyc_zoning_district_lookup.csv ../../../_lib/source_pipeline_utils.R ../input/zap_zoning_map_special_permit_project_classification.csv ../input/zap_project_bbl.parquet ../input/ccdist2010_mappluto_bbl_lookup.parquet ../input/dcp_mappluto_current_25v4.parquet ../input/ccdist2010_homeownership_1990_measure.csv ../input/ccdist2010_redevelopment_potential.csv | ../output
	Rscript --vanilla $<

../output/zap_rezoning_direction_project_classification.csv \
../output/zap_rezoning_direction_parse_pairs.csv \
../output/zap_rezoning_direction_city_year.csv \
../output/zap_rezoning_direction_period.csv \
../output/zap_rezoning_direction_diagnostic_year.csv \
../output/zap_rezoning_direction_tercile_year.csv \
../output/zap_rezoning_direction_tercile_diagnostic_year.csv \
../output/zap_rezoning_direction_ccd_year_panel.csv \
../output/zap_rezoning_direction_event_design_panel.csv \
../output/zap_rezoning_direction_event_coefficients_5yr_bins.csv \
../output/zap_rezoning_direction_event_coefficients_5yr_bins.pdf \
../output/zap_rezoning_direction_long_difference_estimates.csv \
../output/zap_rezoning_direction_long_difference.tex \
../output/zap_rezoning_direction_parse_coverage_by_period.csv \
../output/zap_rezoning_direction_coverage_by_period_group.csv \
../output/zap_rezoning_direction_scope_match_quality.csv \
../output/zap_rezoning_direction_unrecognized_zoning_codes.csv \
../output/zap_rezoning_direction_manual_review.csv \
../output/zap_rezoning_direction_manual_classification_queue.csv \
../output/zap_rezoning_direction_manual_classification_chatgpt_prompt.md \
../output/zap_rezoning_direction_manual_classification_summary.csv \
../output/zap_rezoning_direction_text_candidate_queue.csv \
../output/zap_rezoning_direction_text_candidate_summary.csv \
../output/zap_rezoning_direction_unknown_audit.csv \
../output/zap_rezoning_direction_unknown_audit_summary.csv \
../output/zap_rezoning_direction_resolution_waterfall.csv \
../output/zap_rezoning_direction_source_reviewed_cases.csv \
../output/zap_rezoning_direction_block_expanded_scope_audit.csv \
../output/zap_rezoning_direction_review_ledger.csv \
../output/zap_rezoning_direction_review_ledger_summary.csv \
../output/zap_rezoning_direction_reviewed_city_year.csv \
../output/zap_rezoning_direction_reviewed_period.csv \
../output/zap_rezoning_direction_top_abs_far_acres.csv \
../output/zap_rezoning_direction_zoning_district_lookup.csv \
../output/zap_rezoning_direction_city_trends.pdf \
../output/zap_rezoning_direction_tercile_trends.pdf: ../output/zap_rezoning_direction_qc.csv
	@test -s "$@"

../input/zap_zoning_map_special_permit_project_classification.csv: ../../summarize_zap_zoning_actions_by_decade/output/zap_zoning_map_special_permit_project_classification.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_project_bbl.parquet: ../../../stage_zap_datasets/output/zap_project_bbl.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ccdist2010_mappluto_bbl_lookup.parquet: ../../../build_ccd2010_mappluto_construction_proxy/output/ccdist2010_mappluto_bbl_lookup.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/dcp_mappluto_current_25v4.parquet: ../../../stage_mappluto_lots/output/dcp_mappluto_current_25v4.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ccdist2010_homeownership_1990_measure.csv: ../../../build_ccd2010_homeownership_1990_measure/output/ccdist2010_homeownership_1990_measure.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ccdist2010_redevelopment_potential.csv: ../../../build_ccd2010_redevelopment_potential/output/ccdist2010_redevelopment_potential.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_zoning_map_special_permit_project_classification.csv ../input/zap_project_bbl.parquet ../input/ccdist2010_mappluto_bbl_lookup.parquet ../input/dcp_mappluto_current_25v4.parquet ../input/ccdist2010_homeownership_1990_measure.csv ../input/ccdist2010_redevelopment_potential.csv

include ../../generic.make
