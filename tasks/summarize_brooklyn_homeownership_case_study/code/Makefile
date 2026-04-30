R := Rscript --vanilla

all: ../output/brooklyn_homeownership_case_study_cd_summary.csv ../output/brooklyn_homeownership_case_study_era_outcomes.csv ../output/brooklyn_homeownership_case_study_regressions.csv ../output/brooklyn_homeownership_case_study_block_regressions.csv ../output/brooklyn_homeownership_case_study_block_diagnostics.csv ../output/brooklyn_homeownership_case_study_leave_one_cd_out.csv ../output/brooklyn_homeownership_case_study_size_bin_summary.csv ../output/brooklyn_homeownership_case_study_zap_summary.csv ../output/brooklyn_homeownership_case_study_zap_block_regressions.csv ../output/brooklyn_homeownership_case_study_qc.csv ../output/brooklyn_homeownership_case_study_plots.pdf ../output/brooklyn_homeownership_case_study_control_flip_plots.pdf

../input/brooklyn_homeownership_case_study_controls.csv: ../../build_brooklyn_homeownership_case_study_controls/output/brooklyn_homeownership_case_study_controls.csv | ../input
	ln -sf "../../build_brooklyn_homeownership_case_study_controls/output/brooklyn_homeownership_case_study_controls.csv" "$@"

../input/cd_homeownership_long_units_series.csv: ../../build_cd_homeownership_long_units_series/output/cd_homeownership_long_units_series.csv | ../input
	ln -sf "../../build_cd_homeownership_long_units_series/output/cd_homeownership_long_units_series.csv" "$@"

../input/cd_homeownership_dcp_supply_panel.csv: ../../cd_homeownership_dcp_supply_panel/output/cd_homeownership_dcp_supply_panel.csv | ../input
	ln -sf "../../cd_homeownership_dcp_supply_panel/output/cd_homeownership_dcp_supply_panel.csv" "$@"

../input/cd_homeownership_permit_nb_panel.csv: ../../cd_homeownership_permit_nb_panel/output/cd_homeownership_permit_nb_panel.csv | ../input
	ln -sf "../../cd_homeownership_permit_nb_panel/output/cd_homeownership_permit_nb_panel.csv" "$@"

../input/dcp_boundary_community_districts_20260412.parquet: ../../stage_dcp_boundaries/output/dcp_boundary_community_districts_20260412.parquet | ../input
	ln -sf "../../stage_dcp_boundaries/output/dcp_boundary_community_districts_20260412.parquet" "$@"

../input/zap_ulurp_redev_cd_year_panel.csv: ../../summarize_zap_ulurp_redev_pipeline/output/zap_ulurp_redev_cd_year_panel.csv | ../input
	ln -sf "../../summarize_zap_ulurp_redev_pipeline/output/zap_ulurp_redev_cd_year_panel.csv" "$@"

../input/zap_ulurp_redev_mature_cohort_panel.csv: ../../summarize_zap_ulurp_redev_pipeline/output/zap_ulurp_redev_mature_cohort_panel.csv | ../input
	ln -sf "../../summarize_zap_ulurp_redev_pipeline/output/zap_ulurp_redev_mature_cohort_panel.csv" "$@"

../input/zap_ulurp_redev_yield_panel.csv: ../../summarize_zap_ulurp_redev_pipeline/output/zap_ulurp_redev_yield_panel.csv | ../input
	ln -sf "../../summarize_zap_ulurp_redev_pipeline/output/zap_ulurp_redev_yield_panel.csv" "$@"

../output/brooklyn_homeownership_case_study_cd_summary.csv ../output/brooklyn_homeownership_case_study_era_outcomes.csv ../output/brooklyn_homeownership_case_study_regressions.csv ../output/brooklyn_homeownership_case_study_block_regressions.csv ../output/brooklyn_homeownership_case_study_block_diagnostics.csv ../output/brooklyn_homeownership_case_study_leave_one_cd_out.csv ../output/brooklyn_homeownership_case_study_size_bin_summary.csv ../output/brooklyn_homeownership_case_study_zap_summary.csv ../output/brooklyn_homeownership_case_study_zap_block_regressions.csv ../output/brooklyn_homeownership_case_study_qc.csv ../output/brooklyn_homeownership_case_study_plots.pdf ../output/brooklyn_homeownership_case_study_control_flip_plots.pdf: summarize_brooklyn_homeownership_case_study.R ../../_lib/source_pipeline_utils.R ../input/brooklyn_homeownership_case_study_controls.csv ../input/cd_homeownership_long_units_series.csv ../input/cd_homeownership_dcp_supply_panel.csv ../input/cd_homeownership_permit_nb_panel.csv ../input/dcp_boundary_community_districts_20260412.parquet ../input/zap_ulurp_redev_cd_year_panel.csv ../input/zap_ulurp_redev_mature_cohort_panel.csv ../input/zap_ulurp_redev_yield_panel.csv | ../output
	$(R) $< ../input/brooklyn_homeownership_case_study_controls.csv ../input/cd_homeownership_long_units_series.csv ../input/cd_homeownership_dcp_supply_panel.csv ../input/cd_homeownership_permit_nb_panel.csv ../input/dcp_boundary_community_districts_20260412.parquet ../input/zap_ulurp_redev_cd_year_panel.csv ../input/zap_ulurp_redev_mature_cohort_panel.csv ../input/zap_ulurp_redev_yield_panel.csv ../output/brooklyn_homeownership_case_study_cd_summary.csv ../output/brooklyn_homeownership_case_study_era_outcomes.csv ../output/brooklyn_homeownership_case_study_regressions.csv ../output/brooklyn_homeownership_case_study_block_regressions.csv ../output/brooklyn_homeownership_case_study_block_diagnostics.csv ../output/brooklyn_homeownership_case_study_leave_one_cd_out.csv ../output/brooklyn_homeownership_case_study_size_bin_summary.csv ../output/brooklyn_homeownership_case_study_zap_summary.csv ../output/brooklyn_homeownership_case_study_zap_block_regressions.csv ../output/brooklyn_homeownership_case_study_qc.csv ../output/brooklyn_homeownership_case_study_plots.pdf ../output/brooklyn_homeownership_case_study_control_flip_plots.pdf

../input ../output:
	mkdir -p $@

link-inputs: ../input/brooklyn_homeownership_case_study_controls.csv ../input/cd_homeownership_long_units_series.csv ../input/cd_homeownership_dcp_supply_panel.csv ../input/cd_homeownership_permit_nb_panel.csv ../input/dcp_boundary_community_districts_20260412.parquet ../input/zap_ulurp_redev_cd_year_panel.csv ../input/zap_ulurp_redev_mature_cohort_panel.csv ../input/zap_ulurp_redev_yield_panel.csv
