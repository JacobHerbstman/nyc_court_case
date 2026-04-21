R := Rscript --vanilla

all: ../output/redevelopment_potential_first_pass_memo.pdf

../input/cd_homeownership_1990_measure.csv: ../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	ln -sf "../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv" "$@"

../input/cd_baseline_1990_controls.csv: ../../build_cd_baseline_1990_controls/output/cd_baseline_1990_controls.csv | ../input
	ln -sf "../../build_cd_baseline_1990_controls/output/cd_baseline_1990_controls.csv" "$@"

../input/cd_homeownership_dcp_supply_panel.csv: ../../cd_homeownership_dcp_supply_panel/output/cd_homeownership_dcp_supply_panel.csv | ../input
	ln -sf "../../cd_homeownership_dcp_supply_panel/output/cd_homeownership_dcp_supply_panel.csv" "$@"

../input/cd_homeownership_long_units_series.csv: ../../build_cd_homeownership_long_units_series/output/cd_homeownership_long_units_series.csv | ../input
	ln -sf "../../build_cd_homeownership_long_units_series/output/cd_homeownership_long_units_series.csv" "$@"

../input/dcp_mappluto_archive_18v1_1.parquet: ../../stage_mappluto_lots/output/dcp_mappluto_archive_18v1_1.parquet | ../input
	ln -sf "../../stage_mappluto_lots/output/dcp_mappluto_archive_18v1_1.parquet" "$@"

../input/dcp_mappluto_current_25v4.parquet: ../../stage_mappluto_lots/output/dcp_mappluto_current_25v4.parquet | ../input
	ln -sf "../../stage_mappluto_lots/output/dcp_mappluto_current_25v4.parquet" "$@"

../input/mappluto_lot_qc.csv: ../../stage_mappluto_lots/output/mappluto_lot_qc.csv | ../input
	ln -sf "../../stage_mappluto_lots/output/mappluto_lot_qc.csv" "$@"

../input/dcp_housing_database_project_level_25q4.parquet: ../../stage_dcp_housing_database/output/dcp_housing_database_project_level_25q4.parquet | ../input
	ln -sf "../../stage_dcp_housing_database/output/dcp_housing_database_project_level_25q4.parquet" "$@"

../input/cd_homeownership_permit_nb_panel.csv: ../../cd_homeownership_permit_nb_panel/output/cd_homeownership_permit_nb_panel.csv | ../input
	ln -sf "../../cd_homeownership_permit_nb_panel/output/cd_homeownership_permit_nb_panel.csv" "$@"

../input/dcp_boundary_community_districts_20260412.parquet: ../../stage_dcp_boundaries/output/dcp_boundary_community_districts_20260412.parquet | ../input
	ln -sf "../../stage_dcp_boundaries/output/dcp_boundary_community_districts_20260412.parquet" "$@"

../input/mappluto_construction_proxy_lot_level.parquet: ../../build_mappluto_construction_proxy/output/mappluto_construction_proxy_lot_level.parquet | ../input
	ln -sf "../../build_mappluto_construction_proxy/output/mappluto_construction_proxy_lot_level.parquet" "$@"

../output/input_inventory.csv: build_input_inventory.R ../input/cd_homeownership_1990_measure.csv ../input/cd_baseline_1990_controls.csv ../input/cd_homeownership_dcp_supply_panel.csv ../input/cd_homeownership_long_units_series.csv ../input/dcp_mappluto_archive_18v1_1.parquet ../input/dcp_mappluto_current_25v4.parquet ../input/mappluto_lot_qc.csv ../input/dcp_housing_database_project_level_25q4.parquet ../input/cd_homeownership_permit_nb_panel.csv ../input/dcp_boundary_community_districts_20260412.parquet | ../output
	$(R) $< ../input/cd_homeownership_1990_measure.csv ../input/cd_baseline_1990_controls.csv ../input/cd_homeownership_dcp_supply_panel.csv ../input/cd_homeownership_long_units_series.csv ../input/dcp_mappluto_archive_18v1_1.parquet ../input/dcp_mappluto_current_25v4.parquet ../input/mappluto_lot_qc.csv ../input/dcp_housing_database_project_level_25q4.parquet ../input/cd_homeownership_permit_nb_panel.csv ../input/dcp_boundary_community_districts_20260412.parquet ../output/input_inventory.csv

../output/cd_redevelopment_potential_baseline.csv: build_cd_redevelopment_potential.R ../input/cd_homeownership_1990_measure.csv ../input/cd_baseline_1990_controls.csv ../input/dcp_mappluto_archive_18v1_1.parquet ../input/dcp_mappluto_current_25v4.parquet ../input/mappluto_lot_qc.csv | ../output
	$(R) $< ../input/cd_homeownership_1990_measure.csv ../input/cd_baseline_1990_controls.csv ../input/dcp_mappluto_archive_18v1_1.parquet ../input/dcp_mappluto_current_25v4.parquet ../input/mappluto_lot_qc.csv ../output/cd_redevelopment_potential_baseline.csv ../output/cd_redevelopment_potential_qc.csv ../output/cd_redevelopment_potential_index_correlations.csv ../output/cd_redevelopment_potential_sensitivity.csv

../output/cd_redevelopment_potential_qc.csv: ../output/cd_redevelopment_potential_baseline.csv

../output/cd_redevelopment_potential_index_correlations.csv: ../output/cd_redevelopment_potential_baseline.csv

../output/cd_redevelopment_potential_sensitivity.csv: ../output/cd_redevelopment_potential_baseline.csv

../output/tables/redev_potential_by_treatment_tercile.csv: summarize_redevelopment_potential.R ../input/cd_homeownership_1990_measure.csv ../input/cd_baseline_1990_controls.csv ../output/cd_redevelopment_potential_baseline.csv ../input/cd_homeownership_long_units_series.csv ../input/cd_homeownership_dcp_supply_panel.csv ../input/cd_homeownership_permit_nb_panel.csv ../input/dcp_boundary_community_districts_20260412.parquet | ../output/tables ../output/figures
	$(R) $< ../input/cd_homeownership_1990_measure.csv ../input/cd_baseline_1990_controls.csv ../output/cd_redevelopment_potential_baseline.csv ../input/cd_homeownership_long_units_series.csv ../input/cd_homeownership_dcp_supply_panel.csv ../input/cd_homeownership_permit_nb_panel.csv ../input/dcp_boundary_community_districts_20260412.parquet ../output/tables/redev_potential_by_treatment_tercile.csv ../output/tables/treatment_by_redev_tercile.csv ../output/tables/two_by_two_cell_summary.csv ../output/tables/two_by_two_era_outcomes.csv ../output/figures/homeownership_vs_redev_potential_scatter.pdf ../output/figures/redev_potential_maps.pdf ../output/figures/two_by_two_total_units_paths.pdf ../output/figures/two_by_two_50plus_units_paths.pdf ../output/figures/two_by_two_gross_additions_paths.pdf ../output/figures/two_by_two_50plus_project_count_paths.pdf

../output/tables/treatment_by_redev_tercile.csv: ../output/tables/redev_potential_by_treatment_tercile.csv

../output/tables/two_by_two_cell_summary.csv: ../output/tables/redev_potential_by_treatment_tercile.csv

../output/tables/two_by_two_era_outcomes.csv: ../output/tables/redev_potential_by_treatment_tercile.csv

../output/figures/homeownership_vs_redev_potential_scatter.pdf: ../output/tables/redev_potential_by_treatment_tercile.csv

../output/figures/redev_potential_maps.pdf: ../output/tables/redev_potential_by_treatment_tercile.csv

../output/figures/two_by_two_total_units_paths.pdf: ../output/tables/redev_potential_by_treatment_tercile.csv

../output/figures/two_by_two_50plus_units_paths.pdf: ../output/tables/redev_potential_by_treatment_tercile.csv

../output/figures/two_by_two_gross_additions_paths.pdf: ../output/tables/redev_potential_by_treatment_tercile.csv

../output/figures/two_by_two_50plus_project_count_paths.pdf: ../output/tables/redev_potential_by_treatment_tercile.csv

../output/tables/redev_interaction_model_summary.csv: estimate_redevelopment_potential_interactions.R ../output/cd_redevelopment_potential_baseline.csv ../input/cd_homeownership_long_units_series.csv ../input/cd_homeownership_dcp_supply_panel.csv ../input/cd_homeownership_permit_nb_panel.csv ../input/mappluto_construction_proxy_lot_level.parquet | ../output/tables ../output/figures
	$(R) $< ../output/cd_redevelopment_potential_baseline.csv ../input/cd_homeownership_long_units_series.csv ../input/cd_homeownership_dcp_supply_panel.csv ../input/cd_homeownership_permit_nb_panel.csv ../input/mappluto_construction_proxy_lot_level.parquet ../output/tables/redev_interaction_model_summary.csv ../output/tables/nested_controls_coefficient_diagnostics.csv ../output/tables/borough_sensitivity_redev.csv ../output/tables/manhattan_baseline_anatomy.csv ../output/figures/nested_controls_coefficients.pdf ../output/figures/redev_interaction_event_coefficients.pdf ../output/figures/borough_specific_two_by_two_paths.pdf ../output/figures/non_manhattan_redev_paths.pdf

../output/tables/nested_controls_coefficient_diagnostics.csv: ../output/tables/redev_interaction_model_summary.csv

../output/tables/borough_sensitivity_redev.csv: ../output/tables/redev_interaction_model_summary.csv

../output/tables/manhattan_baseline_anatomy.csv: ../output/tables/redev_interaction_model_summary.csv

../output/figures/nested_controls_coefficients.pdf: ../output/tables/redev_interaction_model_summary.csv

../output/figures/redev_interaction_event_coefficients.pdf: ../output/tables/redev_interaction_model_summary.csv

../output/figures/borough_specific_two_by_two_paths.pdf: ../output/tables/redev_interaction_model_summary.csv

../output/figures/non_manhattan_redev_paths.pdf: ../output/tables/redev_interaction_model_summary.csv

../output/redevelopment_potential_first_pass_memo.tex: write_redevelopment_potential_first_pass_memo.R ../output/input_inventory.csv ../output/cd_redevelopment_potential_baseline.csv ../output/cd_redevelopment_potential_qc.csv ../output/cd_redevelopment_potential_index_correlations.csv ../output/cd_redevelopment_potential_sensitivity.csv ../output/tables/two_by_two_cell_summary.csv ../output/tables/two_by_two_era_outcomes.csv ../output/tables/redev_interaction_model_summary.csv ../output/tables/borough_sensitivity_redev.csv ../output/tables/manhattan_baseline_anatomy.csv | ../output
	$(R) $< ../output/input_inventory.csv ../output/cd_redevelopment_potential_baseline.csv ../output/cd_redevelopment_potential_qc.csv ../output/cd_redevelopment_potential_index_correlations.csv ../output/cd_redevelopment_potential_sensitivity.csv ../output/tables/two_by_two_cell_summary.csv ../output/tables/two_by_two_era_outcomes.csv ../output/tables/redev_interaction_model_summary.csv ../output/tables/borough_sensitivity_redev.csv ../output/tables/manhattan_baseline_anatomy.csv ../output/redevelopment_potential_first_pass_memo.md ../output/redevelopment_potential_first_pass_memo.tex

../output/redevelopment_potential_first_pass_memo.pdf: ../output/redevelopment_potential_first_pass_memo.tex ../output/figures/homeownership_vs_redev_potential_scatter.pdf ../output/figures/redev_potential_maps.pdf ../output/figures/two_by_two_total_units_paths.pdf ../output/figures/two_by_two_50plus_units_paths.pdf ../output/figures/two_by_two_gross_additions_paths.pdf ../output/figures/two_by_two_50plus_project_count_paths.pdf ../output/figures/nested_controls_coefficients.pdf ../output/figures/redev_interaction_event_coefficients.pdf ../output/figures/borough_specific_two_by_two_paths.pdf ../output/figures/non_manhattan_redev_paths.pdf
	cd ../output && pdflatex -interaction=nonstopmode -halt-on-error redevelopment_potential_first_pass_memo.tex

../input ../output ../output/tables ../output/figures:
	mkdir -p $@

link-inputs: ../input/cd_homeownership_1990_measure.csv ../input/cd_baseline_1990_controls.csv ../input/cd_homeownership_dcp_supply_panel.csv ../input/cd_homeownership_long_units_series.csv ../input/dcp_mappluto_archive_18v1_1.parquet ../input/dcp_mappluto_current_25v4.parquet ../input/mappluto_lot_qc.csv ../input/dcp_housing_database_project_level_25q4.parquet ../input/cd_homeownership_permit_nb_panel.csv ../input/dcp_boundary_community_districts_20260412.parquet ../input/mappluto_construction_proxy_lot_level.parquet
