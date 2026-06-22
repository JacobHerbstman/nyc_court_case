SHELL := bash
include ../../shell_functions.make

all: ../output/zap_housing_project_base_audited.csv ../output/zap_housing_project_cd_primary.csv ../output/zap_housing_project_cd_bbl_fractional.csv ../output/zap_housing_cd_year_panel_primary.csv ../output/zap_housing_cd_year_panel_bbl_fractional.csv ../output/zap_housing_mature_status_panel_primary.csv ../output/zap_housing_mature_status_panel_bbl_fractional.csv

../output/zap_housing_project_base_audited.csv: build_zap_housing_pipeline_from_raw.R ../../_lib/source_pipeline_utils.R ../input/zap_project_data.parquet ../input/zap_project_bbl.parquet ../input/dcp_mappluto_current_25v4.parquet ../input/cd_homeownership_1990_measure.csv ../input/cd_redevelopment_potential_baseline.csv ../input/zap_outcome_usability_by_period.csv ../input/zap_source_integrity_qc.csv | ../output
	$(R) $<

../output/zap_housing_project_cd_primary.csv: ../output/zap_housing_project_base_audited.csv

../output/zap_housing_project_cd_bbl_fractional.csv: ../output/zap_housing_project_base_audited.csv

../output/zap_housing_cd_year_panel_primary.csv: ../output/zap_housing_project_base_audited.csv

../output/zap_housing_cd_year_panel_bbl_fractional.csv: ../output/zap_housing_project_base_audited.csv

../output/zap_housing_mature_status_panel_primary.csv: ../output/zap_housing_project_base_audited.csv

../output/zap_housing_mature_status_panel_bbl_fractional.csv: ../output/zap_housing_project_base_audited.csv

../input/zap_project_data.parquet: ../../stage_zap_datasets/output/zap_project_data.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_project_bbl.parquet: ../../stage_zap_datasets/output/zap_project_bbl.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/dcp_mappluto_current_25v4.parquet: ../../stage_mappluto_lots/output/dcp_mappluto_current_25v4.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/cd_homeownership_1990_measure.csv: ../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/cd_redevelopment_potential_baseline.csv: ../../redevelopment_potential_first_pass/output/cd_redevelopment_potential_baseline.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_outcome_usability_by_period.csv: ../../audits/audit_zap_source_integrity/output/zap_outcome_usability_by_period.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_source_integrity_qc.csv: ../../audits/audit_zap_source_integrity/output/zap_source_integrity_qc.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_project_data.parquet ../input/zap_project_bbl.parquet ../input/dcp_mappluto_current_25v4.parquet ../input/cd_homeownership_1990_measure.csv ../input/cd_redevelopment_potential_baseline.csv ../input/zap_outcome_usability_by_period.csv ../input/zap_source_integrity_qc.csv

include ../../generic.make
