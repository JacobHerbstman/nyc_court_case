SHELL := bash

all: ../output/zap_source_field_coverage_by_era.csv ../output/zap_action_code_recovery_by_era.csv ../output/zap_housing_flag_overlap.csv ../output/zap_project_manual_review_sample.csv ../output/zap_geography_assignment_audit.csv ../output/zap_outcome_usability_by_period.csv ../output/zap_source_integrity_qc.csv

../output/zap_source_integrity_qc.csv: audit_zap_source_integrity.R ../../../_lib/source_pipeline_utils.R ../input/zap_project_data.parquet ../input/zap_project_bbl.parquet ../input/dcp_mappluto_current_25v4.parquet ../input/cd_homeownership_1990_measure.csv ../input/zap_columns_metadata.csv ../input/source_catalog.csv | ../output
	$(R) $<

../output/zap_source_field_coverage_by_era.csv: ../output/zap_source_integrity_qc.csv

../output/zap_action_code_recovery_by_era.csv: ../output/zap_source_integrity_qc.csv

../output/zap_housing_flag_overlap.csv: ../output/zap_source_integrity_qc.csv

../output/zap_project_manual_review_sample.csv: ../output/zap_source_integrity_qc.csv

../output/zap_geography_assignment_audit.csv: ../output/zap_source_integrity_qc.csv

../output/zap_outcome_usability_by_period.csv: ../output/zap_source_integrity_qc.csv

../input/zap_project_data.parquet: ../../../build_zap_datasets/output/zap_project_data.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_project_bbl.parquet: ../../../build_zap_datasets/output/zap_project_bbl.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/dcp_mappluto_current_25v4.parquet: ../../../build_mappluto_lots/output/dcp_mappluto_current_25v4.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/cd_homeownership_1990_measure.csv: ../../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_columns_metadata.csv: ../../../load_zap_raw/output/zap_columns_metadata.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/source_catalog.csv: ../../../source_registry/code/source_catalog.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_project_data.parquet ../input/zap_project_bbl.parquet ../input/dcp_mappluto_current_25v4.parquet ../input/cd_homeownership_1990_measure.csv ../input/zap_columns_metadata.csv ../input/source_catalog.csv

include ../../generic.make
