SHELL := bash
include ../../shell_functions.make

all: ../output/zap_capacity_weighted_project_bbl.csv ../output/zap_capacity_weighted_cd_year.csv ../output/zap_capacity_weighted_tercile_year.csv ../output/zap_capacity_weighted_tercile_trends_per_10000.pdf ../output/zap_capacity_weighted_tercile_trends_per_residential_acre.pdf ../output/zap_capacity_weighted_tercile_trends_qc.csv

../output/zap_capacity_weighted_tercile_trends_qc.csv: summarize_zap_capacity_weighted_tercile_trends.R ../input/zap_housing_project_base_audited.csv ../input/zap_project_bbl.parquet ../input/dcp_mappluto_current_25v4.parquet ../input/cd_homeownership_1990_measure.csv ../input/cd_redevelopment_potential_baseline.csv ../input/zap_outcome_usability_by_period.csv | ../output
	$(R) $<

../output/zap_capacity_weighted_project_bbl.csv ../output/zap_capacity_weighted_cd_year.csv ../output/zap_capacity_weighted_tercile_year.csv ../output/zap_capacity_weighted_tercile_trends_per_10000.pdf ../output/zap_capacity_weighted_tercile_trends_per_residential_acre.pdf: ../output/zap_capacity_weighted_tercile_trends_qc.csv
	@test -s "$@"

../input/zap_housing_project_base_audited.csv: ../../build_zap_housing_pipeline_from_raw/output/zap_housing_project_base_audited.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_project_bbl.parquet: ../../stage_zap_datasets/output/zap_project_bbl.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/dcp_mappluto_current_25v4.parquet: ../../stage_mappluto_lots/output/dcp_mappluto_current_25v4.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/cd_homeownership_1990_measure.csv: ../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/cd_redevelopment_potential_baseline.csv: ../../redevelopment_potential_first_pass/output/cd_redevelopment_potential_baseline.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_outcome_usability_by_period.csv: ../../audit_zap_source_integrity/output/zap_outcome_usability_by_period.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_housing_project_base_audited.csv ../input/zap_project_bbl.parquet ../input/dcp_mappluto_current_25v4.parquet ../input/cd_homeownership_1990_measure.csv ../input/cd_redevelopment_potential_baseline.csv ../input/zap_outcome_usability_by_period.csv

include ../../generic.make
