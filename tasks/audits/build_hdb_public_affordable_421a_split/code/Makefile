SHELL := bash
include ../../../shell_functions.make

all: ../output/hdb_public_affordable_421a_job_classified.csv ../output/hdb_public_affordable_421a_cd_year.csv ../output/hdb_public_affordable_421a_tercile_trends.pdf ../output/hdb_public_affordable_421a_summary.csv ../output/hdb_public_affordable_421a_qc.csv

../output/hdb_public_affordable_421a_job_classified.csv: build_hdb_public_affordable_421a_split.R ../../../_lib/source_pipeline_utils.R ../input/dcp_housing_database_project_level_25q4.parquet ../input/zap_housing_hdb_link_candidates.csv ../input/zap_housing_project_base_audited.csv ../input/hpd_affordable_housing_bbl_year.csv ../input/dof_421a_exempt_bbl_year.csv ../input/cd_redevelopment_potential_baseline.csv | ../output
	$(R) $<

../output/hdb_public_affordable_421a_cd_year.csv: ../output/hdb_public_affordable_421a_job_classified.csv
../output/hdb_public_affordable_421a_tercile_trends.pdf: ../output/hdb_public_affordable_421a_job_classified.csv
../output/hdb_public_affordable_421a_summary.csv: ../output/hdb_public_affordable_421a_job_classified.csv
../output/hdb_public_affordable_421a_qc.csv: ../output/hdb_public_affordable_421a_job_classified.csv

../input/dcp_housing_database_project_level_25q4.parquet: ../../../stage_dcp_housing_database/output/dcp_housing_database_project_level_25q4.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_housing_hdb_link_candidates.csv: ../../../build_zap_housing_hdb_link/output/zap_housing_hdb_link_candidates.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_housing_project_base_audited.csv: ../../../build_zap_housing_pipeline_from_raw/output/zap_housing_project_base_audited.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/hpd_affordable_housing_bbl_year.csv: ../../stage_hpd_affordable_housing_production/output/hpd_affordable_housing_bbl_year.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/dof_421a_exempt_bbl_year.csv: ../../stage_dof_421a_exempt_properties/output/dof_421a_exempt_bbl_year.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/cd_redevelopment_potential_baseline.csv: ../../redevelopment_potential_first_pass/output/cd_redevelopment_potential_baseline.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/dcp_housing_database_project_level_25q4.parquet ../input/zap_housing_hdb_link_candidates.csv ../input/zap_housing_project_base_audited.csv ../input/hpd_affordable_housing_bbl_year.csv ../input/dof_421a_exempt_bbl_year.csv ../input/cd_redevelopment_potential_baseline.csv

include ../../generic.make
