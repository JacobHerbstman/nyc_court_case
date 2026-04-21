SHELL := bash
include ../../shell_functions.make

all: ../output/zap_housing_hdb_link_candidates.csv ../output/zap_housing_hdb_project_summary.csv ../output/zap_housing_hdb_link_qc.csv

../input/zap_housing_cohort_base.csv: ../../build_zap_housing_cohort_base/output/zap_housing_cohort_base.csv | ../input
	ln -sf "$<" "$@"

../input/zap_project_bbl.parquet: ../../stage_zap_datasets/output/zap_project_bbl.parquet | ../input
	ln -sf "$<" "$@"

../input/dcp_housing_database_project_level_25q4.parquet: ../../stage_dcp_housing_database/output/dcp_housing_database_project_level_25q4.parquet | ../input
	ln -sf "$<" "$@"

../output/zap_housing_hdb_link_candidates.csv: build_zap_housing_hdb_link.R ../input/zap_housing_cohort_base.csv ../input/zap_project_bbl.parquet ../input/dcp_housing_database_project_level_25q4.parquet | ../output
	$(R) $< ../input/zap_housing_cohort_base.csv ../input/zap_project_bbl.parquet ../input/dcp_housing_database_project_level_25q4.parquet ../output/zap_housing_hdb_link_candidates.csv ../output/zap_housing_hdb_project_summary.csv ../output/zap_housing_hdb_link_qc.csv

../output/zap_housing_hdb_project_summary.csv: ../output/zap_housing_hdb_link_candidates.csv

../output/zap_housing_hdb_link_qc.csv: ../output/zap_housing_hdb_link_candidates.csv

link-inputs: ../input/zap_housing_cohort_base.csv ../input/zap_project_bbl.parquet ../input/dcp_housing_database_project_level_25q4.parquet

include ../../generic.make
