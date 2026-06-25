SHELL := bash
include ../../shell_functions.make

all: ../output/zap_housing_hdb_link_candidates.csv ../output/zap_housing_hdb_project_summary.csv

../output/zap_housing_hdb_link_candidates.csv: build_zap_housing_hdb_link.R ../../_lib/source_pipeline_utils.R ../input/zap_housing_cohort_base.csv ../input/zap_project_bbl.parquet ../input/dcp_housing_database_project_level_25q4.parquet | ../output
	$(R) $<

../output/zap_housing_hdb_project_summary.csv: ../output/zap_housing_hdb_link_candidates.csv

../input/zap_housing_cohort_base.csv: ../../build_zap_housing_cohort_base/output/zap_housing_cohort_base.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_project_bbl.parquet: ../../build_zap_datasets/output/zap_project_bbl.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/dcp_housing_database_project_level_25q4.parquet: ../../build_dcp_housing_database/output/dcp_housing_database_project_level_25q4.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_housing_cohort_base.csv ../input/zap_project_bbl.parquet ../input/dcp_housing_database_project_level_25q4.parquet

include ../../generic.make
