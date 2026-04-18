SHELL := bash
include ../../shell_functions.make

all: ../output/zap_project_counts_by_decade_status.csv ../output/zap_bbl_link_completeness.csv ../output/zap_geography_coverage.csv ../output/zap_summary_qc.csv

../input/zap_stage_qc.csv: ../../stage_zap_datasets/output/zap_stage_qc.csv | ../input
	ln -sf "../../stage_zap_datasets/output/zap_stage_qc.csv" "$@"

../input/zap_project_data.parquet: ../../stage_zap_datasets/output/zap_project_data.parquet | ../input
	ln -sf "../../stage_zap_datasets/output/zap_project_data.parquet" "$@"

../input/zap_project_bbl.parquet: ../../stage_zap_datasets/output/zap_project_bbl.parquet | ../input
	ln -sf "../../stage_zap_datasets/output/zap_project_bbl.parquet" "$@"

../output/zap_project_counts_by_decade_status.csv: summarize_zap_datasets.R ../input/zap_stage_qc.csv ../input/zap_project_data.parquet ../input/zap_project_bbl.parquet | ../output
	$(R) $< ../input/zap_stage_qc.csv ../input/zap_project_data.parquet ../input/zap_project_bbl.parquet ../output/zap_project_counts_by_decade_status.csv ../output/zap_bbl_link_completeness.csv ../output/zap_geography_coverage.csv ../output/zap_summary_qc.csv

../output/zap_bbl_link_completeness.csv: ../output/zap_project_counts_by_decade_status.csv

../output/zap_geography_coverage.csv: ../output/zap_project_counts_by_decade_status.csv

../output/zap_summary_qc.csv: ../output/zap_project_counts_by_decade_status.csv

link-inputs: ../input/zap_stage_qc.csv ../input/zap_project_data.parquet ../input/zap_project_bbl.parquet

include ../../generic.make
