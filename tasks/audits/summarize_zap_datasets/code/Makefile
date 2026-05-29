all: ../output/zap_project_counts_by_decade_status.csv ../output/zap_bbl_link_completeness.csv ../output/zap_geography_coverage.csv ../output/zap_summary_qc.csv

../output/zap_summary_qc.csv: summarize_zap_datasets.R ../input/zap_stage_qc.csv ../input/zap_project_data.parquet ../input/zap_project_bbl.parquet | ../output
	Rscript --vanilla $<

../output/zap_project_counts_by_decade_status.csv ../output/zap_bbl_link_completeness.csv ../output/zap_geography_coverage.csv: ../output/zap_summary_qc.csv
	@test -s "$@"

../input/zap_stage_qc.csv: ../../../stage_zap_datasets/output/zap_stage_qc.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_project_data.parquet: ../../../stage_zap_datasets/output/zap_project_data.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_project_bbl.parquet: ../../../stage_zap_datasets/output/zap_project_bbl.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_stage_qc.csv ../input/zap_project_data.parquet ../input/zap_project_bbl.parquet

include ../../generic.make
