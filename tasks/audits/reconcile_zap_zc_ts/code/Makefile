all: ../output/zap_zc_ts_recreated_year.csv \
	../output/zap_zc_ts_reconciliation_year.csv \
	../output/zap_zc_ts_reconciliation_summary.csv \
	../output/zap_zc_ts_council_match_summary.csv \
	../output/zap_zc_ts_recreated.pdf \
	../output/zap_zc_ts_reconciliation.pdf

../output/zap_zc_ts_recreated_year.csv: reconcile_zap_zc_ts.R ../input/zap_project_data.parquet ../input/council_land_use_decision_panel.csv | ../output
	Rscript --vanilla $<

../output/zap_zc_ts_reconciliation_year.csv: ../output/zap_zc_ts_recreated_year.csv
	@test -s "$@"

../output/zap_zc_ts_reconciliation_summary.csv: ../output/zap_zc_ts_recreated_year.csv
	@test -s "$@"

../output/zap_zc_ts_council_match_summary.csv: ../output/zap_zc_ts_recreated_year.csv
	@test -s "$@"

../output/zap_zc_ts_recreated.pdf: ../output/zap_zc_ts_recreated_year.csv
	@test -s "$@"

../output/zap_zc_ts_reconciliation.pdf: ../output/zap_zc_ts_recreated_year.csv
	@test -s "$@"

../input/zap_project_data.parquet: ../../../stage_zap_datasets/output/zap_project_data.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/council_land_use_decision_panel.csv: ../../../build_council_land_use_decision_panel/output/council_land_use_decision_panel.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_project_data.parquet ../input/council_land_use_decision_panel.csv

include ../../generic.make
