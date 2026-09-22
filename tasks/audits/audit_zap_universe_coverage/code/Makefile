CLUSTER_MIN_PROJECTS := 10
START_YEAR := 1975
END_YEAR := 2025
PERIOD_STARTS := 1975,1990,2002,2014
MA_YEARS := 3

all: ../output/zap_project_coverage.csv ../output/zap_status_coverage.csv \
	../output/zap_dates_by_year.csv ../output/zap_date_clusters.csv \
	../output/zap_snapshot_changes.csv ../output/zap_recovered_actions.csv \
	../output/zap_universe_findings.md ../output/zap_withdrawals_over_time.pdf \
	../output/zap_withdrawals_over_time.png ../output/zap_withdrawal_project_terciles.csv \
	../output/zap_withdrawal_tercile_year.csv ../output/zap_withdrawal_tercile_period.csv \
	../output/zap_withdrawals_by_homeowner_tercile.pdf ../output/zap_withdrawals_by_homeowner_tercile.png \
	../output/zap_withdrawal_action_year.csv ../output/zap_withdrawal_action_period.csv \
	../output/zap_withdrawal_action_findings.md ../output/zap_withdrawals_by_action.pdf \
	../output/zap_withdrawals_by_action.png ../output/zap_withdrawal_action_tercile_year.csv \
	../output/zap_withdrawal_action_tercile_period.csv \
	../output/zap_withdrawals_by_action_homeowner_tercile.pdf \
	../output/zap_withdrawals_by_action_homeowner_tercile.png \
	../output/zap_withdrawals_by_action_homeowner_tercile_ma.pdf \
	../output/zap_withdrawals_by_action_homeowner_tercile_ma.png

../output/zap_withdrawals_by_action_homeowner_tercile_ma.png: ../output/zap_withdrawals_by_action_homeowner_tercile_ma.pdf
	pdftoppm -scale-to 1800 -png -singlefile $< ../output/zap_withdrawals_by_action_homeowner_tercile_ma

../output/zap_withdrawals_by_action_homeowner_tercile.png: ../output/zap_withdrawals_by_action_homeowner_tercile.pdf
	pdftoppm -scale-to 1800 -png -singlefile $< ../output/zap_withdrawals_by_action_homeowner_tercile

../output/zap_withdrawals_by_action.png: ../output/zap_withdrawals_by_action.pdf
	pdftoppm -scale-to 1800 -png -singlefile $< ../output/zap_withdrawals_by_action

../%/zap_withdrawal_action_year.csv ../%/zap_withdrawal_action_period.csv \
../%/zap_withdrawal_action_findings.md ../%/zap_withdrawals_by_action.pdf \
../%/zap_withdrawal_action_tercile_year.csv ../%/zap_withdrawal_action_tercile_period.csv \
../%/zap_withdrawals_by_action_homeowner_tercile.pdf \
../%/zap_withdrawals_by_action_homeowner_tercile_ma.pdf: plot_zap_withdrawal_actions.R \
	../../../_lib/data_reports.R ../output/zap_project_coverage.csv \
	../output/zap_withdrawal_project_terciles.csv Makefile | ../output ../report
	$(R) $< $(START_YEAR) $(END_YEAR) $(PERIOD_STARTS) $(MA_YEARS)

../output/zap_withdrawals_by_homeowner_tercile.png: ../output/zap_withdrawals_by_homeowner_tercile.pdf
	pdftoppm -scale-to 1800 -png -singlefile $< ../output/zap_withdrawals_by_homeowner_tercile

../%/zap_withdrawal_project_terciles.csv ../%/zap_withdrawal_tercile_year.csv ../%/zap_withdrawal_tercile_period.csv ../%/zap_withdrawals_by_homeowner_tercile.pdf: plot_zap_withdrawal_terciles.R ../../../_lib/data_reports.R ../input/zap_project_universe.csv ../input/cd_homeownership_1990_measure.csv Makefile | ../output ../report
	$(R) $< $(START_YEAR) $(END_YEAR)

../output/zap_withdrawals_over_time.png: ../output/zap_withdrawals_over_time.pdf
	pdftoppm -scale-to 1800 -png -singlefile $< ../output/zap_withdrawals_over_time

../output/zap_withdrawals_over_time.pdf: plot_zap_withdrawals.R ../output/zap_dates_by_year.csv ../output/zap_date_clusters.csv Makefile | ../output
	$(R) $< $(START_YEAR) $(END_YEAR)

../%/zap_project_coverage.csv ../%/zap_status_coverage.csv ../%/zap_dates_by_year.csv ../%/zap_date_clusters.csv ../%/zap_snapshot_changes.csv ../%/zap_recovered_actions.csv ../%/zap_universe_findings.md: audit_zap_universe_coverage.py ../../../_lib/data_reports.py ../input/zap_project_universe.csv ../input/zap_missing_project_details.jsonl ../input/previous_zap_project_data.csv ../input/ulurp_corpus_application_spine.csv ../input/ulurp_cpc_report_manifest.csv Makefile | ../output ../report
	python3 $< $(CLUSTER_MIN_PROJECTS) > audit_zap_universe_coverage.log 2>&1

../input/cd_homeownership_1990_measure.csv: ../../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	ln -sf $< $@

../input/zap_project_universe.csv: ../../../build_zap_project_universe/output/zap_project_universe.csv | ../input
	ln -sf "$<" "$@"

../input/zap_missing_project_details.jsonl: ../../../fetch_zap_missing_project_details/output/zap_missing_project_details.jsonl | ../input
	ln -sf "$<" "$@"

../input/previous_zap_project_data.csv: ../../../../data_raw/dcp_zap_project_data/20260501/zap_project_data.csv | ../input
	ln -sf "$<" "$@"

../input/ulurp_corpus_application_spine.csv: ../../../build_ulurp_corpus_spine/output/ulurp_corpus_application_spine.csv | ../input
	ln -sf "$<" "$@"

../input/ulurp_cpc_report_manifest.csv: ../../../build_ulurp_cpc_report_corpus/output/ulurp_cpc_report_manifest.csv | ../input
	ln -sf "$<" "$@"

link-inputs: ../input/zap_project_universe.csv ../input/zap_missing_project_details.jsonl ../input/previous_zap_project_data.csv ../input/ulurp_corpus_application_spine.csv ../input/ulurp_cpc_report_manifest.csv ../input/cd_homeownership_1990_measure.csv

include ../../generic.make
