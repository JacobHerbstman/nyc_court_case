CLUSTER_MIN_PROJECTS := 10

all: ../output/zap_project_coverage.csv ../output/zap_status_coverage.csv ../output/zap_dates_by_year.csv ../output/zap_date_clusters.csv ../output/zap_snapshot_changes.csv ../output/zap_recovered_actions.csv ../output/zap_universe_findings.md

../output/zap_project_coverage.csv: audit_zap_universe_coverage.py ../../../_lib/data_reports.py ../input/zap_project_universe.csv ../input/zap_missing_project_details.jsonl ../input/previous_zap_project_data.csv ../input/ulurp_corpus_application_spine.csv ../input/ulurp_cpc_report_manifest.csv Makefile | ../output ../report
	python3 $< $(CLUSTER_MIN_PROJECTS) > audit_zap_universe_coverage.log 2>&1

../output/zap_status_coverage.csv ../output/zap_dates_by_year.csv ../output/zap_date_clusters.csv ../output/zap_snapshot_changes.csv ../output/zap_recovered_actions.csv ../output/zap_universe_findings.md: ../output/zap_project_coverage.csv
	@test -f "$@" || python3 audit_zap_universe_coverage.py $(CLUSTER_MIN_PROJECTS) > audit_zap_universe_coverage.log 2>&1

../input/zap_project_universe.csv: ../../../build_zap_project_universe/output/zap_project_universe.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_missing_project_details.jsonl: ../../../fetch_zap_missing_project_details/output/zap_missing_project_details.jsonl | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/previous_zap_project_data.csv: ../../../../data_raw/dcp_zap_project_data/20260501/zap_project_data.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_corpus_application_spine.csv: ../../../build_ulurp_corpus_spine/output/ulurp_corpus_application_spine.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_cpc_report_manifest.csv: ../../../build_ulurp_cpc_report_corpus/output/ulurp_cpc_report_manifest.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_project_universe.csv ../input/zap_missing_project_details.jsonl ../input/previous_zap_project_data.csv ../input/ulurp_corpus_application_spine.csv ../input/ulurp_cpc_report_manifest.csv

../report:
	mkdir -p $@

include ../../generic.make
