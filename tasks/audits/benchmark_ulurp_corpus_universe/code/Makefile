SHELL := bash

all: ../output/ulurp_corpus_universe_benchmark_summary.csv \
	../output/ulurp_corpus_universe_benchmark_qc.csv

../output/ulurp_corpus_universe_benchmark_summary.csv: benchmark_ulurp_corpus_universe.R \
	../input/zap_project_data.parquet \
	../input/ulurp_corpus_project_spine.csv \
	../input/ulurp_corpus_application_spine.csv \
	../input/ulurp_cpc_report_manifest.csv \
	../input/ulurp_cpc_report_fetch_failures.csv \
	../input/council_zap_land_use_volume_comparison_qc.csv \
	../input/council_zap_land_use_volume_comparison_period_summary.csv | ../output
	Rscript --vanilla $<
	touch $@

../output/ulurp_corpus_universe_benchmark_qc.csv: ../output/ulurp_corpus_universe_benchmark_summary.csv
	@test -s "$@"
	touch $@

../input/zap_project_data.parquet: ../../../build_zap_datasets/output/zap_project_data.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_corpus_project_spine.csv: ../../build_ulurp_corpus_spine/output/ulurp_corpus_project_spine.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_corpus_application_spine.csv: ../../build_ulurp_corpus_spine/output/ulurp_corpus_application_spine.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_cpc_report_manifest.csv: ../../build_ulurp_cpc_report_corpus/output/ulurp_cpc_report_manifest.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_cpc_report_fetch_failures.csv: ../../build_ulurp_cpc_report_corpus/output/ulurp_cpc_report_fetch_failures.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/council_zap_land_use_volume_comparison_qc.csv: ../../compare_council_zap_land_use_volume/output/council_zap_land_use_volume_comparison_qc.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/council_zap_land_use_volume_comparison_period_summary.csv: ../../compare_council_zap_land_use_volume/output/council_zap_land_use_volume_comparison_period_summary.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_project_data.parquet \
	../input/ulurp_corpus_project_spine.csv \
	../input/ulurp_corpus_application_spine.csv \
	../input/ulurp_cpc_report_manifest.csv \
	../input/ulurp_cpc_report_fetch_failures.csv \
	../input/council_zap_land_use_volume_comparison_qc.csv \
	../input/council_zap_land_use_volume_comparison_period_summary.csv

include ../../generic.make
