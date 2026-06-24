SHELL := bash
include ../../../shell_functions.make

all: ../output/council_zap_land_use_volume_comparison_qc.csv \
	../output/council_zap_land_use_volume_comparison_year.csv \
	../output/council_zap_land_use_volume_comparison_period_summary.csv \
	../output/council_zap_land_use_volume_raw_facets.pdf \
	../output/council_zap_land_use_volume_indexed.pdf

../output/council_zap_land_use_volume_comparison_qc.csv: compare_council_zap_land_use_volume.R \
	../../../_lib/source_pipeline_utils.R \
	../input/council_land_use_decision_trends_year.csv \
	../input/citywide_ulurp_application_year.csv | ../output
	$(R) $<

../output/council_zap_land_use_volume_comparison_year.csv: ../output/council_zap_land_use_volume_comparison_qc.csv
	@test -s "$@"

../output/council_zap_land_use_volume_comparison_period_summary.csv: ../output/council_zap_land_use_volume_comparison_qc.csv
	@test -s "$@"

../output/council_zap_land_use_volume_raw_facets.pdf: ../output/council_zap_land_use_volume_comparison_qc.csv
	@test -s "$@"

../output/council_zap_land_use_volume_indexed.pdf: ../output/council_zap_land_use_volume_comparison_qc.csv
	@test -s "$@"

../input/council_land_use_decision_trends_year.csv: ../../summarize_council_land_use_decision_trend_variants/output/council_land_use_decision_trends_year.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/citywide_ulurp_application_year.csv: ../../../summarize_citywide_ulurp_application_trends/output/citywide_ulurp_application_year.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/council_land_use_decision_trends_year.csv \
	../input/citywide_ulurp_application_year.csv

include ../../../generic.make
