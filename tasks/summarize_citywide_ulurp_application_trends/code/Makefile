SHELL := bash
include ../../shell_functions.make

all: ../output/citywide_ulurp_application_trends_qc.csv \
	../output/citywide_ulurp_application_year.csv \
	../output/citywide_ulurp_application_tercile_year.csv \
	../output/citywide_ulurp_demographic_tercile_year.csv \
	../output/citywide_ulurp_application_period_summary.csv \
	../output/citywide_ulurp_application_source_coverage.csv \
	../output/citywide_ulurp_residential_flag_examples.csv \
	../output/citywide_ulurp_negative_amenity_summary.csv \
	../output/citywide_ulurp_negative_amenity_examples.csv \
	../output/citywide_ulurp_residential_manual_validation_sample.csv \
	../output/citywide_ulurp_residential_manual_validation_summary.csv \
	../output/citywide_ulurp_application_tercile_trends.pdf \
	../output/citywide_ulurp_housing_facility_tercile_trends.pdf \
	../output/citywide_ulurp_demographic_tercile_trends.pdf \
	../output/citywide_ulurp_application_trends.pdf

../output/citywide_ulurp_application_trends_qc.csv: summarize_citywide_ulurp_application_trends.R ../../_lib/source_pipeline_utils.R ../input/zap_project_data.parquet ../input/cd_baseline_1990_controls.csv ../input/citywide_ulurp_residential_manual_validation_labels.csv | ../output
	$(R) $<

../output/citywide_ulurp_application_year.csv ../output/citywide_ulurp_application_tercile_year.csv ../output/citywide_ulurp_demographic_tercile_year.csv ../output/citywide_ulurp_application_period_summary.csv ../output/citywide_ulurp_application_source_coverage.csv ../output/citywide_ulurp_residential_flag_examples.csv ../output/citywide_ulurp_negative_amenity_summary.csv ../output/citywide_ulurp_negative_amenity_examples.csv ../output/citywide_ulurp_residential_manual_validation_sample.csv ../output/citywide_ulurp_residential_manual_validation_summary.csv ../output/citywide_ulurp_application_tercile_trends.pdf ../output/citywide_ulurp_housing_facility_tercile_trends.pdf ../output/citywide_ulurp_demographic_tercile_trends.pdf ../output/citywide_ulurp_application_trends.pdf: ../output/citywide_ulurp_application_trends_qc.csv
	@test -s "$@"

../input/zap_project_data.parquet: ../../stage_zap_datasets/output/zap_project_data.parquet | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/cd_baseline_1990_controls.csv: ../../build_cd_baseline_1990_controls/output/cd_baseline_1990_controls.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/zap_project_data.parquet ../input/cd_baseline_1990_controls.csv

include ../../generic.make
