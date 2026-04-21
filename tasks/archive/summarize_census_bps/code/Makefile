SHELL := bash
include ../../shell_functions.make

all: ../output/census_bps_audit_summary.csv ../output/census_bps_variable_quality.csv ../output/census_bps_anomalies.csv ../output/census_bps_benchmark_rows.csv ../output/census_bps_figures.pdf

../input/census_bps_index.csv: ../../stage_census_bps/output/census_bps_index.csv | ../input
	ln -sf "../../stage_census_bps/output/census_bps_index.csv" "$@"

../input/census_bps_qc.csv: ../../stage_census_bps/output/census_bps_qc.csv | ../input
	ln -sf "../../stage_census_bps/output/census_bps_qc.csv" "$@"

../input/census_bps_borough_year.parquet: ../../stage_census_bps/output/census_bps_borough_year.parquet | ../input
	ln -sf "../../stage_census_bps/output/census_bps_borough_year.parquet" "$@"

../input/census_bps_city_year.parquet: ../../stage_census_bps/output/census_bps_city_year.parquet | ../input
	ln -sf "../../stage_census_bps/output/census_bps_city_year.parquet" "$@"

../output/census_bps_audit_summary.csv: summarize_census_bps.R ../input/census_bps_index.csv ../input/census_bps_qc.csv ../input/census_bps_borough_year.parquet ../input/census_bps_city_year.parquet | ../output
	$(R) $< ../input/census_bps_index.csv ../input/census_bps_qc.csv ../input/census_bps_borough_year.parquet ../input/census_bps_city_year.parquet ../output/census_bps_audit_summary.csv ../output/census_bps_variable_quality.csv ../output/census_bps_anomalies.csv ../output/census_bps_benchmark_rows.csv ../output/census_bps_figures.pdf

../output/census_bps_variable_quality.csv: ../output/census_bps_audit_summary.csv

../output/census_bps_anomalies.csv: ../output/census_bps_audit_summary.csv

../output/census_bps_benchmark_rows.csv: ../output/census_bps_audit_summary.csv

../output/census_bps_figures.pdf: ../output/census_bps_audit_summary.csv

link-inputs: ../input/census_bps_index.csv ../input/census_bps_qc.csv ../input/census_bps_borough_year.parquet ../input/census_bps_city_year.parquet

include ../../generic.make
