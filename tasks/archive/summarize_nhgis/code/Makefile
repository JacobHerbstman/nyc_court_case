SHELL := bash
include ../../shell_functions.make

all: ../output/nhgis_audit_summary.csv ../output/nhgis_variable_quality.csv ../output/nhgis_homeowner_share_extremes.csv ../output/nhgis_county_counts.csv ../output/nhgis_suspicious_tracts.csv ../output/nhgis_1980_unresolved_centroids.csv ../output/nhgis_1990_unresolved_centroids.csv ../output/nhgis_1990_group_quarters_review.csv ../output/nhgis_figures.pdf ../output/nhgis_maps.pdf

../input/nhgis_files.csv: ../../stage_nhgis/output/nhgis_files.csv | ../input
	ln -sf "$<" "$@"

../input/nhgis_qc.csv: ../../stage_nhgis/output/nhgis_qc.csv | ../input
	ln -sf "$<" "$@"

../input/nhgis_1980_tract_extract.parquet: ../../stage_nhgis/output/nhgis_1980_tract_extract.parquet | ../input
	ln -sf "$<" "$@"

../input/nhgis_1990_tract_extract.parquet: ../../stage_nhgis/output/nhgis_1990_tract_extract.parquet | ../input
	ln -sf "$<" "$@"

../input/nhgis_1980_reconciliation.csv: ../../stage_nhgis/output/nhgis_1980_reconciliation.csv | ../input
	ln -sf "$<" "$@"

../output/nhgis_audit_summary.csv: summarize_nhgis.R ../input/nhgis_files.csv ../input/nhgis_qc.csv ../input/nhgis_1980_tract_extract.parquet ../input/nhgis_1990_tract_extract.parquet ../input/nhgis_1980_reconciliation.csv | ../output
	$(R) $< ../input/nhgis_files.csv ../input/nhgis_qc.csv ../input/nhgis_1980_tract_extract.parquet ../input/nhgis_1990_tract_extract.parquet ../input/nhgis_1980_reconciliation.csv ../output/nhgis_audit_summary.csv ../output/nhgis_variable_quality.csv ../output/nhgis_homeowner_share_extremes.csv ../output/nhgis_county_counts.csv ../output/nhgis_suspicious_tracts.csv ../output/nhgis_1980_unresolved_centroids.csv ../output/nhgis_1990_unresolved_centroids.csv ../output/nhgis_1990_group_quarters_review.csv ../output/nhgis_figures.pdf ../output/nhgis_maps.pdf

../output/nhgis_variable_quality.csv: ../output/nhgis_audit_summary.csv

../output/nhgis_homeowner_share_extremes.csv: ../output/nhgis_audit_summary.csv

../output/nhgis_county_counts.csv: ../output/nhgis_audit_summary.csv

../output/nhgis_suspicious_tracts.csv: ../output/nhgis_audit_summary.csv

../output/nhgis_1980_unresolved_centroids.csv: ../output/nhgis_audit_summary.csv

../output/nhgis_1990_unresolved_centroids.csv: ../output/nhgis_audit_summary.csv

../output/nhgis_1990_group_quarters_review.csv: ../output/nhgis_audit_summary.csv

../output/nhgis_figures.pdf: ../output/nhgis_audit_summary.csv

../output/nhgis_maps.pdf: ../output/nhgis_audit_summary.csv

link-inputs: ../input/nhgis_files.csv ../input/nhgis_qc.csv ../input/nhgis_1980_tract_extract.parquet ../input/nhgis_1990_tract_extract.parquet ../input/nhgis_1980_reconciliation.csv

include ../../generic.make
