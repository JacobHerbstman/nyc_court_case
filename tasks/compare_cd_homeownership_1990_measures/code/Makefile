SHELL := bash
include ../../shell_functions.make

all: ../output/cd_homeownership_1990_measure_comparison.csv ../output/cd_homeownership_1990_measure_comparison_summary.csv ../output/cd_homeownership_1990_measure_top_gap_review.csv

../input/dcp_cd_homeownership_1990.csv: ../../summarize_dcp_cd_homeownership_1990/output/dcp_cd_homeownership_1990.csv | ../input
	ln -sf "$<" "$@"

../input/nhgis_cd_homeownership_1990.csv: ../../summarize_cd_homeownership_1990/output/nhgis_cd_homeownership_1990.csv | ../input
	ln -sf "$<" "$@"

../output/cd_homeownership_1990_measure_comparison.csv: compare_cd_homeownership_1990_measures.R ../input/dcp_cd_homeownership_1990.csv ../input/nhgis_cd_homeownership_1990.csv | ../output
	$(R) $< ../input/dcp_cd_homeownership_1990.csv ../input/nhgis_cd_homeownership_1990.csv ../output/cd_homeownership_1990_measure_comparison.csv ../output/cd_homeownership_1990_measure_comparison_summary.csv ../output/cd_homeownership_1990_measure_top_gap_review.csv

../output/cd_homeownership_1990_measure_comparison_summary.csv: ../output/cd_homeownership_1990_measure_comparison.csv

../output/cd_homeownership_1990_measure_top_gap_review.csv: ../output/cd_homeownership_1990_measure_comparison.csv

link-inputs: ../input/dcp_cd_homeownership_1990.csv ../input/nhgis_cd_homeownership_1990.csv

include ../../generic.make
