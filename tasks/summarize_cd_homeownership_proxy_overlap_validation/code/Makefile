R := Rscript

all: ../output/cd_homeownership_proxy_overlap_tercile_year.csv ../output/cd_homeownership_proxy_overlap_borough_year.csv ../output/cd_homeownership_proxy_overlap_cd_year_residual.csv ../output/cd_homeownership_proxy_overlap_metrics.csv ../output/cd_homeownership_proxy_overlap_qc.csv ../output/cd_homeownership_proxy_overlap_plots.pdf

../input/cd_homeownership_1990_measure.csv: ../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv | ../input
	ln -sf "../../build_cd_homeownership_1990_measure/output/cd_homeownership_1990_measure.csv" "$@"

../input/mappluto_construction_proxy_cd_year.csv: ../../build_mappluto_construction_proxy/output/mappluto_construction_proxy_cd_year.csv | ../input
	ln -sf "../../build_mappluto_construction_proxy/output/mappluto_construction_proxy_cd_year.csv" "$@"

../input/dcp_housing_database_files.csv: ../../stage_dcp_housing_database/output/dcp_housing_database_files.csv | ../input
	ln -sf "../../stage_dcp_housing_database/output/dcp_housing_database_files.csv" "$@"

../output/cd_homeownership_proxy_overlap_tercile_year.csv ../output/cd_homeownership_proxy_overlap_borough_year.csv ../output/cd_homeownership_proxy_overlap_cd_year_residual.csv ../output/cd_homeownership_proxy_overlap_metrics.csv ../output/cd_homeownership_proxy_overlap_qc.csv ../output/cd_homeownership_proxy_overlap_plots.pdf: summarize_cd_homeownership_proxy_overlap_validation.R ../input/cd_homeownership_1990_measure.csv ../input/mappluto_construction_proxy_cd_year.csv ../input/dcp_housing_database_files.csv | ../output
	$(R) $< ../input/cd_homeownership_1990_measure.csv ../input/mappluto_construction_proxy_cd_year.csv ../input/dcp_housing_database_files.csv ../output/cd_homeownership_proxy_overlap_tercile_year.csv ../output/cd_homeownership_proxy_overlap_borough_year.csv ../output/cd_homeownership_proxy_overlap_cd_year_residual.csv ../output/cd_homeownership_proxy_overlap_metrics.csv ../output/cd_homeownership_proxy_overlap_qc.csv ../output/cd_homeownership_proxy_overlap_plots.pdf

../input ../output:
	mkdir -p $@

link-inputs: ../input/cd_homeownership_1990_measure.csv ../input/mappluto_construction_proxy_cd_year.csv ../input/dcp_housing_database_files.csv
