SHELL := bash
include ../../shell_functions.make

all: ../output/zap_housing_pipeline_tercile_year.csv ../output/zap_housing_pipeline_tercile_trends_per_10000.pdf ../output/zap_housing_pipeline_tercile_trends_per_residential_acre.pdf ../output/zap_housing_pipeline_tercile_trends_qc.csv

../input/zap_housing_cd_year_panel_primary.csv: ../../build_zap_housing_pipeline_from_raw/output/zap_housing_cd_year_panel_primary.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/zap_housing_cd_year_panel_bbl_fractional.csv: ../../build_zap_housing_pipeline_from_raw/output/zap_housing_cd_year_panel_bbl_fractional.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../output/zap_housing_pipeline_tercile_trends_qc.csv: summarize_zap_housing_pipeline_tercile_trends.R ../input/zap_housing_cd_year_panel_primary.csv ../input/zap_housing_cd_year_panel_bbl_fractional.csv | ../output
	$(R) $< ../input/zap_housing_cd_year_panel_primary.csv ../input/zap_housing_cd_year_panel_bbl_fractional.csv ../output/zap_housing_pipeline_tercile_year.csv ../output/zap_housing_pipeline_tercile_trends_per_10000.pdf ../output/zap_housing_pipeline_tercile_trends_per_residential_acre.pdf ../output/zap_housing_pipeline_tercile_trends_qc.csv

../output/zap_housing_pipeline_tercile_year.csv: ../output/zap_housing_pipeline_tercile_trends_qc.csv

../output/zap_housing_pipeline_tercile_trends_per_10000.pdf: ../output/zap_housing_pipeline_tercile_trends_qc.csv

../output/zap_housing_pipeline_tercile_trends_per_residential_acre.pdf: ../output/zap_housing_pipeline_tercile_trends_qc.csv

link-inputs: ../input/zap_housing_cd_year_panel_primary.csv ../input/zap_housing_cd_year_panel_bbl_fractional.csv

include ../../generic.make
