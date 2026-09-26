BIN_SCHEMES = 5yr_bins decade_bins decade_pre_bins

all: $(foreach scheme,$(BIN_SCHEMES),../output/cd_homeownership_long_units_drivers_coefficients_$(scheme).csv ../output/cd_homeownership_long_units_drivers_leave_one_out_$(scheme).csv ../output/cd_homeownership_long_units_drivers_contributions_$(scheme).csv ../output/cd_homeownership_long_units_drivers_plots_$(scheme).pdf)

../output/cd_homeownership_long_units_drivers_coefficients_%.csv ../output/cd_homeownership_long_units_drivers_leave_one_out_%.csv ../output/cd_homeownership_long_units_drivers_contributions_%.csv ../output/cd_homeownership_long_units_drivers_plots_%.pdf: summarize_cd_homeownership_long_units_drivers.R major_upzonings_2001_2009.csv Makefile ../../../_lib/data_reports.R ../input/cd_homeownership_long_units_series.csv ../input/zap_project_data.parquet ../input/cd_homeownership_long_units_event_coefficients_raw_units_%.csv | ../output ../report
	Rscript --vanilla $< $*

../input/cd_homeownership_long_units_series.csv: ../../../build_cd_homeownership_long_units_series/output/cd_homeownership_long_units_series.csv | ../input
	ln -sf "$<" "$@"

../input/cd_homeownership_long_units_event_coefficients_raw_units_5yr_bins.csv: ../../../estimate_cd_homeownership_long_units_event_study/output/cd_homeownership_long_units_event_coefficients_raw_units_5yr_bins.csv | ../input
	ln -sf "$<" "$@"

../input/cd_homeownership_long_units_event_coefficients_raw_units_decade_bins.csv: ../../../estimate_cd_homeownership_long_units_event_study/output/cd_homeownership_long_units_event_coefficients_raw_units_decade_bins.csv | ../input
	ln -sf "$<" "$@"

../input/cd_homeownership_long_units_event_coefficients_raw_units_decade_pre_bins.csv: ../../../estimate_cd_homeownership_long_units_event_study/output/cd_homeownership_long_units_event_coefficients_raw_units_decade_pre_bins.csv | ../input
	ln -sf "$<" "$@"

../input/zap_project_data.parquet: ../../../build_zap_datasets/output/zap_project_data.parquet | ../input
	ln -sf "$<" "$@"

link-inputs: ../input/cd_homeownership_long_units_series.csv ../input/cd_homeownership_long_units_event_coefficients_raw_units_5yr_bins.csv ../input/cd_homeownership_long_units_event_coefficients_raw_units_decade_bins.csv ../input/cd_homeownership_long_units_event_coefficients_raw_units_decade_pre_bins.csv ../input/zap_project_data.parquet

include ../../generic.make
