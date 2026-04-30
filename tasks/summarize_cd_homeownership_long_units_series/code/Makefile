R := Rscript --vanilla

all: ../output/cd_homeownership_long_units_city_year.csv ../output/cd_homeownership_long_units_tercile_year.csv ../output/cd_homeownership_long_units_tercile_era.csv ../output/cd_homeownership_long_units_summary_qc.csv ../output/cd_homeownership_long_units_plots.pdf

../input/cd_homeownership_long_units_series.csv: ../../build_cd_homeownership_long_units_series/output/cd_homeownership_long_units_series.csv | ../input
	ln -sf "../../build_cd_homeownership_long_units_series/output/cd_homeownership_long_units_series.csv" "$@"

../output/cd_homeownership_long_units_city_year.csv ../output/cd_homeownership_long_units_tercile_year.csv ../output/cd_homeownership_long_units_tercile_era.csv ../output/cd_homeownership_long_units_summary_qc.csv ../output/cd_homeownership_long_units_plots.pdf: summarize_cd_homeownership_long_units_series.R ../input/cd_homeownership_long_units_series.csv | ../output
	$(R) $< ../input/cd_homeownership_long_units_series.csv ../output/cd_homeownership_long_units_city_year.csv ../output/cd_homeownership_long_units_tercile_year.csv ../output/cd_homeownership_long_units_tercile_era.csv ../output/cd_homeownership_long_units_summary_qc.csv ../output/cd_homeownership_long_units_plots.pdf

link-inputs: ../input/cd_homeownership_long_units_series.csv
