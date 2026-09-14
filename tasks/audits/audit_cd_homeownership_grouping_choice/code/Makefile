START_YEAR := 1970
END_YEAR := 2025
MOVING_WINDOW_YEARS := 3

all: ../output/cd_homeownership_grouping_comparison.csv \
	../output/cd_homeownership_grouping_comparison.pdf

../output/cd_homeownership_grouping_comparison.csv: audit_cd_homeownership_grouping_choice.R \
	../input/cd_homeownership_long_units_series.csv | ../output
	Rscript --vanilla $< $(START_YEAR) $(END_YEAR) $(MOVING_WINDOW_YEARS)

../output/cd_homeownership_grouping_comparison.pdf: ../output/cd_homeownership_grouping_comparison.csv
	@test -s "$@"

../input/cd_homeownership_long_units_series.csv: ../../../build_cd_homeownership_long_units_series/output/cd_homeownership_long_units_series.csv | ../input
	ln -sf "$<" "$@"

link-inputs: ../input/cd_homeownership_long_units_series.csv

include ../../../generic.make
