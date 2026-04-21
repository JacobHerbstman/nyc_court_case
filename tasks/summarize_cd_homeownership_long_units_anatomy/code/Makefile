SHELL := bash
include ../../shell_functions.make

all: ../output/cd_homeownership_long_units_anatomy_top_cd.csv ../output/cd_homeownership_long_units_anatomy_top_cd_year.csv ../output/cd_homeownership_long_units_anatomy_qc.csv

../input/cd_homeownership_long_units_series.csv: ../../build_cd_homeownership_long_units_series/output/cd_homeownership_long_units_series.csv | ../input
	ln -sf "$<" "$@"

../output/cd_homeownership_long_units_anatomy_top_cd.csv: summarize_cd_homeownership_long_units_anatomy.R ../input/cd_homeownership_long_units_series.csv | ../output
	$(R) $< ../input/cd_homeownership_long_units_series.csv ../output/cd_homeownership_long_units_anatomy_top_cd.csv ../output/cd_homeownership_long_units_anatomy_top_cd_year.csv ../output/cd_homeownership_long_units_anatomy_qc.csv

../output/cd_homeownership_long_units_anatomy_top_cd_year.csv: ../output/cd_homeownership_long_units_anatomy_top_cd.csv

../output/cd_homeownership_long_units_anatomy_qc.csv: ../output/cd_homeownership_long_units_anatomy_top_cd.csv

link-inputs: ../input/cd_homeownership_long_units_series.csv

include ../../generic.make
