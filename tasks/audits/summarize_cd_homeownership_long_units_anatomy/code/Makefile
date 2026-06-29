all: ../output/cd_homeownership_long_units_anatomy_top_cd.csv ../output/cd_homeownership_long_units_anatomy_top_cd_year.csv ../output/cd_homeownership_long_units_anatomy_qc.csv

../output/cd_homeownership_long_units_anatomy_top_cd.csv: summarize_cd_homeownership_long_units_anatomy.R ../input/cd_homeownership_long_units_series.csv | ../output
	Rscript --vanilla $<

../output/cd_homeownership_long_units_anatomy_top_cd_year.csv: ../output/cd_homeownership_long_units_anatomy_top_cd.csv
	@test -s "$@"

../output/cd_homeownership_long_units_anatomy_qc.csv: ../output/cd_homeownership_long_units_anatomy_top_cd.csv
	@test -s "$@"

../input/cd_homeownership_long_units_series.csv: ../../build_cd_homeownership_long_units_series/output/cd_homeownership_long_units_series.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/cd_homeownership_long_units_series.csv

include ../../generic.make
