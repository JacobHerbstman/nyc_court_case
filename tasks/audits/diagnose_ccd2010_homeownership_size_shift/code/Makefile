SHELL := bash

all: ../output/ccdist2010_homeownership_size_shift_annual.csv \
	../output/ccdist2010_homeownership_size_shift_period_counts.csv \
	../output/ccdist2010_homeownership_size_shift_relative_declines.csv \
	../output/ccdist2010_homeownership_size_shift_period_shares.csv \
	../output/ccdist2010_homeownership_size_shift_composition.csv \
	../output/ccdist2010_homeownership_size_shift_decomposition.csv \
	../output/ccdist2010_homeownership_size_shift_concentration.csv \
	../output/ccdist2010_homeownership_size_shift_cd_reconciliation.csv \
	../output/ccdist2010_homeownership_size_shift_plots.pdf \
	../output/ccdist2010_homeownership_size_shift_qc.csv

../output/ccdist2010_homeownership_size_shift_annual.csv: diagnose_ccd2010_homeownership_size_shift.R ../../../_lib/source_pipeline_utils.R ../input/ccdist2010_mappluto_construction_proxy_district_year.csv ../input/ccdist2010_homeownership_1990_measure.csv ../input/cd_homeownership_long_units_tercile_year.csv | ../output
	$(R) $<

../output/ccdist2010_homeownership_size_shift_period_counts.csv \
../output/ccdist2010_homeownership_size_shift_relative_declines.csv \
../output/ccdist2010_homeownership_size_shift_period_shares.csv \
../output/ccdist2010_homeownership_size_shift_composition.csv \
../output/ccdist2010_homeownership_size_shift_decomposition.csv \
../output/ccdist2010_homeownership_size_shift_concentration.csv \
../output/ccdist2010_homeownership_size_shift_cd_reconciliation.csv \
../output/ccdist2010_homeownership_size_shift_plots.pdf \
../output/ccdist2010_homeownership_size_shift_qc.csv: ../output/ccdist2010_homeownership_size_shift_annual.csv
	@test -s "$@"

../input/ccdist2010_mappluto_construction_proxy_district_year.csv: ../../../build_ccd2010_mappluto_construction_proxy/output/ccdist2010_mappluto_construction_proxy_district_year.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ccdist2010_homeownership_1990_measure.csv: ../../../build_ccd2010_homeownership_1990_measure/output/ccdist2010_homeownership_1990_measure.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/cd_homeownership_long_units_tercile_year.csv: ../../summarize_cd_homeownership_long_units_series/output/cd_homeownership_long_units_tercile_year.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/ccdist2010_mappluto_construction_proxy_district_year.csv ../input/ccdist2010_homeownership_1990_measure.csv ../input/cd_homeownership_long_units_tercile_year.csv

include ../../generic.make
