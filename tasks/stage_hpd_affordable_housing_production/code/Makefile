SHELL := bash
include ../../shell_functions.make

all: ../output/hpd_affordable_housing_bbl_year.csv

../output/hpd_affordable_housing_bbl_year.csv: stage_hpd_affordable_housing_production.R ../input/hpd_affordable_housing_production_by_building.csv | ../output
	$(R) $<

../input/hpd_affordable_housing_production_by_building.csv: ../../fetch_hpd_affordable_housing_production/output/hpd_affordable_housing_production_by_building.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/hpd_affordable_housing_production_by_building.csv

include ../../generic.make
