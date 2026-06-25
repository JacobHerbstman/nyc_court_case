SHELL := bash
include ../../../shell_functions.make

all: ../output/hpd_affordable_housing_production_by_building.csv

../output/hpd_affordable_housing_production_by_building.csv: fetch_hpd_affordable_housing_production.R | ../output
	$(R) $<

include ../../generic.make
