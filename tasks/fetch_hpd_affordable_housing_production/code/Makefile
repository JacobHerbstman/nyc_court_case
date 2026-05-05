SHELL := bash
include ../../shell_functions.make

all: ../output/hpd_affordable_housing_production_by_building.csv ../output/hpd_affordable_housing_production_by_building_metadata.json ../output/hpd_affordable_housing_production_fetch_qc.csv

../output/hpd_affordable_housing_production_by_building.csv: fetch_hpd_affordable_housing_production.R | ../output
	$(R) $< ../output/hpd_affordable_housing_production_by_building.csv ../output/hpd_affordable_housing_production_by_building_metadata.json ../output/hpd_affordable_housing_production_fetch_qc.csv

../output/hpd_affordable_housing_production_by_building_metadata.json: ../output/hpd_affordable_housing_production_by_building.csv
../output/hpd_affordable_housing_production_fetch_qc.csv: ../output/hpd_affordable_housing_production_by_building.csv

link-inputs:

include ../../generic.make
