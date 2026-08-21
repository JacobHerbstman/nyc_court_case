SHELL := bash
include ../../../shell_functions.make

all: ../output/dcp_cd_profiles_1990_2000_files.csv

../output/dcp_cd_profiles_1990_2000_files.csv: build_dcp_cd_profiles_1990_2000.R ../output/dcp_cd_profiles_1990_2000_raw_files.csv | ../output
	$(R) $<

../output/dcp_cd_profiles_1990_2000_raw_files.csv: load_dcp_cd_profiles_1990_2000_raw.py ../output/dcp_cd_profiles_1990_2000_source_files.csv | ../output
	python3 $<

../output/dcp_cd_profiles_1990_2000_source_files.csv: fetch_dcp_cd_profiles_1990_2000.R | ../output
	$(R) $<

link-inputs:

include ../../generic.make
