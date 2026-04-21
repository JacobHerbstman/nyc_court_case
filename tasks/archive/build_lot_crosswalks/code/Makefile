SHELL := bash
include ../../shell_functions.make

all: ../output/lot_identifier_crosswalk.csv ../output/lot_identifier_crosswalk_qc.csv

../input/mappluto_lot_files.csv: ../../stage_mappluto_lots/output/mappluto_lot_files.csv | ../input
	ln -sf "../../stage_mappluto_lots/output/mappluto_lot_files.csv" "$@"

../input/dob_open_data_files.csv: ../../stage_dob_open_data/output/dob_open_data_files.csv | ../input
	ln -sf "../../stage_dob_open_data/output/dob_open_data_files.csv" "$@"

../output/lot_identifier_crosswalk.csv: build_lot_crosswalks.R ../input/mappluto_lot_files.csv ../input/dob_open_data_files.csv | ../output
	$(R) $< ../input/mappluto_lot_files.csv ../input/dob_open_data_files.csv ../output/lot_identifier_crosswalk.csv ../output/lot_identifier_crosswalk_qc.csv

../output/lot_identifier_crosswalk_qc.csv: ../output/lot_identifier_crosswalk.csv

link-inputs: ../input/mappluto_lot_files.csv ../input/dob_open_data_files.csv

include ../../generic.make
