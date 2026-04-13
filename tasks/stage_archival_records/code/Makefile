SHELL := bash
include ../../shell_functions.make

all: ../output/archival_record_inventory.csv ../output/archival_record_inventory_qc.csv

../input/source_catalog.csv: ../../source_registry/code/source_catalog.csv | ../input
	ln -sf "$<" "$@"

../input/archive_requests.csv: ../../source_registry/code/archive_requests.csv | ../input
	ln -sf "$<" "$@"

../input/archival_record_raw_files.csv: ../../load_archival_records_raw/output/archival_record_raw_files.csv | ../input
	ln -sf "../../load_archival_records_raw/output/archival_record_raw_files.csv" "$@"

../output/archival_record_inventory.csv: stage_archival_records.R ../input/source_catalog.csv ../input/archive_requests.csv ../input/archival_record_raw_files.csv | ../output
	$(R) $< ../input/source_catalog.csv ../input/archive_requests.csv ../input/archival_record_raw_files.csv ../output/archival_record_inventory.csv ../output/archival_record_inventory_qc.csv

../output/archival_record_inventory_qc.csv: ../output/archival_record_inventory.csv

link-inputs: ../input/source_catalog.csv ../input/archive_requests.csv ../input/archival_record_raw_files.csv

include ../../generic.make
