SHELL := bash
include ../../shell_functions.make

all: ../output/archival_record_inventory.csv

../input/source_catalog.csv: ../../source_registry/code/source_catalog.csv | ../input
	ln -sf "$<" "$@"

../input/archive_requests.csv: ../../source_registry/code/archive_requests.csv | ../input
	ln -sf "$<" "$@"

../output/archival_record_inventory.csv: stage_archival_records.R ../input/source_catalog.csv ../input/archive_requests.csv | ../output
	$(R) $< ../input/source_catalog.csv ../input/archive_requests.csv $@

link-inputs: ../input/source_catalog.csv ../input/archive_requests.csv

include ../../generic.make
