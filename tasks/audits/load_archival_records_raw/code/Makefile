SHELL := bash
include ../../../shell_functions.make

all: ../output/archival_record_raw_files.csv

../output/archival_record_raw_files.csv: load_archival_records_raw.R ../input/source_catalog.csv ../input/archive_requests.csv | ../output
	$(R) $<

../input/source_catalog.csv: ../../../source_registry/code/source_catalog.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/archive_requests.csv: ../../../source_registry/code/archive_requests.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/source_catalog.csv ../input/archive_requests.csv

include ../../generic.make
