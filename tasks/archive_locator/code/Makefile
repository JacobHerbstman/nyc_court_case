SHELL := bash
include ../../shell_functions.make

all: ../output/archive_locator_queue.csv

../input/source_catalog.csv: ../../source_registry/code/source_catalog.csv | ../input
	ln -sf "$<" "$@"

../input/archive_requests.csv: ../../source_registry/code/archive_requests.csv | ../input
	ln -sf "$<" "$@"

../output/archive_locator_queue.csv: archive_locator.R ../input/source_catalog.csv ../input/archive_requests.csv | ../output
	$(R) $< ../input/source_catalog.csv ../input/archive_requests.csv $@

link-inputs: ../input/source_catalog.csv ../input/archive_requests.csv

include ../../generic.make
