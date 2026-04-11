SHELL := bash
include ../../shell_functions.make

all: ../output/source_registry_checks.csv

../output/source_registry_checks.csv: validate_source_registry.R source_catalog.csv manual_manifest.csv archive_requests.csv | ../output
	$(R) $< source_catalog.csv manual_manifest.csv archive_requests.csv $@

link-inputs:

include ../../generic.make
