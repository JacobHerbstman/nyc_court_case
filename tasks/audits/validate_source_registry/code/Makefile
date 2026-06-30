SHELL := bash
include ../../../shell_functions.make

all: ../output/source_registry_checks.csv

../output/source_registry_checks.csv: validate_source_registry.R \
	../../../source_registry/code/source_catalog.csv \
	../../source_registry_context/code/manual_manifest.csv \
	../../source_registry_context/code/archive_requests.csv | ../output
	$(R) $<

link-inputs:

include ../../generic.make
