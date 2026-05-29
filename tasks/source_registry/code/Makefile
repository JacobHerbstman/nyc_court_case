all: ../output/source_registry_checks.csv

../output/source_registry_checks.csv: validate_source_registry.R source_catalog.csv manual_manifest.csv archive_requests.csv | ../output
	Rscript --vanilla $<

link-inputs:

include ../../generic.make
