START_YEAR := 1975
END_YEAR := 2025

all: ../output/official_cpc_report_index.csv

../output/official_cpc_report_index.csv: fetch_ulurp_cpc_report_index.py ../input/source_catalog.csv | ../output
	python3 $< $(START_YEAR) $(END_YEAR)

../input/source_catalog.csv: ../../source_registry/output/source_catalog.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/source_catalog.csv

include ../../generic.make
