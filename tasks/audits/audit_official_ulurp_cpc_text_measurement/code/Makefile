START_YEAR := 1975
END_YEAR := 2025
DOCUMENTS_PER_DECADE := 100

all: ../output/official_ulurp_cpc_text_measurement_summary.csv \
	../output/official_ulurp_cpc_narrative_manifest.csv \
	../output/official_ulurp_cpc_narrative_boundary_sample.csv

../output/official_ulurp_cpc_narrative_manifest.csv: audit_official_ulurp_cpc_text_measurement.py \
	../input/official_ulurp_cpc_report_manifest.csv \
	../input/official_ulurp_cpc_narrative_boundary_exceptions.csv | ../output
	python3 $< $(START_YEAR) $(END_YEAR) $(DOCUMENTS_PER_DECADE)

../output/official_ulurp_cpc_text_measurement_summary.csv: ../output/official_ulurp_cpc_narrative_manifest.csv
	@test -s "$@"

../output/official_ulurp_cpc_narrative_boundary_sample.csv: ../output/official_ulurp_cpc_narrative_manifest.csv
	@test -s "$@"

../input/official_ulurp_cpc_report_manifest.csv: ../../../build_ulurp_cpc_report_corpus/output/ulurp_cpc_report_manifest.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/official_ulurp_cpc_narrative_boundary_exceptions.csv: ../../../record_ulurp_cpc_source_corrections/output/ulurp_cpc_narrative_boundary_exceptions.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/official_ulurp_cpc_report_manifest.csv \
	../input/official_ulurp_cpc_narrative_boundary_exceptions.csv

include ../../generic.make
