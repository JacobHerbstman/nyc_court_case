NARRATIVES_PER_DECADE := 10
CANDIDATE_ZONING_PER_DECADE := 3
CANDIDATE_OTHER_PER_DECADE := 3

all: ../output/official_ulurp_cpc_event_validation_sample.csv

../output/official_ulurp_cpc_event_validation_sample.csv: sample_official_ulurp_cpc_event_validation.py \
	../input/official_ulurp_cpc_narrative_manifest.csv | ../output
	python3 $< $(NARRATIVES_PER_DECADE) $(CANDIDATE_ZONING_PER_DECADE) $(CANDIDATE_OTHER_PER_DECADE)

../input/official_ulurp_cpc_narrative_manifest.csv: ../../audit_official_ulurp_cpc_text_measurement/output/official_ulurp_cpc_narrative_manifest.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/official_ulurp_cpc_narrative_manifest.csv

include ../../generic.make
