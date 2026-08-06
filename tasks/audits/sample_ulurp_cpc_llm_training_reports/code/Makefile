START_YEAR := 1976
END_YEAR := 2025
COMMON_REPORTS := 100
UNIQUE_REPORTS_PER_CODER := 200
SAMPLE_SEED := cpc-llm-training-v1

all: ../output/cpc_llm_training_labels_jacob.xlsx \
	../output/cpc_llm_training_labels_tyler.xlsx \
	../output/cpc_llm_training_label_guide.tex \
	../output/cpc_llm_training_label_guide.pdf

../output/cpc_llm_training_labels_jacob.xlsx: build_cpc_llm_training_workbooks.mjs \
	../input/official_ulurp_cpc_narrative_manifest.csv \
	../input/ulurp_cpc_initial_event_labels.csv \
	../input/ulurp_cpc_event_validation_labels.csv | ../output ../temp/cpc_llm_training_labels_jacob/
	node $< $(START_YEAR) $(END_YEAR) $(COMMON_REPORTS) $(UNIQUE_REPORTS_PER_CODER) $(SAMPLE_SEED) jacob

../output/cpc_llm_training_labels_tyler.xlsx: build_cpc_llm_training_workbooks.mjs \
	../input/official_ulurp_cpc_narrative_manifest.csv \
	../input/ulurp_cpc_initial_event_labels.csv \
	../input/ulurp_cpc_event_validation_labels.csv | ../output ../temp/cpc_llm_training_labels_tyler/
	node $< $(START_YEAR) $(END_YEAR) $(COMMON_REPORTS) $(UNIQUE_REPORTS_PER_CODER) $(SAMPLE_SEED) tyler

../output/cpc_llm_training_label_guide.pdf: ../output/cpc_llm_training_label_guide.tex | ../temp
	pdflatex -interaction=nonstopmode -halt-on-error -output-directory=../temp $<
	cp ../temp/cpc_llm_training_label_guide.pdf $@

../output/cpc_llm_training_label_guide.tex: build_cpc_llm_training_workbooks.mjs \
	../input/official_ulurp_cpc_narrative_manifest.csv \
	../input/ulurp_cpc_initial_event_labels.csv \
	../input/ulurp_cpc_event_validation_labels.csv | ../output
	node $< $(START_YEAR) $(END_YEAR) $(COMMON_REPORTS) $(UNIQUE_REPORTS_PER_CODER) $(SAMPLE_SEED) guide

../input/official_ulurp_cpc_narrative_manifest.csv: ../../audit_official_ulurp_cpc_text_measurement/output/official_ulurp_cpc_narrative_manifest.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_cpc_initial_event_labels.csv: ../../record_ulurp_cpc_text_analysis_decisions/output/ulurp_cpc_initial_event_labels.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_cpc_event_validation_labels.csv: ../../record_ulurp_cpc_text_analysis_decisions/output/ulurp_cpc_event_validation_labels.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/official_ulurp_cpc_narrative_manifest.csv \
	../input/ulurp_cpc_initial_event_labels.csv \
	../input/ulurp_cpc_event_validation_labels.csv

include ../../generic.make
