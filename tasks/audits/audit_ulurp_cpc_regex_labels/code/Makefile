DEVELOPMENT_SAMPLE_SIZE := 100
DEVELOPMENT_SAMPLE_SEED := 20260823
HOLDOUT_SAMPLE_SIZE := 100
HOLDOUT_SAMPLE_SEED := 20260825

all: ../output/ulurp_cpc_regex_training_agreement.csv \
	../output/ulurp_cpc_regex_validation_agreement.csv \
	../output/ulurp_cpc_regex_validation_disagreements.csv \
	../output/ulurp_cpc_regex_holdout_agreement.csv \
	../output/ulurp_cpc_regex_holdout_disagreements.csv

../output/ulurp_cpc_regex_training_agreement.csv: compare_ulurp_cpc_regex_labels.py \
	../input/ulurp_cpc_text_labels.csv \
	../input/ulurp_cpc_training_labels_jacob.csv \
	../input/ulurp_cpc_regex_validation_labels_codex.csv \
	../input/ulurp_cpc_regex_holdout_labels_codex.csv \
	../input/ulurp_cpc_report_manifest.csv | ../output
	python3 $< $(DEVELOPMENT_SAMPLE_SIZE) $(DEVELOPMENT_SAMPLE_SEED) \
		$(HOLDOUT_SAMPLE_SIZE) $(HOLDOUT_SAMPLE_SEED)

../output/ulurp_cpc_regex_validation_agreement.csv \
../output/ulurp_cpc_regex_validation_disagreements.csv \
../output/ulurp_cpc_regex_holdout_agreement.csv \
../output/ulurp_cpc_regex_holdout_disagreements.csv: ../output/ulurp_cpc_regex_training_agreement.csv
	@test -s "$@"

../input/ulurp_cpc_text_labels.csv: ../../../summarize_text_cpc_trends/output/ulurp_cpc_text_labels.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_cpc_training_labels_jacob.csv: ../../../record_ulurp_cpc_training_labels/output/ulurp_cpc_training_labels_jacob.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_cpc_regex_validation_labels_codex.csv: ../../record_ulurp_cpc_regex_validation_labels/output/ulurp_cpc_regex_validation_labels_codex.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_cpc_regex_holdout_labels_codex.csv: ../../record_ulurp_cpc_regex_validation_labels/output/ulurp_cpc_regex_holdout_labels_codex.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_cpc_report_manifest.csv: ../../../build_ulurp_cpc_report_corpus/output/ulurp_cpc_report_manifest.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/ulurp_cpc_text_labels.csv \
	../input/ulurp_cpc_training_labels_jacob.csv \
	../input/ulurp_cpc_regex_validation_labels_codex.csv \
	../input/ulurp_cpc_regex_holdout_labels_codex.csv \
	../input/ulurp_cpc_report_manifest.csv

include ../../generic.make
