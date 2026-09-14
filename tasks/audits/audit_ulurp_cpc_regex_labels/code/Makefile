HUMAN_REVIEW_SAMPLE_SIZE := 150
HUMAN_REVIEW_SAMPLE_SEED := 20260914
DEVELOPMENT_SAMPLE_SIZE := 100
DEVELOPMENT_SAMPLE_SEED := 20260823
HOLDOUT_SAMPLE_SIZE := 100
HOLDOUT_SAMPLE_SEED := 20260825

all: ../output/ulurp_cpc_regex_training_agreement.csv \
	../output/ulurp_cpc_regex_validation_agreement.csv \
	../output/ulurp_cpc_regex_validation_disagreements.csv \
	../output/ulurp_cpc_regex_holdout_agreement.csv \
	../output/ulurp_cpc_regex_holdout_disagreements.csv \
	../output/ulurp_cpc_regex_coverage.csv \
	../output/ulurp_cpc_regex_coverage.md \
	../output/ulurp_cpc_regex_review_queue.csv \
	../output/ulurp_cpc_regex_human_review_sample.csv

../%/ulurp_cpc_regex_coverage.csv ../%/ulurp_cpc_regex_coverage.md ../%/ulurp_cpc_regex_review_queue.csv ../%/ulurp_cpc_regex_human_review_sample.csv: summarize_regex_coverage.py rule_development_reports.csv Makefile \
	../../../_lib/data_reports.py \
	../input/ulurp_cpc_text_labels.csv \
	../input/ulurp_cpc_report_manifest.csv \
	../input/ulurp_cpc_training_labels_jacob.csv \
	../input/ulurp_cpc_regex_validation_labels_codex.csv \
	../input/ulurp_cpc_regex_holdout_labels_codex.csv | ../output ../report
	python3 $< $(HUMAN_REVIEW_SAMPLE_SIZE) $(HUMAN_REVIEW_SAMPLE_SEED)

../%/ulurp_cpc_regex_training_agreement.csv ../%/ulurp_cpc_regex_validation_agreement.csv ../%/ulurp_cpc_regex_validation_disagreements.csv ../%/ulurp_cpc_regex_holdout_agreement.csv ../%/ulurp_cpc_regex_holdout_disagreements.csv: compare_ulurp_cpc_regex_labels.py Makefile ../../../_lib/data_reports.py \
	../input/ulurp_cpc_text_labels.csv \
	../input/ulurp_cpc_training_labels_jacob.csv \
	../input/ulurp_cpc_regex_validation_labels_codex.csv \
	../input/ulurp_cpc_regex_holdout_labels_codex.csv \
	../input/ulurp_cpc_report_manifest.csv | ../output ../report
	python3 $< $(DEVELOPMENT_SAMPLE_SIZE) $(DEVELOPMENT_SAMPLE_SEED) \
		$(HOLDOUT_SAMPLE_SIZE) $(HOLDOUT_SAMPLE_SEED)

../input/ulurp_cpc_text_labels.csv: ../../../summarize_text_cpc_trends/output/ulurp_cpc_text_labels.csv | ../input
	ln -sf "$<" "$@"

../input/ulurp_cpc_training_labels_jacob.csv: ../../../record_ulurp_cpc_training_labels/output/ulurp_cpc_training_labels_jacob.csv | ../input
	ln -sf "$<" "$@"

../input/ulurp_cpc_regex_validation_labels_codex.csv: ../../record_ulurp_cpc_regex_validation_labels/output/ulurp_cpc_regex_validation_labels_codex.csv | ../input
	ln -sf "$<" "$@"

../input/ulurp_cpc_regex_holdout_labels_codex.csv: ../../record_ulurp_cpc_regex_validation_labels/output/ulurp_cpc_regex_holdout_labels_codex.csv | ../input
	ln -sf "$<" "$@"

../input/ulurp_cpc_report_manifest.csv: ../../../build_ulurp_cpc_report_corpus/output/ulurp_cpc_report_manifest.csv | ../input
	ln -sf "$<" "$@"

link-inputs: ../input/ulurp_cpc_text_labels.csv \
	../input/ulurp_cpc_training_labels_jacob.csv \
	../input/ulurp_cpc_regex_validation_labels_codex.csv \
	../input/ulurp_cpc_regex_holdout_labels_codex.csv \
	../input/ulurp_cpc_report_manifest.csv

include ../../generic.make
