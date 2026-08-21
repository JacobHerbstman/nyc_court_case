all: ../output/ulurp_cpc_regex_training_agreement.csv

../output/ulurp_cpc_regex_training_agreement.csv: compare_ulurp_cpc_regex_labels.py \
	../input/ulurp_cpc_text_labels.csv \
	../input/ulurp_cpc_training_labels_jacob.csv | ../output
	python3 $<

../input/ulurp_cpc_text_labels.csv: ../../../summarize_text_cpc_trends/output/ulurp_cpc_text_labels.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

../input/ulurp_cpc_training_labels_jacob.csv: ../../../record_ulurp_cpc_training_labels/output/ulurp_cpc_training_labels_jacob.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/ulurp_cpc_text_labels.csv ../input/ulurp_cpc_training_labels_jacob.csv

include ../../generic.make
