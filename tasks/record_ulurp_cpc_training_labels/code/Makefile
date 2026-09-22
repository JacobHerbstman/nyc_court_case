all: ../output/ulurp_cpc_training_labels_jacob.csv ../output/ulurp_cpc_training_labels_tyler.csv \
	../output/ulurp_cpc_coding_adjudications.csv

../output/ulurp_cpc_training_labels_tyler.csv: import_tyler_coding.R \
	../input/cpc_llm_training_labels_tyler.xlsx ../../_lib/data_reports.R Makefile | ../output ../report
	$(R) $<

../input/cpc_llm_training_labels_tyler.xlsx: ../../../data_raw/cpc_human_coding/20260915/cpc_llm_training_labels_tyler.xlsx | ../input
	ln -sf $< $@

link-inputs: ../input/cpc_llm_training_labels_tyler.xlsx

include ../../generic.make
