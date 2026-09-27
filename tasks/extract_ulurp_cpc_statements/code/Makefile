MODEL := gpt-6-sol
REASONING := high
MAX_PACKET_CHARACTERS := 400000
RUN_ID := full_sol_high_20260927
RUNS := first100_sol_high_20260926 $(RUN_ID)
DOCUMENTS := ../output/ulurp_cpc_statement_run_order.csv
SAMPLE_SEED := cpc-statements-first100-20260926
ORDER_SEED := cpc-statements-full-20260927
WORKERS := 4
TIMEOUT_SECONDS := 3600

all: ../output/ulurp_cpc_statement_run_order.csv \
	$(foreach run,$(RUNS),../output/ulurp_cpc_statements_$(run).csv ../output/ulurp_cpc_statement_status_$(run).csv)

../output/ulurp_cpc_statement_sample.csv: select_statement_sample.py Makefile \
	../input/ulurp_cpc_reading_roster.csv ../input/ulurp_cpc_human_coding.csv | ../output ../report
	python3 $< $(SAMPLE_SEED)

../output/ulurp_cpc_statement_run_order.csv: order_statement_run.py cpc_statement_packets.py Makefile ../output/ulurp_cpc_statement_sample.csv \
	../input/ulurp_cpc_reading_roster.csv ../input/ulurp_cpc_reading_segments.csv | ../output ../report
	python3 $< $(ORDER_SEED) $(MAX_PACKET_CHARACTERS)

../output/ulurp_cpc_statements_%.csv ../output/ulurp_cpc_statement_status_%.csv: build_ulurp_cpc_statements.py cpc_statement_packets.py \
	../../_lib/data_reports.py ../input/ulurp_cpc_reading_roster.csv ../input/ulurp_cpc_reading_segments.csv \
	../../../data_raw/cpc_statement_extraction/%/attempts.jsonl | ../output ../report
	python3 $< $*

# Model calls happen only here, never in `all`.
.PHONY: acquire
acquire: run_statement_extraction.py cpc_statement_packets.py statement_instructions.md statement_schema.json $(DOCUMENTS) \
	../input/ulurp_cpc_reading_roster.csv ../input/ulurp_cpc_reading_segments.csv
	python3 $< $(MODEL) $(REASONING) $(MAX_PACKET_CHARACTERS) $(RUN_ID) $(DOCUMENTS) $(WORKERS) $(TIMEOUT_SECONDS)

# Codex app route: write prompt files for app subagents, then log their answers.
.PHONY: app-prompts app-record
app-prompts: prepare_statement_prompts.py cpc_statement_packets.py statement_instructions.md statement_schema.json whole_report_review.md $(DOCUMENTS) \
	../input/ulurp_cpc_reading_roster.csv ../input/ulurp_cpc_reading_segments.csv
	python3 $< $(RUN_ID) $(MODEL) $(REASONING) $(MAX_PACKET_CHARACTERS) $(DOCUMENTS)

app-record: record_statement_answers.py cpc_statement_packets.py
	python3 $< $(RUN_ID)

../input/ulurp_cpc_reading_roster.csv: ../../build_ulurp_cpc_reading_text/output/ulurp_cpc_reading_roster.csv | ../input
	ln -sf $< $@

../input/ulurp_cpc_reading_segments.csv: ../../build_ulurp_cpc_reading_text/output/ulurp_cpc_reading_segments.csv | ../input
	ln -sf $< $@

../input/ulurp_cpc_human_coding.csv: ../../summarize_text_cpc_trends/output/ulurp_cpc_human_coding.csv | ../input
	ln -sf $< $@

link-inputs: ../input/ulurp_cpc_reading_roster.csv ../input/ulurp_cpc_reading_segments.csv ../input/ulurp_cpc_human_coding.csv

include ../../generic.make
