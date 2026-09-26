MODEL := gpt-6-sol
REASONING := high
MAX_PACKET_CHARACTERS := 400000
RUN_ID := pilot_app_20260926
DOCUMENTS := pilot_documents.txt
WORKERS := 2
TIMEOUT_SECONDS := 3600

all: ../output/ulurp_cpc_statements.csv ../output/ulurp_cpc_statement_status.csv

../out%/ulurp_cpc_statements.csv ../out%/ulurp_cpc_statement_status.csv: build_ulurp_cpc_statements.py cpc_statement_packets.py statement_schema.json Makefile \
	../../_lib/data_reports.py ../input/ulurp_cpc_reading_roster.csv ../input/ulurp_cpc_reading_segments.csv \
	../../../data_raw/cpc_statement_extraction/$(RUN_ID)/attempts.jsonl | ../output ../report
	python3 $< $(RUN_ID)

# Model calls happen only here, never in `all`.
.PHONY: acquire
acquire: run_statement_extraction.py cpc_statement_packets.py statement_instructions.md statement_schema.json $(DOCUMENTS) \
	../input/ulurp_cpc_reading_roster.csv ../input/ulurp_cpc_reading_segments.csv
	python3 $< $(MODEL) $(REASONING) $(MAX_PACKET_CHARACTERS) $(RUN_ID) $(DOCUMENTS) $(WORKERS) $(TIMEOUT_SECONDS)

# Codex app route: write prompt files for app subagents, then log their answers.
.PHONY: app-prompts app-record
app-prompts: prepare_statement_prompts.py cpc_statement_packets.py statement_instructions.md statement_schema.json $(DOCUMENTS) \
	../input/ulurp_cpc_reading_roster.csv ../input/ulurp_cpc_reading_segments.csv
	python3 $< $(RUN_ID) $(MODEL) $(REASONING) $(MAX_PACKET_CHARACTERS) $(DOCUMENTS)

app-record: record_statement_answers.py cpc_statement_packets.py
	python3 $< $(RUN_ID)

../input/ulurp_cpc_reading_roster.csv: ../../build_ulurp_cpc_reading_text/output/ulurp_cpc_reading_roster.csv | ../input
	ln -sf $< $@

../input/ulurp_cpc_reading_segments.csv: ../../build_ulurp_cpc_reading_text/output/ulurp_cpc_reading_segments.csv | ../input
	ln -sf $< $@

link-inputs: ../input/ulurp_cpc_reading_roster.csv ../input/ulurp_cpc_reading_segments.csv

include ../../generic.make
