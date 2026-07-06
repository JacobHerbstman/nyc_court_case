PROMPT_VERSION = v1
REVISION_BATCH_SIZE = 25
REVISION_MAX_REPORTS = 0
DECADE_SAMPLE_PER_DECADE = 40
MAX_REVISION_CHARS = 12000
MAX_DECADE_SNIPPET_CHARS = 1800

all: ../output/ulurp_cpc_llm_exploration_frame.csv \
	../output/ulurp_cpc_revision_narrative_prompt.md \
	../output/ulurp_cpc_revision_narrative_batches.jsonl \
	../output/ulurp_cpc_revision_narrative_next_batch.md \
	../output/ulurp_cpc_decade_comparison_sample.csv \
	../output/ulurp_cpc_decade_comparison_prompt.md \
	../output/ulurp_cpc_decade_comparison_batches.jsonl

../output/ulurp_cpc_llm_exploration_frame.csv: build_ulurp_cpc_llm_exploration.py ../input/ulurp_cpc_report_manifest.csv | ../output
	python3 $< $(PROMPT_VERSION) $(REVISION_BATCH_SIZE) $(REVISION_MAX_REPORTS) $(DECADE_SAMPLE_PER_DECADE) $(MAX_REVISION_CHARS) $(MAX_DECADE_SNIPPET_CHARS)

../output/ulurp_cpc_revision_narrative_prompt.md \
../output/ulurp_cpc_revision_narrative_batches.jsonl \
../output/ulurp_cpc_revision_narrative_next_batch.md \
../output/ulurp_cpc_decade_comparison_sample.csv \
../output/ulurp_cpc_decade_comparison_prompt.md \
../output/ulurp_cpc_decade_comparison_batches.jsonl: ../output/ulurp_cpc_llm_exploration_frame.csv
	@test -s "$@"

../input/ulurp_cpc_report_manifest.csv: ../../../build_ulurp_cpc_report_corpus/output/ulurp_cpc_report_manifest.csv | ../input
	@test "$$(readlink "$@")" = "$<" || ln -sf "$<" "$@"

link-inputs: ../input/ulurp_cpc_report_manifest.csv

include ../../generic.make
