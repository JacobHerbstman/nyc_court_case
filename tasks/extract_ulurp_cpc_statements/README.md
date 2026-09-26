# Extract ULURP CPC Statements

Extracts one row per statement or action from each CPC narrative's reading text
(`build_ulurp_cpc_reading_text`). Report-level topic and actor measures are
derived from these rows downstream, so definitions can change without rereading
reports.

- `code/statement_instructions.md`: the reader instruction.
- `code/statement_schema.json`: the required answer format.
- `code/run_statement_extraction.py`: sends each narrative to the model with
  `codex exec` on Jacob's ChatGPT plan (it refuses to run unless Codex is logged
  in with ChatGPT, and strips API keys from its environment). The whole report
  goes in the prompt; narratives over `MAX_PACKET_CHARACTERS` are split at
  segment boundaries. Every attempt is logged, and every answer is saved
  unedited to `data_raw/cpc_statement_extraction/<RUN_ID>/`. A part with a valid
  answer is never sent again; invalid answers and timeouts get one retry; a
  usage limit or Codex error stops the run, and rerunning resumes.
- `code/build_ulurp_cpc_statements.py`: validates the saved answers and writes
  `output/ulurp_cpc_statements.csv` (one row per statement) and
  `output/ulurp_cpc_statement_status.csv` (one row per roster narrative:
  `complete`, `failed` or `not_run`, with token counts).
- `code/cpc_statement_packets.py`: packet rendering and answer validation
  shared by both scripts. A valid answer passes the schema, cites only supplied
  segments, lists all of them as read, and every quote appears in a cited segment.

`make acquire` sends the narratives in `$(DOCUMENTS)` for `$(RUN_ID)`; it is the
only target that calls the model. `make` builds the tables from saved answers.

Codex app route (readers are app subagents, not `codex exec`):
`make app-prompts` writes one prompt file per narrative part to
`data_raw/cpc_statement_extraction/<RUN_ID>/prompts/` with `prompts.csv`, and
records the intended model and reasoning in `run.json`. Readers write answers to
`responses/<document_id>_part<k>_attempt<a>.json`. `make app-record`
(`record_statement_answers.py`) validates new answer files and appends them to
`attempts.jsonl`; token counts are blank on this route. `make` then builds the
tables exactly as for the script route.
