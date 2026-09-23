# Extract ULURP CPC Statements

Draft, September 22. Will extract one row per statement or action from each CPC
narrative's reading text (`build_ulurp_cpc_reading_text`). Report-level topic
and actor measures are derived from these rows downstream, so definitions can
change without rereading reports.

- `code/statement_instructions.md`: the reader instruction.
- `code/statement_schema.json`: the required output format; every response is
  validated against it, and every quote must appear exactly in a cited segment.

Not yet decided: the model and API, and the runner. The Makefile arrives with
the runner. Raw responses will be archived under `data_raw/` and treated as
fixed after the single full run.
