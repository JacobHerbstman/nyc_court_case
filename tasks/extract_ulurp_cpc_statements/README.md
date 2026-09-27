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
  `output/ulurp_cpc_statements_<RUN_ID>.csv` (one row per statement) and
  `output/ulurp_cpc_statement_status_<RUN_ID>.csv` (one row per roster narrative:
  `complete`, `in_progress`, `failed`, `needs_cross_part_review` or `not_run`). A
  split report awaiting unattempted parts is `in_progress`; an attempted part
  without a valid answer is `failed`. The app route
  leaves token counts blank because the subagent interface does not supply them.
  Complete means the required readings passed structural and quotation checks;
  it is not an accuracy certification.
  `selected_for_run` identifies the 100-report denominator within the full roster.
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

## First 100, September 26

Jacob authorized GPT-6 Sol at **high** reasoning, with a checkpoint after 100
reports before expanding. The checkpoint is resolved; see the full run below. The existing
`pilot_app_20260926` prompt-only run is unchanged and has no submitted answers.

`select_statement_sample.py` selects four known attachment-repair cases and
sixteen further human-coded controls, plus eighty fresh reports. Six fresh
reports are deliberately long (two need split packets); the remaining 74 are
drawn in seeded order across decade and unresolved-page cells. This is a stress
sample, not a population-weighted accuracy sample. The saved sample contains
100 distinct narratives, 54 with unresolved pages, and 103 reading packets.
No human labels are supplied to readers. The source is the repaired reading
text prepared by `build_ulurp_cpc_reading_text`.

Run `make app-prompts` to prepare the frozen prompts, schema, instructions and
sample in `data_raw/cpc_statement_extraction/first100_sol_high_20260926/`.
App readers receive disjoint document IDs and write draft answers outside the
response directory. After validating a draft, they publish its response file
once and leave it unchanged. Corrections use a new attempt number. The parent
records completed answers with `make app-record`, then runs `make`. Logged
answers have a content hash so later edits cannot silently replace them.
`assignments.jsonl` records dispatched readers; consult active agents and saved
answers before resuming to avoid duplicate work.

For a split report, first read every numbered part. A Sol high reader then
follows the frozen `whole_report_review.md`, reads the whole bundle and its
first-pass rows, and resolves repetitions and request/response links across
parts. It publishes one combined answer as `<document_id>_part0_attempt1.json`,
with globally unique statement IDs and every original segment included. The
recorder validates it against all source segments. Until that review is valid,
the report has `needs_cross_part_review` status and contributes no final rows.
The original part answers remain preserved.

A continuation every ten minutes resumes these 100 reports using up to ten
Sol high readers, subject to the active session's concurrency limit. Jacob
requested the higher concurrency after the initial three-reader run; the
project Codex configuration requests ten for newly loaded sessions. It stops
at this checkpoint. It never calls Jev,
the paid API or `codex exec`, and does not redeem usage credits. Usage limits
leave completed work saved for resumption. This schedule is operational;
Make and the frozen run files remain the data provenance.

After all 100 finish, audit 20 against their complete source text, including
the four known attachment cases, both split bundles and fresh reports spanning
decades and unresolved-page status. Freeze that audit selection before opening
the statement answers for substantive review. Audit readers first list the
material source statements without seeing the extraction, then compare the
saved answers. Report missed material statements, unsupported statements,
application and actor errors, topic errors, request/adoption errors, and lost
changes in position across stages. Report both statement-level counts and the
number of affected reports. Human coding is an imperfect overlapping reference,
not the truth for these finer fields. The independent Sol source review is an
AI-assisted audit, not a new set of human gold-standard labels. Preserve raw answers during the audit,
report unresolved judgments, and do not expand the corpus automatically.

The audit IDs are already fixed by
`tasks/audits/audit_ulurp_cpc_statements/output/cpc_statement_audit_sample.csv`,
using only the frozen sample and prompt manifest. A copy is preserved as
`audit_sample.csv` inside this run. It contains eight human-coded reports and
twelve fresh reports; eleven have unresolved pages. Use this selection when
the hundred are finished, rather than choosing reports from observed errors.

## Full run, September 27

After the audit, report-level measures built from the first-100 rows
(`tasks/audits/summarize_cpc_statement_report_measures`) barely moved when the
audit's corrections were applied, so Jacob approved scaling up. One change
came first: a `procedural_action` field (study, monitoring or reporting, task
force, outreach or consultation) for requests, commitments, requirements,
modifications and decisions. The earlier schema could not support the
codebook's procedural-response measure. All reports, including the first 100,
are read once under this version, as run `full_sol_high_20260927`.

Each run folder keeps its own copy of the instructions and schema. The build
validates a run against that copy and writes tables named by run, so
`first100_sol_high_20260926` still rebuilds unchanged. `order_statement_run.py`
fixes the reading order in `output/ulurp_cpc_statement_run_order.csv`:
- the first 100;
- then the other 8,872 single-packet reports in seeded random order, so any
  stopping point is a random sample;
- then the 91 reports that need split packets and a whole-report review.

`codex exec` cannot run GPT-6 Sol on the ChatGPT plan with the installed CLI
(0.144.6); the server rejects the model. The run therefore uses the Codex app
route, like the first 100. `make app-prompts` has written 9,187 prompt files to
`data_raw/cpc_statement_extraction/full_sol_high_20260927/prompts/`, in
`prompts.csv` order.

To read them from the Codex app:
1. Readers take prompts in `prompts.csv` order and write each answer once to
   `responses/<answer_file>`.
2. For split reports, a reader then follows `whole_report_review.md` and
   publishes `<document_id>_part0_attempt1.json`.
3. Run `make app-record` from `code/` to validate and log new answers, then
   `make` to build the tables.
4. A failed validation gets a new attempt number; saved answers are never edited.
