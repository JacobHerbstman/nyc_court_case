# Pilot ULURP CPC LLM Labels

Tests whether an OpenAI model can reproduce the human CPC-report coding
scheme before any full-corpus labeling. The pilot uses 20 completed
`jacob_only` reports balanced across vote decades. Common reports and Tyler's
unique reports remain outside prompt development.

The request file is deterministic and compatible with the OpenAI Batch API.
The response file preserves the complete API responses and token usage. The
comparison table is long by report and field so disagreements can be reviewed
without opening raw JSON.

The task requires `OPENAI_API_KEY` in the environment or in the ignored
project-root `.env` file. The key is used only for authentication and is never
written to an output.

The original `v1` pilot used a single development-direction field. Its request,
response, and comparison files are retained unchanged. The `v2` prompt splits
literal `zone_change` from the dominant practical `dev_direction` and adds the
materiality rule developed from the first pilot's disagreements. Rerunning the
same 20 reports is a paired prompt-development check, not an independent test.

The default `make` only generates the deterministic `v2` request file and does
not contact the API. After explicit cost approval, requesting
`../output/ulurp_cpc_llm_pilot_comparison_sol_medium_v2.csv` runs Sol with
medium reasoning and writes the complete responses and comparison. A separate
untouched sample is required for the final out-of-sample evaluation.

Outputs:

- `ulurp_cpc_llm_pilot_requests_sol_medium_v1.jsonl`
- `ulurp_cpc_llm_pilot_responses_sol_medium_v1.jsonl`
- `ulurp_cpc_llm_pilot_comparison_sol_medium_v1.csv`
- `ulurp_cpc_llm_pilot_requests_sol_medium_v2.jsonl`
- `ulurp_cpc_llm_pilot_responses_sol_medium_v2.jsonl`, after approval
- `ulurp_cpc_llm_pilot_comparison_sol_medium_v2.csv`, after approval
