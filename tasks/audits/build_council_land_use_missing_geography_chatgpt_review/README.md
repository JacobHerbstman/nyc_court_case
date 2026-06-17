# Council Land-Use Missing Geography ChatGPT Review

This audit task prepares ChatGPT-assisted first-pass geography review for final-action Council land-use roll-call signatures that lack affected Council districts in the strict decision panel.

The task does not treat ChatGPT output as final data. It creates a reproducible queue, batches, response ledger, validation output, candidate repair file, and human verification queue. Suggested districts should be promoted upstream only after source review.

## Inputs

- `../input/council_land_use_missing_geography_roll_call_repair_queue.csv`
  - Produced by `tasks/audits/investigate_council_land_use_split_votes/`.
  - One row per missing-geography roll-call signature.

## Main Outputs

- `../output/council_land_use_missing_geography_chatgpt_review_frame.csv`
  - Full review frame with stable `signature_review_id` values.
- `../output/batches/council_land_use_missing_geography_chatgpt_review_batch_*.md`
  - First-pass prompt batches for ChatGPT.
- `../output/batches/council_land_use_missing_geography_second_pass_batch_*.md`
  - Second-pass prompt batches for ChatGPT. These review every first-pass classification.
- `../output/batches/council_land_use_missing_geography_adjudication_batch_*.md`
  - Third-pass prompt batches for stricter AI-assisted adjudication. These review every second-pass classification and ask whether official evidence is strong enough to spot-check for manual entry.
- `code/chatgpt_geography_review_responses.jsonl`
  - Append-only raw first-pass ChatGPT response ledger.
- `code/chatgpt_geography_second_pass_responses.jsonl`
  - Append-only raw second-pass ChatGPT response ledger.
- `code/chatgpt_geography_adjudication_responses.jsonl`
  - Append-only raw adjudication-pass ChatGPT response ledger.
- `code/council_land_use_missing_geography_manual_verdicts.csv`
  - Human verification ledger. This is the only place to promote an AI suggestion into a verified geography repair.
- `../output/council_land_use_missing_geography_chatgpt_review_responses_combined.csv`
  - Parsed and validated ChatGPT responses.
- `../output/council_land_use_missing_geography_chatgpt_manual_review_queue.csv`
  - Original queue plus ChatGPT suggestions for human review.
- `../output/council_land_use_missing_geography_ai_repair_candidates.csv`
  - All ChatGPT suggestions with explicit review categories and recommended next actions.
- `../output/council_land_use_missing_geography_human_verification_queue.csv`
  - Rows that can plausibly be repaired but still need human source verification.
- `../output/council_land_use_missing_geography_second_pass_ai_repair_candidates.csv`
  - Second-pass classifications, ordered so the 17 first-pass official candidates and 73 first-pass human-verification rows come before unresolved and citywide/not-project challenge rows.
- `../output/council_land_use_missing_geography_second_pass_human_verification_queue.csv`
  - Rows that still need source verification after the second pass.
- `../output/council_land_use_missing_geography_adjudication_ai_repair_candidates.csv`
  - Third-pass classifications over all 161 rows, combining the original queue, first-pass claims, second-pass claims, and stricter adjudication result.
- `../output/council_land_use_missing_geography_adjudication_spot_check_queue.csv`
  - Highest-priority rows for researcher inspection after the adjudication pass: accepted project-geography candidates, rejected prior-geography claims, or response issues.
- `../output/council_land_use_missing_geography_human_verified_repairs.csv`
  - Human-verified project-geography repairs from the manual verdict ledger.
- `../output/council_land_use_missing_geography_manual_verdict_errors.csv`
  - Validation errors in the manual verdict ledger.
- `../output/council_land_use_missing_geography_browser_chatgpt_workflow.md`
  - Browser/ChatGPT operating procedure for future batches.
- `../output/council_land_use_missing_geography_chatgpt_review_qc.csv`
  - Counts of signatures, batches, missing responses, and validation errors.

## First-Pass Review Workflow

1. Run `make` from this task's `code/` folder.
2. Open the next batch, starting with:
   - `../output/council_land_use_missing_geography_chatgpt_review_next_batch.md`
3. Submit the batch to ChatGPT.
4. Append ChatGPT's JSONL response to:
   - `code/chatgpt_geography_review_responses.jsonl`
5. Rerun `make`.
6. Read:
   - `../output/council_land_use_missing_geography_chatgpt_review_qc.csv`
   - `../output/council_land_use_missing_geography_chatgpt_manual_review_queue.csv`
   - `../output/council_land_use_missing_geography_ai_repair_candidates.csv`
7. Verify cited sources for promising rows.
8. Enter final source-checked decisions in:
   - `code/council_land_use_missing_geography_manual_verdicts.csv`
9. Rerun `make` and use:
   - `../output/council_land_use_missing_geography_human_verified_repairs.csv`

Valid ChatGPT rows must use the controlled vocabulary in the prompt. Rows that fail parsing or validation appear in `../output/council_land_use_missing_geography_chatgpt_review_response_errors.csv`.

## Second-Pass Review Workflow

1. Run `make` from this task's `code/` folder.
2. Open the next second-pass batch:
   - `../output/council_land_use_missing_geography_second_pass_next_batch.md`
3. Submit the batch to ChatGPT.
4. Append ChatGPT's JSONL response to:
   - `code/chatgpt_geography_second_pass_responses.jsonl`
5. Rerun `make`.
6. Read:
   - `../output/council_land_use_missing_geography_second_pass_ai_repair_candidates.csv`
   - `../output/council_land_use_missing_geography_second_pass_human_verification_queue.csv`
   - `../output/council_land_use_missing_geography_second_pass_response_errors.csv`

The second-pass queue covers all 161 first-pass rows. It prioritizes the 17 first-pass official-source candidates and 73 first-pass human-verification rows, then sends the 28 unresolved rows and 43 citywide/not-project rows through a challenge pass so those classifications are not simply accepted by default.

## Adjudication-Pass Review Workflow

1. Run `make` from this task's `code/` folder.
2. Open the next adjudication batch:
   - `../output/council_land_use_missing_geography_adjudication_next_batch.md`
3. Submit the batch to ChatGPT.
4. Append ChatGPT's JSONL response to:
   - `code/chatgpt_geography_adjudication_responses.jsonl`
5. Rerun `make`.
6. Read:
   - `../output/council_land_use_missing_geography_adjudication_ai_repair_candidates.csv`
   - `../output/council_land_use_missing_geography_adjudication_spot_check_queue.csv`
   - `../output/council_land_use_missing_geography_adjudication_response_errors.csv`

The adjudication pass covers all 161 rows again. It treats first-pass and second-pass answers as claims, asks ChatGPT to re-check official records, and classifies each row as accepted project geography, rejected prior geography, citywide/text-only, not land use, unresolved, or ambiguous. These outputs still do not promote geography into the analysis sample; promotion requires an entry in the manual verdict ledger.

## Candidate Categories

- `official_high_confidence_ai_candidate`
  - AI found a resolved, high-confidence official-source district and did not mark it for human review. These are the fastest rows to spot-check.
- `needs_human_verification`
  - AI found a plausible district through title inference, outside search, medium confidence, partial evidence, or explicit human-review flag. Dock Street is this type of row.
- `not_project_geography`
  - Citywide, text-only, or not a local project geography.
- `unresolved_after_ai_review`
  - AI could not recover usable geography.
- `response_needs_correction`
  - JSON parsed but failed controlled-vocabulary or required-field validation.

## Adjudication-Pass Candidate Categories

- `ai_adjudicated_official_project_geography`
  - Third pass found official, high-confidence project geography and did not mark it for additional human review. These are the fastest rows to spot-check.
- `ai_adjudicated_project_geography_needs_spot_check`
  - Third pass found plausible project geography, often through address or project-area inference, but it still requires a researcher spot check.
- `ai_adjudicated_not_project_geography`
  - Third pass classified the row as citywide, text-only, or not project geography.
- `ai_rejected_prior_geography`
  - Third pass rejected a prior AI geography suggestion.
- `unresolved_after_adjudication`
  - Third pass still could not recover defensible project geography.
- `response_needs_correction`
  - JSON parsed but failed controlled-vocabulary or required-field validation.

## Second-Pass Candidate Categories

- `official_high_confidence_second_pass_candidate`
  - Second pass confirmed or corrected a district using high-confidence official evidence and did not flag the row for human review.
- `needs_human_verification`
  - Second pass found plausible project geography, but source review is still needed before promotion.
- `not_project_geography`
  - Second pass classified the row as citywide, text-only, or not a project geography.
- `unresolved_after_second_pass`
  - Second pass could not recover usable project geography.
- `response_needs_correction`
  - JSON parsed but failed controlled-vocabulary or required-field validation.

## Manual Verdict Statuses

- `verified_project_geography`
  - Use when a human has checked the cited source and accepted affected Council district(s).
- `verified_citywide_or_text_only`
  - Use when the row should not have project geography because it is citywide or text-only.
- `verified_not_land_use`
  - Use when the row is not a land-use/project geography item for this purpose.
- `verified_unresolved`
  - Use when a human search found no defensible geography.
- `reject_ai_suggestion`
  - Use when ChatGPT suggested a district but source review rejects it.

## Research Status

ChatGPT labels are first-pass suggestions. They are useful for triage and source discovery, not for final geography assignment without review. A future repair task should read only human-verified rows, record the official source used, and then feed the repaired geography back into the member-deference event series.
