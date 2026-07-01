# Accepted Council Land-Use Geography Repair Ledger

This CSV is the source of truth for researcher-accepted geography repairs used in the member-deference voting series. Each row is one accepted decision for one Council matter row, identified by `query_year`, `vote_date`, and `matter_file`.

The ledger is not a raw ChatGPT export. ChatGPT and other AI tools were used only as review aids for some rows. The accepted research decision is the row recorded here: the accepted Council district or districts, the confidence, the promotion decision, the evidence type, the explanatory note, and the source URLs.

## Why This Exists

Some Council land-use roll-call matters are bundled, missing direct affected-district fields, or only indirectly linked to ZAP/ULURP geography. Those rows cannot enter the local-member deference series unless we can assign affected Council district geography. This ledger records the cases where we accepted a geography repair after deterministic checks, official-source review, AI-assisted review, or researcher adjudication.

Rows not represented here are not silently promoted by the main workflow. Review tasks may generate candidate rows, prompts, and ChatGPT responses, but a candidate affects the main dataset only after it is entered into this ledger.

## Key Fields

- `query_year`, `vote_date`, `matter_file`: matter-level key for the Council vote row being repaired.
- `signature_review_id`: review-bundle identifier. Multiple matter rows can share one bundle when they are substantively one roll-call/project bundle.
- `accepted_council_districts`: semicolon-separated Council district or districts accepted for the matter.
- `repair_source`: provenance category for the accepted decision.
- `repair_confidence`: high, medium, or low confidence assigned at promotion.
- `repair_promotion_decision`: whether the row is promoted directly, promoted with caveat, or deterministically verified.
- `repair_evidence_type`: type of evidence supporting the assignment, such as explicit official district, official boundary/source evidence, or current-boundary inference.
- `repair_note`: short explanation of the accepted judgment and caveats.
- `signature_matter_files`: all matter files in the same review bundle.
- `application_keys`, `zap_project_ids`, `zap_project_names`: parsed application/project identifiers when available.
- `title_examples`: official Council title text or representative title text used in review.
- `matter_urls`, `history_detail_urls`: source URLs used to identify the matter and vote/action.

## Research Interpretation

Treat `deterministic_geography_verification` rows as mechanical or near-mechanical repairs.

Treat `manual_queue_ai_review_researcher_accepted` and `remaining_queue_ai_review_researcher_adjudicated` rows as researcher-accepted subjective geography decisions. These are appropriate for the main repaired series only because the final accepted decision is recorded here with evidence and caveats.

The `remaining_split_vote_geography_ai_review_researcher_accepted` pass is not accepted in this ledger. A later review found that 107 of 109 ChatGPT responses from that pass cited Council matter files outside the review bundle, indicating a batch-alignment failure. Those rows must be re-reviewed before entering the main series.

Rows with `promote_with_caveat`, medium confidence, or low confidence should be easy to inspect from this file. They are included in the repaired main series, but they should remain visible in robustness checks and discussion of measurement uncertainty.

## Review Trail

The prompt batches, raw ChatGPT response JSONL, candidate classifications, missing-response lists, and response-error notes live in:

`tasks/audits/build_remaining_council_land_use_split_geography_review/`

That review task documents how candidate decisions were generated. This main task documents which decisions were accepted.
