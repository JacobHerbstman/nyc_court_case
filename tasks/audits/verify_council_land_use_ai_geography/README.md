# Verify Council Land-Use AI Geography

This audit checks AI-assisted geography suggestions for Council land-use
roll-call signatures that were missing affected Council districts.

This task is not the source file used by the member-deference panel. Accepted
rows are copied into the reviewed source ledger in
`tasks/create_council_land_use_geography_review_ledgers/code/accepted_ai_geography_repair_ledger.csv`,
which writes the repair file used downstream.

## Inputs

- `../input/council_land_use_missing_geography_adjudication_ai_repair_candidates.csv`
  - Third-pass ChatGPT adjudication candidates from
    `tasks/audits/build_council_land_use_missing_geography_chatgpt_review/`.
- `../input/zap_bbl.csv`
  - ZAP project-to-BBL links used for deterministic project geography checks.
- `../input/nyc_mappluto_25v4_shp.zip`
  - Current MapPLUTO lot geometries used to place BBLs in Council districts.
- `../input/dcp_council_boundary_archive_*`
  - Council district boundary archive used for district assignment.
- `code/*_researcher_*.csv`
  - Researcher spot-check and adjudication ledgers.

## Outputs

- `../output/council_land_use_ai_geography_deterministic_verification.csv`
  - Row-level comparison between AI-suggested districts and deterministic
    BBL/address-derived checks where possible.
- `../output/council_land_use_ai_geography_deterministic_repairs.csv`
  - High-confidence deterministic repairs.
- `../output/council_land_use_ai_geography_deterministic_manual_queue.csv`
  - Rows needing web or document review after deterministic checks.
- `../output/full_document_conflict_prompts/`
  - Prompt packets for cases where deterministic checks conflict with AI
    suggestions.
- `../output/manual_queue_web_review_batches/`
  - Web-review batches and summaries for rows needing document checks.
- `../output/remaining_queue_web_review_batches/`
  - Web-review batches and summaries for other unresolved or citywide rows.
- `../output/council_land_use_ai_geography_accepted_repairs.csv`
  - Audit-side accepted repair candidates after deterministic checks, ChatGPT
    review, and researcher adjudication.
- `../output/council_land_use_ai_geography_accepted_repairs_excluded_by_current_queue.csv`
  - Accepted review rows that are no longer missing from the upstream panel.

## Research Status

This audit records how the missing-geography rows were reviewed. It does not
silently promote AI labels into the main sample. The production panel should use
only rows copied into the staged repair ledger, with source notes and explicit
review status.
