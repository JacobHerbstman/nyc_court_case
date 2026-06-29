# Build Remaining Council Land-Use Split Geography Review

This audit prepared ChatGPT review packets for split-vote Council land-use
matter bundles whose affected Council districts were still unresolved after
deterministic matching.

The response pass in `output/council_land_use_remaining_split_geography_responses_combined.csv`
is not accepted into production. A follow-up check found that 107 of 109
responses cited Council matter files outside the queued review bundle, which is
evidence of a batch-alignment failure. The production repair ledger in
`tasks/create_council_land_use_ai_geography_repairs/` therefore excludes
`remaining_split_vote_geography_ai_review_researcher_accepted` rows until this
review is rebuilt and reverified.
