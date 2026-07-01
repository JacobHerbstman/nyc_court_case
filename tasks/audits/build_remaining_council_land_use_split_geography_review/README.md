# Build Remaining Council Land-Use Split Geography Review

This audit builds review packets for split-vote Council land-use matter bundles
whose affected Council districts could not be assigned from the structured
records alone.

The response pass in `output/council_land_use_remaining_split_geography_responses_combined.csv`
is not accepted into production. A check found that 107 of 109 responses cited
Council matter files outside the review bundle. The production repair ledger in
`tasks/create_council_land_use_geography_review_ledgers/` therefore excludes
`remaining_split_vote_geography_ai_review_researcher_accepted` rows.
