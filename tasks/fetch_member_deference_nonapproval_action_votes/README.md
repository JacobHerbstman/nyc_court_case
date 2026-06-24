# Fetch Non-Approval Final-Action Votes

This task fetches Legistar action-detail pages for the first-pass non-approval
City Council land-use actions identified by
`tasks/build_member_deference_vote_panel`, after the conservative
geography incorporation step in
`tasks/verify_member_deference_nonapproval_geography`.

The upstream approval-vote pull only captured final Council approvals. This task
checks the adjacent margin: land-use matters that were disapproved, filed by the
Council, or filed through a withdrawal/motion-to-file path. The goal is to learn
whether those final action pages contain individual member votes and, when they
do, make them available for the production decision panel.

The task is intentionally separate from the offline vote-panel builder because it
downloads web pages.

The input queue keeps all 220 first-pass non-approval matters. It carries usable
affected-district geography for 194 of them and leaves 26 rows blank pending
manual review. Local-member vote outputs therefore distinguish matters with no
geography from matters with known geography but missing roster matches.

The local-member outputs are descriptive. An `Affirmative` vote is an affirmative
vote on the final Council action shown by Legistar, such as filing or
disapproval; it is not automatically support for the underlying land-use
application.

The production-facing decision ledger is
`member_deference_nonapproval_local_member_vote_status.csv`. It records one row
per queued matter, the affected local members, whether their final-action votes
were observed, and the standardized local-member vote status used by
`tasks/build_council_land_use_decision_panel`.
