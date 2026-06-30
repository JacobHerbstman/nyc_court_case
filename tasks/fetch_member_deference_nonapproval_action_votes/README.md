# Fetch Non-Approval Final-Action Votes

Fetches Legistar action-detail pages for non-approval land-use matters and
parses the individual member votes shown on those pages.

The input is the conservative non-approval geography queue. It contains 491 core
non-approval matters, including 399 with usable affected-district geography and
92 left without geography pending review.

Creates action-level vote details and
`member_deference_nonapproval_local_member_vote_status.csv`, the matter-level
local-member vote-status file used by the decision panel.

An `Affirmative` vote here is a vote on the final Council action shown by
Legistar, such as filing or disapproval; it is not automatically support for the
underlying land-use application.

Runtime: about 6 seconds when the Legistar action-detail pages are already
cached. A first run can take longer because it downloads Legistar pages.
