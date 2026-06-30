# Fetch Council Land-Use Nonapproval Votes

Fetches Legistar action-detail pages for non-approval land-use matters and
parses the individual member votes shown on those pages.

The input is the conservative nonapproval geography queue.

Creates `member_deference_nonapproval_action_details.csv`, the action-level
vote-detail file, and
`member_deference_nonapproval_local_member_vote_status.csv`, the matter-level
local-member vote-status file used by the decision panel.

An `Affirmative` vote here is a vote on the final Council action shown by
Legistar, such as filing or disapproval; it is not automatically support for the
underlying land-use application.
