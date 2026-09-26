# Fetch Council Land-Use Nonapproval Votes

Fetches Legistar action-detail pages for nonapproval land-use matters and
parses the individual member votes shown on those pages.

Inputs are the conservative nonapproval geography list and the Council member
roster. Local members serving on the final-action date are matched to the roll
call by Legistar `person_id` with the same shared rule as the approval-side vote
panel. A saved page must match the HistoryDetail ID of the current final action;
three pages saved under the earlier final-action rule were moved to
`output/source_files/member_deference_nonapproval_action_pages/superseded/` on
2026-09-26.

Creates `member_deference_nonapproval_action_details.csv`, the action-level
vote-detail file, and
`member_deference_nonapproval_local_member_vote_status.csv`, the matter-level
local-member vote-status file used by the decision panel.

An `Affirmative` vote here is a vote on the final Council action shown by
Legistar, such as filing or disapproval; it is not automatically support for the
underlying land-use application.
