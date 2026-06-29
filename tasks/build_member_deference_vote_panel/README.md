# Build Member-Deference Vote Panel

Builds the approval-side local-member vote panel for NYC Council land-use
matters from 1998-2025.

Inputs are annual Legistar matter, history, action-detail, and member-vote
files; the Council member roster; ZAP project records; and the accepted
geography repair ledger. Some repair-ledger rows started as ChatGPT-assisted
location reads for matters without clear affected districts, but this task reads
only the committed ledger with source notes.

Creates `member_deference_matter_universe.csv`,
`member_deference_vote_panel.csv`, and
`member_deference_final_action_vote_queue.csv`.

Final votes do not capture pre-vote bargaining, withdrawals, modifications,
committee gatekeeping, or agenda control.

Runtime is about 35 seconds.
