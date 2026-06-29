# Build Member-Deference Vote Panel

Builds the approval-side local-member vote panel for NYC Council land-use
matters from 1998-2025.

Inputs are annual Legistar matter, history, action-detail, and member-vote
files; the Council member roster; ZAP project records; and the accepted
geography repair ledger. The repair ledger covers matters whose affected
districts were missing after the deterministic Legistar/ZAP geography pass. Some
rows began as ChatGPT-assisted location reads, but this task reads only the
committed accepted ledger with reviewed district assignments, promotion
decisions, evidence notes, and source URLs.

Creates `member_deference_matter_universe.csv`,
`member_deference_vote_panel.csv`, and
`member_deference_final_action_vote_queue.csv`.

Final votes do not capture pre-vote bargaining, withdrawals, modifications,
committee gatekeeping, or agenda control.

Runtime is about 1 minute.
