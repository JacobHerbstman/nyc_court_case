# Build Member-Deference Vote Panel

Builds the approval-side member-deference vote panel from NYC Council Legistar
land-use records.

The panel uses Legistar final Council approval action details and member vote
rows for 1998-2025. It recovers affected Council districts from Legistar text
when available and otherwise uses application-number matches to staged ZAP
projects as a secondary geography source.

The primary downstream outputs are:

- `member_deference_matter_universe.csv`, the recalled land-use matter universe
- `member_deference_vote_panel.csv`, approval-side member votes and local-member
  vote positions
- `member_deference_final_action_vote_queue.csv`, non-approval and other final
  action matters that need separate geography and vote recovery

This does not prove member deference. Final votes miss pre-vote bargaining,
withdrawals, modifications, committee gatekeeping, and agenda control.
