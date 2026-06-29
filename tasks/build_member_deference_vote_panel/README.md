# Build Member-Deference Vote Panel

Builds the approval-side member-deference vote panel from NYC Council Legistar
land-use records.

The panel uses Legistar final Council approval action details and member vote
rows for 1998-2025. It recovers affected Council districts from Legistar text
when available and otherwise uses application-number matches to ZAP
projects as a secondary geography source. If Legistar and ZAP do not identify a
district, the task can use accepted geography repairs from
`tasks/create_council_land_use_ai_geography_repairs/`. Those repairs come from
land-use decisions with no clear affected Council district that were fed into
ChatGPT with matter text, project identifiers, source links, and geography clues;
the production input is the committed accepted ledger, not the raw chat.

The primary downstream outputs are:

- `member_deference_matter_universe.csv`, the recalled land-use matter universe
- `member_deference_vote_panel.csv`, approval-side member votes and local-member
  vote positions
- `member_deference_final_action_vote_queue.csv`, non-approval and other final
  action matters that need separate geography and vote recovery

This does not prove member deference. Final votes miss pre-vote bargaining,
withdrawals, modifications, committee gatekeeping, and agenda control.
