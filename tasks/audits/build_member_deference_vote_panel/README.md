# Build Member-Deference Vote Panel

Builds a matter-level audit panel from NYC Council Legistar land-use approval
votes. This task is intentionally diagnostic: it separates strong observable
exceptions to member deference from votes that are only consistent with member
deference and from unresolved geography/member-match cases.

The panel uses Legistar final Council approval action details and member vote
rows for 1998-2025. It recovers affected Council districts from Legistar text
when available and otherwise uses application-number matches to staged ZAP
projects as a secondary geography source.

This does not prove member deference. Final votes miss pre-vote bargaining,
withdrawals, modifications, committee gatekeeping, and agenda control.
