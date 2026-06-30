# Build Council Land-Use Decision Panel

Builds the matter-level Council land-use decision panel from the approval and
non-approval vote workflows.

Inputs are the recalled land-use matter universe, approval-side local-member
votes, the conservative non-approval geography queue, and parsed non-approval
final-action vote files. Some affected-district assignments come from
version-controlled repair ledgers because the original records did not state a
clear affected Council district. Those rows were reviewed in ChatGPT, then
accepted in a version-controlled ledger or verified against official records and
BBL-to-district matches. This task reads the accepted or verified assignments,
not raw AI suggestions.

Output: `council_land_use_decision_panel.csv`, with one row per Legistar matter
and the vote and geography fields used in the Council land-use decision trend
series.
