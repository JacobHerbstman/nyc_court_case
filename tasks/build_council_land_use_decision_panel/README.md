# Build Council Land-Use Decision Panel

Builds the matter-level Council land-use decision panel from the approval and
non-approval vote workflows.

Inputs are the recalled land-use matter universe, approval-side local-member
votes, the conservative non-approval geography queue, and parsed non-approval
final-action vote files. Some affected-district assignments come from upstream
review ledgers because the original land-use decision records did not state a
clear affected Council district. Those rows were first reviewed with ChatGPT to
surface likely locations and source records, then either accepted into a
committed repair ledger or verified against official records and BBL-to-district
matches. This task reads those reviewed repair decisions or official
verification outputs with source notes, not raw AI suggestions.

Creates `council_land_use_decision_panel.csv` with one row per Legistar matter
and the vote/geography fields needed for the Council land-use decision trend
series.
