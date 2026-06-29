# Build Council Land-Use Decision Panel

Builds the matter-level Council land-use decision panel from the approval and
non-approval vote workflows.

Inputs are the recalled land-use matter universe, approval-side local-member
votes, the conservative non-approval geography queue, and parsed non-approval
final-action vote files. Some affected-district assignments come from upstream
review ledgers for matters whose geography was missing after deterministic
matching. ChatGPT was used only to surface likely locations and source records;
this task reads reviewed repair decisions or official verification outputs with
source notes, not raw AI suggestions.

Creates `council_land_use_decision_panel.csv` with one row per Legistar matter
and the vote/geography fields needed for the Council land-use decision trend
series.
