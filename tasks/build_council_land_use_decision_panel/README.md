# Build Council Land-Use Decision Panel

Builds the matter-level Council land-use decision panel from the approval and
nonapproval vote workflows.

Inputs are the recalled land-use matter universe, approval-side local-member
votes, the conservative nonapproval geography list, and parsed nonapproval
final-action vote files. Some affected-district assignments come from accepted
review ledgers or official-record verification when source records did not state
a clear Council district.

Output: `council_land_use_decision_panel.csv`, with one row per Legistar matter
and the vote and geography fields used in the Council land-use decision trend
series.
