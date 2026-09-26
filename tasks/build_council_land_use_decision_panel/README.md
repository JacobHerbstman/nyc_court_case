# Build Council Land-Use Decision Panel

Builds the matter-level Council land-use decision panel from the approval and
nonapproval vote workflows.

Inputs are the recalled land-use matter universe, approval-side local-member
votes, the conservative nonapproval geography list, and parsed nonapproval
final-action vote files. Some affected-district assignments come from accepted
review ledgers or official-record verification when source records did not state
a clear Council district.

The panel adds the fields the trend series use:

- `project_outcome`: `approved` or `disapproved` from the Legistar disposition.
  An adopted "Resolution disapproving ..." is a disapproved project.
- `rollcall_direction`: `approve_project` for a Council approval vote;
  `reject_project` for votes to disapprove, file, or override a veto of a
  disapproval, and for a Council approval of a disapproved matter or of a
  disapproval resolution (for example LU 0468-2005, East 91st Street).
- `local_member_project_position`: `supports` or `opposes`, from the local
  members' votes and the roll-call direction. Any local no vote counts, including
  on items touching several districts; `n_affected_districts` records how many.
  Abstentions count as not voting and are listed in `local_member_abstain`.
- `event_id`: matters in the same query year that share any ZAP project id or
  application key (connected components) form one land-use event.

Output: `council_land_use_decision_panel.csv`, with one row per Legistar matter
and the vote and geography fields used in the Council land-use decision trend
series.
