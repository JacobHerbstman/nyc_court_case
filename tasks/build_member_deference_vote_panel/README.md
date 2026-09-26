# Build Member-Deference Vote Panel

Builds the approval-side local-member vote panel for NYC Council land-use
matters from 1998-2025.

Inputs are annual Legistar matter, history, action-detail, and member-vote
files; the Council member roster; ZAP project records; and the accepted
geography-repair ledger. Some district assignments came from AI-assisted review
in ChatGPT, but this task uses only the committed ledger of accepted district
assignments, evidence notes, and source URLs.

Outputs:

- `member_deference_matter_universe.csv`
- `member_deference_vote_panel.csv`
- `member_deference_final_action_vote_queue.csv`

The final action for a matter is taken from its latest history date: the
first-listed substantive City Council action on that date, else the first
substantive action, else the first action. Legistar lists dates newest first but
does not order same-day actions chronologically. Local members are matched to
roll-call votes by Legistar `person_id` (surname only for roster gap fills
without one), with the shared rule in `_lib/member_deference_utils.py`; any local
Negative is a local no vote, and abstentions count as not voting.
Council districts in text come from the shared parser: explicit "Council
District" statements and "22CD" first; a bare "CD" (which can mean Community
District) only when the text names a Community Board separately or the number is
above 18, never in "Queens CD 3" or "within CD 7" forms. District ranges such as
"CD 33-37" are expanded. `final_history_date` and `vote_date` are ISO dates. Each output has a data
report in `report/`.

Final votes do not capture pre-vote bargaining, withdrawals, modifications,
committee gatekeeping, or agenda control.

Runtime: about 30 seconds.
