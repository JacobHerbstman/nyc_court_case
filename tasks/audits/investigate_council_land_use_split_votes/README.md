# Investigate Council Land-Use Split Votes

This audit decomposes split final-action votes in the 1998-2025 Council
land-use decision panel.

The task is descriptive. It does not build a district panel or estimate a
treatment effect. It asks whether the post-2002 increase in split votes is
driven by approvals or nonapprovals, one-member or broader dissent, negative
votes or abstentions, local-member nonaffirmative votes, or particular matter
types.

The task also builds a roll-call signature diagnostic, grouping split matters
with the same year, vote source, vote date, vote margin, negative count, and
abstain count. This is not an official project or vote-event identifier. It is a
first check for whether apparent annual spikes are many independent matters or
repeated application/resolution rows from the same contested bundle.
The local-member no/abstain trend is reported both by Legistar matter row and by
this roll-call signature diagnostic.
The rolling-average versions use three- and five-year trailing windows.
Rate plots use local-member-observed roll-call signatures as the denominator, so
unassigned geography or missing roster matches do not enter as zero local-member
opposition.
The multi-district diagnostic checks whether the local-member opposition rate is
driven by the changing share of approval roll-call signatures that affect more
than one Council district. It reports five-year trailing multi-district shares
and separate local no/abstain rates for single-district and multi-district
approval signatures.
The task also exports the red-event rows and overlaps them with the Charter
report seed cases in `tasks/audits/build_member_deference_overrule_candidates`.

For approval votes, named nonaffirmative member rows are matched to the raw
Legistar member-vote rows using both `matter_id` and the final-action
`history_detail_url`. For nonapproval votes, the task uses the previously
fetched final-action nonapproval member-vote rows.

These outputs are evidence about final Council roll-call behavior. They do not
directly observe agenda control, pre-vote bargaining, committee gatekeeping, or
withdrawn proposals.
