# Build Council Member Roster

Builds a historical NYC Council member roster for matching land-use matters to
affected local members.

The task downloads or reuses saved Legistar roster source pages, then parses
them into a district-by-date roster. The output is
`council_member_roster_master.csv`, a district-by-date roster used by the
member-deference vote tasks.
