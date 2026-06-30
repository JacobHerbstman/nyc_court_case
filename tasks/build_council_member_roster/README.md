# Build Council Member Roster

Builds a historical NYC Council member roster for matching land-use matters to
affected local members.

The input is `council_member_roster_source_files.csv`, which indexes the fetched
Legistar roster source pages. The output is
`council_member_roster_master.csv`, a district-by-date roster used by the
member-deference vote tasks.

Runtime: about 4 seconds.
