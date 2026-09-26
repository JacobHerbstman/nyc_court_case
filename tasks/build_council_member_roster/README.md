# Build Council Member Roster

Builds a historical NYC Council member roster for matching land-use matters to
affected local members.

The task downloads or reuses saved Legistar roster source pages and Wikipedia
district-history pages, then writes `council_member_roster_master.csv`, one row
per member term and district, with the Legistar `person_id` used to match
roll-call votes.

Source priority, recorded row by row in `roster_source`:

- `legistar_office_record`: official Legistar office records, all Council
  titles (Council Member, Speaker, Majority/Minority Leader, Whips) except
  Public Advocate. Linares (1998-2001) comes from his PersonDetail page because
  the all-term grid intermittently omits him.
- `legistar_office_record_wikipedia_district`: official rows whose grid and
  PersonDetail page omit the district; the district comes from the one
  Wikipedia district page listing the same surname in an overlapping period.
- `legistar_office_record_corrected`: official rows changed by
  `code/official_roster_corrections.csv` (Brannan listed in District 47 while
  holding District 43; Grodenchik's start overlapping Mark Weprin).
- `wikipedia_gap_fill`: a 1998-2025 period with no official row, filled only when
  one Wikipedia member spans the whole gap and either has no official term in
  that district or has official terms on both sides of it. Dates are clipped to
  the gap. Other gaps are vacancies and stay empty.

Official rows left without a district are printed in the log (Pedro Espada, Jr.,
2003). The build stops on overlapping district intervals and on known-member
checks (for example District 1: Freed through 2001, Gerson from 2002, Chin
2010-2021, Marte from 2022).
