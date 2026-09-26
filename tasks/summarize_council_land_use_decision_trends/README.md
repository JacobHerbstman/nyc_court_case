# Summarize Council Land-Use Decision Trends

Creates the descriptive member-deference plots from the 1998-2025 Council
land-use decision panel.

The input is `council_land_use_decision_panel.csv`. The plotted unit is a
land-use event, not a Legistar matter: matters in the same query year that share
a ZAP project id or application key are one event (`event_id` in the panel). The
sample is adopted or disapproved matters where the affected local members'
roll-call votes give a position on the project (`local_member_project_position`);
adoption comes from the matter's disposition (`project_outcome`). Filed and
withdrawn matters are excluded. As for matters touching several districts, one
local no vote on any of an event's matters makes the event locally opposed; the
log counts events with both supporting and opposing matters. Events whose
matters disagree on the outcome are left out and counted in the log.

Outputs:

- `council_land_use_adoption_over_local_member_rollcall_opposition_rolling5_with_raw_clean.pdf`:
  share of all events with a local position that were adopted over local
  opposition (annual raw share in grey, trailing 5-year share overlaid).
- `council_land_use_adoption_over_local_member_rollcall_opposition_count_rolling5_with_raw_clean.pdf`:
  the same numerator as counts, with a trailing 5-year average.
- `council_land_use_adoption_share_among_local_member_rollcall_opposition_rolling5_with_raw_clean.pdf`:
  among events the local member opposed, the share adopted anyway
  (overruled / opposed), annual and trailing 5-year.
- `council_land_use_adoption_over_local_member_rollcall_opposition_rolling5_with_raw_clean.csv`:
  annual counts and shares behind all three plots (`event_rows`,
  `override_events`, `opposed_events`, `override_events_multi_district`, and the
  rolling series), with a data report in `report/`.
