# Summarize Council Land-Use Decision Trends

Creates the canonical descriptive member-deference plot from the 1998-2025
Council land-use decision panel.

The input is `council_land_use_decision_panel.csv`. The main output is
`council_land_use_adoption_over_local_member_rollcall_opposition_rolling5_with_raw_clean.pdf`,
which shows the share of substantive land-use events adopted over affected
local-member roll-call opposition, with the raw annual share in the background
and a trailing 5-year share overlaid. The plotted unit is a land-use event, not
a raw Legistar matter row: companion matters sharing a ZAP project or
application key are collapsed, and procedural filed/withdrawn matters are
excluded from the clean series. The denominator is event-level land-use
decisions with an observed affected local-member roll-call position. The
matching CSV contains the plotted annual event counts, annual shares, and
5-year rolling share:
`council_land_use_adoption_over_local_member_rollcall_opposition_rolling5_with_raw_clean.csv`.

The task also writes
`council_land_use_adoption_over_local_member_rollcall_opposition_count_rolling5_with_raw_clean.pdf`,
which plots the same numerator in raw count units: annual override-event counts
in the background and the trailing 5-year average count overlaid.
