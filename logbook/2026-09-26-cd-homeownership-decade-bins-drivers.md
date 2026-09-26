---
title: "What drives the post-2010 homeowner gradient in 5+ unit construction"
author: "Research note prepared by Claude for Jacob Herbstman and Tyler Jacobson"
date: "September 26, 2026"
---

The paper's event study finds that community districts with higher 1990
homeownership, compared within borough, built fewer units in 5+ unit buildings
after 2010. The coefficients are -168 in 2010-2014, -274 in 2015-2019 and -358
in 2020-2025, in units a year per standard deviation of homeownership. The
result holds when the bins change. It is mostly driven by five districts:
three renter-heavy waterfront districts, Downtown Brooklyn, and Park
Slope/Gowanus. In share terms it begins in the 1990s. Three follow-up exercises
address timing, the pre-production control, and which districts produce the
result. None of this changes the paper; the wording choices below are for
Jacob.

## Decade bins

The production event-study task now runs two more bin schemes from its
Makefile, and the paper's five-year outputs are unchanged byte for byte. The
paper's control is mean 1970-1988 production, which overlaps the pre-period
bins and the omitted bin. The new schemes use mean 1960-1969 production, before
the first estimated bin. With decade bins (omitted 1980s), the 5+ coefficients
are -20 (43) for the 1970s, -28 (25) for the 1990s, -51 (42) for the 2000s,
-228 (108) for the 2010s and -355 (142) for 2020-2025. Keeping five-year bins
from 1980 but merging the 1970s gives -19 (40) in 2000-2004, -73 (69) in
2005-2009, and then -174, -272 and -350. In raw units, the break after 2010
does not depend on the bins. The choice of control window also matters
little after 2010: the 2020-2025 coefficient runs from -350 to -384 across
1960-1969, 1970-1984, 1970-1988 and no control.

The 1-4 unit placebo is not flat. With decade bins it is -28 (11) in the 1970s,
+45 (17) in the 1990s, +100 (30) in the 2000s and +4 (11) in the 2010s.
Relative to the 1980s there is no late decline. Relative to the 2000s there is
a drop of about 96 units a year.

## What carries the 5+ coefficient

Because every control is interacted with period, each coefficient equals a
cross-district regression of change in production on residualized treatment.
That gives an exact contribution for each district. With decade bins, CDs 301
(Greenpoint-Williamsburg), 302 (Downtown Brooklyn/Fort Greene), 401
(Astoria/Long Island City) and 402 (Sunnyside/Hunters Point) contribute -194 of
the 2010s coefficient of -228. In 2020-2025, those four contribute -182, CD 306
(Park Slope/Gowanus) -142, and the other 54 districts -32. Dropping the four
gives -59 (33) and -232 (141). Dropping all five gives -41 (33) and -81 (74).
The 5+ result is almost entirely a 50+ result: in 2020-2025, 50+ buildings give
-327 (146), and 5-49 unit buildings -28 (18).

The income control drives much of this. Income explains 79% of within-borough
variation in 1990 homeownership, so the regression compares homeownership with
what income predicts. CDs 302 and 306 have slightly above-average homeownership
for Brooklyn (z = +0.10 and +0.17) but high incomes. After the controls, their
treatment is -0.67 and -0.91, so their booms count against homeowners. Without
controls the gradient is smaller after 2010 but more precise, and it begins
earlier. In decade bins it is -49 (23) in the 1990s, -98 (32) in the 2000s,
-193 (44) in the 2010s and -236 (45) in 2020-2025.

The four waterfront and downtown districts had large city-led upzonings in
2001-2005. Their divergence begins in 2005-2009, and their contribution grows
from -104 in that bin to -254 in 2015-2019 as projects were completed. A
hand-coded list names 23 major 2001-2009 upzonings, each tied to its ZAP record.
The existing ZAP FAR parse records no parsed change for Downtown Brooklyn,
Greenpoint-Williamsburg, Hudson Yards, West Chelsea or either Hunters Point
action. Dropping the 17 districts on the list gives -15 (34), -93 (49) and -131
(70) for 2010-2014 through 2020-2025. A large-upzoning indicator by period
leaves 2020-2025 at -330 (114). The gradient therefore persists within upzoned
districts; it does not merely separate upzoned from other districts.

Scale changes the timing. Mean 5+ production per district rose from 187 units
a year in 1985-1989 to 429 in 2020-2025. The raw-unit coefficients therefore
grow as the city builds more, even if the pattern of shares does not change.
Measured as a district's percent of its borough's 5+ units, the decade
coefficients are -3.6 (1.5) in the 1990s, -3.3 (1.2) in the 2000s, -5.8 (2.2) in
the 2010s and -5.6 (2.0) in 2020-2025. Without the four districts they are
-2.7, -2.2, -2.6 and -3.9: present from the 1990s, with little change after
2010. The new within-borough share page of the tercile figure shows the same
pattern. Before 1990, each tercile built about a third of its borough's 5+
units. The high tercile's share fell to 18% in 1995, 13% in 2008 and 9% in
2010, then recovered to 15-18%.

## Implications and open questions

These are Claude's suggestions; Jacob has not decided on them. The sentence
"After 2010, the ranking reverses" does not describe the within-borough
comparison. Averaged within boroughs, the high tercile had the largest 5+ share
in only 2 of the 19 years from 1971 to 1989. The pooled figure's pre-1990 lead
comes mainly from Manhattan. The within-borough gap
opens during 1990-2010. One possible replacement is: "Within boroughs, the
three terciles built similar shares of large-building units before 1990.
Homeowner-heavy districts' share then fell through the 1990s and 2000s, and the
gap in raw units widened after 2010 as citywide production grew." The
long-difference table repeats the event-study coefficients with
heteroskedasticity-robust standard errors. It could be dropped, or it could
report the decade and share versions. The text should note that the post-2010
raw-unit estimates rest on a few districts rezoned under Bloomberg. Those
rezonings were approved by the post-Morris Council, so they may be part of the
mechanism rather than a confound; this audit cannot separate the two.

The affordable/421-a split could not be rebuilt through Make, because its
upstream chain would re-fetch HPD and DOF data and stops on a missing NHGIS
rule. A one-off cross-section from its existing output puts 57% of the
2010-2025 50+ gradient in the HPD/affordable proxy and 17% in observed 421-a.
That estimate is unverified. The upzoning list reflects Claude's judgment and
has not been reviewed.

## Reproduction

Run `make` in each of these `code/` folders, in order:

- `tasks/estimate_cd_homeownership_long_units_event_study/`
- `tasks/summarize_cd_homeownership_long_units_series/`
- `tasks/audits/summarize_cd_homeownership_long_units_drivers/`

The audit README gives full scenario tables. The branch is `cpc_llm_training` at
`2ec4b7c` plus uncommitted changes. The audit reproduces the production
coefficients exactly, and each set of contributions sums to its coefficient.

![Decade-bin event study: 1-4 vs 5+ unit buildings.](input/cd_homeownership_long_units_event_coefficients_raw_units_decade_bins.pdf)
