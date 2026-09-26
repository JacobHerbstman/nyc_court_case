# What Drives the Post-2010 5+ Unit Homeowner Gradient

Audit of the community-district event study in
`tasks/estimate_cd_homeownership_long_units_event_study`. For each bin scheme
(`5yr_bins`, `decade_bins`, `decade_pre_bins`, passed from the Makefile) the
script re-estimates the production specification under different samples,
outcomes and controls, and checks that its baseline reproduces the production
coefficients exactly.

Inputs are the production long-units series and event-study coefficients, the
ZAP project table, and `code/major_upzonings_2001_2009.csv`. That hand-coded
table lists 23 city-sponsored rezonings adopted 2001-2009 that raised
residential capacity over many blocks, each with its ZAP project id. The script
checks that each listed district is one ZAP records for the project. Scale
(`large`, `moderate`) is Claude's judgment from project descriptions and public
knowledge; Jacob has not reviewed it. The existing ZAP FAR parse
(`summarize_zap_rezoning_direction_scope`) was not used: it records no parsed
zoning change, and zero upzoned FAR-acres, for Downtown Brooklyn,
Greenpoint-Williamsburg, Hudson Yards, West Chelsea, Hunters Point or Hunters
Point South.

Outputs per scheme: `cd_homeownership_long_units_drivers_coefficients_*.csv`
(all scenarios), `..._leave_one_out_*.csv` (5+ units, one district dropped at a
time), `..._contributions_*.csv` (exact district contributions) and
`..._plots_*.pdf`. A contribution is the district's residualized treatment times
its change from the omitted bin, divided by the sum of squared residualized
treatments; contributions add up to the coefficient.

## Findings (September 26, 2026)

Coefficients are annual units per one within-borough SD of 1990 homeownership;
standard errors, clustered by district, are in parentheses.

**Size.** The 5+ result is a 50+ result. In 2020-2025 the 50+ coefficient is
-327 (146) of the 5+ coefficient of -358 (146); 5-49 unit buildings give -28
(18).

**Timing.** With decade bins (omitted 1980-1989) the 5+ coefficients are -28
(25) in the 1990s, -51 (42) in the 2000s, -228 (108) in 2010-2019 and -355 (142)
in 2020-2025. In raw units the break is after 2010. The 2000s gradient comes
from CDs 301, 302, 401 and 402, which contribute -59. CD 306 contributes -16,
and the other 54 districts contribute +23. It starts in 2005-2009, soon after those districts' 2001-2005
rezonings. Measured as a district's percent of its borough's 5+ units, the
gradient appears earlier: -3.6 (1.5) points in the 1990s, -3.3 (1.2) in the
2000s, -5.8 (2.2) in the 2010s and -5.6 (2.0) in 2020-2025. The average
district's share is about 7%. Raw-unit coefficients grow after 2010 partly
because citywide production grew. Mean 5+ production per district was 187 units
a year in 1985-1989, 245 in the 2000s, 322 in the 2010s and 429 in 2020-2025.

**Five districts.** In 2010-2019, CDs 301 (Greenpoint-Williamsburg), 302
(Downtown Brooklyn/Fort Greene), 401 (Astoria/Long Island City) and 402
(Sunnyside/Hunters Point) contribute -194 of the -228 coefficient. In 2020-2025
they contribute -182, CD 306 (Park Slope/Gowanus) -142, and the other 54
districts -32. Dropping the four gives -59 (33) and -232 (141). Dropping all
five gives -41 (33) and -81 (74). In five-year bins, dropping the five moves
2015-2019 from -274 to -58 (54) and 2020-2025 from -358 to -83 (79). Leave-one-out
confirms this ranking: the largest single shifts are CD 302 in 2010-2019 (+94)
and CD 306 in 2020-2025 (+107).

**The income control.** Income alone explains 79% of within-borough variation
in 1990 homeownership. The regression therefore uses homeownership relative to
what income predicts. CDs 302 and 306 have slightly above-borough homeownership
(z = +0.10 and +0.17) but high incomes, so their residualized treatment is
-0.67 and -0.91. That is why their building booms count against homeowners.
With no controls the 5+ gradient is still negative and more precise: in decade
bins it is -49 (23), -98 (32), -193 (44) and -236 (45) from the 1990s on, with
-27 (27) in the 1970s. In five-year bins the no-control pre-period is negative
too: -111 (43) in 1970-1974 and -64 (31) in 1980-1984.

**Rezonings.** The nine districts with a large 2001-2009 upzoning contribute
-96, -141, -220 and -129 in the four five-year bins from 2005-2009 to 2020-2025.
Dropping them gives -35 (37), -82 (46) and -288 (138) for 2010-2014 through
2020-2025. Dropping all 17 districts with any listed upzoning gives -15 (34),
-93 (49) and -131 (70). Controlling for a large-upzoning indicator by period
changes little: 2020-2025 is -330 (114). The gradient remains within upzoned
districts. CD 302 (residual -0.67) built 1,300-2,100 more units a year than in
1985-1989 in each bin after 2010, contributing -78 to -128. Jamaica, CD 412
(+0.81), was also upzoned. It built 415-749 more units a year after 2015 and
contributes +30 to +55.

**Scale.** Per 1,000 1990 housing units, the 5+ coefficients are -3.5 (2.0),
-5.7 (3.2) and -7.1 (3.2) for 2010-2014 through 2020-2025. Mean production is
7.7 per 1,000 a year in the 2010s and 10.0 in 2020-2025. Without the four
districts they are -0.5 (0.8), -0.9 (1.0) and -4.3 (3.1).

**Boroughs.** Dropping Manhattan leaves 2020-2025 at -377 (140). Brooklyn alone
gives -618 (139), but its pre-period is already negative (-131 (40) in
1970-1974; -63 (23) in 1990-1994). The Bronx alone gives -295 (76) with flat
earlier bins. Queens alone is -1 (289) in 2020-2025, and Manhattan alone is
noisy.

**Pre-production control.** The window matters little after 2010. The
2020-2025 5+ coefficient is -358 with the 1970-1988 control, -367 with
1970-1984, -350 with 1960-1969 and -384 with none. It matters more before
2010: with no control, 1990-1994 is -67 (38) rather than -27 (29).

**1-4 unit placebo.** The series rises to +100 (30) in the 2000s and falls to
+4 (11) in the 2010s. The decade-then-five-year scheme also shows a negative
1970s (-45 (13)) and 1980-1984 (-35 (14)). Relative to 1985-1989 there is no
post-2010 decline. Relative to the 2000s, there is one of about 96 units a year.

**421-a/affordable split (not reproducible here).** This split uses the existing
`build_hdb_public_affordable_421a_split` output, but that task cannot rebuild
through Make. It would re-fetch HPD and DOF data, and its chain stops on a
missing `build_nhgis_extracts` rule. A one-off cross-section of average annual
DCP Housing Database 50+ new-building units in 2010-2025 was therefore run
outside Make. Its gradient is -174 (103): -100 in the HPD/affordable proxy, -29
in observed 421-a and -46 in residual private units. Without the four
districts the total is -27 (43). These are levels, not changes from 1985-1989,
and the affordable proxy is broad.
