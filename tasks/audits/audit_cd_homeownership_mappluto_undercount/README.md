# Audit: MapPLUTO yearbuilt undercount relative to Census year built

The production CD housing series counts units by 25v4 MapPLUTO `yearbuilt`.
For 1970-1999 vintages, MapPLUTO counts are well below Census year-built counts,
and more so in low-homeownership CDs. The 5+ unit event study uses 1985-1989 as
the reference period, so this shortfall could make the post-2010 coefficients
more negative. This audit asks what the gap is, and whether the result survives
when the pre-period is measured with Census counts.

## Findings

**The gap is real, and it is in the Census counts, not missing new construction.**
Units per CD-year in 1985-1989, by within-borough 1990 homeownership tercile:

| Tercile | MapPLUTO 25v4 | Census 1990 (1985-Mar 1990, rescaled to 5 yrs) | Gap |
|---|---|---|---|
| Low (20 CDs) | 159 | 283 | 125 |
| Middle (20) | 253 | 302 | 50 |
| High (19) | 390 | 381 | -8 |

MapPLUTO/Census ratios (low/middle/high) are 0.53/0.80/0.97 for 1985-1989 against
the 1990 Census, 0.57/0.76/0.94 for the 1980s against the 2000 Census, and
0.53/0.60/0.81 against ACS 2020-2024. For post-2000 vintages the sources agree:
MapPLUTO/ACS is 0.99/1.07/1.06 for 2000-2009 and 0.94/0.96/0.86 for 2010-2019,
and MapPLUTO/Housing Database new buildings is 1.08/1.07/1.05 for 2010-2019.

How much each explanation accounts for:

- **Survivorship and later revisions: essentially none.** The 2002 MapPLUTO (02b)
  gives the same 1980s totals as 25v4 (25v4/02b = 0.99, 1.00, 1.00 by tercile).
  The gap is also already present in the 1990 Census, taken right after
  construction.
- **Missing new construction in MapPLUTO: not supported.** At the borough level,
  MapPLUTO 1980s units match Census Building Permits Survey new-construction
  authorizations: 0.98 in the Bronx, 1.13 in Brooklyn, 1.01 in Queens and 0.93 on
  Staten Island (1.29 in Manhattan, where Battery Park City and public projects
  are outside BPS). Meanwhile the 1990 Census reports 28,900 Bronx units built
  1980-Mar 1990, against 8,700 permitted and 8,500 in MapPLUTO. This check is
  only possible by borough, and the event study compares CDs within borough.
- **Buildings counted as new by Census respondents (gut rehab) plus imputation:
  the likely explanation.** The gradient is in 5+ buildings. The ACS 2020-24
  MapPLUTO/ACS ratio for 1980-1999 vintages is 0.53/0.76/1.08 for 5+ buildings and
  0.63/0.49/0.67 (no gradient) for 1-4 unit buildings. Renters occupied 76% of the
  1985-Mar 1990 units in low CDs and 55% in high CDs. For the 1990s, the Census 2000
  gap tracks units altered in pre-1940 5+ buildings (MapPLUTO `yearalter`) and 2002
  city-owned units. With the alteration measure controlled, the homeownership slope
  of the gap falls from -273 to -72 units per SD per decade. This is consistent with
  Section 8 and HPD in-rem gut rehabilitation that residents report as new. For
  1985-1989 neither indicator absorbs the gradient. What does absorb it is the
  1990 Census year-built allocation rate: 44% of NYC units had year built imputed
  (55% in low CDs, 36% in high CDs), and controlling for it moves the slope from
  -259 to +8. The allocation rate is closely tied to renter share, so this is
  suggestive, not proof. The available data cannot separate rehab reported as new
  from respondent or imputation error, and both lie on the Census side.
  Counting every MapPLUTO lot with an alteration year in the decade does not help.
  It adds 336,000 1980s units, 272,000 of them in Manhattan, against a citywide
  1980s gap of 63,000 (Census 1990). `yearalter` records any value-changing
  alteration and is too broad to be a rehab flag.
- **Year-built heaping, unit definitions, geography: minor.** The share of 1980s
  units on years ending in 0 or 5 is 24-27% in every tercile. Post-2000 agreement
  with ACS and the Housing Database suggests unit counts are comparable. Census
  tract counts were allocated to CDs with the same 25v4 lot-to-CD assignment as the
  outcome. Checked against DCP's published 1990 CD totals, the allocation has a
  median absolute error of 0.7% (maximum 9.9%).

**Size of the potential bias.** Within borough, with the production controls, the
1985-1989 Census-minus-MapPLUTO gap (all building sizes) falls by 44 units/yr per
SD of homeownership (SE 21). If the entire gap were in 5+ buildings, correcting
it would move the 2020-2025 coefficient by about +44. To produce the whole -358,
the gradient would need to be about 8 times as large. In tercile terms (the terciles
are 2.1 SD apart), if the high tercile is measured correctly, low-tercile CDs would
have to have built about 840 5+ units/CD/yr in 1985-1989. That is 9 times the 95
MapPLUTO records, and 3 times the 283 units of all sizes that the 1990 Census
reports. The pre-period coefficients are also flat throughout
1970-2004 (-45 to +17), so no single mismeasured reference bin drives the result.

## Re-estimated 5+ event study

Production specification: CD FE, borough-by-period FE, period-interacted 1990
controls, SE clustered by CD. The replication matches production exactly. Cells
show coefficient (SE); * p<.10, ** p<.05.

| Specification | 2010-14 | 2015-19 | 2020-25 |
|---|---|---|---|
| Baseline (production) | -168 (90)* | -274 (139)* | -358 (146)** |
| Reference 2000-2004 (MapPLUTO matches ACS for 2000s) | -156 (79)* | -262 (130)** | -346 (137)** |
| 1985-89 raised to Census 1990, whole gap added to 5+ (upper-bound correction) | -130 (90) | -232 (141) | -317 (145)** |
| All 1970-99 windows raised to Census 1990/2000, gap to 5+ | -116 (90) | -220 (140) | -304 (143)** |
| 1970-99 5+ scaled by CD Census/MapPLUTO ratio | -143 (90) | -248 (139)* | -332 (143)** |
| 5+ scaled by CD ACS/MapPLUTO 5+ ratios, all vintages | -114 (73) | -164 (109) | -259 (130)* |
| 2010-25 replaced by Housing Database new-building 5+ units | -109 (67) | -245 (116)** | -310 (147)** |
| Census 1990 reference + Housing Database post (combined worst case) | -70 (68) | -204 (117)* | -268 (149)* |

Long differences measured only with surveys (production long-difference
specification, per SD, robust SE), shown against MapPLUTO for the same windows:

| Outcome, windows | MapPLUTO | Survey-based |
|---|---|---|
| All units, 2010-19 vs 1985-89 | -245 (124) | -138 (106): ACS post, Census 1990 pre |
| All units, 2010-19 vs 1980-89 | -241 (118) | -156 (95): ACS both periods |
| 5+ units, 2000-19 vs 1980-99 | -127 (72) | -61 (54): ACS occupied units |
| 1-4 units, 2000-19 vs 1980-99 | +23 (12) | -2 (9): ACS occupied units |

Survey-only estimates are about 55-65% of the MapPLUTO estimates and are not
significant. About half of that difference comes from the post period rather
than the reference period: in high-homeownership CDs, ACS and the Housing
Database both record somewhat more post-2000 production than MapPLUTO. The 2010s
gap slope is +63 (SE 32) for ACS minus MapPLUTO (all units) and +44 (SE 30) for
Housing Database minus MapPLUTO (5+). The largest CD differences come from the
timing of a few large projects (e.g. 302, 104, 402).

## Recommendation for the paper

Keep MapPLUTO yearbuilt as the main measure and describe it as new construction.
Say plainly that Census year-built counts exceed it for 1970-1999 vintages,
especially in low-homeownership CDs and 5+ buildings. Also say that the excess
appears in the 1990 Census, the 2000 Census and ACS 2020-24; that it is absent for
post-2000 vintages; and that MapPLUTO tracks building permits (BPS, borough level)
and the Housing Database (2010+). The excess most likely reflects substantially
rehabilitated buildings and imputed year built, not missed new construction.

Add one robustness exhibit. The 2020-2025 coefficient is -317 (p=.03) when the
reference period is raised to the Census 1990 count with the whole gap in 5+
buildings, -346 with a 2000-2004 reference, -310 with Housing Database post
counts, and -268 (p=.08) in the combined worst case. Report the bound that the
reference-period mismeasurement would have to be about 8 times the measured
gradient to explain the coefficient. The 2010-2014 and 2015-2019 coefficients
are marginal in the baseline and lose significance under several corrections, so
the text should lean on 2020-2025 and the overall post-2010 pattern. The
calibrated series are corrections that assume the Census concept; do not use
them as the main series.

## Not verified or not available

- No CD-level record of 1980s-1990s rehabilitation. HPD production data in the
  repo starts in 2014, and in-rem program files are not available, so the gut-rehab
  explanation rests on indirect evidence.
- DOB permit issuance has no unit counts, and its coverage before about 1993 is
  incomplete (16 residential NB permit records in 1989, 522 in 1990, 4,551 in 1993). BIS job filings
  start in 2000 and certificates of occupancy in 2012. There is no independent
  administrative CD count for the 1985-1989 reference period; DOB is used only as
  a 1990s A1 correlate.
- ACS margins of error are not propagated; tract estimates are summed. The ACS 5+
  split covers occupied units only.
- The 2000 Census comes from DCP CD profiles, not tracts.
- Tract-to-CD allocation uses 25v4 residential-unit weights. Tracts with no
  residential lots are dropped: 1,548 units in 1990 and 8,167 in ACS.
- The event-study code is copied from `estimate_cd_homeownership_long_units_event_study.R`
  as of 2026-09-26. The script asserts the production baseline coefficients and
  will stop if the production spec changes.

## Sources and reproduction

- New acquisitions, saved unchanged in `output/`:
  - NHGIS 1990 STF3 tract tables NH25, NH27 and NH77, fetched 2026-09-26 with the
    `IPUMS_API_KEY` in `~/.Renviron` (extract definition
    `code/nhgis_1990_year_built_extract.json`).
  - ACS 2020-2024 five-year tract tables B25034 and B25127 for the five NYC
    counties, fetched 2026-09-26 from `api.census.gov` with the `CENSUS_API_KEY` in
    `~/.Renviron`. The key is not written to disk.
- Existing sources:
  - 25v4 and 02b MapPLUTO (`data_raw/dcp_mappluto_archive`), with a check that 25v4
    counts reproduce the production proxy exactly.
  - DCP 1990-2000 CD profiles, NHGIS 1990 tract shapes and TIGER 2020 tracts.
  - DOB permit issuance (20260501), Housing Database 25Q4 and Census BPS.
  - The production series and treatment files.
- Run `make` from `code/`. The analysis steps need about a minute after the
  fetches. Scripts: `fetch_*` (acquisition), `build_census_year_built_cd.R`,
  `build_mappluto_vintage_cd_year.R`, `build_dob_permit_cd_year.R`,
  `build_hdb_cd_year.R`, `diagnose_mappluto_undercount.R`,
  `estimate_undercount_alternatives.R`.
- Main outputs: `undercount_tercile_summary.csv`, `undercount_borough_summary.csv`,
  `undercount_gap_correlates.csv`, `undercount_event_study_coefficients.csv`,
  `undercount_long_differences.csv`, and the two PDFs.
