# Rezoning scope by 1990 homeownership, and its link to local control

Measures how much land each adopted zoning map amendment upzoned, downzoned
or contextually rezoned from 2002 to 2025, and how much residential capacity
it added or removed. It classifies amendments as upzonings or downzonings and
summarizes them by within-borough 1990 homeownership tercile and Council
term. It then asks whether downzonings run through the local
control channel: who initiated them, whether the CPC report says local actors
asked for them, and how the local Council Member voted.

Run `make` from `code/` (about 30 minutes from scratch, most of it reading the
PLUTO releases).

## Sources

- **Lot zoning, 24 releases, one per year.** MapPLUTO 02b (zoning as of July
  2002); tabular PLUTO 03c, 04c, 05d, 06c, 07c, 09v1, 10v1, 11v1, 12v1, 13v1,
  14v1, 15v1, 16v1, 17v1 (fetched September 26, 2026 by
  `tasks/fetch_mappluto_archive`); MapPLUTO shapefile releases 18v1.1, 19v1,
  20v1, 21v1, 22v1, 23v1, 24v1, 25v1 and 25v4. There is no 2008 release: the
  08B file on BYTES is a 2020 re-export without a zoning date. 07c (January
  2008) and 09v1 (April 2009) bracket 2008.
- **Zoning map amendments.** The `nyzma` layer of DCP NYC GIS Zoning Features
  release 202608: adopted amendments with ULURP number, effective date and
  footprint. 770 amendments took effect between July 2002 and January 2026.
- ZAP public project universe (sponsor), the Council land-use decision panel
  (Legistar outcome and local-member position), the CPC report corpus (report
  text) and the 1990 CD homeownership measure.

Nothing from the earlier ZAP-text and ChatGPT direction work
(`summarize_zap_rezoning_direction_scope` and related audits) is used. Its
project classification was compared afterwards, as described below.

## Measurement

**Zoning date of each release.** This is the month the release's zoning
reflects: PLUTO's `ZoningDate` for 03c-10v1, the 02b metadata, and for 11v1
onward the month DCP wrote the lot files. PLUTO lags adoption. The 03c release
(`ZoningDate` September 2003) still lacks the April 2003 Park Slope amendment,
which first appears in 04c.

**Residential FAR.** Maximum residential FAR is a fixed lookup from a lot's
primary zoning district (`zonedist1`) to the ResidFAR that PLUTO itself reports
for that district. It is the modal ResidFAR among single-district lots in 24v1,
the last release before the December 2024 City of Yes text changes. Districts
absent from 24v1 take the mode from the closest release that has one. Paired
M/R districts that no release covers take their R district's FAR, as PLUTO
does for the pairs it has. Unpaired manufacturing districts retired before 13v1
get 0. Five rare codes (C4-7A, C6-7.5, NZS, PARKNY, DROP LOT) have no FAR and
are left out.

Holding the lookup fixed means capacity changes only when the mapped district
changes. Zoning text changes are not counted. PLUTO's own ResidFAR moves for
other reasons as well:
- The lookup matches ResidFAR on 100% of lots in every release from 19v1 to
  24v1.
- Before 19v1, PLUTO added the 20% attic allowance in R2X, R3, R4 and C3
  districts. Outside those districts the lookup matches 99.1% of lots in 13v1
  through 18v1.1.
- 25v4 reflects City of Yes and matches on only 29% of lots.
The FAR is the base maximum. It ignores bonuses (inclusionary, MIH,
community facility), wide-street and height-factor options, and special-district
rules. Height-factor districts carry PLUTO's top value: R6 2.43, R7 3.44,
R8 6.02. So an R6-to-R6B change (2.43 to 2.0) counts as a downzoning, and
R6-to-R6A (2.43 to 3.0) as an upzoning. On paper this capacity is only a
ceiling.

**Lot changes.** Consecutive releases are compared lot by lot on BBL. Lots
kept in both releases hold 99.2-99.9% of lot area in every interval. Lots
coded PARK in either release are excluded, because flips between PARK and a
residence district on large park parcels were the main unexplained changes.
There are 407,541 lot-interval changes in `zonedist1`, of which 11,811 have no
coordinate.

**Attribution.** A change is attributed to an adopted amendment when three
conditions hold:
- the amendment's footprint contains the lot's PLUTO coordinate;
- it took effect between the two releases' zoning dates, extended to 180 days
  before the earlier date and 31 days after the later one to allow for PLUTO's
  lag;
- if several amendments qualify, it is the latest one inside the interval, or
  otherwise the closest one outside it.

Only 0.3% of changes have more than one candidate. Measured as full-lot
capacity change, 80% is attributed. Excluding 17v1-18v1.1, when PLUTO switched
to assigning zoning from the GIS layer and only 34% is attributed, the figure
is 85%. The slack windows supply 7% of attributed change. Unattributed changes
are kept only as a noise gauge in the CD summary, at full lot area.

**Area inside the footprint (the counting rule).** An attributed lot counts
only for the part of it inside the amendment footprint. That part is the area
of the lot polygon intersected with the `nyzma` polygon, in square feet (State
Plane Long Island). Its capacity change is (FAR after - FAR before) x that
area. Lot polygons come from the MapPLUTO shapefile nearest in time to the
interval that has the BBL: 02b for 288,349 lot changes, 18v1.1 for 80,058 and
25v4 for 4,965. The 32 lots in none of the three are flagged
(`lots_without_polygon`, `full_lot_change_without_polygon` in the rezoning
table) and add no area; they carry 0.17 million sq ft of full-lot change.

Polygon area differs from PLUTO LotArea in two ways:
- It drops the underwater part of waterfront tax lots. LotArea includes that
  part (BBL 5004870110, the Stapleton homeport: LotArea 6.74 million sq ft,
  polygon 2.32 million, 1.78 million inside the footprint).
- It keeps only the rezoned part of lots that straddle a footprint boundary
  (BBL 4000720001: 12.6% inside).

Split-zoned lots (a second district in either release) have 95% of their
polygon area inside the footprint, against 99.7% for other lots. Across all
attributed changes, the absolute capacity change falls from 914 to 868
million sq ft. The same polygon areas give the CD denominators: July 2002 lot
area (5.08 billion sq ft) and capacity (5.90 billion sq ft) of 02b lots with a
mapped district other than PARK. The footprint totals in the rezoning table
use them too.

Changes on large lots that are not split-zoned were checked by hand. All are
real site rezonings:
- Western Rail Yards (1006760003), 553,000 sq ft, M2-3 to C6-4;
- Hunters Point South (4000010001, 4000060001, 4000110001);
- Hudson Yards blocks;
- LIC 2025 (4000250015);
- Bronx Metro-North 2024 (2040420200).
Gateway Estates II (3044520170, 2009) remains the largest single change,
+8.3 million sq ft, because the whole 4.3 million sq ft polygon went from
R3-2 to R6 with C4-2 and R7A. Today's zoning map has that area 56% C4-2 (the
Gateway Center mall, an R6-equivalent district), 25% R6 and 17% PARK. It is
real on-paper capacity, most of it under a mall.

**Direction.** Direction follows Furman Center, *How Have Recent Rezonings
Affected the City's Ability to Grow?* (Armstrong, Been, Madar and McDonnell,
March 2010), the lot-level analysis behind Been, Madar and McDonnell (2014,
*JELS*).
- *Lots:* upzoned if capacity rises to at least 110% of the pre-rezoning
  capacity, downzoned if it falls below 90%, contextual otherwise.
  Manufacturing-to-manufacturing lots (no residential FAR before or after)
  are in no class. Land area upzoned, downzoned and contextually rezoned sums
  area inside footprints by lot class. Contextual land is rezoned with FAR
  unchanged or within 10%, for example R2 to R2A or R3-2 to R3X.
- *Amendments:* the class compares net capacity change with the rezoned lots'
  pre-rezoning capacity inside the footprint:
  - *upzoning*: net change of +10% or more;
  - *downzoning*: -10% or less;
  - *mixed*: neither, but gross additions and gross removals are each at least
    10%;
  - *contextual_neutral*: otherwise;
  - *no_district_change_measured*: no attributed lot area. There are 141, most
    of them private overlay-only or small-site changes.

**Local request in the CPC report.** A regex runs over the report sentences for
each amendment. It flags a sentence stating that the rezoning or study was
undertaken at the request of, or in response to requests or concerns of, local
actors (council members, elected officials, community boards, civic
associations, residents). It excludes sentences about modifications,
applicants or developers, and sentences citing another ULURP number. The regex
was developed on two random samples of 30 DCP-sponsored reports with no hit.
`code/cpc_request_hand_check.csv` records a reading of all 69 hits and of a
third, fresh sample of 30 DCP non-hits. Precision is 67/69 (97%). In the fresh
sample, 5 of 30 non-hits (3 clear, 2 weak) do state a request. Recall among
DCP amendments is therefore about 81-88%, and the flag undercounts requests.
The build stops if the regex's hit set changes without a new reading.
`cpc_request_names_member` marks request sentences naming a council member or
"elected officials". That phrase also covers state legislators.

## Validation

Numbers below are after the area-inside-footprint rule; the earlier full-lot
figure is in brackets where it moved by more than 1 million sq ft.

| Amendment | Measured | Net capacity (M sq ft) | Acres up / down / rezoned |
|---|---|---:|---:|
| Downtown Brooklyn 2004 (040171ZMK) | upzoning | +11.8 | 42 / 3 / 50 |
| Hudson Yards 2005 (040499AZMM) | upzoning | +18.9 [19.9] | 62 / 0 / 64 |
| Greenpoint-Williamsburg 2005 (050111AZMK) | upzoning | +25.3 [31.6] | 204 / 47 / 289 |
| Jamaica Plan 2007 (070314AZMQ) | upzoning | +28.7 | 383 / 121 / 709 |
| Hunters Point South 2008 (080362ZMQ) | upzoning | +11.9 [16.3] | 31 / 0 / 31 |
| New Stapleton Waterfront 2006 (060471ZMR) | upzoning | +5.8 [20.7] | 45 / 0 / 45 |
| Gateway Estates II 2009 (090079ZMK) | upzoning | +8.3 [12.0] | 99 / 0 / 99 |
| East New York 2016 (160035ZMK) | upzoning | +19.4 | 153 / 1 / 273 |
| Park Slope 2003 (030194AZMK) | mixed (4th Avenue up, brownstone blocks down) | +1.3 | 101 / 186 / 297 |
| Bay Ridge 2005 (050134AZMK) | downzoning | -16.8 | 141 / 385 / 543 |
| Middle Village-Glendale 2006 (060153ZMQ) | downzoning | -4.7 | 64 / 245 / 326 |
| Sunset Park 2009 (090387ZMK) | mixed | -1.1 | 89 / 260 / 349 |
| Bayside 2005 (050149ZMQ) | contextual_neutral (R2 to R2A, same FAR) | +0.2 | 27 / 3 / 737 |
| Douglaston-Little Neck 2006 (060562ZMQ) | contextual_neutral | 0 | 0 / 3 / 272 |

Each amendment lands in the interval after its effective date. Furman counted
76 city-initiated rezonings in 2003-2007, about 6 billion sq ft of citywide
capacity in 2003, and a net gain of 1.7% (about 100 million sq ft). Here, the
73 DCP amendments of 2003-2007 add a net 97 million sq ft, or 1.65% of the 5.90
billion sq ft of July 2002 capacity. Among their district-changed lots, 23%
are up, 32% down and 45% contextual (by area: 24/26/50), against Furman's
14/23/63. Furman's denominator also includes lots inside a rezoning whose
district did not change. Of the 637 amendments that also appear in the earlier
ZAP-text classification, 366 had a direction there. The two agree on 311 of
them (85%).

## Outputs

- `rezonings.csv`: one row per amendment. It has:
  - effective date, sponsor, and primary and all CDs;
  - footprint lots, area and capacity;
  - changed lots (up, down, contextual) and lots without a polygon;
  - area rezoned, upzoned, downzoned and contextual (sq ft);
  - capacity added, removed and net (sq ft, % of rezoned lots, % of
    footprint);
  - direction;
  - Council matters, outcome, modifications and local member position;
  - the CPC request flag with its sentence.
- `rezoning_cd_term_summary.csv`: 59 CDs x 6 terms. Counts by direction are
  assigned to the primary CD. Land area upzoned, downzoned and contextual, and
  capacity added, removed and net, go by each lot's own CD, with removal split
  by initiation. All are given in sq ft and as % of the CD's July 2002 lot area
  (`_pct_lot_area`) or capacity (`_pct_capacity`). The last column is
  unattributed net change.
- `rezoning_homeownership_gradients.csv`: coefficient on `treat_z_boro` with
  borough fixed effects and robust SEs, per term and pooled over 2002-25, in
  three specifications: all 59 CDs; without CDs 301, 302, 401 and 402; and
  with log 1990 median household income added.
- `rezoning_local_control_link.csv`: amendments by direction, tercile and
  initiation. For each cell it gives the number DCP-sponsored, with a CPC
  request, naming a member, with a recorded local position, opposed by the
  local member, approved over local opposition, modified by the Council, and
  filed as an amended (A) application.
- `rezoning_land_area_by_tercile_term.pdf`: land area upzoned, downzoned and
  contextually rezoned as % of lot area, by tercile and term.
- `rezoning_capacity_by_tercile_term.pdf`: capacity added, removed and net by
  tercile, and removed capacity by initiation.
- `rezoning_homeownership_gradients.pdf`: per-term and pooled gradients for
  land area and capacity.
- `rezoning_counts_by_tercile_term.pdf`: amendment counts by direction, for all
  applicants and for DCP only.

Terciles are within-borough `ntile(treat_pp, 3)`, as in the long-units
series. Tercile shares pool the CDs' 2002 area or capacity, so Manhattan weighs
heavily in capacity shares. The regressions weight CDs equally.

## Findings (September 26, 2026; revised the same day for the area rule)

Land area rezoned, as % of the tercile's July 2002 lot area (low / middle /
high):

| Term | Upzoned | Downzoned | Contextual |
|---|---|---|---|
| 2002-05 | 2.2 / 1.8 / 1.1 | 1.8 / 1.3 / 2.3 | 5.2 / 9.7 / 7.7 |
| 2006-09 | 2.5 / 4.6 / 1.9 | 2.6 / 4.1 / 1.3 | 1.5 / 4.2 / 5.2 |
| 2010-13 | 2.4 / 1.5 / 0.9 | 1.2 / 2.0 / 0.6 | 1.5 / 0.7 / 8.2 |
| 2014-17 | 0.5 / 0.5 / 0.03 | 0.1 / 0 / 0 | 0.4 / 0.3 / 0.1 |
| 2018-21 | 0.9 / 0.6 / 0.03 | 0 / 0 / 0 | 0 / 0.1 / 0.4 |
| 2022-25 | 0.4 / 0.2 / 1.0 | 0.1 / 0 / 0 | 0.1 / 0 / 0 |
| 2002-25 | 8.9 / 9.2 / 5.0 | 5.7 / 7.4 / 4.2 | 8.7 / 15.0 / 21.6 |

Capacity added and removed, as % of the tercile's 2002 capacity, 2002-25:
13.8 / 13.2 / 8.4 added and 2.5 / 3.6 / 3.2 removed. Amendment counts are
204 / 174 / 91 upzonings, 17 / 15 / 15 downzonings and 10 / 25 / 39
contextual.

Pooled gradients per 1 SD of homeownership, in percentage points of the CD's
lot area or capacity (SE). The last column is the 2002-25 CD mean:

| Outcome | All 59 CDs | Without 301/302/401/402 | + log 1990 income | Mean |
|---|---:|---:|---:|---:|
| Lot area upzoned | -3.26 (0.95) | -3.21 (1.08) | -2.64 (1.52) | 10.1 |
| Lot area downzoned | 0.00 (0.95) | -0.05 (1.05) | -1.98 (1.63) | 6.5 |
| Lot area contextual | +6.03 (1.25) | +5.60 (1.24) | +7.96 (2.33) | 8.6 |
| of which DCP, local request cited | +2.76 (0.94) | +2.39 (0.95) | +4.45 (1.95) | 4.5 |
| Capacity added | -3.33 (1.44) | -1.59 (1.10) | -3.94 (2.79) | 12.0 |
| Capacity removed | +0.55 (0.52) | +0.62 (0.56) | -0.47 (0.81) | 3.4 |
| Upzoning count | -2.59 (0.82) | -1.71 (0.61) | -2.13 (1.52) | 8.0 |
| Downzoning count | +0.04 (0.16) | +0.03 (0.18) | +0.01 (0.20) | 0.8 |

**Homeowner districts got less land upzoned and more land contextually
rezoned, not more land downzoned.** One SD of homeownership means 3.3 points
less of a CD's land upzoned and 6.0 points more contextually rezoned. Both
survive dropping the four big-upzoning CDs and adding income; the upzoned-area
estimate is marginal with income. Downzoned land shows no gradient.
Contextual rezoning keeps the maximum FAR but restricts building type and
envelope, for example R2 to R2A or R3-2 to R3X. Capacity measures miss it by
construction.

By term, the contextual gradient is +2.2, +1.8 and +2.0 points in 2002-05,
2006-09 and 2010-13, then about zero. Downzoned and contextual area both end
after 2013. The upzoned-area gradient is -0.4, -1.3, -1.1, -0.3 and -0.5 through
2018-21.

**Capacity.** Capacity added falls by 3.3 points per SD, against a mean of
12.0. Without the four CDs it is -1.6 (1.1). Private and other non-DCP
additions have a steady gradient: -0.18 (0.06) in 2014-17 and -0.14 (0.04) in
2018-21. Capacity removed was 62, 82 and 38 million sq ft in the first three
terms and about 1 million per term after 2013. Its gradient is +0.55 (0.52).
In 2022-25 the high tercile's added capacity rises to 3.8%, from Midtown South
(CD 105) and the 2025 Jamaica plan (CD 412).

**Link to member deference.** No local Council Member voted against any
downzoning, mixed or contextual amendment: 0 of the 115 with a recorded
position. They opposed 7 of 364 upzonings, and all 7 were adopted anyway.
Adopted amendments therefore show no case of the Council overriding a member
on a protective rezoning. The Council disapproved 12 zoning-map applications
from 2004 to 2024, and the local member opposed each of the 9 with a recorded
position. They were never adopted, so they have no footprint, their direction
cannot be measured here, and ZAP's public universe does not list them.

DCP's protective amendments (down, mixed or contextual) cite a local request
in the CPC report in 36 of 74 cases, and name a council member or elected
officials in 23. DCP upzonings do so in 25 of 63. By tercile, the protective
request rate is 5/11 low, 18/28 middle and 13/35 high. The contextual-area
gradient is partly in these requested DCP rezonings: +2.8 of the +6.0
points. The rest comes from DCP studies without a cited request and from
other applicants. Downzoned area by requested DCP rezonings has no gradient,
-0.32 (0.85).

**Modifications and withdrawals.** The Council modified 75 of 471 upzonings
(16%) and 4 of 48 downzonings. The modified share of upzonings is similar
across terciles: 17% low, 16% middle, 15% high. The data cannot say whether
modifications shrank upzonings. ZAP and PLUTO record only the adopted map, not
the proposal. Withdrawals cannot be classified by direction either: withdrawn
applications never get a footprint, and ZAP has no proposed zoning. The
existing ZAP audit (`audit_zap_universe_coverage`) shows ZM withdrawal shares
rising with homeownership in 2014-25 (3.5%, 5.9%, 10.8%). Their direction is
unknown.

**Reading.** Homeowner districts got less land upzoned and much more land
contextually rezoned in 2002-2013. They did not get more land downzoned.
Contextual rezonings were mostly DCP studies, often requested locally, and
never drew a local no vote. That fits local influence over what gets proposed
and protected. It does not show the Council deferring to members against
upzonings in homeowner districts. The mechanism that could link the two, fewer
private applications or withdrawals where members are hostile, is not directly
observed.

## Limits

- Capacity is the base maximum residential FAR of the primary district, applied
  to the lot area inside the footprint. A split lot's secondary districts,
  bonuses, wide-street rules and special-district FARs are not modeled.
- "Contextual" means the primary district changed with FAR within 10%. It
  does not measure how restrictive the new envelope is.
- Attribution relies on the 2026 `nyzma` footprints and on PLUTO lot
  coordinates. About 15% of measured change outside the 2017-18 method switch
  is unattributed.
- Adopted amendments only. Disapproved, withdrawn and modified-away capacity is
  unobserved.
- The CPC request flag undercounts requests (recall about 81-88%). A request
  statement is the report's own account, not proof of who initiated the study.
