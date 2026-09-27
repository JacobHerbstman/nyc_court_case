---
title: "Homeowner districts got less land upzoned and more land contextually rezoned, not more land downzoned"
date: "2026-09-26"
---

We asked whether high-homeownership community districts were downzoned more
after 2002, and whether any such downzoning ran through member deference or
local control. To measure rezoning without ZAP text or LLM review, we compared
lot zoning across 24 PLUTO/MapPLUTO releases, one a year from July 2002 to
February 2026. Fourteen tabular releases, 2003-2017, were newly fetched from
DCP BYTES. Each lot's maximum residential FAR comes from a fixed lookup of its
primary zoning district, taken from PLUTO's own ResidFAR in 24v1. The lookup
matches ResidFAR on every lot in 19v1-24v1. A lot's change is attributed to an
adopted zoning map amendment when the lot sits inside the amendment's DCP
`nyzma` footprint and the amendment took effect between the two releases'
zoning dates. That covers 85% of measured change, outside the 2017-18 switch
in PLUTO's zoning method. Only the part of the lot polygon inside the footprint
counts. A first version applied the FAR change to PLUTO's whole LotArea, which
includes underwater land. That credited the Stapleton homeport with 20.7
million sq ft instead of 5.8 million and Hunters Point South with 16.3 million
instead of 11.9 million. The correction lowered the capacity-added gradient
from -4.1 to -3.3 points per SD. Lots are classed up, down or contextual
(FAR within 10%) with the Furman Center (2010) rule.

The known cases come out as expected:
- Greenpoint-Williamsburg 2005 adds 25.3 million sq ft on 204 upzoned acres.
- Bay Ridge 2005 removes 16.8 million on 385 downzoned acres.
- Bayside 2005 contextually rezones 737 acres with no capacity change.
- DCP's 2003-2007 rezonings add a net 1.65% of 2002 capacity, against
  Furman's 1.7%.

Measured by land, the difference between homeowner districts and others is
contextual rezoning, not downzoning. Over 2002-2025 the high within-borough
tercile had 21.6% of its lot area contextually rezoned, against 15.0% in the
middle and 8.7% in the low tercile. It had 4.2% downzoned against 7.4% and
5.7%, and 5.0% upzoned against 9.2% and 8.9%. Across the 59 CDs, with borough
fixed effects, one SD of homeownership adds 6.0 points of contextually rezoned
land (SE 1.25; mean 8.6), removes 3.3 points of upzoned land (SE 0.95; mean
10.1), and does nothing to downzoned land (0.00, SE 0.95). Both the contextual
and upzoned gradients survive dropping CDs 301, 302, 401 and 402, and adding
log 1990 income; the upzoned one is marginal with income. The capacity
gradient in upzoning is weaker and depends on those four CDs (-3.3 falls to
-1.6). Downzoning and contextual rezoning both end after 2013. The upzoned-land
gradient persists, smaller, through 2018-21, and private upzonings add almost
no capacity in the high tercile from 2014 to 2021.

On the local-control link, no local Council Member voted against any adopted
downzoning, mixed or contextual amendment (0 of 115 with a recorded position).
Members opposed 7 of 364 upzonings, and each passed anyway. The 12 zoning-map
applications the Council disapproved in 2004-2024 were never mapped, so their
direction cannot be measured. A hand-checked regex over CPC reports (precision
97%, recall roughly 81-88%) finds a local request in 36 of 74 DCP protective
amendments and 25 of 63 DCP upzonings. DCP rezonings that cite a local request
account for 2.8 of the 6.0-point contextual-area gradient.

Claude's reading is that local control appears here as homeowner districts
getting protective contextual zoning, often at local request, and fewer
upzonings. It does not appear as the Council overriding members or as heavier
downzoning. The data cannot show why upzonings are rarer: developers may not
apply, members may discourage applications, applications may be withdrawn, or
demand may be lower. Only adopted maps are observed. Jacob has not reviewed
these choices; the base-FAR measure, the 10% rule and the treatment of
contextual rezoning are the main ones to check.

Reproduction: `make` in `tasks/fetch_mappluto_archive/code` (downloads,
checksums in `source_snapshot_sha256.json`), then `make` in
`tasks/audits/summarize_rezoning_scope_by_homeownership/code`. The code
revision is d44ecac plus uncommitted files. Full tables and the measurement
rules are in that task's README.
