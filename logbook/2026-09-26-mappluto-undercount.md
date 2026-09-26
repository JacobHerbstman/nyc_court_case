---
title: "MapPLUTO yearbuilt against Census year built: what the 1980s gap is and whether it drives the 5+ result"
date: "2026-09-26"
---

The CD housing series counts units by 25v4 MapPLUTO `yearbuilt`. For buildings
dated 1970-1999, MapPLUTO counts fall well short of Census year-built counts, and
more so in low-homeownership CDs. The 5+ unit event study uses 1985-1989 as its
reference period, so this could push the post-2010 coefficients toward the
finding. Using the tract-level 1990 Census year built (newly pulled from NHGIS),
MapPLUTO records 159 units per CD-year in 1985-1989 in the low within-borough
homeownership tercile, against 283 in the Census. In the high tercile the two
agree (390 versus 381). The same pattern appears for the 1980s in the 2000 Census
(ratios 0.57/0.76/0.94 by tercile) and in ACS 2020-2024 (0.53/0.60/0.81). For
buildings dated after 2000 the sources agree: MapPLUTO/ACS is 0.99/1.07/1.06 for
the 2000s and 0.94/0.96/0.86 for the 2010s, and MapPLUTO is within 8% of Housing
Database completions in every tercile for 2010-2019.

The gap does not look like new construction missing from MapPLUTO. The 2002
MapPLUTO gives the same 1980s totals as 25v4, so demolitions and later record
revisions explain nothing, and the gap is already present in the 1990 Census. By
borough, MapPLUTO's 1980s counts match Census Building Permits Survey
new-construction permits (0.98 of permits in the Bronx, 1.13 in Brooklyn, 1.01 in
Queens). For the Bronx, the 1990 Census counts 28,900 units built 1980-March 1990,
against 8,700 permitted. The excess is concentrated in 5+ buildings: the
ACS/MapPLUTO gradient is steep for 5+ buildings and flat for 1-4 unit buildings.
For the 1990s it tracks alterations of pre-1940 5+ buildings and city-owned stock.
Both patterns are what one would expect if residents of substantially
rehabilitated Section 8 and in-rem buildings report them as new. For 1985-1989,
the homeownership gradient in the gap is fully absorbed by the 1990 Census
year-built imputation rate (55% of units imputed in low CDs, 36% in high). That
rate is closely tied to renter share, so rehab reported as new and
respondent/imputation error cannot be told apart. Either way the discrepancy
comes from the Census side.

We re-estimated the production 5+ event study (replicated exactly) with the
reference period, or all pre-2000 periods, rebuilt from Census counts. The
2020-2025 coefficient moves from -358 (SE 146) to -317 (SE 145) when the whole
1985-1989 Census gap is assigned to 5+ buildings, which is an upper-bound
correction. It is -346 with a 2000-2004 reference period, -310 with Housing
Database counts after 2010, and -268 (SE 149, p=0.08) when both of the last two
changes are combined. The 2010-2014 and 2015-2019 coefficients lose significance
under most corrections. For the reference-period error alone to explain -358, its
homeownership gradient would need to be about 8 times the measured -44 units/yr
per SD. Long differences built only from surveys (ACS post, Census pre) are about
55-65% of the MapPLUTO ones and not significant. About half of that shortfall comes
from ACS and the Housing Database recording somewhat more post-2000 production than
MapPLUTO in high-homeownership CDs, not from the reference period.

Codex recommends keeping MapPLUTO as the new-construction measure; Jacob has not
decided. The paper would state the Census discrepancy and its likely source, and
report the Census-reference, 2000-2004-reference and Housing Database versions of
the 2020-2025 coefficient with the bound. Rehabilitation is still inferred
indirectly. The repo has no CD-level 1980s rehab records, and DOB permit data has
no unit counts and is incomplete before about 1993, so there is no independent
administrative count for 1985-1989.

Reproduction: `make` in `tasks/audits/audit_cd_homeownership_mappluto_undercount/code`
(code revision 2ec4b7c plus uncommitted audit files). The NHGIS 1990 STF3 extract
(NH25, NH27, NH77) and the ACS 2020-2024 B25034/B25127 tract tables were fetched
2026-09-26 using the keys in `~/.Renviron`. Details and all tables are in the task
README.
