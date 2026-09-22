---
title: "Withdrawals within zoning projects"
author: "Research note prepared by Codex for Jacob Herbstman and Tyler Jacobson"
date: "September 15, 2026"
---

Withdrawals remain less common among later ZAP cohorts when the comparison
focuses on zoning projects, but the decline is smaller than in the full ULURP
sample. Among projects with a recorded zoning-map change, the withdrawn share
is 13.5% for the 1990-2001 reported certification/referral cohorts, 9.4% for
2002-2013, and 6.3% for 2014-2025. The corresponding shares for all explicit
ULURP projects are 18.9%, 8.3%, and 4.8%. The denominators include ongoing
projects, and the later cohorts have less follow-up. These are descriptive
current-status comparisons, not final failure probabilities or a 2001 effect.

Jacob asked to continue using the corrected CPC dataset without another large
manual review now, and to preserve withdrawals while comparing more substantive
projects. The CPC preservation checks support proceeding with the documented
working sample: they establish retention, linkage, and reproducibility of the
collected text. They do not validate every substantive text label or prove
complete historical source coverage. Manual validation of the measures used in
the eventual analysis remains later work. No manual case classification or paid
model extraction was used for the withdrawal comparison here.

## Recorded action types

The existing ZAP coverage audit now classifies every public project using
literal action fields and the action-code suffix in application numbers,
supplemented by the saved API action records. The public bulk action field is
often blank even for completed projects; using that field alone would discard
much of the historical sample. Each source's codes remain separate alongside
their union. The parser follows the suffix structure described in the saved
DCP metadata and preserves unparsed tokens and differing code sets.

The principal comparison retains all explicit ULURP projects, then requires
any ZM/ZR/ZS action, then requires ZM alone. These mean zoning-map changes,
zoning-text changes, and special permits. The narrower zoning-map series offers
a transparent first proxy for projects changing development permissions.
Neither restriction establishes physical size or guarantees that a change is
nontechnical. A zoning project with a disposition stays included; a project
with only disposition actions does not enter either zoning sample. Other
substantive projects can use different action types and are outside these
proxies. PP-only projects and projects with no action evidence are separately
retained in the comparison CSVs.

Action evidence is available for 12,165 of 12,273 explicit ULURP projects
(99.1%), including 1,513 of 1,515 withdrawn or terminated projects (99.9%).
The two terminal projects with no action evidence remain in the master table:
P2016K0347, 4217 New Utrecht Avenue Rezoning, and P2018K0100, 215 Moore Street.
Both also lack the reported cohort date. A project name alone does not supply
a verified action code, and neither is recoded as small or technical.

The 18 explicit ULURP projects with differing nonempty action-code sets retain
those differences. The sources agree on membership in both zoning samples for
all 18. Two projects have an EAS token outside the two-letter action-code
format; one has a malformed bulk number, N20014 ZRM. Those values remain
flagged rather than corrected by guessing. The raw snapshots were unchanged.

## Composition, dates, and CPC links

The large 1991 spike does not appear in the zoning-restricted series. Among
the 95 withdrawals sharing November 13, 1991 as the reported certification/
referral date, 70 have PP only, 23 have DM only, and two have PL only. None has
a recorded ZM/ZR/ZS action. This connects the suspicious cluster to project
composition but does not establish why the date was recorded. The date is
retained, and no historical correction is made.

All 32,964 public ZAP project records remain in the coverage table. Every
withdrawal/termination remains whether or not it has a CPC report, an action
code, or a usable date. The new project fields preserve linked CPC document
and application identifiers; recovered API action rows now also carry the
matched report identifier and PDF URL. Among explicit ULURP withdrawals and
terminations, 361 projects match an entry in the CPC manifest. Checking those
matches against the narrative-source table on September 15 gives 357 projects
with a retained narrative link. The other four match five recorded applications
whose CPC source files are known to be unavailable: C 960601 ZMQ, C 960620 ZMQ,
C 000074 PPM, and the paired C 020278 PSK / C 020279 HDK. Thus the earlier
361-project count measures identifier linkage, not 361 readable reports.
All 1,515 withdrawn or terminated explicit ULURP projects remain in the project
table: 1,367 withdrawn and 148 terminated. Restricting to readable CPC narratives
would exclude 1,158 of these projects. Such a link does not mean that the project
ultimately succeeded or that the narrative itself records withdrawal. CPC-stage
decisions and final project status remain distinct.

The plot excludes 253 explicit ULURP projects with no reported cohort date,
including 84 withdrawals, and the 42 projects dated after 2025. Those records
remain in the period table. The zoning sample has 147 missing-date projects,
including 51 withdrawals; the zoning-map sample has 108, including 31
withdrawals. Missing dates constrain the time comparison even though action
coverage is high. The year is never filled using completion dates or digits
from an application number.

![Withdrawals by recorded action, with one project per sample and reported entry-year cohort.](input/zap_withdrawals_by_action.pdf)

## Zoning withdrawals by homeowner tercile

The follow-up comparison splits the ZM and ZM/ZR/ZS samples by the existing
1990 community-district homeownership terciles within borough. Projects with
multiple listed districts are assigned only when every district belongs to
the same tercile. The geography assignments and citywide comparison data are
unchanged. Among 1975-2025 dated projects, 1,661 of 1,734 zoning-map projects
are assigned, including 202 of 210 withdrawals. The broader sample assigns
2,970 of 3,070 projects, including 362 of 370 withdrawals. Unassigned projects
remain in the saved tables; missing dates remain in the period table.

For the 2014-2025 cohorts, zoning-map withdrawal shares are 3.5% in the low
tercile (6/171 projects), 5.9% in the middle (8/135), and 10.8% in the high
(10/93). The broader zoning/special-permit sample has shares of 3.5% (7/202),
6.9% (14/203), and 8.2% (12/147). These recent differences are descriptive;
they do not establish that homeownership caused withdrawals. Earlier periods
do not show the same consistent ordering, as the generated table shows.

Annual rates are volatile because some cells contain few projects. For example,
the 50% zoning-map withdrawal share in the high tercile in 2016 is two out of
four projects. The figure therefore shows the denominators beneath the rates.
Both samples include ongoing projects and use current withdrawal status and
reported entry dates; recent cohorts have less follow-up.

![Zoning withdrawals and project denominators by the existing homeowner terciles.](input/zap_withdrawals_by_action_homeowner_tercile.pdf)

Jacob requested a three-year moving average to make the annual pattern easier
to see. The added figure uses a centered, equally weighted average: the point
at 2000 averages the annual values for 1999, 2000, and 2001. This averages
annual withdrawal shares, rather than pooling projects across those years.
The lower panels similarly average annual project counts. Full windows are
required; endpoint windows are not shortened, and a missing annual rate leaves
the smoothed rate missing. The original annual data and figure remain intact.

![Centered three-year moving averages of annual withdrawal percentages and project counts.](input/zap_withdrawals_by_action_homeowner_tercile_ma.pdf)

## Reproduction and checks

Sources are the unchanged September 14, 2026 public ZAP snapshot (release
20260706), the saved September 14 API detail responses, and the corrected CPC
manifest. The action-suffix definition comes from the `ulurp_numbers` field in
the saved DCP project metadata. The code base remains on `cpc_llm_training`,
with HEAD `6077733` and the current uncommitted preservation/withdrawal changes.
Run `make` in `tasks/audits/audit_zap_universe_coverage/code/`, then
`make output/2026-09-15-withdrawal-project-types.pdf` in `logbook/`.

Project and action records are preserved; unrestricted annual counts reproduce
the prior audit. Sample membership, unresolved records, report links, and period
totals were checked. CSVs have deterministic data reports. A missing period CSV
regenerates its producer once with identical data and findings. After the image
refresh, an unchanged build does no work. The figure and note were inspected.
The tercile extension preserves the prior citywide CSVs and geography table
byte for byte. Independent project-level aggregation checks each new annual
and period cell; summing assigned and unassigned groups reproduces the
unrestricted zoning counts. The following tables are generated from the
comparison dataset through Make.

\newpage
