---
title: "Which CPC reports enter our analysis?"
author: "Research note prepared by Codex for Jacob Herbstman and Tyler Jacobson"
date: "September 14, 2026"
---

This is a dated account of the current CPC report collection, narrative dataset,
and the restrictions used by their analysis consumers. No sample definition was
changed during this review. The central distinction is between a list of
projects and a collection of reports. A project can request several actions,
produce several reports, and have the same narrative repeated across reports.
Conversely, a publicly observed project can stop before producing any CPC
report. The final narrative dataset is not one row per project.

## 1. The two original sources are separate

The CPC collection starts with DCP's
[CPC Reports search](https://a030-cpc.nyc.gov/html/cpc/index.aspx), not the old
ZAP application spine. DCP describes these reports as records of Commission
actions, including ULURP matters and other matters. A CPC determination is not
necessarily the final disposition of a project; see
[About CPC Reports](https://a030-cpc.nyc.gov/html/cpc/about-reports.html).

The ZAP project source is NYC Open Data dataset
[hgx4-8ukb](https://data.cityofnewyork.us/City-Government/Zoning-Application-Portal-ZAP-Project-Data/hgx4-8ukb).
The older staged snapshot has 32,892 projects and is dated May 1, 2026. The
separate September 14 full-public-export snapshot has 32,964 projects. The
CPC corpus currently uses the older staged ZAP data to attach project metadata
by application number. Failure to match ZAP does not remove a CPC report:
521 corpus entries and 439 final analysis narratives have no stored ZAP link.

This means the missing-withdrawal problem in the old application spine is not
an explicit withdrawal-status filter in the CPC report collection. A project
without a report nevertheless cannot contribute a CPC narrative. Some projects
that eventually withdrew did produce CPC reports and remain eligible.

## 2. What the index download requests

The downloader searches each calendar year from 1975 through 2025 using the
website's Vote Date field. Borough, community district, and application type
are all set to ALL; application number and project-name keyword are blank.
It follows the available pagination links and saves 14,469 index entries.
These are index rows, not necessarily distinct PDF files or projects.

Reports before 1975 and reports dated 2026 are outside this download. The site
itself extends back to 1938. DCP's
[search instructions](https://a030-cpc.nyc.gov/html/cpc/how-to-search.html)
explain that older reports use CP numbers and can share a meeting-date PDF.
Thus the site's historical extent is broader than our downloaded window and
broader still than the identifier format selected in the next step.

There is a separate source-availability limit: DCP explicitly says many zoning
text amendment reports (ZR) from 1987 through 2003 are unavailable. Its site
also warns about errors in OCR of older scans. Matching our saved index cannot
establish that every historical CPC report is present in the public archive.
The recorded 106 unavailable entries below are not a census of all archive gaps.

The acquisition code raises errors for invalid dates, duplicate fetched row
keys, failed searches, and empty requested pages. It skips HTML table rows
without the expected report links and metadata nodes, and stops when no next
numbered page is exposed. It saves the parsed CSV, rather than a dated archive
of every HTML response. Therefore the counts here describe the captured index;
this review did not independently prove historical website completeness or
re-download the archive.

## 3. The ULURP-related report restriction

The builder applies 55 recorded source corrections to identifiers, links,
dates, or classification. It then keeps a canonical application identifier
beginning with C followed by six digits. This is an identifier-based selection
rule; it does not independently verify every certification milestone.

An N-prefixed report is also kept when it is a designated lead for a paired
certified application with the same normalized project name and vote date, or
when a recorded source decision explicitly includes it as the related lead.
Other N reports, CP-numbered reports, and other formats are outside this
selection. This excludes substantial non-ULURP land-use material, including
standalone zoning-text reports; it is not a complete rezoning or CPC archive.
Although the requested window starts in 1975, the resulting collection begins
in 1976 because none of the captured 1975 identifiers passes this rule.

Twelve index entries have explicit exclusion decisions: ten were reclassified
from a C identifier to non-ULURP zoning-text matters after reading the printed
report, one was a duplicate index entry, and one was a post-approval modification.
Separately, 111 documented index omissions are added from official source
reconciliation. Six have usable reports; 105 are retained as entries with no
available authentic report.

| Step | Entries removed or added | Entries remaining |
|---|---:|---:|
| Saved 1975-2025 index | | 14,469 |
| Other identifiers outside the selected scope | -3,274 | 11,195 |
| Explicit source exclusions | -12 | 11,183 |
| Documented index omissions | +111 | 11,294 |

The 11,183 retained index rows comprise 11,148 C-identifier reports and 35
related N leads. After additions, the manifest contains 11,258 certified-ULURP
entries and 36 related narrative leads. Corrected dates are checked again
against 1975-2025; that additional check removes zero rows in this snapshot.
There is no approval, applicant-type, housing-use, homeowner, or borough
restriction at this stage.

## 4. A usable report is needed to supply text

The manifest retains all 11,294 entries. Text is available for 11,188. The 106
unavailable entries comprise the 105 index omissions without public reports
plus one indexed entry whose authentic source is unavailable. They are kept in
the manifest but cannot supply a narrative.

The builder tries embedded PDF text, repairs sparse pages with OCR, and uses
full-document OCR when needed. Embedded text must contain at least 50 words
and at least 75% ASCII-compatible characters. The full-OCR path requires at
least 50 words. Targeted OCR ordinarily repairs sparse pages only through the
detected main-report resolution page, so this is not a promise that every
attachment has been fully transcribed. Expected usable sources with failed
downloads or text extraction fail the build rather than silently dropping out.

The label builder also requires the recorded text file to exist, be locally
readable, and contain at least 100 bytes. These are mechanical readability
checks, not guarantees that all words were transcribed correctly.

## 5. The narrative restrictions

The parser normally retains the substantive text before the Commission's
formal resolution. It uses resolution headings, filing paragraphs, adoption
language, or commissioner signatures to locate the end, with 24 recorded
exceptions for unusual documents. This is a restriction on the text used for
measurement as well as a potential reason to exclude a report. The original
available PDF and extracted text remain separate from the selected narrative.

For lead-report groups sharing normalized project name and vote date, the
non-lead rows are not separate analysis observations. A recorded companion
decision is handled similarly. Eligible related narratives can still provide
evidence to the retained observation. DCP's designated-lead convention begins
in July 2003, so lead-based consolidation is itself tied to a change in source
format over time.

The following is a sequential, nonoverlapping accounting. Related-report
removal is counted first, so the one manually excluded companion is included
in the 730 rather than counted again among the seven source exclusions.

| Step | Rows removed | Rows remaining |
|---|---:|---:|
| Manifest | | 11,294 |
| Authentic source unavailable | 106 | 11,188 |
| Related report represented by a lead or recorded companion | 730 | 10,458 |
| Narrative end could not be identified | 0 | 10,458 |
| Incomplete source or supplemental statement without main report | 7 | 10,451 |
| Selected narrative shorter than 100 words | 1 | 10,450 |
| Repeated normalized narrative: retain one representative | 1,545 | 8,905 |

The seven source exclusions are six incomplete scans and one supplemental
statement without its main report. The short narrative is C 840641 PSQ,
"Clark Lift Bldg," with 86 selected words. A short narrative need not be an
incorrect document; the 100-word threshold is an analytical choice.

Exact-text grouping ignores case, whitespace, and certain page headers. It
operates across the eligible sample, without requiring the same project or
vote date. There are 420 repeated-text groups and 1,545 redundant rows. One
group has different recorded dates: C 890407 PPQ on May 17, 1989 and
C 890406 PPQ on May 18, 1989, both named C-O-P. That date discrepancy remains
unresolved. Within a duplicate group the code prefers a lead, then sorts by
application number. The final result is 8,905 narrative observations, not
8,905 independently verified projects.

Related evidence is assembled through shared ZAP projects on the same vote
date, explicit application references on that date, lead groups, identical
narratives, and recorded companion relationships. The same boundary and
minimum-length requirements apply to companion narrative text. The current
dataset retains companion application identifiers and text hashes; 2,906
observations have companion narrative text.

## 6. Restrictions that apply only to particular analyses

The citywide trend program draws three separate samples from the 8,905 rows:

| Figure sample | Narratives |
|---|---:|
| All narratives surviving the restrictions above | 8,905 |
| Exclude PP-only groups | 7,254 |
| Groups containing ZM, ZR, or ZS actions | 2,636 |

These flags can inherit the action scope of paired certified reports. They
are not necessarily the action code printed on the retained lead alone. The
plot label "All CPC narratives" is too broad: it refers to the first row of
this table, not to every narrative in DCP's archive.

The CPC homeowner-tercile plots additionally require assignment to one or
more of the 59 standard community districts. They first use linked project
parcels matched to current MapPLUTO, excluding joint-interest and nonstandard
areas. Matched parcels determine fractional district weights. When there is
no parcel-based assignment, the code uses parsed, corrected official report
districts with equal weights across valid districts. It does not require all
of a report's parcels or district tokens to match before assigning it. There
are 5,113 parcel-based assignments and 3,785 official-district fallbacks:
8,898 narratives assigned and seven omitted from these plots. Each assigned
narrative's district weights sum to one. This differs from the direct ZAP
district assignment used for the exploratory withdrawal plots.

Participation-count means use only resolved values: 4,525 narratives have
resolved proposal-aligned Community Board votes and 6,032 have resolved CPC
speaker totals. Both are resolved for 3,445 narratives. Other observations
remain in the narrative dataset with review statuses; missing counts are not
zero and are not included in these count means. Companion evidence is used
only for absent focal evidence and when resolved companion results agree.

Most contextual signal curves pool a centered three-year window and require
observations in all three years plus at least 20 narratives (weighted counts
for terciles). Count-coverage and length curves have their own 20-observation
rules; count means require a positive resolved-count denominator and do not
apply that same 20-resolved-record minimum. These are display/denominator rules,
not removals from the saved narrative dataset. Boilerplate occurring in more
than 5% of narratives and certain procedural sentences is skipped when
constructing contextual signals; the corresponding documents remain present.
The 135-word context setting also affects measurement rather than membership.

The current narrative rows include 173 labeled CPC denials and 5,853 unknown
CPC dispositions, as well as 2,879 labeled approvals. There is no known-approval
requirement for entering the narrative dataset. These disposition labels are
parser outputs, not newly verified outcomes.

The separate Community-Board-opposition approval audit further restricts to
rows coded as opposed and to an observed outcome for its approval denominator.
Its Council comparison starts in 1998 and uses exact application-number
matches to single-application LU records. Unresolved end-of-session filings
are outside approval-rate denominators. Its all/non-PP/zoning comparisons and
project-level transition summaries are additional analysis samples, not
upstream restrictions on report acquisition. The code can override opposition
coding for Jacob's completed human training records. These choices should be
reported whenever that audit's results are used.

The regex validation sheets are also separate samples. The two 100-report
Codex benchmarks restrict to reports with their own ZM/ZR/ZS action code and
omit unclear reference values from agreement calculations. The fresh 150-row
human sheet is stratified and excludes previously coded or inspected related
bundles. Benchmark agreement is not a full-corpus accuracy estimate.

## 7. Precisely where the old ZAP spine lost withdrawals

This is a parallel path, not a prerequisite selecting CPC report rows:

| Old May 1 ZAP path | Projects |
|---|---:|
| Staged public project snapshot | 32,892 |
| Explicit ULURP classification | 12,241 |
| Nonmissing reference year in 1975-2025 | 12,134 |
| At least one parsed application number | 10,529 |

Its reference date uses certification/referral first, then filing, notice,
approval, completion, and the staged reference date. It expands each project's
parsed application-number list into rows. An empty list yields no rows. That
step loses 1,605 projects: 1,330 withdrawn, 126 terminated, 7 terminated for
applicant nonresponse, 101 complete, 37 active, and 4 on hold. The surviving
10,529 projects produce 12,899 application rows after identifier parsing and
duplicate-row removal. There is no explicit status filter; the missing-number
requirement caused the selection. The full September project universe retains
projects regardless of identifiers, dates, or status.

## Implications and verification

The earlier 99.1% usable-text statement applies to 11,188 of 11,294 entries
inside our chosen report scope. It does not establish 99.1% coverage of all
CPC narratives or ULURP projects. Source publication, date and identifier
scope, narrative selection, geography assignment, and measurement availability
need to be described separately.

The existing `audit_official_ulurp_cpc_text_measurement` task already records
report-to-narrative inclusion and exclusion reasons in its output file
`official_ulurp_cpc_narrative_manifest.csv`.
Its 8,905 included identifiers match the production labels. However, 152 of its
recorded narrative hashes differ from current production even though the source
text hashes agree. The production parser has updated handling of quoted
resolutions that the separate audit parser does not share. The membership
agreement therefore does not establish agreement about the exact text used.

The next safeguard should reconcile that existing ledger with production,
extend traceability back through the raw-index restrictions, and show counts
by year and action type beside each result. This is a recommendation, not an
implemented change or a new approved sample definition. There is no need to
create another competing audit framework. No archived exploratory task was
reactivated here.

This note freezes a read-only reconstruction against the production code at
commit `6077733`, with the separate withdrawal-plot work still uncommitted.
The narrative selection was executed only through construction of its in-memory
document list, before measurement or output writes. Its 8,905 identifiers match
every identifier in the current saved labels. Index decisions reconcile to
the manifest, and the old ZAP spine was reconstructed without saving it.
Official website documentation was checked on September 14, 2026.

The relevant producers are `fetch_ulurp_cpc_report_index`,
`build_ulurp_cpc_report_corpus`, `summarize_text_cpc_trends`, and the separate
`build_ulurp_corpus_spine`. Their task-local Makefiles show the actual source
dependencies. Source decisions reside in `record_ulurp_cpc_source_corrections`.
This is a static research record; rebuilding its PDF does not rerun acquisition
or silently update the counts.

SHA-256 fingerprints of the inspected saved CSV bytes:

- Official index: `213852c7299cb3e799b46188ff55e2f406778f6b38e78cc0f4a0afbb709b82e5`
- Report manifest: `d5942deecf5fcad50fcb8134b38a42a3f23f48b9eadabb8a70acb15c970b0426`
- Narrative labels: `0bb8948934dac903eace748da400bb734667f3fa2feefd1813bbad8e2365181d`

## Follow-up on the N-report rule and repeated narratives

Later on September 14, Jacob asked for direct checks of these two restrictions.
The earlier sections preserve the baseline account. The follow-up found that
the lead flag misses useful N reports: C 130190 ZMM, for example, directs the
reader to N 130189(A) ZRM for the Governors Island background. Both first pages
were inspected, and the N report also contains the project hearing discussion
and Commission consideration. The corpus includes the C report but excludes
that N report. Twenty excluded N sources have explicit referrals in reviewed
certified-report passages; not all twenty were downloaded or independently
established as the sole main narrative. Other possible referrals remain unreviewed.

All 420 repeated-text groups have identical narrative prefixes even before
normalization. Counting that text once is supported. Treating the other
application rows as disposable is not: 380 representatives omit ZAP project
IDs attached to other group members. Four PP representatives also have a PQ
application in the same group, so their current non-PP flags exclude narratives
that cover acquisition as well as disposition. C 940499 PPM and C 940500 PQM
provide one concrete example. This does not establish how many geography
assignments would change; that requires rebuilding after the member links are
preserved. No such sample or geography change was made during this audit.

The audit's older boundary rule has now been replaced by the same shared
function used in production. The affected tasks were rebuilt. All 11,294
report narrative hashes now agree with the production reconstruction, and the
entire 8,905-row label CSV remains byte-for-byte identical to the fingerprint
above. The existing 31 count-parser checks also pass. This fixes the audit
discrepancy; it does not fix the N exclusions or lost member metadata.

The existing text-measurement audit now owns the exclusion inventory, C-to-N
reference inventory, duplicate-group comparisons, and findings below. Manual
passage and attachment reviews are preserved in the source-validation audit
ledger with source hashes and explicit limits. These are Codex reviews, not an
independent human validation sample. The proposed correction is to preserve
every application-to-narrative link and add verified N companions through
documented relationships; a mention or a shared name alone should not merge
observations. The recorded metadata discrepancies still need production fixes
before relying on the affected action and geography restrictions.

For reproduction, run `make` in each of these directories, in order:

- `tasks/summarize_text_cpc_trends/code/`
- `tasks/audits/audit_official_ulurp_cpc_text_measurement/code/`
- `logbook/`

## Implemented preservation changes

Jacob then authorized the corrections. The existing corpus builder now reads
one small, reviewed companion table from `record_ulurp_cpc_source_corrections`.
Its twenty C-to-N relationships record the referring source hash, referral
passage, resolved public PDF URL, and review date. All twenty N PDFs were
retrieved and their extracted openings and narrative boundaries inspected.
They provide usable context. They are attached after the existing narrative
groups are formed, so a new N source cannot join otherwise separate cases.

The corpus now contains 11,314 source records. Every one of the original
11,294 manifest records is unchanged, including the 106 documented unavailable
sources. The analysis still contains the same 8,905 narrative identifiers and
focal-text hashes. Twenty new N sources contribute to 28 existing narratives.
For example, the text attached to C 130190 ZMM (Governors Island) increases
from 886 to 10,759 words when its explicitly referenced N report is included.
Removing only these new sources reconstructs every original bundled-text hash;
no existing measured text was lost.

The same text-label producer now also writes
`ulurp_cpc_narrative_sources.csv`: 19,276 unique narrative/source pairs,
covering 11,212 distinct sources. Of these, 11,180 are represented applications;
other links provide context. Each link retains the original application,
project IDs, action, district, vote date, and text hashes. Flags distinguish
represented applications from context sources and identify which unique text
excerpts were used, in what order. Every source collapsed as a duplicate,
designated-lead action, or recorded companion has a representation link.

The labels combine project IDs and action flags across represented applications.
Source-specific district corrections are applied before combining districts
in `represented_community_districts`; original values remain in the source
table and focal district column. Context-only sources do not expand action
scope or geography. The changes have the following consequences:

- All 380 duplicate groups previously missing member project IDs now retain
  them. Across duplicate and lead groups, 449 narrative project-ID fields change.
  Narratives with no project ID decline from 439 to 432.
- The four mixed PP/PQ narratives now enter the non-PP sample, increasing it
  from 7,254 to 7,258. The ZM/ZR/ZS sample remains 2,636.
- Geographic weights change for 359 narratives. Assignment still covers
  8,898 of 8,905 narratives, with total weight one per assigned narrative.
  Parcel-based assignment increases from 5,113 to 5,235; district fallback
  falls from 3,785 to 3,663. The same 59 districts retain their 20/20/19 terciles.
- Speaker-count extraction becomes resolved for five additional narratives,
  increasing coverage from 6,032 to 6,037. Community Board count coverage
  is unchanged. All text-measure changes occur among the 28 augmented narratives.
  Resolved extraction still requires external validation before it can be
  treated as accurate coding.

The source-link audit reconstructs every saved analysis-text hash and checks
metadata unions, unique keys, represented-application coverage, and inclusion
of every reviewed N relationship. The corpus source audit has no failed checks,
including source identity and sparse-page validation. Both geographic consumers
were rebuilt using the corrected combined district field, and each assigned
narrative's weights still sum to one. The existing 31 count-parser checks pass.
Deterministic dataset reports accompany the new and revised CSV outputs.
Deleting the new source-link output and rebuilding regenerates both primary
CSVs once, byte-for-byte identical to the verified versions. The same check
passes for a missing secondary CSV in the approval audit. After refreshing
dependent figures, dry-run Make checks schedule no primary or corpus producer
reruns or input relinks. Reproduction uses the task-local Makefiles, including
`audit_ulurp_cb_opposition_approval`; the corpus check targets
`official_ulurp_cpc_corpus_summary.csv` without refreshing unrelated ZAP queries.

These corrections recover the reviewed omissions without changing the basic
unit of observation. They do not certify complete historical coverage: 784
excluded indexed N identifiers are still mentioned by certified reports and
need assessment, and additional references are absent from the saved index.
The review inventory remains available rather than silently deleting those
candidates. Withdrawn projects without CPC reports remain a separate issue
for analyses of whether applications advance.

The following findings are connected to the audit through Make; unlike the
frozen baseline prose, they refresh when their declared inputs change.

\newpage
