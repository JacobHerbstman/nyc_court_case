# Audit ZAP universe and identifier coverage

Run `make` from `code/`. Inputs are the unfiltered September 14, 2026 public
project universe and targeted API details, the May 1, 2026 raw project snapshot,
and the existing application spine and CPC report manifest.

Outputs are:

- `zap_project_coverage.csv`: every current project, with separate bulk fields,
  API recovery measures, and links to existing samples. It now also retains
  source-specific action-code sets, their union, unresolved tokens, overlapping
  zoning flags, a mutually exclusive action group, and linked CPC identifiers.
- `zap_status_coverage.csv`: missingness and inclusion counts by reported status
  and explicit ULURP, other process, or unknown scope.
- `zap_dates_by_year.csv`: status counts for each date field separately,
  including missing dates. No date is inferred from a project ID or another
  milestone. These are descriptive inventory counts, not a failure hazard.
- `zap_withdrawals_over_time.pdf` and `.png`: annual counts and shares of
  projects currently withdrawn, with withdrawals plus terminations shown
  separately, grouped by reported certification/referral year (1975-2025).
  Missing dates and the incomplete 2026 cohort are excluded from the figure;
  ongoing projects remain in each plotted cohort's denominator. Date clusters
  are retained. Each project appears once; these are not application counts
  or counts of withdrawal events occurring in that year.
- `zap_withdrawals_by_homeowner_tercile.pdf` and `.png`: the same annual
  project cohorts split by 1990 community-district homeownership tercile
  within borough, matching the current CPC plots. These lines show withdrawals
  only, with each tercile's own project count as the share denominator.
- `zap_withdrawal_project_terciles.csv`: all explicit ULURP projects, preserving
  missing dates and an explicit geography assignment reason. The reported ZAP
  district is used directly. Multiple districts are accepted only when every
  district is standard and belongs to the same tercile; other projects remain
  unassigned. There is no first-district choice or parcel fallback.
- `zap_withdrawal_tercile_year.csv` and `zap_withdrawal_tercile_period.csv`:
  project counts, withdrawals, terminations, and withdrawal shares, retaining
  an unassigned-geography group. Annual empty cells have missing shares.
- `zap_date_clusters.csv`: repeated dates with at least ten projects within a
  status/scope/date-field group; this is a diagnostic threshold, not a cleaning
  or exclusion rule.
- `zap_snapshot_changes.csv`: additions, removals, and changed columns between
  the two bulk snapshots, keyed by project ID.
- `zap_recovered_actions.csv`: project-action records returned by the API,
  with raw application numbers, action codes/statuses, exact normalized-number
  matches to existing CPC reports, matched document IDs and PDF URLs, source
  URLs, and source hashes.
- `zap_withdrawals_by_action.pdf` and `.png`: the annual withdrawal counts and
  shares for all explicit ULURP projects, projects with ZM/ZR/ZS actions, and
  projects with ZM actions. The same reported certification/referral cohorts
  and current project statuses are used in every series.
- `zap_withdrawal_action_year.csv`, `zap_withdrawal_action_period.csv`, and
  `zap_withdrawal_action_findings.md`: sample-specific denominators, withdrawal
  and termination counts/shares, action coverage, CPC-link coverage, and the
  comparison table. The CSVs also retain PP-only and unknown-action samples;
  the period table preserves missing dates and dates outside the plot range.
- `zap_withdrawals_by_action_homeowner_tercile.pdf` and `.png`: zoning-map
  changes (ZM) and zoning changes plus special permits (ZM/ZR/ZS), split by
  the existing 1990 within-borough homeowner terciles. The top panels show
  annual withdrawal shares; the bottom panels show the project denominators.
- `zap_withdrawals_by_action_homeowner_tercile_ma.pdf` and `.png`: centered
  three-year moving averages of those annual rates and counts. Each year gets
  equal weight. Full windows are required; a missing annual rate leaves the
  moving average missing. `MA_YEARS` in the Makefile sets the odd window width.
  The original annual CSVs and unsmoothed figure are retained.
- `zap_withdrawal_action_tercile_year.csv` and
  `zap_withdrawal_action_tercile_period.csv`: counts and shares for those two
  overlapping samples, including unassigned geography. The period table also
  preserves missing dates and dates outside the plot range. The assignment
  table is reused unchanged, and summing all four geography groups must
  reproduce the corresponding unrestricted sample counts. The findings file
  includes the period comparison and geography coverage.

Action classification uses the union of codes explicitly recorded in bulk/API
action fields and codes parsed from bulk/API application numbers. The saved DCP
metadata defines the number suffix as an optional amendment letter, two action
letters, and one borough letter after six digits. The parser accepts historical
prefix letters and citywide borough codes without deriving a date from the
number. Malformed numbers and non-two-letter action tokens remain explicit.
Different nonempty source sets are flagged, not overwritten or automatically
called errors; a source can list additional companion actions.

These are recorded-action proxies, not validated project-size classifications.
ZM is a zoning-map change; ZR is a zoning-text change; ZS is a special permit.
Disposition-only projects do not enter the zoning samples, but a mixed project
with both a disposition and zoning action does. Some zoning changes are small
or technical, and other substantive projects use different powers. No prose
keyword rule or manual review is required for this first comparison. Missing
action evidence never becomes a negative classification of project substance.
The samples overlap: a ZM project enters all three principal comparisons, once
per sample. Unknown actions remain in the all-ULURP denominator.

Project and report keys are checked before linkage. Report memberships are
explicit sets, and actions remain keyed by `(project_id, action_id)`; projects
are not multiplied by their actions, parcels, or reports. Application-number
matching removes whitespace/punctuation and one leading C/N/M/I before digits.
An absent match is unresolved and is never called proof of no report. Existing
CPC project links and newly recovered action links are shown separately.

Multiple-output pattern rules regenerate each producer's outputs together on
GNU Make 3.81. Reports and logs are side effects, never Make targets.
`report/summary.json` contains the principal coverage counts, and each CSV gets
a deterministic data report.

This audit does not change paper samples, infer approval from `Complete`,
assign missing ULURP classifications, or treat historical date clusters as
proven errors. It establishes what the sources contain and what still needs
reconciliation before project failure rates or process durations are estimated.
