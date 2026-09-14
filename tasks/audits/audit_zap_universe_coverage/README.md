# Audit ZAP universe and identifier coverage

Run `make` from `code/`. Inputs are the unfiltered September 14, 2026 public
project universe and targeted API details, the May 1, 2026 raw project snapshot,
and the existing application spine and CPC report manifest.

Outputs are:

- `zap_project_coverage.csv`: every current project, with separate bulk fields,
  API recovery measures, and links to existing samples.
- `zap_status_coverage.csv`: missingness and inclusion counts by reported status
  and explicit ULURP, other process, or unknown scope.
- `zap_dates_by_year.csv`: status counts for each date field separately,
  including missing dates. No date is inferred from a project ID or another
  milestone. These are descriptive inventory counts, not a failure hazard.
- `zap_date_clusters.csv`: repeated dates with at least ten projects within a
  status/scope/date-field group; this is a diagnostic threshold, not a cleaning
  or exclusion rule.
- `zap_snapshot_changes.csv`: additions, removals, and changed columns between
  the two bulk snapshots, keyed by project ID.
- `zap_recovered_actions.csv`: project-action records returned by the API,
  with raw application numbers, action codes/statuses, exact normalized-number
  matches to existing CPC reports, source URLs, and source hashes.

Project and report keys are checked before linkage. Report memberships are
explicit sets, and actions remain keyed by `(project_id, action_id)`; projects
are not multiplied by their actions, parcels, or reports. Application-number
matching removes whitespace/punctuation and one leading C/N/M/I before digits.
An absent match is unresolved and is never called proof of no report. Existing
CPC project links and newly recovered action links are shown separately.

The canonical coverage output owns the multi-output producer on GNU Make 3.81.
Secondary files depend on it; if a secondary file alone is removed, its recipe
runs the same producer to regenerate the missing data. Reports and logs are
side effects, never Make targets. `report/summary.json` contains the principal
counts, and each CSV gets a deterministic data report.

This audit does not change paper samples, infer approval from `Complete`,
assign missing ULURP classifications, or treat historical date clusters as
proven errors. It establishes what the sources contain and what still needs
reconciliation before project failure rates or process durations are estimated.
