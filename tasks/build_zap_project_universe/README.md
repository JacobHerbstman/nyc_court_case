# Full public ZAP project universe

Run `make` from `code/`. This task downloads the **unfiltered public** ZAP
project export, companion project-parcel export, their metadata and independent
Socrata row counts, and the project data dictionary. It preserves all received
bytes under `data_raw/dcp_zap_full_export/20260914/`.

The source is NYC Department of City Planning, NYC Open Data datasets
[`hgx4-8ukb`](https://data.cityofnewyork.us/d/hgx4-8ukb) and
[`2iga-a6mk`](https://data.cityofnewyork.us/d/2iga-a6mk). The extract date is
September 14, 2026; both metadata descriptions identify release `20260706`.
The complete CSV export endpoints use **no status, year, ULURP, identifier,
geography, or document filters**. Independent count requests use only
`$select=count(*)`, avoiding the default row limit of a paginated JSON query.

`output/zap_project_universe.csv` has one row per nonmissing unique `project_id`.
All 35 original source columns are retained as strings. Additional columns
identify source provenance, missing identifiers, terminal statuses, unknown
ULURP scope, a suspicious historical date cluster, and parcel coverage. There
is no restriction on year and no constructed fallback year. `Complete` is
retained verbatim; this task does not relabel it as approved. A blank
certification date does not establish that a project failed before certification.

The raw parcel table contains repeated project-parcel pairs. It stays intact.
For project-level coverage only, rows are counted and the distinct nonblank
BBL strings are counted within each project. This is an explicit aggregation,
not a many-to-many merge or a validated geography assignment.

The separate raw directory prevents the older `build_zap_datasets` task's
automatic latest-vintage selection from silently changing existing analyses.
`report/raw_export.json` records received-file checksums, counts, parcel-key
duplicates, and column coverage; `report/zap_project_universe.json` profiles
the saved dataset. These reports and the execution log are build side effects.

The URLs are mutable. `code/sources.make` declares the acquisition recipes
separately so changes to analysis/report settings do not refresh downloads.
`code/source_snapshot_sha256.json` pins all received bytes; downloads are
validated before publication, and a changed upstream file fails rather than
silently replacing this vintage. Preserve the saved raw files for exact
reproduction; refresh into a new explicitly dated snapshot and compare reports.
The public
export excludes records not released for general public visibility. Matching
the official export count therefore establishes export completeness, not
complete historical coverage of every proposal ever considered by DCP.

Downstream, `fetch_zap_missing_project_details` retrieves detail records for
terminal and unclassified projects, and `audits/audit_zap_universe_coverage`
compares the universe with the previous raw snapshot and existing report sample.
