# Source Registry

Stores committed source catalogs used by data-fetching tasks.

The key input files are `source_catalog.csv`, `archive_requests.csv`,
`manual_manifest.csv`, and `benchmark_catalog.csv` in `code/`. This registry has
no default generated output.

Runtime: `make` completes immediately because the task stores committed catalog
files.
