# NYC Court Case Project

This repository is the reproducible data-collection scaffold for the NYC member deference project.

## Principles

- Lots first: canonical raw outputs stay at the lot, job, certificate, tract, or archival-record level.
- Archival ready: the workflow includes FOIL, Municipal Archives, and Municipal Library lanes from day one.
- Reproducible by task: each source family has its own task with a minimal `Makefile`, explicit inputs, and linear scripts.
- Manual drops are allowed when necessary, but every such file must be documented in tracked manifests under `tasks/source_registry/code/`.

## Start Here

1. Run `make` in `tasks/setup_environment/code/`.
2. Run `make` in `tasks/source_registry/code/`.
3. Review `tasks/source_registry/code/source_catalog.csv`, `manual_manifest.csv`, and `archive_requests.csv`.
4. Place any required manual files into `data_raw/<source_id>/<vintage_or_pull_date>/`.
5. Run source-family tasks from their `code/` folders.

## Task Order

- `tasks/source_registry`
- `tasks/fetch_mappluto_archive`
- `tasks/stage_mappluto_lots`
- `tasks/fetch_dob_open_data`
- `tasks/fetch_dcp_boundaries`
- `tasks/fetch_census_bps`
- `tasks/stage_nhgis`
- `tasks/stage_furman_coredata`
- `tasks/archive_locator`
- `tasks/stage_archival_records`
- `tasks/build_lot_crosswalks`
- `tasks/build_source_coverage_audit`
