# NYC Court Case Project

This repository is the reproducible data-collection scaffold for the NYC member deference project.

## Principles

- Lots first: canonical raw outputs stay at the lot, job, certificate, tract, or archival-record level.
- Archival ready: the workflow includes FOIL, Municipal Archives, and Municipal Library lanes from day one.
- Reproducible by task: each source family has its own task with a minimal `Makefile`, explicit inputs, and linear scripts.
- Heavy source access should happen once: every active source family should split into `fetch`, `load_raw`, `stage`, and `summarize` tasks when those phases exist.
- Versioned public sources first: current `PLUTO` and `MapPLUTO` come from DCP's versioned release system, not the live Socrata table export.
- Manual drops are allowed when necessary, but every such file must be documented in tracked manifests under `tasks/source_registry/code/`.
- NHGIS is scripted through `ipumsr` and reads `IPUMS_API_KEY` from `~/.Renviron`; do not create a repo-local secret file.
- Human QA and integrity deep dives are preserved under `tasks/archive/` when you want them, but the active `tasks/` tree is limited to source intake, cleaning, canonical builds, and summary outputs.
- Retired integrity and backup tasks live under `tasks/archive/`, which is ignored so the active `tasks/` surface stays readable.

## Start Here

1. Run `make` in `tasks/setup_environment/code/`.
2. Run `make` in `tasks/source_registry/code/`.
3. Review `tasks/source_registry/code/source_catalog.csv`, `manual_manifest.csv`, and `archive_requests.csv`.
4. Make sure `IPUMS_API_KEY` is available in `~/.Renviron` for the NHGIS task.
5. Place any required manual files into `data_raw/<source_id>/<vintage_or_pull_date>/`.
6. Run source-family tasks from their `code/` folders.

## Active Task Order

- `tasks/source_registry`
- `tasks/fetch_mappluto_archive`
- `tasks/load_mappluto_raw`
- `tasks/stage_mappluto_lots`
- `tasks/summarize_mappluto_lots`
- `tasks/fetch_dob_permit_issuance_current`
- `tasks/load_dob_permit_issuance_current_raw`
- `tasks/build_dob_permit_issuance_harmonized`
- `tasks/summarize_dob_permit_issuance`
- `tasks/fetch_dob_open_data`
- `tasks/load_dob_open_data_raw`
- `tasks/stage_dob_open_data`
- `tasks/summarize_dob_open_data`
- `tasks/fetch_dcp_boundaries`
- `tasks/load_dcp_boundaries_raw`
- `tasks/stage_dcp_boundaries`
- `tasks/summarize_dcp_boundaries`
- `tasks/fetch_census_bps`
- `tasks/load_census_bps_raw`
- `tasks/stage_census_bps`
- `tasks/summarize_census_bps`
- `tasks/fetch_nhgis_extracts`
- `tasks/load_nhgis_raw`
- `tasks/stage_nhgis`
- `tasks/summarize_nhgis`
- `tasks/fetch_census_acs_housing`
- `tasks/load_census_acs_housing_raw`
- `tasks/stage_census_acs_housing`
- `tasks/fetch_dcp_housing_database`
- `tasks/load_dcp_housing_database_raw`
- `tasks/stage_dcp_housing_database`
- `tasks/load_furman_coredata_raw`
- `tasks/stage_furman_coredata`
- `tasks/archive_locator`
- `tasks/load_archival_records_raw`
- `tasks/stage_archival_records`
- `tasks/build_lot_crosswalks`
