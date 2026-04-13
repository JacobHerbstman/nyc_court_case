# Raw Data Contract

- Every source has a stable `source_id`.
- Raw files live only in `data_raw/<source_id>/<vintage_or_pull_date>/`.
- Fixed-vintage files use the official vintage in the folder name.
- Source-native versioned releases such as `25v4` or `18v2.1` count as fixed vintages and should be used directly in folder names.
- Rolling API pulls use `YYYYMMDD`.
- Multi-file API extracts keep every returned file together in the same `data_raw/<source_id>/<vintage_or_pull_date>/` folder.
- Public-source metadata snapshots such as discovery JSON or API view metadata should be saved as raw provenance files under the same `source_id`, using a pull-date folder.
- Raw files stay untracked by git; tracked manifests live in this task's `code/` folder.
- Heavy parsing or unzip steps should live in a dedicated `load_*_raw` task; lighter cleaning or standardization should happen downstream in a separate `stage_*` task.
- Downstream tasks may read from `data_raw/`, but they should never write outside their own `output/` folder.
