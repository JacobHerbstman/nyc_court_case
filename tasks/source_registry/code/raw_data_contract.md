# Raw Data Contract

- Every source has a stable `source_id`.
- Raw files live only in `data_raw/<source_id>/<vintage_or_pull_date>/`.
- Fixed-vintage files use the official vintage in the folder name.
- Rolling API pulls use `YYYYMMDD`.
- Raw files stay untracked by git; tracked manifests live in this task's `code/` folder.
- Downstream tasks may read from `data_raw/`, but they should never write outside their own `output/` folder.
