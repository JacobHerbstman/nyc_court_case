# Build ZAP Datasets

Downloads and standardizes NYC Planning ZAP project and project-BBL records.

The input is `source_catalog.csv`. The outputs are `zap_project_data.parquet`,
a keyed project-level table, and `zap_project_bbl.parquet`, the project-BBL
link table.

Runtime: about 5 seconds.
