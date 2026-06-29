# Build NHGIS Extracts

Fetches and standardizes NHGIS tract extract files used for the 1990
homeownership measure.

Inputs are `source_catalog.csv`, the task's NHGIS extract request JSON files,
and the task-local table map. Creates `nhgis_raw_files.csv`, which indexes the
raw extract files, and `nhgis_1990_tract_extract.parquet`, the cleaned 1990
tract covariate file used downstream.
