# Build NHGIS Extracts

Fetches and standardizes NHGIS tract extract files used for the 1990
homeownership measure.

Inputs are `source_catalog.csv`, the task's NHGIS extract request JSON files,
and the task-local table map. Outputs are `nhgis_1990_tract_extract.parquet`
and `nhgis_1990_tract_gis_zip.csv`.

If the raw NHGIS zips are already cached in `data_raw/`, the task uses those
files. If they are missing, the fetch step submits the stored extract request
through `ipumsr` and requires `IPUMS_API_KEY`.
