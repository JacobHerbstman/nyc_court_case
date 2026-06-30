# Build NHGIS Extracts

Fetches and standardizes NHGIS tract extract files used for the 1990
homeownership measure.

Inputs are `source_catalog.csv`, the task's NHGIS extract request JSON files,
and the task-local table map. Creates `nhgis_1990_tract_extract.parquet`, the
cleaned 1990 tract covariate file, and `nhgis_1990_tract_gis_zip.csv`, the GIS
zip pointer used to read tract geometry downstream.

If the raw NHGIS zips are already cached in `data_raw/`, the task uses those
files. If they are missing, the fetch step submits the stored extract request
through `ipumsr` and requires `IPUMS_API_KEY`.

Runtime: about 6 seconds when the raw NHGIS zips are already cached. A first
run that submits an NHGIS extract depends on NHGIS API processing time.
