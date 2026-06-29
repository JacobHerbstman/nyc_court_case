# Build CCD2010 MapPLUTO BBL Lookup

Builds an audit-side lookup from current MapPLUTO BBLs to 2010 Council
districts.

This lookup is used by ZAP and rezoning audit tasks that need strict
BBL-to-2010-district assignment. It is not part of the main housing-production
pipeline, so it lives under `tasks/audits/` rather than in the production
MapPLUTO construction-proxy task.

Inputs are the current MapPLUTO archive and the 2010 Council district
homeownership file. The task places parcel surface points in 2010 Council
district geometries and creates `ccdist2010_mappluto_bbl_lookup.parquet`.

Recent rebuild runtime is about 2 minutes.
