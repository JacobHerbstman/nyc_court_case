# Fetch MapPLUTO Current

Downloads the DCP current MapPLUTO release used by the production paper path.

The input is `source_catalog.csv`. The output is `mappluto_files.csv`, an index
of cached raw files in `data_raw/`.

This production task intentionally fetches only the current 25v4 MapPLUTO
shapefile. Broader historical MapPLUTO archive discovery belongs in audit tasks
unless a paper result needs those releases.

Runtime: about 45 seconds when the 25v4 ZIP must be downloaded.
