# Fetch MapPLUTO Current

Downloads the pinned DCP MapPLUTO 25v4 release used by the production paper
path.

The input is `source_catalog.csv`. The output is `mappluto_files.csv`, an index
of cached raw files in `data_raw/`.

This production task intentionally fetches only one official archived 25v4
MapPLUTO shapefile ZIP. Broader historical MapPLUTO archive discovery belongs
in audit tasks unless a paper result needs those releases.

Runtime: about 45 seconds when the 25v4 ZIP must be downloaded.
