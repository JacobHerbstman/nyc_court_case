# Fetch MapPLUTO Archive

Downloads DCP PLUTO and MapPLUTO release files listed in the source catalog.

The input is `source_catalog.csv`. The output is `mappluto_files.csv`, an index
of cached raw files in `data_raw/`.

The production housing-production path currently uses the current 25v4
MapPLUTO shapefile rows from this inventory. The same inventory also records
archived MapPLUTO releases so historical-release analyses use the same
raw-source contract.

Cached rebuild runtime: about 45 seconds.
