# Fetch MapPLUTO Archive

Downloads DCP lot and zoning files from BYTES of the BIG APPLE into `data_raw/`.

- `fetch_mappluto_archive.R` downloads the pinned MapPLUTO 25v4 shapefile ZIP
  used by the paper and writes `output/mappluto_files.csv`, an index of that file
  (input: `source_catalog.csv`). Runtime: about 45 seconds when the ZIP is missing.
- Make rules download 14 archived tabular PLUTO releases, one per year from
  2003 to 2017 (03c, 04c, 05d, 06c, 07c, 09v1, 10v1, 11v1, 12v1, 13v1, 14v1,
  15v1, 16v1, 17v1), from
  `https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/pluto/nyc_pluto_<release>.zip`
  into `data_raw/dcp_mappluto_archive/<release>/`. The release links are the ones
  listed in the DCP archive index saved at
  `data_raw/dcp_mappluto_archive/20260501/mappluto_archive_index.json`.
- A Make rule downloads the DCP NYC GIS Zoning Features release 202608
  (shapefiles, including the zoning map amendment layer `nyzma`) from
  `https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/gis-zoning-features/nycgiszoningfeatures_202608shp.zip`
  into `data_raw/dcp_gis_zoning_features/202608/`.

The tabular files were fetched on September 26, 2026 (about 780 MB for the 14
PLUTO ZIPs, 5.2 MB for the zoning features). `code/source_snapshot_sha256.json`
pins their bytes; a download is published only after `unzip -t` and the
checksum pass, so a changed upstream file fails instead of replacing the
recorded one. Use a new release directory for a new vintage.

No 2008 release is fetched. The only 2008 PLUTO file on BYTES (08B) is a
September 2020 re-export without the `ZoningDate` field, so the month its zoning
reflects cannot be established from the file. Releases 07c (zoning as of
January 2008) and 09v1 (April 2009) bracket 2008. The 2002 MapPLUTO (02b) and
the 2018-2025 MapPLUTO shapefile releases were already in `data_raw/`.

Run `make` from `code/`.
