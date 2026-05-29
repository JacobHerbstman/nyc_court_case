# Manual Geocode Upzonings

This task prepares the 118 increased-residential ZAP project records that are missing strict BBL-to-2010-Council-district assignment for manual address research and free geocoding.

The current strict assignment is:

`ZAP project -> ZAP BBL -> current MapPLUTO 25v4 lot -> 2010 Council district`

Rows enter this task when that chain fails. They are not assumed to be outside Council districts. They need a documented fallback location.

## Workflow

1. Use `output/manual_geocode_upzoning_queue.csv` as the research queue.
2. For each row, use the Google-query columns and browser research to find a proper geocodable address or intersection.
3. Enter only researched values in `code/manual_geocode_upzonings.csv`.
4. Re-run `make` from `code/`.
5. Rows with `geocode_review_status` equal to `ready_for_batch` or `confirmed_address` and a nonempty `proper_geocode_address` are written to `output/manual_geocode_upzoning_census_batch_template.csv`.

## Geocoding Options

The Census Geocoder is free and supports single-address and batch geocoding. It can process batch files with up to 10,000 addresses, so scale is not the problem for these 118 rows. It is best for proper street addresses, but it geocodes from MAF/TIGER address ranges, so NYC address precision can be weaker than city-native tools.

NYC GeoSearch is also free to query and is built for NYC address/place-name search using NYC Planning's Property Address Directory. It is likely a better first-pass tool for NYC addresses, vanity addresses, named buildings, and place names. It returns GeoJSON points with confidence scores. After coordinates are obtained, we still spatially join to the 2010 Council district geometry rather than taking Census geographies.

Area-wide rezonings should not silently receive a single point. Use `area_or_multi_site` or `ambiguous` status unless a documented representative project site is defensible.

Useful references:

- Census Geocoder documentation: https://www.census.gov/programs-surveys/geography/technical-documentation/complete-technical-documentation/census-geocoder.html
- Census Geocoding Services API: https://geocoding.geo.census.gov/geocoder/Geocoding_Services_API.html
- NYC GeoSearch docs: https://geosearch.planninglabs.nyc/docs/
