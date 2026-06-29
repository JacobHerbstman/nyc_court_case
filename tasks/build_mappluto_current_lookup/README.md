# Build Current MapPLUTO Lookup

Standardizes the current 25v4 MapPLUTO release into a parcel lookup for
downstream geography and address joins.

The input is `mappluto_files.csv`. The output is
`mappluto_current_lot_lookup.parquet`, with standardized BBLs, addresses,
community districts, lot attributes, and geometry-derived fields.

Rebuild runtime: about 20 seconds.
