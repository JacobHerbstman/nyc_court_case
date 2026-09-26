# Estimate Community-District Housing Trends

Estimates the paper's raw-unit event-study and long-difference specifications
on the 59 community districts. Treatment is standardized 1990 homeownership
within borough; outcomes use the full-period 25v4 MapPLUTO year-built proxy.

The event study runs once for each bin scheme listed in the Makefile:

- `5yr_bins` (paper): five-year bins from 1970, omitted 1985-1989, with a
  pre-production control measured over 1970-1988.
- `decade_bins`: 1970s, 1980s (omitted), 1990s, 2000s, 2010s, 2020-2025.
- `decade_pre_bins`: 1970s, 1980-1984, 1985-1989 (omitted), then five-year bins.

The 1970-1988 control overlaps the estimated pre-period bins and the omitted
bin. The decade schemes instead use mean annual production in 1960-1969, before
the first estimated bin. Before 1985, MapPLUTO year-built values heap at years
ending in 0 and 5, and DCP treats them as accurate only to the decade. A
full-decade average limits that problem, and no estimated bin uses those years. The drivers audit reports both control windows
and no control.

Creates `cd_homeownership_long_units_event_coefficients_raw_units_<scheme>.csv`
and `.pdf` for each scheme, plus the long-difference CSV and LaTeX table.
