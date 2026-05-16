# Fetch Council Land-Use Records

Fetches official Council-process source files used to seed and validate the
member-deference-overrule time series. This is the Council-process complement
to the ZAP pipeline: Legistar matter pages, meeting minutes, transcripts,
committee reports, and the 2025 Charter land-use history report are retained in
task output source-file folders or `data_raw/`, depending on whether the task
script creates the files, with checksums.

The task deliberately starts with high-confidence seed records. It now also has
year-specific Legistar broad-recall pulls, starting with 1998 and the 2001
validation year. Each pull fetches all public search-result pages for Land Use
Application, Land Use Call-Up, and Resolution matter types, stages detail pages
for rows flagged as land-use-relevant, parses the Legistar history grid, and
fetches final Council approval action details so member-level vote records can
be screened. Script-created Legistar HTML snapshots are retained under
`output/source_files/`. Manually acquired pre-1998 City Record or Municipal
Library files should be added to the archival source inventory once those pulls
begin.
