# Fetch Council Land-Use Records

Fetches official Council-process source files used to seed and validate the
member-deference-overrule time series. This is the Council-process complement
to the ZAP pipeline: Legistar matter pages, meeting minutes, transcripts,
committee reports, and the 2025 Charter land-use history report are retained in
`data_raw/` with checksums.

The task deliberately starts with high-confidence seed records. It now also has
a 2001 Legistar broad-recall pilot that fetches all public search-result pages
for Land Use Application, Land Use Call-Up, and Resolution matter types, stages
detail pages for rows flagged as land-use-relevant, and checks that the known
M 820995 LaGuardia hotel seed case appears. The pilot also parses the Legistar
history grid from each detail page into an audit table. Manually acquired
pre-1998 City Record or Municipal Library files should be added to the same
source inventory format once archival pulls begin.
