# Build ULURP CPC Report Corpus

Downloads and extracts the long-run, comparable CPC report corpus for parsed
ULURP application numbers.

The task starts from the audit-stage ULURP corpus spine, constructs official
NYC Planning CPC report PDF URLs from application numbers, downloads each
available report, extracts text to one file per report, and records explicit
failure rows for missing or unreadable reports.

This is intentionally separate from modern ZAP uploaded application packets.
Those packet documents are richer for recent cases, but they are not available
consistently for historical LUCATS-era records.
