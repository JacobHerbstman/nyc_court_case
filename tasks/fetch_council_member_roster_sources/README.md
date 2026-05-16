# Fetch Council Member Roster Sources

Fetches source files for a historical NYC Council member roster.

The official 1998-present spine is the Legistar City Council office-record grid,
using the public all-term roster view. Wikipedia district-history pages are
fetched as a secondary broad-recall source for pre-Legistar coverage and are
kept explicitly flagged for audit.

Because these files are created by this scripted task, fetched HTML is retained
under this task's `output/source_files/` tree rather than `data_raw/`.
