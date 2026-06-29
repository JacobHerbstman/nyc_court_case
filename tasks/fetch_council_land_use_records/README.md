# Fetch Council Land-Use Records

Reads NYC Council Legistar pages for 1998-2025 land-use-relevant matters.

The inputs are public Legistar pages queried by year for three matter types:
Land Use Application, Land Use Call-Up, and Resolution. Resolution rows enter
when the committee or title indicates land-use content, including ULURP, CPC,
UDAAP, and related 197-c/197-d language.

The task saves the raw Legistar HTML under `output/source_files/` and rebuilds
the tabular outputs from those cached pages when they exist. The current source
snapshot uses May 13, 2026 cache folders for 1998-2010 and June 3, 2026 cache
folders for 2011-2025. A full cached rebuild takes roughly 25-35 minutes on the
local machine.

The task extracts matter records, history events, final Council action details,
and member votes. It creates annual
`legistar_*_broad_recall_matter_index.csv`,
`legistar_*_broad_recall_history_events.csv`,
`legistar_*_broad_recall_action_details.csv`, and
`legistar_*_broad_recall_member_votes.csv` files.
