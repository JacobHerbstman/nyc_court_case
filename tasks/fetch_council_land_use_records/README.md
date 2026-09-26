# Fetch Council Land-Use Records

Reads NYC Council Legistar pages for 1998-2025 land-use-relevant matters.

The inputs are public Legistar pages queried by year for Land Use Applications,
Land Use Call-Ups, and land-use-related Resolutions. Resolution rows enter when
the committee or title indicates land-use content, including ULURP, CPC, UDAAP,
and related 197-c/197-d language.

The task saves raw Legistar HTML under `output/source_files/` and extracts
matter records, history events, final Council action details, and member votes.
It creates annual
`legistar_*_broad_recall_matter_index.csv`,
`legistar_*_broad_recall_history_events.csv`,
`legistar_*_broad_recall_action_details.csv`, and
`legistar_*_broad_recall_member_votes.csv` files.

The action-detail file keeps the latest Council approval event for each matter
(the lowest `history_sequence`, since Legistar lists history newest first). A
saved action-detail page is checked against the HistoryDetail ID it was fetched
for; a page saved for a different event stops the run. Two pages saved under the
earlier event rule (M 0745-2003, M 0746-2003) were moved to
`superseded_action_detail_pages/` on 2026-09-26. Application numbers and Council
districts in titles are parsed with the shared `_lib/member_deference_utils.py`
patterns. Each CSV has a data report in `report/`.

Runtime: about 25-35 minutes for a full rebuild when the Legistar HTML files
are already saved locally.
