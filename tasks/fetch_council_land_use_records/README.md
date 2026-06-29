# Fetch Council Land-Use Records

Reads NYC Council Legistar pages for 1998-2025 land-use-relevant matters.

The inputs are public Legistar pages queried by year. The task extracts matter
records, history events, final Council action details, and member votes. It
creates annual
`legistar_*_broad_recall_matter_index.csv`,
`legistar_*_broad_recall_history_events.csv`,
`legistar_*_broad_recall_action_details.csv`, and
`legistar_*_broad_recall_member_votes.csv` files.
