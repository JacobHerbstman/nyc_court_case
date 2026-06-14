# Accepted Council Land-Use AI Geography Repairs

This task stages the accepted AI/manual geography repair ledger for Council land-use matters that were missing affected Council districts in the main Legistar/ZAP sources.

The task is intentionally upstream of the member-deference panel. The ledger in `code/accepted_ai_geography_repair_ledger.csv` is a reviewed source file, not an output of the downstream audit workflow that discovered the cases. `code/normalize_accepted_ai_geography_repairs.py` validates uniqueness by `query_year`, `vote_date`, and `matter_file`, checks that district assignments are valid Council District numbers, and writes the normalized output consumed by the panel builder.
