# Recover Member-Deference Non-Approval Geography

Recovers affected Council districts for non-approval land-use matters that do
not already have districts in the Legistar matter index.

Inputs are the non-approval final-action queue, ZAP project records, ZAP
project-BBL links, and the current MapPLUTO lot lookup.

Creates `member_deference_nonapproval_geography_recovery.csv`. The matching
hierarchy uses existing Legistar geography, ZAP application matches, ZAP BBL
matches, BBLs parsed from matter titles, exact address matches, and small
rule-based address variants.

Current MapPLUTO matches are location-based backups rather than direct
historical district statements, so the output carries source flags for downstream
review.

Runtime: about 1 minute.
