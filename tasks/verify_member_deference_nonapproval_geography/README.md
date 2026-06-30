# Verify Member-Deference Non-Approval Geography

Verifies geography leads for unresolved non-approval land-use matters against
official records and creates the conservative queue used for vote fetching.

Inputs are the non-approval final-action queue, deterministic geography recovery
file, ChatGPT review-response ledger, matter universe, current MapPLUTO lot
lookup, and Council member roster. ChatGPT was used to suggest locations for
matters without a clear affected district, but those suggestions enter this
task only as leads. A row is verified only when an official Legistar record
lists a Council district for the same or related matter, or when official matter
text gives BBLs that can be mapped to current MapPLUTO districts. Unsupported
suggestions remain unresolved.

Creates `member_deference_nonapproval_geography_conservative_queue.csv`. The
current queue covers 491 core non-approval matters: 236 original Legistar/ZAP
geography assignments, 126 deterministic recovery assignments, 37
official-verification assignments, and 92 unresolved rows.

Runtime: about 15 seconds when the official-page cache is already filled. A
first run can take longer because it downloads official pages.
