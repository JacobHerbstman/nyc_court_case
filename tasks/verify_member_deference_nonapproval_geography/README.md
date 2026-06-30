# Verify Member-Deference Non-Approval Geography

Verifies geography leads for unresolved nonapproval land-use matters against
official records and creates the conservative queue used for vote fetching.

Inputs are the nonapproval final-action queue, rule-based geography recovery
file, ChatGPT review-response ledger, matter universe, current MapPLUTO lot
lookup, and Council member roster.

ChatGPT suggestions enter only as leads. A row is verified when an official
Legistar record lists a Council district for the same or related matter, or when
official matter text gives BBLs that map to current MapPLUTO districts.
Unsupported suggestions remain unresolved.

Output: `member_deference_nonapproval_geography_conservative_queue.csv`. The
current queue covers 491 core non-approval matters: 236 original Legistar/ZAP
geography assignments, 126 rule-based recovery assignments, 37 official-record
verification assignments, and 92 unresolved rows.

Runtime: about 15 seconds when the official-page cache is already filled. A
first run can take longer because it downloads official pages.
