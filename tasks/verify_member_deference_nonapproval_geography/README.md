# Verify Member-Deference Nonapproval Geography

Verifies geography leads for unresolved nonapproval land-use matters against
official records and creates the conservative list used for vote fetching.

Inputs are the nonapproval final-action list, rule-based geography recovery
file, ChatGPT review-response ledger, matter universe, current MapPLUTO lot
lookup, and Council member roster.

ChatGPT suggestions enter only as leads. A row is verified when an official
Legistar record lists a Council district for the same or related matter, or when
official matter text gives BBLs that map to current MapPLUTO districts.
Unsupported suggestions remain unresolved.

Output: `member_deference_nonapproval_geography_conservative_queue.csv`. The
file keeps verified geography where the records support it and leaves
unsupported rows unresolved.

Runtime: about 15 seconds when the official pages are already saved. A first run
can take longer because it downloads official pages.
