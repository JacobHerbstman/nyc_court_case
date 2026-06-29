# Create Member-Deference Nonapproval Geography Review

Exposes committed geography-review records for non-approval land-use matters
used in the member-deference decision series.

Land-use decisions with no clear affected Council district were converted into
plain-text review prompts and fed into ChatGPT to infer the likely affected
district from matter titles, application numbers, addresses, BBL references, and
source links.

This task has no upstream inputs. It provides
`member_deference_nonapproval_geography_review_queue.csv` and
`member_deference_nonapproval_geography_chatgpt_review_responses.csv` as
committed production inputs. The queue records the exact matter text and clues
sent for review. The response ledger records the AI-suggested location,
district, confidence, source to check, reasoning notes, and source links.

These files are geography leads, not final assignments. Downstream verification
decides which leads are supported by official records or BBL-to-district checks
before they enter the production decision panel. This task does not call
ChatGPT; it preserves the review records needed to reproduce the judgment trail.

Runtime: under 1 second.
