# Create Member-Deference Nonapproval Geography Review

Exposes version-controlled geography-review records for non-approval land-use
matters used in the Council decision series.

Land-use decisions with no clear affected Council district were converted into
plain-text prompts and reviewed in ChatGPT to identify likely locations from
matter titles, application numbers, addresses, BBL references, and source links.

This task has no upstream inputs. It provides
`member_deference_nonapproval_geography_review_queue.csv` and
`member_deference_nonapproval_geography_chatgpt_review_responses.csv` as
version-controlled inputs. The queue records the exact matter text and clues
sent for review. The response ledger records the suggested location,
district, confidence, source to check, reasoning notes, and source links.

These files are geography leads, not final assignments. Downstream verification
decides which leads are supported by official records or BBL-to-district checks
before they enter the production decision panel. This task does not perform AI
inference or external review; it preserves the review records used by the
verification step.
