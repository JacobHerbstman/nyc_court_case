# Create Member-Deference Nonapproval Geography Review

Stores reviewed geography records for non-approval land-use matters used in the
member-deference decision series.

Land-use decisions with no clear affected Council district were converted into
plain-text review prompts and fed into ChatGPT to infer the likely affected
district from matter titles, application numbers, addresses, BBL references, and
source links. The committed outputs preserve the review queue and the structured
ChatGPT responses used by downstream verification.

Creates `member_deference_nonapproval_geography_review_queue.csv` and
`member_deference_nonapproval_geography_chatgpt_review_responses.csv`. These
committed outputs record the AI-reviewed geography leads; downstream tasks decide
which leads are conservative enough to enter the production decision panel.
