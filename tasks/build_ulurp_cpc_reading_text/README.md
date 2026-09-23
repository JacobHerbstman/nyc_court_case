# Build ULURP CPC Reading Text

Decides which PDF pages each CPC narrative's reader receives, and splits those
pages into citable segments. This is the source-text input for model reading of
CPC reports; it makes no model calls.

For every narrative in `summarize_text_cpc_trends/output/ulurp_cpc_text_labels.csv`,
the script gathers the source reports linked with `text_included_flag == TRUE`
(plus distinct full texts of repeated narratives, whose attachments can differ)
and assigns each page an application scope with `tasks/_lib/cpc_pages.py`:

- main-report pages up to the parsed CPC resolution are `in_scope`;
- attachments need a docket identity for the focal application or a linked
  companion, or a recorded decision in
  `record_ulurp_cpc_source_corrections/output/ulurp_cpc_page_scope_reviews.csv`;
- pages that cannot be attributed are `unresolved`; known unrelated pages are
  `out_of_scope`.

Outputs:

- `ulurp_cpc_page_scope.csv`: one row per narrative, source report and PDF page,
  with the scope decision and reason.
- `ulurp_cpc_reading_segments.csv`: in-scope page text for ready narratives,
  one row per page (pages over 12,000 characters are split at a space). Each
  segment keeps its source application, PDF page and source-text hash.
- `ulurp_cpc_reading_roster.csv`: every narrative with `preparation_status`
  `ready`, `needs_scope_review` (any unresolved page) or `no_usable_text`.

A held report stays on the roster and is never treated as lacking a concern.
On the September 22 repaired text, 3,995 of 9,063 narratives are ready and
5,068 are held; most held pages are continuations of attachments without an
explicit docket identity.

The page-scope logic was first written for the retired Jev pipeline and moved
here unchanged on September 22; see `logbook/2026-09-22-cpc-llm-pilots.md`.

Run `make` from `code/`.
