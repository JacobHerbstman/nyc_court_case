# Reconcile a split CPC report

Read every part of this one report bundle and its saved first-pass statement
answers. Apply the frozen statement instructions and schema. This is a reading
of the whole report, not a vote between answers.

Keep every supported distinct statement, actor, stage and application. Merge
only genuine repetitions with the same actor, application, stage, position and
status. Keep conflicting votes or positions with their sources and notes.
Check each concern and request against later material in every part. Link an
adoption only when the report supplies the connection; a similar later feature
alone is insufficient. Restore material statements omitted by an earlier part.

Return one JSON object for the whole report, using unique statement IDs across
all parts and updated response_statement_ids. segments_read must include every
segment from every part. Cite original segment IDs and exact source quotations.
Record limitations and unresolved cross-part relationships in reading_notes.

Validate the combined answer against the union of all supplied segments before
publishing responses/<document_id>_part0_attempt1.json. Preserve every earlier
part answer unchanged. A schema/quote check does not establish completeness;
the substantive reconciliation is your responsibility.
