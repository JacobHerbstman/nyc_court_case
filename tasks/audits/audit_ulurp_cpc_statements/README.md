# Audit CPC statement extraction

This task owns the source-based audit at the first-100 checkpoint. Its first
output freezes which twenty reports will be reviewed, using sample metadata
only. No extracted statements or human label values enter selection, and no
model calls run through `make`.

The sample includes the four repaired-attachment controls, both split bundles,
four other human-coded reports, two other long fresh reports, and eight fresh
reports spanning decades and unresolved-page status. Overlapping categories
are counted only once. The selected IDs all belong to the frozen hundred.
This deliberate stress sample does not support a population accuracy estimate.

Run `make ../output/cpc_statement_audit_sample.csv` from `code/` to freeze the
selection and its standard data report. Selection uses the first-100 run's
frozen sample and prompt manifest, not extraction results. Once all review
records exist, `make` validates them and produces the report-level summary,
every statement assessment, and the source-backed findings tables, each with
its standard data report. An unfinished review does not produce final tables.

## Review after all 100 readings finish

Keep the extraction instructions and raw answers unchanged. Use independent
GPT-6 Sol high readers with fresh contexts. For each selected report, first
read every frozen source segment and save an inventory of material statements
without seeing the extracted rows or human coding. Only then supply the saved
extraction for comparison. Both split bundles must be reviewed across all parts.

Record source segment IDs and supporting quotes for each alleged omission or
error. Distinguish missed material statements, unsupported rows, wrong project
or application, wrong actor or project-team status, wrong topic, request versus
adoption, and lost changes in position across stages. Separate OCR/source
limitations, ambiguous judgments, and differences in how statements were split
from clear substantive errors. Do not count several labels on the same mistaken
statement as several independently wrong statements.

The final review must report the number of source statements assessed, extracted
statements assessed, supported omissions/errors, unresolved judgments, and
reports affected. Compare relevant broad measures with the frozen human
reference only after the independent source comparison; those older codes use
coarser definitions and are not gold-standard answers for every new field.
This is an AI-assisted audit, not new human ground truth. Save the original
answers and audit judgments separately. Stop and report the checkpoint before
processing any reports outside the hundred.

## Saved review records

Save each independent source inventory under the frozen run's
`audit/source_inventories/<document_id>_attempt1.json`. Use the frozen statement
schema and validate against every source segment in the whole bundle. The
inventory is published once before its reader sees extraction answers. Keep
it unchanged during comparison, even if comparison reveals inventory omissions.

Save comparisons separately as `audit/comparisons/<document_id>_attempt1.json`.
Record `document_id`, `inventory_sha256`, `extraction_sha256`,
`source_segments_read`, and the following lists:

- `inventory_assessments`: one entry for every inventory `statement_id`, with
  `assessment` (`covered`, `partly_covered`, `omitted`, or `unclear`), matching
  `extraction_statement_ids`, and `issue_ids`.
- `extraction_assessments`: one entry for every extracted `statement_id`, with
  `assessment` (`supported`, `clear_error`, `unclear`, or `duplicate`) and
  `issue_ids`. Additional supported extraction statements can reveal gaps in
  the independent inventory; those are not extraction failures.
- `issues`: entries with unique `issue_id`, `classification` (`clear_error`,
  `unclear`, `source_limit`, `granularity`, or `inventory_omission`), `categories`,
  `inventory_statement_ids`, `extraction_statement_ids`, `segment_ids`, exact
  `quote`, and `explanation`. Categories identify omissions, unsupported rows,
  application, actor, project-team, topic, request/adoption, or stage/position
  errors. A quotation must support the stated judgment; absence claims need an
  explicit explanation of which source material was checked.

Treat different splits or combinations of the same supported information as
granularity differences. A source conflict is not a model error when the
extraction preserves it accurately. Clearly omitted or partly covered material
needs a source-backed issue. Clearly erroneous extracted rows need an issue;
count each affected row once, even when several fields are wrong. Unclear
judgments stay separate from clear errors. Record explanatory `review_notes`.

Read and compare all inventory and extraction rows, not only suggested errors.
Human reference values are opened only after these source comparisons finish.
Published extraction answers and inventories remain unchanged throughout.

The comparison baseline is a byte-preserving snapshot of each accepted answer
at `audit/extraction_reference/<document_id>.json`. The adjacent
`audit/extraction_reference.json` records original response paths and hashes.
Comparisons must match these hashes and their inventory hashes. The table
builder checks complete assessment coverage and source quotations; it does
not turn an auditor's judgment into human ground truth. Counts of omitted
inventory statements depend on the auditor's granularity. Report clear errors,
ambiguous judgments, and differences in detail separately.

Readers use newly created, uniquely named temporary directories. Audit
incidents and replaced assignments are preserved separately in the frozen
run. A reader exposed to a prior draft cannot supply the independent source
inventory; a fresh reader must replace it without accessing that draft.

After all twenty source comparisons are saved, compare the eight overlapping
human-coded reports. Keep their reference values unchanged and retain both
coders' values and disagreements. Save `audit/human_comparisons/<document_id>_attempt1.json`
with `document_id`, `rows`, and `review_notes`. Each row records the original
`source_document_id`, `field`, `human_value`, `human_status`, `jacob_value`,
`tyler_value`, an independently supported `extraction_value`, `comparison`,
`extraction_statement_ids`, and `explanation`. Values may be binary, original
codebook categories, or explicit numerical counts, represented as strings.
Use `unclear` or `not_comparable` where warranted. Comparison classifications
are `agree`, `disagree`, `unclear`, `not_comparable`, and `human_disagreement`.
Do not replace a categorical stance with mere actor presence or force
agreement across different definitions. `cpc_statement_human_comparison.csv`
preserves these judgments and their evidence separately from the source audit.
Published corrections use new attempts. The Makefile explicitly selects
attempt2 for the four `human_overlap_a` reports, correcting a presence-only
projection of categorical stance fields; their attempt1 records remain saved.
The other four human comparisons use attempt1. This changes the comparison
format, not the original human labels or extraction answers.
