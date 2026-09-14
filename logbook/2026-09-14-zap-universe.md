---
title: "Recovering the ULURP project universe"
author: "Research note prepared by Codex for Jacob Herbstman and Tyler Jacobson"
date: "September 14, 2026"
---

The project list should start with every publicly released ZAP project, including
withdrawals, terminations, missing application numbers, and unknown process
classifications. CPC reports are documents attached to that list. Selecting the
project list from available reports or application numbers conditions the sample
on a later stage of the process and omits cases needed to study attrition.

The new project universe and targeted API retrieval implement this change in
the data inventory. Existing estimates have not been reinterpreted as estimates
for the expanded population. The generated audit at the end of this note gives
the exact counts, recovery results, and vintage comparisons.

## What was missing, and what we recovered

The May 1 raw ZAP export already included withdrawn and terminated projects.
The existing application spine first selects explicitly marked ULURP projects
with an in-range reference date, then expands each project's parsed application
numbers. Projects with an empty number list disappear at that step. The CPC
corpus has a separate source: the official report index. Its coverage cannot
establish the completeness of projects entering the planning process.

The current full public CSV and companion parcel table have been downloaded
from NYC Open Data with no sample filter. Each export's row count was checked
against an independent Socrata count request. Project IDs are unique, source
bytes are retained, and the generated project table preserves every raw project
field. Missing process classifications remain explicit unresolved cases.

A missing bulk number need not mean there was never a number. For example,
withdrawn project **P1981Q0478**, "107-06 150TH ST", has no number in the bulk
CSV, but its public API action is **I810017HDQ**, "Disposition of Urban Renewal
Site", with action status "Withdrawn". The detail acquisition consequently
requests every terminal, closed, or unclassified project. It retains each
response, action identifier, milestone, and available document relationship.
It does not infer project approval from one action's outcome.

Some withdrawn projects also have CPC reports. The recovered Industry City
City Map Amendment action, **C160146MMK** on project **2018K03531**, links to
the existing report adopted August 19, 2020. That report approves the map
amendment, while the bulk project and API action are now marked withdrawn.
This directly illustrates why reaching CPC, receiving a CPC recommendation,
and ultimately surviving the process must be measured separately.

There are several limits to the word *complete*. DCP's published scope begins
with publicly released filed/noticed projects; informal inquiries and proposals
that never became visible are outside that frame. DCP's export code explicitly
selects projects marked "General Public". The data dictionary attributes much
identifier missingness to legacy migration. Unknown classifications, missing
dates, missing report links, and failed API requests still need reconciliation.
The existing CPC index should also be checked back against the project list;
an unmatched report is a queue item, not a project to discard.

## What the dates and statuses can tell us

The dictionary defines public status "Completed" as including approved,
disapproved, withdrawn, and terminated applications. Preserve the detailed
project status and action-level decisions. Do not code either "Completed" or
"Complete" mechanically as project approval. Multi-action projects may contain
different outcomes and require an explicit project-level definition.

Use different denominators for different questions:

- **Attrition before formal review:** publicly observed filings, including those
  without certification. Missing historical filing dates constrain this analysis.
- **Survival through ULURP:** projects with independently supported certification,
  retaining withdrawals and terminations after entry. Keep unresolved entry
  dates outside timing estimates but inside the coverage inventory.
- **Community-board response and later outcomes:** projects reaching the board
  with observable recommendations, with explicit analysis of selective reporting.
- **What happens at CPC or Council:** projects reaching that stage. This estimates
  a conditional outcome, not success among all proposals.

Avoid counting actions or reports as separate projects. Keep a project table,
a project-action table, a dated event table, and document records linked by
their own IDs. Track amended proposals and resubmissions explicitly; sharing
an address or parcel is insufficient to declare them one project.

The audit reproduces a substantial difference between projects dated before
and after 2001. It also finds a large cluster of unclassified records assigned
January 31, 2011 and a cluster of explicit ULURP withdrawals dated November 13,
1991. These are flags, not corrections. Compare actual milestone histories,
action dates, source-system coverage, and availability of fields across the
boundary before giving the pattern a political interpretation. Never replace
the date with the apparent year in a project ID without source confirmation.
Recent active cases have incomplete follow-up, so a cross-sectional terminal
share is not a final probability of failure.

## Reading Tyler's extraction results

Tyler's August 28 write-up describes a useful deterministic pipeline: OCR
normalization, separation of actor sections, position extraction, raw tallies,
and alignment of votes to the proposal. Retaining the raw tally, the applied
rule, and its evidence is particularly valuable.

The document reports 11,188 usable reports, with usable proposal-aligned board
tallies for 4,816. It reports 96.3% and 99.2% exact agreement for speaker counts
among 136 and 130 comparable hand-coded observations. Those rates do not
measure recall over the whole corpus. Its board-position comparison excludes
"not reported" cases. The report also explicitly says board vote tallies lack
a direct hand-coded benchmark; 919 inversions depend on that unvalidated step.
These are the supplied write-up's results, not newly replicated results from
Tyler's code. The local repository has a different, evolving regex pipeline
and separate Codex-coded validation sets; those should not be conflated with
Tyler's hand coding or described as independent human validation.

Two errors deserve special attention. Votes for a motion to disapprove are not
votes for the proposal. But failure of a motion to approve is not automatically
a passed motion to disapprove. The printed motion, tally, recommendation,
decision rule, and any conditions must be represented separately. Also,
"not reported" can mean absent from the document, unreadable scan, missed actor
section, or failed extraction. These are different data states.

## A practical combined approach

My recommendation is to benchmark a combined process rather than assume that
either regex or full-context LLM reading is sufficient. The LLM's advantage in
coverage is plausible, but it needs a measured error rate on the fields we use.

1. **Preserve reusable source text.** Retain the original PDF, checksum, page
   boundaries, OCR method, and original text. Create page-level JSONL and readable
   Markdown once. OCR only pages that require it. Link every document to its
   project and relevant action, and deduplicate identical documents without
   deleting their project memberships. Existing CPC text/OCR artifacts should
   be reused after checking quality, not regenerated for each model call.
2. **Extract evidence before summarizing.** First produce structured observations
   about actors, motions, tallies, conditions, dates, and proposal versions.
   Each nonmissing value must point to a source document, page, and supporting
   quotation. Keep a short summary as a convenience derived from those records;
   never make it the sole input for subsequent extraction. Long documents can
   be searched by section, with full context used when needed to resolve meaning.
3. **Route fields according to measured reliability.** Let regex propose values
   for clear repeated language. A smaller model can read the relevant section
   plus surrounding context. Send disagreements, missing values, multiple-board
   cases, conflicting versions, and difficult scans to a stronger model reading
   the full relevant document. Complex concepts such as substantive concessions
   may need contextual reading by default. Audit a random share of apparent
   regex successes too; agreement or model self-confidence is not validation.
4. **Use independent human reference labels.** Start with a representative random
   holdout and a separate deliberately difficult development set, covering eras,
   action types, statuses, OCR quality, direct/inverted tallies, and extraction
   failures. Double-code and adjudicate the variables used in the analysis.
   A first pass of roughly 100 random plus 100 difficult cases would identify
   common errors; expand until key positive classes and era-specific error
   rates are estimated precisely enough. Freeze rules/prompts before evaluating
   the holdout. Targeted samples require sampling weights for population rates.
5. **Measure the actual trade-off.** On the same held-out cases, compare regex,
   a smaller contextual model, full-document stronger-model reading, and the
   combined routing policy. Record coverage, precision/recall for binary fields,
   exact tally agreement, evidence accuracy, missingness reasons, wall time,
   tokens, and actual charges. Treat abstention as preferable to unsupported
   completion. Choose acceptance thresholds field by field and examine whether
   remaining errors could change the research conclusions.

Withdrawn cases may require application materials, environmental-review
documents, board records, withdrawal correspondence, or archived docket entries
instead of a CPC report. Absence of a CPC report should not trigger a model to
invent board votes or an explanation for withdrawal. Record document discovery
and acquisition failures distinctly from genuine lack of recorded evidence.

## Proposed machine-readable structure

Keep relational identifiers in the analysis tables and use JSONL for nested
source packets. A single evidence record should be small and explicit:

```json
{
  "project_id": "P1981Q0478",
  "action_id": "source action ID",
  "document_id": "source document ID or checksum",
  "page": 3,
  "actor": "community_board",
  "field": "motion_polarity",
  "value": null,
  "missing_reason": "not_yet_reviewed",
  "evidence_quote": null,
  "extractor_version": "recorded rule or model version"
}
```

This is an illustrative schema, not an extracted finding for that project.
Separate `not_yet_reviewed`, `no_document_found`, `unreadable_source`,
`not_stated_in_source`, `ambiguous`, and `not_applicable`. An observed zero is a
number, not one of these missing states. Preserve the raw recommendation and
motion tally even when a proposal-aligned tally is constructed later.

The new full-project CSV and targeted API JSONL already provide the basic
identifiers and source packets. Bulk document download, page-level conversion,
and paid model processing remain subsequent stages. The next substantive
decision is which small set of outcomes must be reliable enough to support
the analysis; the first benchmark should focus on those outcomes.

## Sources and research record

The official sources are the [ZAP project dataset](https://data.cityofnewyork.us/d/hgx4-8ukb),
[ZAP parcel dataset](https://data.cityofnewyork.us/d/2iga-a6mk), their downloaded
metadata and dictionary, and the [public project portal](https://zap.planning.nyc.gov/projects).
The visibility rule was inspected in DCP's
[export code at a fixed revision](https://github.com/NYCPlanning/data-engineering/blob/f2a0f23835215c794d1403b7048e5ba3f42a7201/products/zap-opendata/src/visible_projects.py).
No access to the internal CRM or Tyler's Dropbox was needed.

The supplied `regex_extraction (1).pdf` was preserved byte-for-byte under
`data_raw/collaborator_notes/20260828/`, named `regex_extraction.pdf`. The reading used
`pdftotext -layout` after Docling was unavailable. Document instructions were
treated as quoted source material, not authorization to run its proposed tasks.

Jacob requested the full data acquisition and a processing discussion. The
combined extraction design and benchmark above are recommendations, not a
claim that Jacob has approved particular labels, sample restrictions, model
expenditures, or exclusion rules. Run `make` in `logbook/` to rebuild this note
and its generated audit through the declared task dependencies.

\newpage
