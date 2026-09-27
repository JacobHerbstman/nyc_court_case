---
title: "First 100 CPC statement readings: frozen design and stopping point"
date: "2026-09-26"
---

Jacob authorized a 100-report run using GPT-6 Sol subagents at high reasoning,
followed by a source-based accuracy and coverage review before expansion. This
replaces the earlier plan to proceed immediately through the full corpus.
The reader extracts evidence-backed statements, actors, requests, commitments,
requirements, changes and findings. Report-level research measures can then be
derived without asking the reader the same topic question repeatedly.

The frozen sample has twenty reports with prior human coding and eighty fresh
reports. It includes the four known missing-attachment cases, six deliberately
long fresh reports, and a seeded fresh sample across decades and the presence
of unresolved pages. Fifty-four have unresolved pages. The sample spans the
1970s through the 2020s and requires 103 packets: Industry City and Riverside
Center need multiple parts. These are selected stress cases, so their error
rate will not estimate population accuracy without appropriate sampling weights.

The instruction retains Jacob's supplied design. Two clarifications preserve
its intent: an earlier objection and later support remain separate statements,
and the example now explicitly connects the parking change to the board's
request. Silence means no response is recorded in the supplied report; it is
not evidence that no response ever happened. A partial packet cannot establish
silence elsewhere in the bundle. A split bundle receives a whole-report
reconciliation before its rows enter the final table.

The exact prompts, schema, sample and unedited answers are preserved by run.
Every answer must cite existing segments and quotations and list every supplied
segment as read. These checks establish evidence traceability, not that every
material statement was found or interpreted correctly. A source-based audit of
twenty reports follows completion of the hundred, with source reading before
the auditor sees extracted answers. It includes the attachment cases and both
split bundles and checks omissions as well as unsupported claims, actors,
application attribution, topics, adoption and changes across stages. Existing
human coding provides an imperfect overlapping comparison.
The independent Sol review is an AI-assisted audit, not new human ground truth.

The twenty audit IDs were frozen from sample metadata during acquisition,
before the audit readings. They contain the four repaired-attachment controls,
both split bundles, four other human-coded reports, two other long fresh
reports and eight further fresh reports spanning decades and page-scope status.
Eight have prior human coding and eleven include unresolved pages. Selection
uses no extracted statement values or human label values. The audit owner is
`tasks/audits/audit_ulurp_cpc_statements`; its Makefile reproduces the selection
from the first-100 run's frozen sample and prompt manifest.

A continuation is limited to these hundred reports and the audit. Initially
hourly with three readers, it was changed to ten-minute checks and a requested
maximum of ten Sol high readers at Jacob's request. Actual concurrency depends
on the loaded session limit. It preserves completed work at usage limits and
stops before the full corpus. No accuracy claim is made at launch. Token
usage is unavailable through this subagent interface and is left missing in
the data rather than recorded as zero.

Reproduction: the owner is `tasks/extract_ulurp_cpc_statements`. Its Makefile
prepares the frozen first-100 prompts with `make app-prompts`, records app
responses with `make app-record`, and builds statement and coverage tables with
`make`. Model acquisition is separate from ordinary builds. The raw run is
named `first100_sol_high_20260926` inside
`data_raw/cpc_statement_extraction/`.

The restarted session supported ten simultaneous Sol high readers. All hundred
reports are complete, including the whole-report reconciliations for both split
bundles. The accepted answers produce 6,562 statement rows. All 140 saved answer
attempts were recorded and their hashes verified unchanged. This establishes
reading and structural completion; substantive accuracy remains under review.

The audit uses frozen byte copies of the twenty accepted extraction answers.
One Riverside auditor accidentally loaded a stale temporary draft containing
prior rows. Its work was stopped before publication and excluded; a fresh,
blinded reader replaced it using a newly created unique temporary directory.
The incident and replacement assignment are preserved with the audit records.
Independent inventories remain unchanged when readers subsequently compare
every source-inventory and extracted statement. No new corpus reports are
authorized at this checkpoint.

All twenty source comparisons are complete. Auditors assessed 2,168 independent
inventory statements and 3,009 extracted rows. They classified 2,895 extracted
rows as supported, 38 as having at least one clear coding error, 75 as unclear,
and one as a duplicate/granularity issue. Clear row errors appear in ten reports.
Fifty-one inventory statements have source-backed omissions or partial
omissions, across fourteen reports; differences in detail and inventory
granularity are counted separately. Missing and misclassified details include
actor/team attribution, stage, component-specific support, request responses,
and selected substantive topic content. The evidence is promising but does not
certify population accuracy or remove uncertainty about uncaptured statements.
Supported additions in the extraction also revealed limitations of the
independent inventories, especially for Riverside's extensive declarations.

The audit task now produces separate report summaries, all statement
assessments, and source-backed findings through Make. Hashes, source quotations,
complete assessment coverage, and reciprocal issue references were checked.
Removing the findings output caused one rebuild and reproduced identical bytes;
an unchanged subsequent build did not rerun its producer. Neither raw readings
nor human labels were edited. Comparison-prose count typos are preserved with
separate errata; the tables use the structured row assessments.

After the twenty source reviews finished, two Sol high readers compared all
166 overlapping human-reference fields across eight reports. Of 151 comparable
decisions, 137 agree and fourteen disagree. Four existing human disagreements,
three unclear comparisons, and eight noncomparable definitions remain separate.
Differences include procedural versus explanatory response, future consultation,
local issue versus proposal description, a four-versus-five hearing-speaker
count, and categorical council/civic positions. These are disagreements with
imperfect earlier labels, not fourteen model errors. A too-narrow initial
human-comparison answer format was corrected with new saved attempts; original
labels and extraction answers remained unchanged. The Makefile explicitly
selects corrected attempts, retaining the first answers for provenance.

The first-hundred checkpoint is finished. The continuation is paused and no
remaining corpus reports are started. Before expanding, the supported actor,
stage, response and omission findings warrant targeted corrections or explicit
analysis exclusions. The saved statement evidence allows those decisions to be
reviewed without treating quotation validation alone as accuracy.
