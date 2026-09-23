# Summarize CPC Text Trends

This production task measures deterministic text signals in readable CPC
reports from 1975 through 2025 and creates two initial motivating summaries.
It uses conservative report sections and local context, removes repeated
boilerplate and known mechanical false positives, and reports document shares
rather than raw mentions.

Reports decided on the same date are bundled when they share a ZAP project ID
or cite one another as related applications. The focal narrative hash remains
unchanged; the output separately records the bundled-text hash, word count,
and contributing companion applications. Historical prose transitions are
used when older reports lack modern CB, BP, hearing, or consideration headings.

The September 21 attribution repair removes shared-title/date links. A generic
title such as C-O-P is not a project identifier. Designated-lead grouping now
requires the same nonempty ZAP project-ID set as well as the original normalized
name/date match. Sources with insufficient evidence remain separate narratives.
The source-link table records `relationship_path` and `relationship_basis`,
including transitive connections and recorded N-companion decisions.

Exact duplicate text is counted once, while every application collapsed into
that narrative keeps a source link. The same applies to actions represented by
a designated lead. `represented_application_numbers` and
`represented_action_codes` expose that membership in the labels; `zap_project_ids`
is the union across represented applications. Action flags use the represented
applications. `represented_community_districts` combines their districts after
source-specific corrections, while `community_district` retains the focal
report's original value. The geography summary uses the combined field.
Context-only companions do not automatically
expand these action or geography definitions.

The reviewed pairs in `ulurp_cpc_companion_reports.csv` add N reports as context
for existing narratives. Their inclusion does not require a lead flag, matching
name, or matching date. The recorded certified-report text hash verifies the
referral evidence. N sources are attached after the existing case groups are
formed, so these new sources cannot join otherwise separate groups or create
independent narrative observations.

The document-level file follows the human coding sheet where regex can make a
defensible measurement: substantial opposition, local requests, revisions or
concessions, responses, unresolved objections, CB opposition, broader CB/BP
activity, councilmember and civic-group positions, and five issue families.
Actor-specific events require an actor and stance or request in the same
sentence. Adjacent sentences are joined only when the second begins with an
explicit continuation such as a pronoun or "in response."
The CB opposition indicator follows the formal recommendation; it is no longer
computed by comparing affirmative and negative totals. A missing recommendation
has `cb_position=not_reported`; use that field to distinguish missing evidence
from an observed supportive recommendation. These remain rule-based proxies
rather than replacements for hand coding. Narrow response and revision rules
favor precision over recall.

The same file records narrative word count and exact reported counts of CPC
speakers in support and opposition and Community Board votes supporting
approval and disapproval. A blank means that an exact count was not
established; zero is used only when the report establishes zero. The plots show
resolved-count coverage separately from mean counts among resolved records,
because reporting completeness changes sharply over time. Partial counts and
cases requiring review remain in the CSV but are excluded from count plots.

- `ulurp_cpc_text_labels.csv` contains one row per analysis narrative.
- `ulurp_cpc_evidence.csv` retains candidate statements behind issue, actor,
  request, and response signals. Each row has a stable event ID, narrative,
  section, sentence positions, excerpt, actor candidates, broad and detailed
  issue matches, and separate stance/request flags. It is an evidence table,
  not a count of projects, people, or independent statements. A linked sentence
  pair can overlap its constituent sentences.
- `ulurp_cpc_human_coding.csv` makes the existing Jacob and Tyler readings
  available by original source document and field. It preserves each coder's
  values, evidence, notes, confidence, and completion status alongside the
  regex value. `human_value` retains an agreed or single-coder value; conflicts
  and nonstandard values stay blank with an explicit status. Provisional values
  are available but remain marked provisional. No new human labels are inferred.
- `ulurp_cpc_reconciled_coding.csv` adds a working value and review provenance
  to every original human row. It applies the recorded source review for each
  Jacob/Tyler disagreement without changing either original column. Unresolved
  reviews stay blank. Agreed and single-coder values remain identifiable as
  original, unreviewed coding. Use this table for subsequent coding development;
  historical comparisons continue to use the original human table.
- `ulurp_cpc_narrative_sources.csv` contains one row per narrative/source pair.
  It retains each source's application, role, project IDs, action, district,
  vote date, and text hashes. `represented_application_flag` distinguishes
  represented applications from context sources. `text_included_flag` and
  `analysis_text_order` identify the unique source excerpts used to reconstruct
  the measured text; a repeated source's metadata remains even when its text
  does not need another copy. The corpus manifest supplies source paths.
- `ulurp_cpc_text_signal_trends.pdf` compares all reports, non-PP reports, and
  ZM/ZR/ZS reports.
- `ulurp_cpc_text_signal_homeowner_tercile_trends.pdf` splits the same signals
  by within-borough terciles of 1990 community-district homeownership.

The reviewed narrative and district corrections are preserved in
`record_ulurp_cpc_source_corrections`.

## Added detail and existing human coding

The existing broad labels, count fields, sample membership, and text hashes are
preserved. New `_detected` columns separate affordability, displacement, traffic,
parking, neighborhood character, scale/density/design, and historic preservation.
Additional columns separate councilmember and civic-group support, opposition,
and requests. These actor flags require a single-sentence candidate with only
that actor class detected. Mixed actor candidates stay in the evidence table.
Zero means no detection by these rules, not a verified absence in the report.
The broad human labels do not automatically become judgments on the new details.

Evidence source matching uses dehyphenated, whitespace-collapsed source text.
`normalized_source_start` and `normalized_source_end` are zero-based character
offsets in that representation, with an exclusive end, filled only for a unique
occurrence. Multiple-source matches, repeated occurrences, and unlocated excerpts
remain explicit. They are not assigned invented PDF page numbers. The source
manifest supplies the original text/PDF paths. Source and analysis hashes remain
available. Candidate phrases do not establish which actor made a statement when
multiple actors appear, or that a request caused a concession.

Human coding is linked by report identifier, with application-number agreement
checked. The source table records representation by other narratives; labels are
not copied from a context companion onto other cases. All 340 currently coded
reports match retained focal narratives. The old coding files have no source-text
hash captured at coding time, so current hashes do not certify unchanged source
content since that reading. Both coder values remain visible when they disagree.
Tyler's older development-direction codes are retained as
`legacy_development_direction`; they are not silently mapped to the newer
`zone_change` and `dev_direction` definitions. His literal `both` civic-group
codes remain nonstandard/unresolved instead of being overwritten.

The separate reconciled table applies
`record_ulurp_cpc_training_labels/output/ulurp_cpc_coding_adjudications.csv`.
Before applying a ruling, it checks unique keys, exact disagreement coverage,
both original values, source-bundle membership, source-text hashes, and each
quotation on its cited PDF page. A matching quotation establishes provenance,
not the correctness of the interpretation. Two literal `both` civic-group codes
are mapped to the common position categories using the source review; their
original categories remain visible.

`RECONCILIATION_ISSUE_SCOPE := broad` in the Makefile is the current working
assumption: count substantive CPC or local discussion, excluding neutral
descriptions and routine findings. It is not a user-confirmed codebook change.
The source ledger also saves the narrower local-actor reading; 14 disputed
fields depend on that choice. Edit this scalar in the Makefile to change the
canonical output. Four opposition differences await a distinction between
any dissent and substantive opposition; a fifth lacks clear actor information.
These source reviews are AI-assisted development judgments, not an independent
human gold standard.

The entire ZAP project universe remains in its existing project table. Neither
an absent CPC report nor an unresolved human or regex field removes a project.

## Vote and hearing codebook

`tasks/_lib/cpc_counts.py` owns the literal counting rules used by this task and
its validation audit. Count extraction uses the focal report's bounded review
section. If the line-based section parser finds none, explicit actor transitions
can recover a section across printed lines; both a beginning and an ending
boundary are required. `source_kind` identifies this route. Only absent evidence
permits a companion report to supply counts. Resolved companions must agree on
the entire count record. A conflict or ambiguous focal tally goes to review.

Board fields retain three distinct objects:

- `cb_position`: support, support with conditions, oppose, oppose unless
  conditions, no recommendation, or not reported.
- `cb_reported_for`, `cb_reported_against`, and `cb_abstentions`: the literal
  tally as reported. These may refer to a motion to disapprove.
- `cb_support_votes` and `cb_opposition_votes`: proposal-aligned counts, filled
  only when the rule establishes orientation. `cb_vote_rule` records inversion.

Abstentions never silently become negative votes. `cb_abstention_rule` records
an explicit report statement that abstentions count as opposition, and
`cb_effective_against` adds them only under that rule. For C 160174 ZSR, the
literal tally is 17 affirmative, 14 negative, and five abstentions; the report
explicitly counts those abstentions as disapproval, making effective opposition
19 and the formal recommendation oppose. A disapproval recommendation with a
contradictory majority, or a condition rejecting the proposed site, requires
review before assigning proposal-aligned counts.

Speaker rules recognize numeric and written quantities, targeted OCR spacing,
and intervening descriptions such as "two speakers representing the applicant."
An unreported count is blank. Closing a hearing alone never establishes zero.
Repeated speaker counts, multiple speaker groups, continued hearings, and multiple boards stay visible and are excluded from the
strict resolved sample. Counts across clearly separated hearings may be present,
but `multiple_hearings` requires review of aggregation and repeat participants.

Both field groups retain extraction status, rule, evidence, source application,
and source-text SHA-256. `resolved` describes a deterministic extraction, not a
validated probability of correctness. Revisions, concessions, issue content,
and whether a condition changes the actual proposal remain contextual judgments.
See `tasks/audits/audit_ulurp_cpc_regex_labels` for measured coverage, existing
coding comparisons, the unresolved queue, and a fresh human review sheet.

These are CPC-report narratives, not the complete ULURP risk set. Projects
without CPC reports, including withdrawals and terminations, remain in
`build_zap_project_universe`; missing report text must not remove them from an
analysis of whether projects advance.
