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
- `ulurp_cpc_text_signal_trends.pdf` compares all reports, non-PP reports, and
  ZM/ZR/ZS reports.
- `ulurp_cpc_text_signal_homeowner_tercile_trends.pdf` splits the same signals
  by within-borough terciles of 1990 community-district homeownership.

The reviewed narrative and district corrections are preserved in
`record_ulurp_cpc_source_corrections`.

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
