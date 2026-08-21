# Sample ULURP CPC LLM Training Reports

This audit draws two independent manual-coding samples from the comparable CPC
narrative universe through 2025. Each workbook contains 200 reports: 100 shared
reports for inter-coder agreement and 100 reports unique to that coder.

The shared reports and unique candidate pools preserve the original sample.
Within each coder's unique pool, selection remains balanced across vote years
and prioritizes ZM/ZR/ZS reports, then other non-disposition reports, then PP
reports. Longer narratives receive priority within those groups. This retains
substantive review histories while avoiding a training sample dominated by
routine dispositions. Previously coded full reports are excluded so the
existing validation labels remain available for model evaluation. The
workbooks contain no rule-based or model-generated coding suggestions.

Jacob's first 50 reports and the ten shared calibration reports remain at the
top in their existing order. All other shared reports follow before the
remaining coder-specific reports. Jacob's committed labels are restored by
document ID when the workbook is rebuilt.

Each report has 22 required coding fields covering the proposal, local demands,
revisions or concessions, unresolved objections, key actors, and five defined
issue groups. Fourteen are binary, one records the literal zoning change, one
records the dominant development direction, two record councilmember and
civic-group positions, and four record reported CPC speaker and
community-board vote counts.

The August 2026 schema split preserves the prior `dev_direction` codes as
`zone_change`. Existing `dev_direction` values are seeded mechanically as
upzone to more, downzone to lower, mixed to mixed, and none to none. Future
coding applies the separate definitions directly. `dev_direction` can capture
a non-zoning approval only when it materially changes development capacity or
enables a substantial redevelopment; routine or merely legal approvals remain
`none`. Four legacy mixed cases were set to lower because their existing coder
notes explicitly described an overwhelming or near-total downzoning; no report
was retrospectively reread.

Outputs:

- `cpc_llm_training_labels_jacob.xlsx`
- `cpc_llm_training_labels_tyler.xlsx`
- `cpc_llm_training_label_guide.tex`
- `cpc_llm_training_label_guide.pdf`

The guide codes one previously reviewed report as a shared reference example.
The codebook sheet in each workbook gives the same concise label definitions.
Workbook and guide links open the official CPC report URLs.
