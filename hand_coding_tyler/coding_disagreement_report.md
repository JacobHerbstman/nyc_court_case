# CPC Hand-Coding Disagreement Report

**Coders:** Tyler, Jacob
**Files:** `cpc_llm_training_labels_tyler.xlsx`, `cpc_llm_training_labels_jacob.xlsx`
**Matched on:** `shared_id`

## Scope
Of 100 shared IDs, **10 documents were coded by both** (`coding_complete==1`): C006, C007, C009, C014, C020, C041, C056, C089, C093, C094. This report covers agreement on those 10.

**Header mismatch:** Tyler uses `approved_unresolved_objection`; Jacob uses `approved_despite_unresolved_objection`. Treated as the same field — should be aligned.

## Overall agreement
Across 18 label columns × 10 docs, **153/180 cells agree (85%)**.

Perfect agreement (10/10): `specific_project`, `local_opposition`, `councilmember_position`, `affordability_displacement`, `coding_confidence`.

Lowest-agreement columns:

| Column | Agreement |
|---|---|
| `local_request_condition` | 60% |
| `scale_character_preservation` | 60% |
| `dev_direction` | 70% |
| `approved_despite_unresolved_objection` | 70% |
| `revision_or_concession` | 80% |
| `procedural_response` | 80% |
| `explicit_local_response` | 80% |
| `bp_request_or_opposition` | 80% |

## Key pattern
The 27 disagreements are **not random**. On nearly every binary conflict, **Jacob codes 1 (feature present) and Tyler codes 0 (absent)**. All disagreements in `local_request_condition`, `scale_character_preservation`, `approved_despite_unresolved_objection`, `revision_or_concession`, and `bp_request_or_opposition` run Jacob=1 / Tyler=0. The one reversal is `explicit_local_response` on C014 (Tyler=1, Jacob=0). This points to a **different threshold for "feature present,"** not scattered noise.

## Categorical (non-binary) disagreements
- **C007** `dev_direction`: Tyler `upzone` vs Jacob `none`
- **C089** `dev_direction`: Tyler `none` vs Jacob `upzone`
- **C020** `dev_direction`: Tyler `none` vs Jacob `mixed`; `civic_group_position`: Tyler `none_or_procedural` vs Jacob `support_or_request`

## Most-contested documents
- **C009** — 6 disagreements
- **C020** — 6 disagreements
- **C056** — 5 disagreements
- **C014** — 4 disagreements

## Recommended next steps
1. Align the `approved_(despite_)unresolved_objection` column name.
2. Reconcile the "present/absent" threshold, starting with `local_request_condition` and `scale_character_preservation`.
3. Adjudicate the three `dev_direction` cases (C007, C020, C089) as genuine judgment splits.

## Tyler's notes — sources of confusion
A couple points of confusion, which likely explain much of the systematic Jacob=1 / Tyler=0 pattern above:

1. **Issue-area undercounting.** I undercounted mention of issue areas because I interpreted the question as being whether locals were *concerned* with those issues, and not just whether those issues were *mentioned* in the report. I think it makes sense to code the conditions as you did and still flag them even if opposition did not raise the points exactly.

2. **Minor conditions as opposition.** I was sort of unwilling to classify pretty minor conditions as evidence of opposition from the borough president or community board. As an example, consider the **77th St Boarding Home (C009)**. The conditions added were:
   - For a three year period, the property should be maintained in an attractive manner.
   - Health and social services should make regular visits to the site.

   The borough president went along with these conditions. In my opinion these are pretty minor and shouldn't lead to the coding of opposition from either the community board or borough president. Obviously, this creates a judgement call.

   This also led us to code `local_request_condition` differently. Any time there was no verbal opposition at a hearing, I said there was not a `local_request_condition`, whereas it seems like you did in this case because of the conditions.

3. **"Upzone" for Park Housing (C007).** I called park housing an upzone because it was associated with the construction of increased density — "upzone" isn't quite the right word here but seems like it should still be coded as a pro-development project.
