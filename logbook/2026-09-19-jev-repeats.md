---
title: "Repeated Jev votes add little; revised wording has modest, uneven gains"
date: "September 19, 2026"
author: "Codex research record"
---

Five identical readings rarely changed Jev's classification. Only two of 92
original-question report/field combinations varied across the five calls.
Majority voting changed one first answer, for the previously unavailable report;
it repaired none of the earlier trial's 20 character/revision mismatches beyond
the first response in this new experiment. The trial completed all 230 planned
requests, returned 3,220 question answers, and incurred no observed credit
charge. The full $5 balance remained available.

Wording helped selectively. On the original sample, the clearest revision
question corrected three of 12 earlier mismatches without losing any of the
13 earlier matches. On the additional balanced sample, voting across the
three clarified phrasings improved revision agreement from 13/20 to 15/20,
but character agreement declined from 14/20 to 13/20. The revision gain
reduced additional positives from six to three while increasing missed
reference positives from one to two. Using all fifteen clarified votes
produced the same results as using each phrasing once. The diagnostic-category
approach tended to overcall positives and did not outperform the direct
questions. These results support further evidence-based refinement, not
hundreds of identical readings per report or automatic adoption of a new coder.

The experiment asks whether majority voting repairs Jev's earlier character
and revision disagreements, and whether clearer questions improve the result.
It separates five identical requests from three different phrasings, rather
than treating repeated calls as independent expert readers. The original
questions remain as controls. Additional categorical questions distinguish a
concrete issue from a description, an adopted change from a request, and a new
change during review from an older commitment. Candidate evidence-page choices
help locate passages but do not verify their interpretation.

The test includes all 26 original bundles that fit in a single request, plus
20 additional already-coded reports. Those additional reports are balanced:
five in each combination of the original binary character and revision codes.
Their original codes must be complete and nonconflicting, and their source
bundles cannot overlap previous pilots, source reconciliation, or each other.
Three candidates exceed the existing whole-bundle context limit. All 340
human-coded reports remain in the selection roster with an explicit reason;
the four original long bundles remain recorded but are not part of this
repeat experiment. Neither the corpus nor the full ZAP sample is restricted.
This is a deliberately selected development test, not a representative
accuracy estimate. Questions were frozen before acquiring its answers.

Two unblinded source checks illustrate why agreement is not correctness.
The nursing-home application `C 010472 ZSX` describes enlargement of an
existing facility and ordinary special-permit conditions. Jev's first response
codes revision positively while selecting no supporting page, exposing an
inconsistency rather than providing an interpretable explanation. Its related
zoning report `C 010471 ZMX` includes explicit borough-president discussion of
scale on page 19 and discussion of buffering on page 8. It would be premature
to call every positive reading wrong just because the existing human codes
are zero.
`C 060272 ZMK`, pages 3 and 5--6, documents a Community Board concern about
out-of-scale development and a signed, binding remediation requirement.
Positive character and broad commitment codes are defensible despite the
existing human zeros. The separate area's rezoning cannot itself be treated
as a revision to the focal application. These examples are diagnostic notes,
not replacement labels or a systematic adjudication of all disagreements.

The broad revision definition is preserved: an undertaken or imposed
substantive commitment may count even if it predates review. It is therefore
not equivalent to a concession caused by local opposition. Similarly, the
working character definition includes substantive CPC discussion as well as
local discussion. The original human reference and the AI-assisted working
reference remain separate in the saved comparisons. Comparing multiple
wordings against imperfect labels does not identify a uniquely correct prompt.

The raw observations use
`data_raw/cpc_jev_pilot/20260919_vercel_v2_repeats/`. Every outgoing page was
verified against extracted text from the 73 public NYC government PDFs;
requests contain no human answers, private notes, or local file paths.
Each of the 46 complete bundles has five identical planned request bodies,
with 14 independently evaluated questions in each. The provider's question
independence contract is documented at
[TypeSafe primitives](https://docs.typesafe.ai/primitives). The original
question definitions and report state are retained, although the batch of
questions differs from the first Jev trial. Choice stability cannot establish
whether any provider caching is involved.

The first run timed out after 26 successful responses. Its uncertain result
was recorded before resuming; no saved answer was replaced. Subsequent
transport failures and transient server errors have bounded retries, and all
attempts remain visible. Account credit checks run before, after, and every
ten successes, with a stop if observed usage rises. This is not an enforced
gateway billing cap. The endpoint returns a model alias rather than a pinned
release, so reproducibility means rebuilding from saved observations rather
than guaranteeing future model answers.

After the initial three-attempt limit, two final readings remained missing.
An explicit final pass permitted at most two more attempts on those readings;
both succeeded. The archive contains 289 attempts in total, including 59
without usable answers. Successful responses report 3,177,660 input tokens
and 322,361 output tokens. The cost statement is based on account observations
at the recorded times, not a separate per-request invoice.

Reproduction: on `cpc_llm_training` based on `6077733`, with the existing
uncommitted pipeline work, run `make` in
`tasks/audits/pilot_ulurp_cpc_llm_labels/code`, then
`make output/2026-09-19-jev-repeats.pdf` in `logbook`. Ordinary Make builds
do not call an API. Disposable fixtures check resumption, changed-request
rejection, timeout recording, the zero-charge observation guard, majority
and tie rules, full sample retention, parallel/missing-output builds, and
changed-input propagation. Earlier coding and pilot outputs remain unchanged.

\newpage
