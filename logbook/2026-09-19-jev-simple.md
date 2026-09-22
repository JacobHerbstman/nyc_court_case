---
title: "Short factual questions recover the explicit evidence Jev previously missed"
date: "September 19, 2026"
author: "Codex research record"
---

Jacob asked whether our question design, rather than Jev's capabilities, explained
the preceding failures. A controlled diagnostic supports a substantial role for
our design. Short factual questions match 24 of 25 frozen expectations on the
earlier excerpts and all 22 available expectations on full source reports. Both
response formats give the same binary answers. Three additional questions on the
page containing a previously missed affordability concern also match expectations.
These are selected, AI-authored diagnostic judgments, not the original human
labels or a representative accuracy estimate. The results establish that Jev can
recognize these explicit facts under simpler instructions; they do not establish
that it can discover and code every subjective issue in the corpus.

The TypeSafe documentation recommends one focused judgment per question,
independent questions against shared text, and a native yes/no probability type
called Noul. Our prior check combined topic membership, attribution, requests,
commitments and exclusions in a six-way choice. The new test asks the identical
short question twice against identical state: once as Choice with yes/no options,
and once as Noul. The question and criteria are otherwise the same. A fixed 0.5
threshold converts both probabilities to binary answers; exact ties are unresolved.
There were no ties or disagreements between formats. Thus this experiment provides
no evidence that adopting Noul itself explains the improvement. It also changes
question complexity and the state layout relative to v5, so it cannot isolate the
effect of wording alone. The provider's recommendation is useful design guidance,
not independent evidence of performance on CPC reports.

There are 25 propositions in nine cases drawn from six public source PDFs. Each
case reuses the exact text fragments of its earlier v5 window. The same questions
are also asked against the complete selected source PDF, including appendices,
without silently substituting a summary. Questions sharing a full source are
batched in one request. The earlier omitted affordability passage receives a
separate guided-page control. Context is allowed to change the expected answer:
a short excerpt can fail to establish a fact stated later in the full report.
The codebook records each expectation, source page, exact reference quotation
and reason before any API call. These references remain separate from Jacob's
and Tyler's original coding and are not transmitted to Jev.

The most informative recoveries are direct statements previously rejected:

- In `C 210192 ZMQ`, page 13, both formats recognize that Community Board 8
  raised infrastructure concerns. Merely naming infrastructure is sufficient;
  the question does not require a sewer, school or other specific service.
- In `C 140388 PCX`, page 3, both recognize a commissioner's question about
  whether DPR was at full capacity in the proposed facility.
- In `C 140409 ZSM`, page 8, both recognize the board's requests to reduce
  parking and building height. They no longer force those requests into an
  adoption-only category.
- In `C 210192 ZMQ`, page 9, both distinguish the Borough President's meeting-space
  recommendation, the applicant's commitment, and the explicit statement that
  meeting space is not a condition of CPC approval. The first two are positive;
  the third is negative.
- Tenant complaints about amenities and services in `C 870568 ZMQ` do not become
  displacement concerns. A planted buffer in `C 870729 HUK` does not become
  protection against eviction. Both formats preserve these negative controls.

The affordability example isolates the cost of supplying the wrong passage.
The earlier page-8 window for `C 220438 ZMK` describes affordable housing but does
not record CPC's affordability concern. Both formats correctly distinguish those
two propositions. Supplied with page 9, both recognize the Commission's explicit
concern. The full-report request for this source failed with HTTP 503, so this
test does not establish that Jev finds the concern when searching that whole
report. All six missing answers from that call remain missing. No retry or
replacement response was used.

There is one remaining substantive disagreement, returned in both formats.
From the board-conditions excerpt for `C 140409 ZSM`, Jev infers that the applicant
agreed to reduce height after certification, with yes probabilities of 0.70 and
0.71. Our frozen expectation is that a board's recommended conditions alone do
not establish that later applicant agreement. The excerpt includes future-tense
conditions and an agreement about a different feature, making this a useful
attribution and timing boundary. The full report explicitly records the height
agreement on page 11; both formats then answer yes appropriately. This illustrates
why a recommendation, an applicant commitment, an imposed condition and timing
must remain separate facts. We did not revise the expected answer or tune the
threshold after seeing this disagreement.

In total, 49 of 50 available proposition/context combinations match the frozen
expectations in each format. Counting both formats as 100 independent observations
would exaggerate the evidence: they ask the same questions on the same text.
The positive controls are recovered without treating every question as yes, but
the cases are deliberately selected and the questions name known actors and
specific provisions. They test recognition of facts, not automated discovery of
which provisions or actors to ask about. Reusable topic questions still need an
additional comparison on already-coded reports not used to develop this design.
None of the original subjective coding can be declared 98 percent accurate from
this diagnostic. Our stronger inference is that the earlier failures did not
justify concluding Jev could not read the explicit evidence.

Observed account usage increased by $0 and the balance remained $5. Fifteen of
16 distinct requests succeeded, returning 59,699 input tokens and 3,218 output
tokens. Each request was attempted once with ten-second pacing; the acquisition
retains the stop-on-rate-limit and observed-charge rules. The native probability
response was added to the existing acquisition runner without another service
wrapper. Exact questions, state text, probabilities, timestamps and credit checks
are archived under `data_raw/cpc_jev_pilot/20260919_vercel_v6_simple/`. All previous
pilot outputs, raw observations, source files and human labels retain their bytes.

Reproduction: branch `cpc_llm_training`, based on `6077733`, preserving prior
uncommitted work. Run `make` in
`tasks/audits/pilot_ulurp_cpc_llm_labels/code`, then
`make output/2026-09-19-jev-simple.pdf` in `logbook`. Ordinary Make uses saved
observations only; the explicit acquisition target is `make acquire-jev-simple`.
The five CSVs have deterministic data reports. Disposable fixtures exercise
native and Choice responses, missing calls, exact probability ties, duplicate
rejection, immutable snapshots, no repeated acquisition, rate/charge stopping,
and parallel/missing-output Make behavior. The PDF is rebuilt through Make and
visually inspected.

Provider references checked September 19, 2026:
[question design](https://docs.typesafe.ai/primitives),
[Noul](https://docs.typesafe.ai/primitives/noul),
[Choice](https://docs.typesafe.ai/primitives/choice), and
[state structure](https://docs.typesafe.ai/concepts/state).

\newpage
