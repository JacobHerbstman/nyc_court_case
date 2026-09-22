---
title: "Jev could make CPC classification inexpensive; its accuracy still needs testing"
date: "September 19, 2026"
author: "Codex research record"
---

Jev is worth testing on the existing CPC coding sample. Its published input
price implies only a few dollars for one classification pass over our current
report bundles. Hundreds of separate passes would instead cost hundreds of
dollars, and repeated answers from the same model would not constitute
independent evidence of correctness. This is a feasibility assessment based on
official documentation and local corpus measurements, not an observed Jev
accuracy result. A subsequent trial request was rejected during account
verification, as recorded below. No successful inference, account purchase, or
production-label change was made.

TypeSafe describes Jev as a model that reads text and returns probabilities
over specified answers, rather than generating prose. This fits questions such
as whether a passage documents a displacement concern or a civic-group request.
It does not directly produce our existing narrative summaries and free-form
evidence explanations. [System One documentation](https://docs.typesafe.ai/concepts/system-one).
Several questions can share the same input in one request; additional questions
add their own tokens without requiring the report to be sent again. This is
particularly useful for our issue and actor fields. [Question documentation](https://docs.typesafe.ai/primitives).

The official rate for Jev 1.13 is $0.042 per million input tokens, with no output
charge. The native API permits 64,000 tokens in the whole request, but the report
text plus the longest individual question must fit within 32,000. It accepts
text rather than PDF images. Our existing extracted text can therefore be reused.
The versioned model identifier is `jev-1.13.0`; a future experiment should pin
and record the actual version. [Model reference](https://docs.typesafe.ai/models).
Jev is also listed by [OpenRouter](https://openrouter.ai/typesafe/jev-1.13/)
and [Vercel AI Gateway](https://vercel.com/ai-gateway/models/jev).

## Cost for the current report sample

The September 19 measurement covers 8,905 retained CPC narratives. Following
every source link marked as text included gives 16,902 source appearances from
9,651 distinct source texts, totaling 47,184,456 whitespace-separated words.
Companions are counted again when another focal narrative would send them again.
These are complete extracted source texts, including appendices, rather than
just the shorter cleaned analysis text. Source hashes were checked against the
recorded links. The mean bundle is 5,299 words and the median 2,435.

The following planning range assumes 1.3–1.7 input tokens per source word and
2,000–4,000 additional tokens for questions and request overhead per narrative.
These are explicit assumptions, not measurements from Jev's tokenizer or billing.
For one pass, the calculation is
`(47,184,456 × tokens per word + 8,905 × question tokens) × 0.042 / 1,000,000`.

| Separate passes per narrative | Model input charges for all 8,905 narratives |
|----------------------------------|--------------------------------------------:|
| 1 | $3.32–$4.87 |
| 3 | $9.97–$14.60 |
| 10 | $33.24–$48.65 |
| 100 | $332.43–$486.50 |

This baseline excludes extra requests for passage selection, repeated context
when splitting documents, retries, gateway funding fees, other models, and
human review. It is not a quote for a complete evidence-producing pipeline.
Between 325 and 441 bundles exceed the 32,000-token state budget under the word
conversion assumptions, even before adding a question. Those reports must be
split or routed onward, never truncated or removed silently. Projects without
CPC text remain in the full ZAP universe; this estimate does not cover acquiring
missing reports.

## Where it could help

My recommendation is to test Jev first on narrow issue, actor, stance, and
request judgments. Revisions need several distinct questions: is a change
requested, is a substantive change or commitment documented, is the action only
a study, and does the report explicitly connect the response to the request?
Combining those answers should follow our codebook, without redefining a broad
commitment as a concession caused by local pressure. TypeSafe itself recommends
narrow questions with explicit rules and composition in code.
[Workflow guidance](https://docs.typesafe.ai/concepts/how-to-build-with-system-one).

Keep exact vote and speaker arithmetic in the existing parser. TypeSafe warns
that this release has unreliable counting, weaker performance with distracting
long inputs, and difficulty with indirect reasoning. Its recommended extraction
approach is to select among supplied candidates.
[Known limitations](https://docs.typesafe.ai/model-jaggedness/jev-1.13).
For our evidence records, code could assign identifiers to source passages,
ask Jev which passage supports a label, and retain that original text and page.
A selected passage can still be irrelevant; provenance alone does not validate
the label. Screening every source passage, rather than only regex hits, would
test whether Jev expands coverage without inheriting the regex's blind spots.
Such a passage workflow would have a different measured cost from the table.

Repeating one question hundreds of times would mostly test stability and would
not remove shared misinterpretations. TypeSafe's own repeated-call example
shows some variation and changes a unique identifier between calls, so it does
not establish exact determinism on identical requests.
[Repeated-call example](https://docs.typesafe.ai/cookbooks/consistency_noul_cookbook).
The vendor's headline workflow evaluations use other models' probabilities as
references; they do not establish accuracy on historical CPC documents. Its
claim of zero hallucinations concerns guaranteed output types, not guaranteed
correct factual judgments. [Launch evaluation discussion](https://typesafe.ai/blog/introducing-system-one-models-and-jev).

## A bounded test using work already completed

Use the existing 30-report pilot to develop the question wording and compare
whole-report decisions with decisions supported by selected passages. Then freeze
the questions and evaluate on 100 additional already-coded reports, excluding
shared source bundles with development examples. Include different decades,
long reports, old OCR, positive cases, and reports with no documented concern.
This is a recommendation; no new sample has been selected or submitted.

Compare with the existing human columns and reconciled working values separately.
The latter include AI-assisted judgments and must not be advertised as an
independent human benchmark. Report false positives and missed positives by
field, alongside how many cases remain unresolved and how many can be accepted
at each confidence threshold. Check high-confidence negatives as well as
positives. The reported confidence is a function of the model's probability
distribution; thresholds must be tested on our documents rather than interpreted
automatically as observed accuracy. [Confidence reference](https://docs.typesafe.ai/confidence).
If three question variants are tested on 100 reports of the overall average
size, the baseline model charges would be roughly $0.11–$0.16, before extra
evidence requests or splitting. A small pilot is economically easy to justify.

If successful, Jev could handle the supported fields and send uncertain or
complex cases to a stronger reader. Keep the source text, question definitions,
version, raw probabilities, usage, and evidence selections. Rebuilding analyses
from saved responses would be reproducible even though future hosted-model
answers need not be identical. Existing human labels and prior pilot results
should remain unchanged.

Reproduction note: the local measurement used branch `cpc_llm_training` based on
`6077733` with existing uncommitted pipeline changes. It reads the narrative
labels and source links from `summarize_text_cpc_trends/output`, keeps links with
`text_included_flag == TRUE`, and resolves full text using the corpus manifest's
`local_text_path`. Count words separately for each included narrative/source
pair. The relevant CSV SHA-256 prefixes are `4dcbe4f743b2` (labels),
`9359e502984e` (source links), and `f58e8d832c42` (manifest). This dated cost
assessment preserves those measurements and the September 19 published price;
it is not a forecast that refreshes automatically when sources or prices change.
Compile with `make output/2026-09-19-jev-feasibility.pdf` from `logbook`.

## Trial preparation and access follow-up

Jacob authorized a small trial covering subjective questions, including Council
and civic-group positions and requests. His TypeSafe account showed waitlist or
pending access. The direct signup credit could not be verified from official
documentation. Vercel's Jev listing instead advertised free promotional input
and output through September 25, 2026 when checked on September 19.
[Vercel model listing](https://vercel.com/ai-gateway/models/jev).
Its documented TypeSafe-compatible endpoint accepts a Vercel AI Gateway key
without a TypeSafe key, so it supplies an alternative access route.
[Gateway API](https://vercel.com/docs/ai-gateway/sdks-and-apis/typesafe).
A subsequent request confirmed that Jacob's account also needs billing
verification before it can use this route, as recorded below.

The existing pilot task now prepares the same 30 human-coded source bundles
for 31 non-count questions. It retains broad and separated issue fields and
separate Council-member/civic-group support, opposition, and request fields.
Definitions incorporate the character/revision diagnosis and use broad CPC or
local issue coverage as the current working scope. This remains a development
sample. Human coding, regex predictions, and prior model answers are excluded
from the requests; the 13 detailed fields without direct human coding still
need a suitable reference before we can claim their accuracy.

The full 653 source pages and 180,853 words produce 35 requests: 26 complete
bundles and four bundles split into two or three parts. Splitting retains all
pages in order, including companions. Part answers are not automatically
treated as report-level labels. This pilot also lacks verified supporting
quotations; success at classification would not establish production readiness.
The gateway model identifier is an alias, so saving its returned identifier
does not necessarily pin a release. Exact requests, raw responses, and usage
will be archived for reproducible downstream processing.

Ordinary Make builds prepare inputs without API calls. An explicit
`make acquire-jev` attempts one request; after inspection, a request limit of
35 completes the batch. Acquisition saves attempts before submission, stops
on failed or unfinished requests, and never automatically retries. Its $0.25
planning budget uses the published non-promotional rate and native maximum
request size; this is separate from a gateway-enforced billing budget.
The live trial remains pending account verification. No account purchase was
made and no model answer was received.

The task was rebuilt and its data report inspected. All eleven previously
recorded pilot/reconciliation output hashes remained unchanged. Disposable
fixtures check page preservation, blinded input fields, missing-output and
changed-input rebuilds, parallel Make behavior, successful acquisition/resume,
changed-request rejection, HTTP failure, timeout, and missing credentials.
Those acquisition checks use simulated responses and do not measure Jev.
After Jacob saved the Vercel key in the ignored project `.env`, the key was
checked for presence without displaying its value. `make acquire-jev` sent the
first prepared request, `0568137a2968387b992b_part1`. Vercel returned HTTP 403,
`customer_verification_required`, saying that a valid credit card must be on
file to unlock free credits. The gateway returned no model answers or token
usage. The full request snapshot and rejected response are preserved under
`data_raw/cpc_jev_pilot/20260919_vercel_v1/`; no automatic retry followed and
no accuracy comparison is possible yet. The user received the billing
verification link from the response. No payment details were collected here.
The generated preparation details follow.
