---
title: "Separating requests, acceptance and timing"
date: "September 19, 2026"
author: "Codex research record"
---

Jacob asked to correct the remaining request-versus-acceptance error while
preserving the preceding gains, before moving to other reports he has coded.
The narrow follow-up corrects that error. Across seven propositions asked on
both excerpts and full reports, 13 of 14 checks match the expectations recorded
before the calls; one is unresolved. All 11 expected positives remain positive.
These are selected diagnostic checks in four source PDFs, with AI-authored
expectations. They do not estimate accuracy on unseen reports or replace
Jacob's and Tyler's original labels.

The earlier question combined applicant agreement and timing. It incorrectly
treated the board's height recommendation in `C 140409 ZSM` as proof that the
applicant agreed to reduce height after certification. We now ask two questions:
does the text explicitly report the applicant's agreement to this particular
change, and does it explicitly place that change after certification? The
acceptance criteria distinguish a reported agreement from a request,
recommendation or proposed condition. The excerpt and full-report text are
exactly the same as in v6; diagnostic references and human labels are not sent
to Jev. Other successful questions remain unchanged in v6 and were not rerun.

On the excerpt, height acceptance and timing now return probabilities of 0.22
and 0.41, below the existing 0.5 threshold. With the full report they return
0.97 and 0.95, consistent with page 11's explicit post-certification agreement.
The earlier compound question returned 0.71 on the excerpt. Separating the facts
and clarifying acceptance jointly improve this result; their individual effects
are not isolated. Neither the threshold nor the expected answers changed.

The same board-conditions excerpt contains an explicit agreement to provide a
minimum share of two-bedroom units. That remains positive, at 0.92, and also
remains positive in the full report. Thus the clarification does not simply
reject everything appearing under a board recommendation. The applicant's
meeting-space commitment, the agreed density cap, and the Public Development
Corporation's planted-buffer agreement all remain positive in both contexts.
An accepted voluntary commitment need not be a binding CPC approval condition.
Acceptance alone also does not establish that review caused a concession or
that the change occurred during the pending review.

The parking check supplies the remaining limitation. The excerpt says parking
will be reduced from 35 to 23 spaces within the board's recommended conditions.
Its probability is exactly 0.50, so our existing rule retains it as unresolved,
not as a correct negative. The full report records revised plans and the
applicant's undertaking to fulfill the board's conditions; that answer is
positive at 0.89. Insufficient evidence in an excerpt is not evidence that no
agreement exists elsewhere. We did not repeat the question, tune the threshold,
or delete the unresolved check to obtain an apparently perfect score.

This supports freezing the distinction for comparison on other already-coded
reports. The current questions name known provisions: they test recognition,
not discovery of every affordability, character or civic-group issue. The next
test needs reusable questions and reports outside this development set. Earlier
missing calls remain missing, with their original denominators.

All eight distinct requests succeeded, with one attempt each and ten-second
pacing. Returned usage was 45,686 input and 314 output tokens. Account usage
increased by $0, and the balance remained $5. The previous 70 pilot outputs
and archived observation files retain identical bytes; human and production
labels are unchanged. The existing comparison script handles both v6 and v7.

Reproduction: branch `cpc_llm_training`, based on `6077733`, retaining prior
uncommitted work. Run `make` in
`tasks/audits/pilot_ulurp_cpc_llm_labels/code`, then
`make output/2026-09-19-jev-acceptance.pdf` in `logbook`. Normal Make only reads
saved observations. The explicit acquisition target is
`make acquire-jev-acceptance`; immutable observations are under
`data_raw/cpc_jev_pilot/20260919_vercel_v7_acceptance/`.
Five CSV data reports record keys, missingness and fingerprints. Disposable
fixtures check missing responses, exact ties, duplicate rejection, no repeated
requests, charge/rate stopping, and parallel/missing-output builds. The logbook
links generated findings through Make, and the rendered PDF is inspected.

\newpage
