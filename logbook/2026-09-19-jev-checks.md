---
title: "Project checks help; topic checks still reject valid evidence"
date: "September 19, 2026"
author: "Codex research record"
---

Jacob requested another iteration of the three-stage topic coding: discussed,
concern/request, and adopted commitment. This test isolates whether an earlier
selected passage establishes the exact topic/event and whether a companion
belongs to the focal project. The project check handles the known wrong links
correctly. The topic check rejects obvious mistakes but also rejects explicit
positive evidence. It should therefore remain a diagnostic, not a rule that
erases earlier positive labels or settles report-wide absence.

All 12 requests succeeded once, at $0 observed account usage; the balance remained
$5. The run checked all 85 positive concern/adoption claims from the preceding
v4 responses. These cover 40 selected passages in 12 reports. There were 79
window/topic questions and seven companion-scope questions. A window contains
the selected passage and its immediate previous/next passage from the same source,
including across page boundaries. The scope question compares the first two pages
of the focal and companion reports. The 19 source PDFs are public NYC CPC reports.
No human labels or private notes were sent. Calls were sequential, paced by ten
seconds, with no repeated requests or retries. The runner stops on the first rate
limit or observed charge; account observation is not a gateway-enforced price cap.

The questions and mappings were frozen before receiving answers. The event
question distinguishes concern only, adoption only, both, neutral description,
absence of this topic, and uncertainty. A concern must concern the specified topic;
a tenant speaker alone does not establish displacement. A commitment must be
accepted or imposed, rather than merely recommended. As in v4, a substantive
commitment may predate the current review; this is not a causal concession measure.
Each question is independent, and code combines the event and source judgments.
The model confirms 46 claims and rejects 39: 36 on the event and three on source
scope. It returns no explicit uncertain choices. That decisiveness does not imply
reliability. The same model produced the candidates and checks.

The seven source judgments match the relationships inspected during development.
For `C 930227 PPQ`, the two other Queens dispositions, `C 930233 PPQ` and
`C 940394 PPQ`, are identified as different projects. Their three selected concern
claims cannot establish issues for the focal disposition. The checker retains
five valid companion relationships, including `N 780405 ZRM` and `C 780265 MMM`
for `C 780522 HUM`, whose explicit cross-references establish a shared proposal
despite different recorded ZAP project IDs. Matching generic project names and
vote dates, or requiring equal ZAP IDs, would mishandle these examples. These are
seven known development relationships, not validation of every corpus link.
The existing source files and links remain preserved for diagnosis.

The topic check removes useful examples of overcalling. The tenant complaints in
`C 870568 ZMQ`, page 3, no longer count as displacement. The landscape-buffer
commitment in the `C 870729 HUK` companion to `C 870731 ZMK`, page 5, also no
longer counts as displacement. Character claims based on neutral descriptions
next to unrelated concerns are often rejected. But a strict filter loses seven
report/topic positives matching the original completed human reference. Against
the same 13 v4-answered reports, affordability recovery falls from 5/5 to 3/5,
infrastructure from 5/6 to 2/6, and character from 5/5 to 3/5. Traffic remains
7/7 and environment 5/7. Extra positives against original zeros fall from 17 to
eight across the five broad topics. Original zeros are not established truth;
several surviving extra character positives have substantive source support.

Reading all seven newly rejected original-positive report/topic pairs explains
why rejection is not equivalent to correcting a model error:

- **Explicit infrastructure evidence rejected:** `C 210192 ZMQ`, page 13,
  records that Community Board 8 cited concerns with infrastructure, parking,
  and affordable housing. The exact infrastructure sentence is in the supplied
  window, but Jev calls the infrastructure topic absent. `C 140388 PCX`, page 3,
  records a commissioner's question about whether DPR was at full capacity in
  the proposed facility; that public-facility capacity question is also rejected.
- **The earlier selected passage is not the best evidence:** for affordability
  in `C 220438 ZMK`, the selected windows contain a proposed affordable program
  and applicant testimony. Page 9, outside those windows, explicitly records CPC
  affordability concerns and urges HPD financing. Rejecting the selected window
  cannot settle the report's concern label. Retrieval needs to find another
  passage after rejection, rather than treating rejection as absence.
- **Category boundaries still matter:** the infrastructure window for
  `C 870731 ZMK` includes relocating bus stops, repaving and traffic measures
  from a previous approval of the same industrial park. Transit belongs in the
  working infrastructure definition, while traffic alone does not. Calling the
  whole infrastructure topic absent obscures that distinction and the difference
  between an earlier commitment and a new request.
- **Some original positives may use a broader meaning:** the selected character
  passages for `C 220438 ZMK` and `C 210192 ZMQ` largely describe compatibility,
  dimensions, and general development. The selected affordability passage for
  `C 870371 ZMK` describes subsidized housing; its companion hearing records
  support and the scarcity of an appropriate site. These do not automatically
  establish the narrower concern/adoption definition. They remain definition or
  evidence questions, not grounds to overwrite either original coder.

Report-level unions also hide stage errors. In `C 140409 ZSM`, the supplied
Community Board conditions request reduced parking and building height. The
checker labels those selected windows adoption-only, rejecting their request
stage even though both are recorded as recommendations. Other accepted claims
keep this report positive in the broad topic comparison. Good broad agreement
therefore does not validate concern/adoption distinctions.

The operational result is to retain the full sample and all raw stages, add
separate source/event diagnoses, and keep unsupported or ambiguous positives
unresolved. The new comparison retains all 150 v4 report/topic rows, including
17 reports whose v4 readings were unavailable. It preserves original Jacob,
Tyler, nonconflicting human, and AI-assisted working values without replacement.
A confirmed-stage indicator means evidence passed this model check; zero is not
proof of absence. Rejected positive candidates receive no final binary negative.
The next methodological priority is recovering another passage where the first
selection is insufficient and avoiding rejection of direct, attributed topic
statements. Blanket topic filtering is not ready for production. Council/civic
fields and count measures are unchanged by this test.

Reproduction: branch `cpc_llm_training`, based on `6077733`, preserving existing
uncommitted work. Ordinary `make` in
`tasks/audits/pilot_ulurp_cpc_llm_labels/code` rebuilds from saved observations;
`make output/2026-09-19-jev-checks.pdf` in `logbook` builds this report and the
linked generated findings. Explicit acquisition is `make acquire-jev-checks`.
Exact requests, model responses, request hashes, timestamps and credit checks
are archived under `data_raw/cpc_jev_pilot/20260919_vercel_v5_checks/`.
The five new CSVs have data reports. Disposable fixtures verify immutable
requests, no repeated attempts after resumption/failure, rate/charge stopping,
source versus event rejection, retained missing/ambiguous cases, unique keys,
unchanged original labels, and parallel/missing-output Make behavior. Earlier
pilot outputs and raw snapshots retain their exact bytes. This is a selected,
unblinded development comparison; it cannot estimate population accuracy.

\newpage
