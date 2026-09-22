---
title: "Other coded reports reveal Council attribution gaps"
date: "September 19, 2026"
author: "Codex research record"
---

Jacob approved freezing the acceptance/timing distinction and moving to other
questions. Ten other human-coded report bundles were tested. Nine calls
succeeded and one returned HTTP 504; that report remains missing without a retry.
Against nonconflicting original codes, the nine available reports recover all
five broad character positives, three of four affordability/displacement
positives, and three of four civic-position positives. Council positions are
the clearest failure: none of three original positives is recovered. Source
inspection shows actual misses, differences in definitions, and imperfect
original coding. These selected results are not population accuracy or new
human adjudications.

The questions separate topic discussion from concerns, questions, objections
and requests. Eight detailed topics map to five original broad fields. Council
members and independent civic groups each have separate support, opposition and
request questions. Each request asks 22 binary questions and 14 independent
supporting-page questions. Jev sees complete source bundles with page identifiers,
not human answers or notes. Acceptance questions in v7 are unchanged and were
not rerun. Concern-only answers are narrower than original topic codes that can
include adopted commitments; they are not interchangeable.

Selection targets positive and negative original labels, including scarce
Council and civic positions. Sources used in earlier pilots or source-review
adjudications are excluded, including shared source hashes. Ten bundles contain
13 public PDFs and 181 nonempty pages. Bundles over the existing 90,000-character
state limit remain in the roster but are not selected; selected text is not
truncated. Every candidate retains its selection reason. These experimental
restrictions do not change the CPC corpus.

Source inspection identifies concrete next refinements:

- **Council representatives:** `C 870650 ZSQ` points to companion
  `C 870605 PSQ`. Page 7 lists a representative of the Councilman from the
  15th district among speakers opposing the project. Jev's Council-opposition
  probability is 0.22, although its page selector finds that exact page.
  This is missed attributed opposition, not missing source text.
- **Requests that initiate projects:** `C 970324 DMM`, page 12, says Council
  Member Linares approached EDC requesting development of the garage. Asking
  only about a change, condition or mitigation inadequately covers a member
  requesting the project itself.
- **Concerns that prompt rezoning:** `C 090387 ZMK`, page 22, says the proposal
  responded to concerns of the local Council member, Community Board and
  residents. This is substantive involvement without an explicit endorsement
  of the resulting application. A concern/involvement field could preserve it
  without inventing support.
- **Civic applicant versus independent group:** in `C 180085 ZMQ`, the Variety
  Boys and Girls Club is the applicant. Pages 9--10 record support from its
  leaders and staff. The original civic code is support/request; the new
  independent-group question is negative. This is a role-definition difference.
  Nonprofit status and applicant role should both remain visible.
- **Original omissions and topic spillover:** `C 970324 DMM`, page 7, explicitly
  records building-design concerns, supporting Jev's broad character positive
  against the original zero. The same page discusses traffic but does not
  establish the separately claimed infrastructure concern. Detailed topic
  attribution needs work even when the broad union is plausible.
- **Negative findings versus concerns:** `C 240244 ZSM`, page 7, says no tenants
  were evicted and no occupancies terminated before demolition. Jev selects
  this for a displacement concern at 0.79. The selected passage documents the
  topic, but negative findings do not establish someone raising that concern.

Some topic discrepancies reflect the narrower measurement. The Rockaway
companion `C 870605 PSQ`, pages 4--5, imposes specific noise and parking conditions.
These support the older broad issue coding without necessarily recording a
concern/request. They must eventually enter through the separately preserved
commitment stage. A concern-only zero cannot establish final absence of an issue.
All original and experimental labels remain intact.

The next refinement should cover personal and representative Council statements,
requests to create a project, and concerns that prompted it. Civic actors need
an explicit applicant/independent role. Topic checks should contrast a raised
problem with a finding that it did not occur. We have not changed v8 questions
or thresholds after seeing results, or retuned the frozen acceptance wording.

There are 15 disagreements between binary answers and page choices. Some page
choices recover missed evidence; others select irrelevant pages. They cannot
automatically override classification. The failed report's seven mapped fields
remain unresolved. No available binary answer is exactly 0.50. Probabilities
are not calibrated accuracy estimates.

Observed account charge was $0 and balance remained $5. Successful calls
reported 168,489 input and 62,269 output tokens, including page-option probability
maps. Each request had one attempt, ten-second pacing and rate/charge stops.
Prior codebooks, outputs and archives retain their bytes; human and production
labels are unchanged.

Reproduction: branch `cpc_llm_training`, based on `6077733`, preserving prior
uncommitted work. Run `make` in
`tasks/audits/pilot_ulurp_cpc_llm_labels/code`, then
`make output/2026-09-19-jev-transfer.pdf` in `logbook`. Normal Make uses saved
observations; acquisition is `make acquire-jev-transfer`. The archive is
`data_raw/cpc_jev_pilot/20260919_vercel_v8_transfer/`. Six CSV data reports record
keys, missingness and fingerprints. Disposable fixtures check missing calls,
ambiguous opposition, duplicate rejection, immutable acquisition and
fresh/parallel/missing-output builds. The rendered logbook is inspected.

\newpage
