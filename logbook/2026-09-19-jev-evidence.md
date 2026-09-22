---
title: "Concrete topic questions preserve earlier coding, but passage checks remain uneven"
date: "September 19, 2026"
author: "Codex research record"
---

The follow-up expanded the no-repeat Jev test to affordability/displacement,
traffic/parking, infrastructure/services, environment/open space, and
character/scale/preservation, with separate local requests, actor positions,
commitments, and responses. It retains the original field names and both
coders' values. Fourteen original fields are compared, including the broad
revision field derived from two explicit components. Seven further component
or detail fields remain exploratory rather than being treated as independently
human-validated outcomes. The earlier 31-question Jev trial is preserved.

Of 20 selected reports, 14 returned both the evidence-selection and verification
stages. Six retrievals failed (five HTTP 503 errors and one timeout); none was
retried. The run made 34 distinct requests, yielding 280 selection answers and
115 checks of selected passages. Recorded account usage increased by $0 and
the balance remained $5. The full sample remains in the output with failed
readings marked missing. The 20 are development examples selected from the
previous 46 complete bundles, not a fresh random holdout. Twelve named hard
cases/controls were supplemented by eight original topic/actor positives.
Original labels influenced selection but were withheld from model requests.

The questions require an observable event. For character, the model must find
a specific concern, question, request, or substantive assessment about scale,
physical design, neighborhood fit, or preservation. For affordability it must
find a substantive issue about rents, income eligibility, housing access, or
protection, not simply an affordable-unit count. Requests and actual commitments
are separate. A revision requires a change to an earlier version of the pending
proposal; a new proposal changing an existing building is not itself a revised
proposal. An older binding mitigation may still qualify for the separate,
broad commitment code. None of these definitions establishes a causal effect
of opposition on concessions.

Agreement with completed, nonconflicting original labels was 11/14 for the
combined affordability field, 12/14 for traffic, 11/13 for infrastructure,
12/13 for environment, and 5/12 for character, among categorical responses.
Availability and positive-code recovery are reported separately below.
Local requests matched 13/13; individual Council and civic positions each
matched 14/14, but each had only one original positive among successful cases.
Those actor results provide little evidence about sensitivity to rare positions.
On the same answered cases, the passage check did not improve affordability
agreement, reduced traffic and character agreement, and modestly improved
infrastructure and procedural-response agreement. Original and AI-assisted
working references are kept separate in the machine-readable comparison.

The additional evidence exposes two kinds of disagreement. In `C 140409 ZSM`,
the selected passage lists a Community Board condition requiring permanently
affordable units with specified income levels and distribution through the
building. The model rejects the combined affordability/displacement code
while accepting the separate affordability code. That is an evident model
inconsistency, not merely disagreement with a human label. The combined and
detailed topics disagree in nine report/topic pairs. A consistency check was
added after inspecting results; it flags these pairs without overwriting any
answer. Simply taking the union would mechanically improve some observed
agreement but would not validate the resulting codes.

Other discrepancies concern the reference definition. `C 860553 ZMM`, page 3,
explicitly describes CPC concern about development pressure and visual impacts
on the Apollo and Loews theatres. A positive character interpretation is
supported under the broad CPC-or-local definition despite the original zero.
`C 850302 HDX`, page 1, describes low-income housing and rent restructuring;
whether that is topic presence or a substantive review concern needs a clear
coding distinction. The latter example should not be made negative merely
to enforce a narrower new definition while claiming to reproduce the old one.
These are unblinded diagnostic readings, not new adjudications of original codes.

All outgoing material came from 33 public NYC government CPC PDFs already
verified in the prior experiment. The first stage retains every source-page
character in contiguous blocks of at most 1,800 characters, with source IDs,
pages, offsets, and hashes. The second stage receives each selected block and
its immediate neighbors from the same source. This is a limited context check;
it may miss supporting material elsewhere. A negative from either stage means
that this procedure did not establish the code, not that no relevant passage
exists. The same Jev alias supplies both stages, so their judgments are correlated.
No private coder notes, human labels, or local paths were submitted.

The next design should retain the older broad topic-presence code separately
from an explicitly raised concern, actor request, and adopted response. Detail
fields should feed a documented combined-topic rule, with contradictions retained
for review, rather than assuming independently asked broad and narrow questions
are logically consistent. This run supports using Jev to locate candidate
passages; it does not establish a reliable final automatic coder for character
or broad concessions. No production labels or corpus/ZAP restrictions changed.

Reproduction: branch `cpc_llm_training`, based on `6077733`, with the existing
uncommitted work retained. Run `make` in
`tasks/audits/pilot_ulurp_cpc_llm_labels/code`, then
`make output/2026-09-19-jev-evidence.pdf` in `logbook`.
The raw vintages are `20260919_vercel_v3_retrieve` and
`20260919_vercel_v3_verify` under `data_raw/cpc_jev_pilot`.
Ordinary Make uses saved observations and never makes inference calls.
Disposable fixtures verify no-repeat resumption, failure recording, the observed
charge guard, missing/unclear codes, changed-input propagation, duplicate rejection,
and parallel/missing-output builds. Every prior pilot output retains identical
bytes. All six new CSVs have verified row counts, keys, and fingerprints in
adjacent data reports. Prompt versioning and archived responses reproduce the
saved analysis, not future responses from an unpinned model alias.

\newpage
