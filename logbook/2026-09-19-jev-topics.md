---
title: "Separate topic stages recover positives but also overcall concerns"
date: "September 19, 2026"
author: "Codex research record"
---

Jacob approved separating whether a topic is discussed, someone raises a concern
or request, and an applicant accepts or an agency imposes a substantive commitment.
The new Jev test keeps all three measurements. Its final review-issue candidate
is concern/request OR adopted commitment; a neutral topic mention alone cannot
recover a legacy positive. The five broad topics are derived from eight detailed
topics by a fixed union, avoiding independent broad questions that contradict
their components. These definitions and mappings were frozen before the calls.
Original human and production codes remain unchanged.

The completed responses find every original positive for affordability/displacement
(5/5), traffic/parking (7/7), and character/scale/preservation (5/5), but also add
four, three, and seven positives, respectively, against original zeros. Character
is positive in 12 of 13 answered reports: its complete recovery of old positives
therefore does not establish good classification. Infrastructure recovers 5/6
positives with one extra; environment recovers 5/7 with two extras. These are
agreement statistics against completed, nonconflicting original coding, not
estimates of correctness. Broad adopted protections and substantive CPC discussion
can differ from the interpretation used in an original local-issue code.

Provider availability sharply limits the comparison. All 30 planned requests
were attempted once: 13 succeeded and 17 returned HTTP 429 with an upstream
high-demand message. Eight successes were among the preceding 20 v3 cases; five
were among ten additional already-coded cases selected for positive coverage.
Only four reports succeeded in both v3 and v4. On those four, affordability,
traffic, and character each recover 2/2 original positives versus 1/2 after the
v3 passage check; however, v3's unchecked candidates already recovered 2/2.
V4 adds extra positives in those categories. Thus the evidence does not isolate
an improvement from the new wording beyond removing the earlier rejection stage.
All 30 reports remain in the dataset; the unavailable readings are missing,
not negative. The 46-report candidate roster and the full corpus/ZAP universe
are retained. The additional ten appeared in v2 and are not an untouched holdout.

The source passages help distinguish promising recovery from model mistakes.
In `C 140409 ZSM`, page 8, the Community Board requests a specified number and
distribution of permanently affordable apartments at stated income levels; the
same page continues with a reduction in parking from 35 to 23 spaces. These are
concrete requests that the earlier checking stage rejected. The new model's
positive readings are supported when the selected passage is read with its
continuation. By contrast, `C 870568 ZMQ`, page 3, lists tenants' concerns about
playgrounds, parking, maintenance, and plumbing/electrical systems. The model
selects that passage as a displacement concern, although it establishes no
relocation, eviction, or comparable displacement issue. In `C 870731 ZMK`, an
adopted landscape buffer in companion `C 870729 HUK`, page 5, is incorrectly
selected for adopted displacement protection. Matching the stage without matching
the topic remains an important error mechanism.

Some original zeros are also questionable. `C 780522 HUM` explicitly refers to
`N 780405 ZRM` for the shared public hearing and project discussion. That companion
records objections to the office building's bulk, placement, effects on views,
and pedestrian circulation. A broad character or traffic positive is defensible.
The relationship is supported by both reports' cross-references, even though their
recorded ZAP project identifiers differ. These source checks are unblinded diagnostic
notes, not new human adjudications or replacements for original coding.

The same examination identifies a clear wrong-source problem. For `C 930227 PPQ`,
Jev selects conditions from `C 930233 PPQ` and `C 940394 PPQ`. These are separate
property dispositions in different Queens community districts, with distinct ZAP
projects. They appear as context companions under the generic name `C-O-P` and a
shared vote date. Their conditions do not establish concerns about the focal
application. A post-run source flag now records nonoverlapping ZAP project IDs
for selected companion evidence: it flags 13 concern/adoption selections in two
focal reports. It does not discard them or automatically reject all such links,
since the valid `C 780522 HUM` example is also flagged. An explicit same-project
relationship must govern use of companion evidence; shared generic names and dates
are insufficient. No source links or production inputs were changed in this trial.

Observed account usage increased by $0 and the final balance remained $5. The
13 successful calls returned 312 topic/stage answers, reporting 368,521 input and
160,172 output tokens. Each received answer and exact request is archived under
`data_raw/cpc_jev_pilot/20260919_vercel_v4_topics/`. Outgoing text came from 52
public NYC PDFs, with every source-page character retained. Neither original
human labels nor private notes were submitted. The inherited acquisition loop
continued to other requests after rate-limit responses; after inspecting the
batch, the v4 runner was changed to stop at the first 429 on future runs. That
change does not alter the recorded attempt history. No failed request or
successful reading was repeated in this experiment.

The three-stage measurement is worth retaining, but the current Jev outputs
remain candidate labels. Before scaling, topic-specific evidence and application
scope need checks that distinguish a tenant complaint from displacement and a
neighboring application's condition from a focal-project condition. The saved
original labels, source passages, and explicit stage fields provide the material
for that work without restarting the human coding.

Reproduction: branch `cpc_llm_training`, based on `6077733`, retaining the existing
uncommitted work. Run `make` in
`tasks/audits/pilot_ulurp_cpc_llm_labels/code`, then
`make output/2026-09-19-jev-topics.pdf` in `logbook`. Normal Make reads saved API
observations and does not run inference. Disposable fixtures verify one-attempt
resumption, charge/rate-limit stopping, immutable request snapshots, fixed union
logic with raw contradictions retained, missing observations, duplicate rejection,
and parallel/missing-output rebuilds. Data reports accompany all six new CSVs;
row counts, keys, fingerprints, and unchanged earlier pilot outputs were checked.

\newpage
