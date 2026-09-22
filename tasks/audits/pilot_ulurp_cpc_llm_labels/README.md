# Pilot ULURP CPC LLM Labels

## Twenty-report expansion, September 22

The frozen v3 test gives twenty additional human-coded reports separate topic
and actor readings by GPT-6 Sol at medium effort, with two reports per reader
job. The topic pass uses the existing 27 topic definitions unchanged. The short
actor pass clarifies project-team roles, individual versus organizational
positions, approval only at another site, and testimony in companion reports.
The source archive is `data_raw/cpc_subagent_pilot/20260922_sol_expansion/`.

The existing `prepare_cpc_sol_validation.py v3` selects from 122 eligible
human-coded reports using fixed hash ranks and explicit strata. It excludes
previous September 22 Sol/Luna source bundles, retains all supplied segments,
and flags older development history. Nine selected reports have multiple
prior Jev request parts. This targeted sample is not a representative holdout.
The source packets, questions and human labels are frozen before dispatch.
Usage interruption left some jobs without saved answers; continuation keeps
completed readings and restarts only missing jobs. Dispatch records distinguish
those attempts. No inference runs through Make, and corpus processing stays paused.

`compare_cpc_sol_pilot.py v3` validates the twenty topic readings and compares
five issue families against completed original human codes, separately for
Jacob, Tyler and their available nonconflicting references. It preserves both
the concern-only and concern-or-obligation mappings. `compare_cpc_sol_actors.py
v3` validates the separate inventories and keeps support, opposition, requests
and concerns before approximating the older actor categories. Both scripts
reproduce saved-response analysis through this task's Makefile; neither
replaces original human codes or production labels.

Known actor defects are rechecked separately on Bartow and Flatbush/Caton.
The first clarification omitted the Bartow operator; the final instruction
explicitly retains project actors and was rechecked on Bartow. Both readings
and prompt versions are retained. Flatbush's companion check uses the earlier
control instruction. These are development controls, not fresh accuracy tests.

The completed expansion returns all 40 report readings: 540 topic judgments,
134 selected topic evidence records, 190 actor/office records and 348 statements.
All positive/evidence quotes match their supplied segments. Two topic fields
remain unresolved; 16 yes/no responses are explicitly normalized to 1/0 while
retaining raw values. The broad topic mapping matches 84/98 original references
and 43/47 positives; Council involvement matches 19/20. These are comparisons
with imperfect historical labels, not validated population accuracy.

Post-reading review confirms three model errors: a councilmember-applicant loses
his Council role, a main zoning rule becomes a specific obligation, and a school
benefit becomes a concern. It also identifies recovered human omissions, mixed
stances and attribution risks. A page audit of 28 source PDFs confirms missing
scanned recommendation attachments in four reports. The producer's partial OCR
stops at the first CPC resolution. The frozen sample is retained, with this
limitation exposed in the source-page review and logbook. Broader processing
should wait for attachment recovery and these narrow classification fixes.
The new logbook entry is `logbook/2026-09-22-sol-expansion.md`; its Make target
includes both generated findings. Earlier results remain unchanged.

## Six-report actor transfer, September 22

The actor prompt remains byte-for-byte unchanged for six additional reports:
DUMBO, 610 Lexington, Parsons Boulevard, Bartow animal shelter, C7 Baychester,
and Flatbush/Caton. Three fresh Sol medium readers receive two packets each.
Selection excludes the earlier September 22 Sol/Luna sources. A fixed hash
rank chooses three cases outside the older pilot/source audits (two civic
opposition, one actor-negative) and three previously studied stress cases
(two Council opposition, one mixed civic). Earlier bulk Jev use is not excluded.
Sources, original human references, selection and dispatch are frozen in
`data_raw/cpc_subagent_pilot/20260922_sol_actor_validation/`.

`prepare_cpc_sol_validation.py actors_v2` reuses the existing selection producer;
its default still reproduces the older six-report topic sample unchanged.
`compare_cpc_sol_actors.py v2` reuses inventory validation, keeps support and
opposition separate, and compares broad categories against Jacob and Tyler
separately, retaining `both`. Its default preserves the first two-report results.
The post-reading manager source-review table is separate from the frozen human
references and never changes scores or raw answers. Make reproduces inventories,
comparisons and reviewed quotations offline.

All six return 63 actor/office records and 99 exact quotations. Both selected
Council-opposition cases are captured; all six Council categories agree with
available human codes. Four civic comparisons differ because support was missed
in an old label or because concern, resident affiliation and organizational
position are not equivalent. The source review also finds actual inventory
problems: a project operator and unnamed petition advocates coded as independent
civic groups, approval conditional on a different site coded as support, and
an unidentified opposing speaker omitted from a paired report. The short prompt
needs the existing civic-role definition and site/companion distinctions restored
in a few plain sentences. No larger run or topic recoding was performed.

## Separate actor pass, September 22

Jacob asked to keep the next refinement small. Two fresh GPT-6 Sol medium
readers each receive one complete report and a short actor-only prompt, with
no topic questions or earlier answers. The selected reports are New York Wheel
and Central Riverdale/Spuyten Duyvil, both already used in v2. These diagnose
the known omitted civic supporter and check a simpler civic-negative case;
they are not a fresh accuracy sample.

The frozen prompt, full packets, source checks, dispatch and raw answers are in
`data_raw/cpc_subagent_pilot/20260922_sol_actors/`. Each actor has a role and
attributed statements with request objects and exact quotations. There is no
actor-count cap. `compare_cpc_sol_actors.py` validates the source packets and
readings, flattens the statements, and keeps separate support and opposition
indicators for Council members and independent civic organizations. An approval
request is not mapped to a substantive-change request. Original v2 labels are
unchanged. Ordinary Make analyzes saved responses only; Jev and bulk work stay
paused. Findings and limitations are added to the existing Sol human-validation
logbook entry.

## Six additional Sol reports, September 22

The v2 Sol test keeps the 36-field codebook unchanged and adds four reader
checks: revisit actor evidence before assigning negatives, recognize definite
undertakings in original proposals, consider each explicitly supported topic,
and check evidence status against the quoted text. These checks do not settle
contested topic boundaries. There is no same-report comparison that isolates
their effect.

`prepare_cpc_sol_validation.py` selects six reports from 75 eligible reports
with completed original human coding. Frozen prior-pilot/source-audit exclusions
and a fixed hash rank select two Council support/request cases, one civic
support/request, one civic opposition, one long issue-positive case, and one
actor-negative case. Selected reports share no source. The 340-row selection
roster retains exclusion reasons. Earlier bulk Jev use and human analysis are
not excluded, so these are additional development reports, not an untouched
holdout. All retained text is supplied, including 50 segments for New York Wheel.

Snapshots and single readings from three GPT-6 Sol medium agents are in
`data_raw/cpc_subagent_pilot/20260922_sol_validation/`. The existing
`compare_cpc_sol_pilot.py` now takes `v1` or `v2`; its shared validation preserves
v1 results byte for byte. V2 saves all 216 detailed labels, selected evidence,
and separate comparisons against original Jacob, Tyler and nonconflicting human
codes. Two explicit topic mappings approximate the older broad issues; actor
stance and broader involvement remain separate. No original label is replaced.

All six reports return. The broad mapping matches 37/39 available original
reference decisions, including all 11 usable actor-position references. This
does not establish detailed accuracy: source review finds that Sol misses the
Trades Council's support for New York Wheel while finding civic opposition.
Tyler's original `both` captures the two stances; that nonstandard category is
preserved outside the scored reference. Several readers also misinterpret the
undefined `source_scope` flag as a substantive concern indicator; it must not
be used to filter reports. The new human-validation logbook records both issues.
The separate actor diagnostic above tests an inventory before aggregation.
Jev and bulk processing remain paused; normal Make makes no model calls.

## Sol versus Luna, September 22

Jacob requested Sol at medium and another small test. Three `gpt-6-sol`
subagents read the same four packets with the identical 36-field codebook and
reader instructions, changing only model and reader names. This holds the
reading procedure fixed more closely than the earlier Jev comparison. Raw
packets, prompt, pre-dispatch hashes, exact dispatch messages and answers are in
`data_raw/cpc_subagent_pilot/20260922_sol_comparison/`. Earlier Luna outputs and
references are preserved; the study is still development on known reports.

`compare_cpc_sol_pilot.py` validates Sol's readings, joins them one-to-one to
all 144 Luna fields, retains the 65 fields without references, and produces the
Sol labels/evidence plus paired comparison, agreement and findings outputs.
All four reports completed, with exact quotes for 47/47 positives and 36/36
evidence records. Sol matches 76/79 references versus Luna's 75/79; it recovers
three Luna disagreements but leaves two earlier positive matches unresolved.
The updated Luna logbook examines the quotations and model disagreements.
The comparison supports trying Sol medium on a fresh small sample, not a claim
of general superiority or a production model change. Jev remains paused.

## Luna feasibility check and Jev pause, September 22

Jacob stopped Jev and requested a small GPT-6 Luna subagent trial. The Jev
pilot runner now exits before credentials or requests; the corpus runner remains
paused. Acquisition commands documented below describe historical runs. The
12-statement v17 experiment was prepared but never submitted.

Three `gpt-6-luna` readers at medium reasoning read four complete source packets:
Commerce Avenue, Whitestone Lanes, Sunset Park Group III, and the ASPCA site.
The unchanged v13 codebook supplies 36 fields; supplementary evidence records
separate speakers, topics and statement status. Readers received no previous
answers. This is instruction-based blinding in a shared workspace. These are
selected difficult development cases, not a representative or fresh sample.

`prepare_cpc_luna_pilot.py` selects all retained segments for these four reports
from the frozen v13 sources. The source packets, codebook, reader instructions,
pre-dispatch hashes, requested model settings and returned readings are preserved
in `data_raw/cpc_subagent_pilot/20260922_luna/`. Codex account usage applies; no
Jev/API requests were made. Per-run dollar cost and immutable model version are
not available, and repeated model readings are not claimed to be deterministic.

`compare_cpc_luna_pilot.py` validates the submitted schema and quotations, and
produces labels, evidence, comparison, agreement and findings outputs. Normal
`make` reproduces these from saved observations without launching agents. All
four readings returned 144 judgments, with exact quotes for all 50 positives.
Only 79 fields have earlier references: Luna matches 75, missing four positives.
Those references are fallible AI/manager readings. The comparison also reports
matched availability for Jev; it does not treat server failures as negative labels.

The source review finds real omissions, questionable supporting reasoning and a
scope issue in the Sunset Park bundle: Groups II, III and IV are related projects,
but Group IV's building-line improvement cannot automatically establish a Group
III condition. Luna flagged that scope issue. Earlier raw answers and references
remain unchanged. The September 22 Luna logbook records these findings and the
limits of the comparison; production labels and the full sample are unchanged.

## Second authorized recovery, September 22 (v16)

Jacob requested another attempt on the 59 remaining received HTTP 503 screening
failures. The existing preparation script's `v16` mode reuses all 35 successful
parts/conditions and schedules exactly those failures once. Sample, source text,
questions, codebook and references stay fixed. Raw v13-v15 archives and outputs
are preserved. `acquire-jev-second-recovery` runs this explicitly; subsequent
checks use `acquire-jev-second-recovery-verification` and exact corrected v14
successes are reused. Normal Make reads the saved responses only.

The second recovery obtains 1 successful baseline section and 58 HTTP 503
failures. Baseline completion rises to 11/20 reports; revised completion stays
at 4/20. All nine evidence checks are exact cache hits. Observed spending is $0
and $5 remains. The fixed evidence-audit result remains 14/20.

The expanded September 22 retry logbook diagnoses the six unresolved or incorrect
fixed-window judgments. They come from three reports and include inconsistent
commitment/basis answers, a declaration whose approval is outside the selected
window, and debatable boundaries between design changes and imposed conditions.
The logbook proposes separate statement status and topic decisions, linked
acceptance passages, and checks on negative screening answers. Those proposed
refinements are not applied to this unchanged-question recovery, and original
reference labels are not revised to improve the score. Bulk extraction is paused.

## Complete-rule retry, September 22 (v14 and v15)

The authorized retry first isolates the v13 prompt-construction bug. The v14
mode of `prepare_cpc_jev_verification.py` copies the exact nine saved evidence
requests and appends every original field criterion, including to commitment
basis questions. It asserts that all rule text is present. Sources, question
batches, answer options, screening answers, source readings and aggregation
stay fixed. The corrected requests and runtime snapshots are recorded before
inference; the old results reproduce unchanged. This is development on already
inspected material, not fresh validation.

The separate v15 recovery then gives one additional attempt to each of the 62
previous screening server failures on the same twenty reports. All 32 prior
successful parts/conditions are reused. Evidence routing and definitions remain
fixed, with the complete v14 criteria. A full request SHA match reuses an exact
successful v14 evidence response; only genuinely new requests go to the API.
The comparison reports availability changes separately from the isolated
nine-window correction. A positive commitment predicate with an incompatible
basis stays unresolved, so predicate agreement alone is not usable accuracy.

The corrected checks return 9/9 valid responses. On the twenty fixed audited
pairs, usable correct judgments rise from 9 to 14 and false positives fall
from 5 to 0, but both versions confirm only 6 of 12 reference positives.
The screening recovery returns 3 successes and 59 HTTP 503 failures; it does
not complete another report. Completion remains 10/20 baseline and 4/20 revised.
All nine corrected checks are reused, with no new verification calls. Observed
spending is $0 and $5 remains. Bulk processing stays paused; an adopted-condition
error also remains outside the twenty audited pairs.

Explicit acquisition targets are `acquire-jev-corrected-verification`,
`acquire-jev-frozen-recovery`, and `acquire-jev-recovered-verification`. The first
and last allow one recovery of received server failures using
`JEV_CORRECTED_MAX_ATTEMPTS=2` or `JEV_RECOVERY_VERIFY_MAX_ATTEMPTS=2`; the screening
recovery allows only one additional attempt. Credit checks and the existing
free-only safeguards remain in force. A complete cache hit records a genuine
zero-request verification batch and credits without making inference calls.
Ordinary `make` reads saved observations only. The `v14` and `v15` outputs use
the existing producers and task, with the original source/reference inputs.

## Frozen comparison, September 22 (v13)

The completed run does not support production use. Baseline screening completed
10/20 reports and revised screening completed 4/20; none of the revised long
reports completed. All nine focused checks eventually returned, but a prompt
construction defect omitted the original per-answer criteria from those checks.
In particular, the explicit exclusion of Community Boards and government
agencies from civic organizations was absent. The frozen request producer and
archived bodies retain that behavior for reproducibility; do not reuse it as a
validated verifier. A subsequent corrected test must transmit both the field
instructions and their substantive answer criteria in a separately recorded
request vintage. The frozen v13 run is not silently rewritten. Observed spend
was $0. The generated findings distinguish the implemented test from the
intended definitions.

The fixed sample has ten selected controls and ten fresh reports: five with
one or two source parts and five with three or more. Fresh selection uses a
recorded seed among source-ready reports, excludes prior coding and audits,
and does not require an earlier Jev success. All supplied text is preserved.
The twenty reports, both question sets, source segments, control expectations,
reader assignments and protocol are frozen in
`data_raw/cpc_jev_corpus_audit/20260922_frozen_comparison/`.

The baseline uses the previous candidate questions on the same repaired
sources. Revised questions distinguish displacement from nuisance, preservation
goals from concerns, and specific undertakings or imposed conditions from
approval of the main action. They retain six separate Council signals and
exclude applicants/co-sponsors from independent civic groups. Historical
adopted fields are only diagnostic predictors of the new obligation construct.
The three new preservation-goal fields have no baseline counterpart. The
prespecified primary set contains 25 fields; other fields remain available.

Three Sol-medium readers supply blinded full-report references for all ten
fresh reports; two reports are read twice. Their judgments are frozen before
the manager inspects new Jev answers. Reader disagreements remain unresolved,
and original human labels are unchanged. These are fallible model readings,
not a human gold standard. A separate fixed-seed sample of twenty routed
window/field pairs receives a blinded evidence reading, without replacing
Jev's answers.

`prepare_cpc_jev_verification.py` routes revised positive target fields and
answer/page conflicts to a distinct question about the selected source segment
and its immediate retained neighbors in the same PDF. Context can cross original
request-part boundaries. Obligations additionally require a qualifying basis.
A rejected positive anchor becomes unresolved; it does not establish absence
from the whole report. A report enters the paired comparison only when all
screening parts and required verifier calls have succeeded. End-to-end yield
also retains failed reports in the fixed-sample denominators.

Run `make acquire-jev-frozen` for the initial screening batch and
`make acquire-jev-frozen JEV_FROZEN_MAX_ATTEMPTS=2` for its one permitted recovery
of received server failures. Then use `make acquire-jev-frozen-verification`
and its corresponding one-recovery invocation. These explicit targets archive
exact requests, every attempt and credit checks; successful requests are never
repeated. Unknown transport outcomes, invalid responses and rate limits are
not retried. Acquisition stops on an observed charge. Normal `make` only reads
the saved observations and does not launch APIs or readers.

The three linear producers prepare the fixed sample, prepare evidence windows,
and compare the saved answers. Their `cpc_jev_*_v13` datasets retain raw screening,
verified decisions, paired changes, service missingness, semantic uncertainty,
references and evidence validity. CSV saves also generate the usual data
reports. `cpc_jev_findings_v13.md` feeds the September 22 logbook PDF. Bulk
processing remains paused and production labels are unchanged.

## Repair validation, September 21 (v12)

Jacob authorized a bounded comparison after the source repair. The corpus runner
remains paused. `prepare_cpc_jev_repair.py` freezes seven development cases and
four fresh reports selected by seed from completed, source-resolved narratives
with at most two request parts. The fresh selection excludes the prior human
coding and source-audit reports and avoids shared sources within the sample.
Two Sol-medium readers supply blinded AI references, preserved separately from
human coding in `data_raw/cpc_jev_corpus_audit/20260921_repair_validation/`.

The comparison has three columns: saved historical answers; corrected text with
the original questions; and the same corrected text with the candidate questions.
Fourteen exact successful requests are reused, leaving eighteen distinct new
requests. The current candidate adds Council review concerns and clarifies civic
actors and adopted commitments. Expected development outcomes were recorded
before calls; fresh references were frozen before the manager inspected new
answers. They are fallible source assessments, not human gold labels.

Run `make acquire-jev-repair` explicitly for the initial one-attempt batch. A
second invocation with `JEV_REPAIR_MAX_ATTEMPTS=2` permits one recovery of received
502/503/504/529 server failures. It never repeats a successful answer or retries
an unknown transport outcome. The runner preserves each attempt and checks
credits before, during and after the batch; an observed charge stops work.
Normal `make` uses saved observations only. The v12 comparison requires every
part to succeed before evaluating a report and reports available and paired
field denominators separately. No production label is overwritten.

## Full corpus audit, September 19

Production extraction now lives in `tasks/extract_ulurp_cpc_jev_labels`. This task retains the earlier pilots and owns the new audit outputs named `cpc_jev_corpus_*_v2`. Normal `make` uses saved observations only.

The corpus audit preserves all 340 previously coded reports, including Jacob's and Tyler's original values and the working reconciliation. Its separate random source audit contains 24 reports selected before Jev results, read by three blinded Sol-medium readers. These are AI reference judgments, not human gold labels. Missing Jev fields stay in the comparison with an unavailable status; positive detection and false positives use explicit available denominators. A targeted review queue is kept separate and is still pending source review.

Since Jacob's September 20 decision to keep only successful results, agreement comparisons and the targeted queue use `cpc_jev_successful_labels_v2.csv`: every requested part must have returned a valid response. Partial and failed reports remain in the full coverage and reference tables, with their Jev comparisons marked unavailable. The saved raw answers and earlier reference judgments are unchanged.

`cpc_jev_corpus_zap_coverage_v2.csv` retains all 32,964 ZAP projects, including withdrawn projects without CPC reports. Coverage refers only to the linked CPC narratives; it is not a claim that all documents for a project have been found. The linked logbook report is `logbook/output/2026-09-19-jev-corpus.pdf`.

## Earlier pilots

The earlier v3 pilot uses Codex subagents running `gpt-5.6-sol` at medium
reasoning, following Jacob's September 15 decision to test existing human-coded
reports without paid API calls. It extends the prior API pilot within this task.
The older v1/v2 prompts, scripts, requests, and responses remain unchanged as
historical development material. Ordinary Make builds do not call the API.

## Council scale-up (Jev v11)

Jacob confirmed that a member's concerns prompting a rezoning count as a yes
for substantive Council involvement. Explicit endorsement stays separate.
The broad legacy support/request comparison includes motivating concerns when
opposition is negative; all five raw signals are retained. The literal v9/v10
component expectations remain historical diagnostics, not a reason to reject
Sunset Park's established involvement.

The existing transfer preparation/comparison scripts now also handle v11.
Fifty other coded bundles include all ten eligible original positives and forty
seeded original negatives, with complete text from seventy public NYC PDFs.
The full 340-document coded roster records every exclusion: prior source overlap,
incomplete references, overlength bundles and cases outside this fixed batch.
These are test restrictions, not deletions from the research universe.

`cpc_jev_codebook_v11.json` freezes the five best v9/v10 questions verbatim and
records the agreed broad mapping. Five independent supporting-page selectors
retain candidate evidence; they cannot override binary answers. Original human
codes and private notes are not sent. `make acquire-jev-council-scale` makes one
attempt per new report, stops on rate limits or observed account charges, and
archives exact requests and raw responses in
`data_raw/cpc_jev_pilot/20260919_vercel_v11_council_scale/`. Ordinary Make produces
six reported CSVs and findings without calling the API. No production label is
automatically replaced by this comparison.

The completed batch returned 47 successes and three server failures, at $0
observed charge with $5 remaining. Broad involvement agrees with original coding
in 46/47 answered reports: nine of ten positives and 37/37 negatives. Detailed
positions agree in 45/47. Six binary/page conflicts remain flagged; one is the
missed nonprofit-partner recommendation. These are comparisons to imperfect
original coding, not corpus accuracy. See the consolidated Council logbook.

## Council refinement (Jev v9/v10)

Five signals separate endorsement, opposition/withdrawal, requests to create the
project, requests for a particular provision, and concerns that prompted it.
Explicit member representatives count; institutional actions and filing do not.
`prepare_cpc_jev_council.py` freezes source judgments and exact text, and the
shared `compare_cpc_jev_simple.py` analyzes v6, v7, v9 and v10. Existing codebooks
and observations remain unchanged.

`make acquire-jev-council` submits twelve distinct v9 requests. The follow-up
`make acquire-jev-council-requests` submits nine v10 requests with revised
endorsement/request questions only. Both retain one-attempt acquisition and
existing rate/charge stops. All 21 succeeded at $0 observed charge. Archives
are `20260919_vercel_v9_council` and `20260919_vercel_v10_council_requests`
under `data_raw/cpc_jev_pilot/`.

The combined full-report result detects all six known Council-involvement cases
and keeps all three controls negative; 44/45 component expectations match.
These are selected diagnostics, not corpus accuracy. Sunset Park retains a
literal project-request diagnostic flag; Jacob accepted its broader involvement
as a fair yes. This flag is not a failure to detect Council involvement.
The final table carries 27 v9 component readings and updates 18 from v10, with
per-signal origins recorded. Failed new answers cannot fall back to old values.
Six CSV data reports per version accompany requests, controls, answers, usage,
agreement, context pairs, Council signals and findings. Normal Make uses saved
observations only. See `logbook/2026-09-19-jev-council.md` for source examples and
remaining limits. Production labels and original human codes remain unchanged.

## Reusable topic and actor questions (Jev v8)

Jacob approved freezing the v7 acceptance/timing distinction on September 19.
Its codebook, questions, raw observations and unresolved parking excerpt remain
unchanged. The next experiment asks other questions on ten other coded reports;
it does not rephrase or rerun the acceptance checks.

`prepare_cpc_jev_transfer.py` selects for original-label coverage, excluding
prior pilot/source-review documents and shared source hashes. The complete
human-coded roster retains exclusion reasons; these restrictions apply only to
the experiment. Complete source bundles within the 90,000-character state limit
are used without truncation. `cpc_jev_codebook_v8.json` defines eight topics with
separate discussion and concern/request questions, and Council/civic support,
opposition and requests. Independent page selectors provide candidate evidence.
The original codes and private notes are never transmitted.

`make acquire-jev-transfer` is the explicit ten-request acquisition entry point,
with one attempt per request and existing pacing, rate and observed-charge
guards. The immutable archive is
`data_raw/cpc_jev_pilot/20260919_vercel_v8_transfer/`. Normal Make produces the
sample, source pages, binary answers, usage, comparison, agreement and findings,
with six CSV data reports, without calls. Exact ties and unavailable calls remain
unresolved. Page selections cannot silently overturn the binary answers.

The topic comparison is concern/request only, which is narrower than the older
review-issue codes that can include commitments. Actor positions use opposition
precedence only when opposition is established; uncertainty is retained.
Original Jacob, Tyler, nonconflicting human and working references remain
separate. These results do not replace any production labels.

Nine of ten calls succeeded; one HTTP 504 remains missing. Observed charge was
$0. Available original positives recovered: character 5/5, affordability and
displacement 3/4, civic positions 3/4, Council positions 0/3. The source inspection
identifies representative Council statements, project-initiating requests and
concerns prompting rezoning as gaps, plus applicant/independent civic-role
ambiguity. See `logbook/2026-09-19-jev-transfer.md`. No v8 wording was changed
after responses.

## Applicant acceptance and timing (Jev v7)

The narrow follow-up preserves every v6 question and output. It tests revised
acceptance wording and a separate timing question against the identical v6
excerpt/full text in four source reports. The seven propositions include
positive commitments, including an explicit agreement inside a board's
recommended conditions. A section heading alone cannot determine acceptance.
Diagnostic expectations are frozen in `cpc_jev_codebook_v7.json`.

`make acquire-jev-acceptance` submitted eight distinct, one-attempt requests,
all successful, at $0 observed charge. Observations are archived under
`data_raw/cpc_jev_pilot/20260919_vercel_v7_acceptance/`. Normal Make prepares
controls and runs the shared `compare_cpc_jev_simple.py` with experiment `v7`;
it produces answers, usage, agreement, context pairs and findings without calls.
The five CSVs have deterministic data reports. The height error is corrected:
13 of 14 checks match frozen expectations, all 11 positives are retained, and
one parking-excerpt answer is an unresolved 0.50. No threshold was tuned and
no unresolved answer was counted as correct. These selected recognition checks
do not establish discovery accuracy on new reports. Earlier successful questions
were retained rather than rerun. See `logbook/2026-09-19-jev-acceptance.md`.

## Short factual questions (Jev v6)

The next controlled diagnosis freezes 25 short propositions in nine cases from
six public source PDFs in `cpc_jev_codebook_v6.json`. Its expected answers are
AI-authored diagnostic references, not replacements for original human coding.
`prepare_cpc_jev_simple.py` compares the exact v5 excerpt text with the entire
selected source PDF, and adds a guided-page control for the omitted affordability
passage. Each question is asked as Choice yes/no and native Noul against identical
text. The diagnostic references and private human labels are never transmitted.

`make acquire-jev-simple` makes at most 16 distinct, one-attempt requests with
existing pacing and rate/charge guards. Observations are preserved under
`data_raw/cpc_jev_pilot/20260919_vercel_v6_simple/`. Fifteen succeeded and one
full-report request returned HTTP 503. Observed cost was $0. Native probabilities
are validated by the existing runner, alongside historical Choice responses.
Normal Make produces controls, answers, usage, agreement, paired-context results
and findings, with five CSV data reports, without calling the API.

Both response types match 24/25 excerpt expectations, 22/22 available full-source
expectations, and 3/3 guided-page expectations. The one disagreement concerns
inferring later applicant acceptance from board conditions. The short questions
recover the explicit concerns and stage distinctions missed by the earlier
compound check. This is evidence that our design contributed to those failures,
not an estimate of accuracy for generic subjective coding. No original labels,
source relationships or sample restrictions change. The substantive record is
`logbook/2026-09-19-jev-simple.md`.

## Topic and project checks (Jev v5)

The next refinement checks every positive v4 concern/adoption claim: 85 claims,
40 selected passages, 12 reports. `prepare_cpc_jev_checks.py` includes immediate
neighboring source passages and source introductions. The frozen
`cpc_jev_codebook_v5.json` asks 79 window/topic questions distinguishing requests,
commitments, descriptions, other topics and uncertainty, plus seven separate
companion-project questions. Shared names/dates or different ZAP identifiers do
not determine project scope. Human answers and private notes are not transmitted.

`make acquire-jev-checks` is the only acquisition entry point for this experiment.
It allows one attempt per distinct request, waits ten seconds between requests,
and stops on a rate limit or an observed account charge. The immutable archive
is `data_raw/cpc_jev_pilot/20260919_vercel_v5_checks/`. Normal Make uses those
observations to produce the claim roster, checks, usage, comparison, agreement,
and findings, with five CSV data reports. It never calls the API.

`compare_cpc_jev_checks.py` retains every v4 report/topic row and all original
stage/human values. A rejected candidate does not establish absence elsewhere
in the report: rejected, ambiguous and unavailable positives remain unresolved,
not final negative codes. Raw v4 negatives are unverified no-candidate results.
Checking can reject a selected false positive, but cannot discover a missed
passage. This is selected development material and the same model supplies the
check; it is not independent validation. See `logbook/2026-09-19-jev-checks.md`
for substantive findings and `output/cpc_jev_findings_v5.md` for generated counts.

## Separate topic stages (Jev v4)

The September 19 follow-up uses `cpc_jev_codebook_v4.json` to ask separately
whether each of eight topics is discussed, raised as a concern/request, or
subject to an accepted/imposed substantive commitment. Broad original topics
are formed by explicit unions in `compare_cpc_jev_topics.py`; the legacy
review-issue candidate is concern/request OR adopted, not mere discussion.
Original Jacob/Tyler codes and production labels are retained. There is no
second model rejection stage; v3 checked and unchecked candidates are both
reported so removing that stage is not mistaken for a pure wording improvement.

`prepare_cpc_jev_topics.py` keeps the prior 20 cases and selects ten other
already-coded cases from the prior 46 for positive coverage. All 46 remain in
the roster. One explicit `make acquire-jev-topics` call prepares 30 requests
with 24 distinct questions each. The immutable observations are under
`data_raw/cpc_jev_pilot/20260919_vercel_v4_topics/`. No repeated readings or
failed-request retries are part of v4. The initial batch returned 13 successes
and 17 upstream high-demand HTTP 429 responses, at $0 observed charge. The v4
runner now stops on the first 429, following that diagnosis; the initial run
continued through all 30 requests. Successful calls are reused on resume.

Normal `make` produces the v4 sample, passages, evidence, usage, comparison,
agreement, and generated findings; six CSV data reports are written by their
producers. Missing responses retain all 24 fields as missing. Raw discussion
contradictions and post-run source/ZAP mismatch flags remain visible without
filtering labels. ZAP mismatch is a review flag, not an exclusion rule: some
explicitly related applications have distinct project IDs. Source excerpts
show both valid earlier omissions and spurious topic/application transfers.
The run is positive-sensitive but not sufficiently specific for automatic
production coding. The substantive record is `logbook/2026-09-19-jev-topics.md`.

## Jev passage-based topics (September 19)

The no-repeat follow-up adds `cpc_jev_codebook_v3.json` and two stages within
this task. It preserves 14 original issue/actor/process fields for comparison,
including the derived broad revision field. Seven further fields separate
specific topics and the two components of revision. The existing counts,
other original fields, and all prior pilot/human outputs remain available.

The sample starts with 12 named revision/character examples and deterministically
adds eight reports for coverage of positive original topic/actor codes. All
20 are among the previous 46 complete source bundles. Selection uses completed,
nonconflicting original labels; those labels and private notes are never sent
to Jev. Full page text from 33 public NYC PDFs is divided into contiguous
passages, without dropping text. A first request selects evidence for 20
questions; a separate request checks those passages with adjacent context.
The same model supplies both stages, so checking does not provide independent
validation or a complete inventory of every actor, request, or supporting passage.

Explicit acquisition commands from `code/` are `make acquire-jev-evidence`
and then `make verify-jev-evidence`. They use at most 20 distinct requests
per stage, one attempt per request, and a zero observed-charge budget. Successful
responses are reused on resume; failed requests are not retried. Archives are
`data_raw/cpc_jev_pilot/20260919_vercel_v3_retrieve/` and
`data_raw/cpc_jev_pilot/20260919_vercel_v3_verify/`. Credit checks follow each
successful call; observed balance changes are not a gateway-enforced price cap.
The completed run made 34 attempts: 14 successful retrievals, six failed
retrievals, and 14 successful checks. It cost $0 by recorded account usage.

Ordinary `make` produces six CSVs with adjacent JSON data reports: sample,
passages, evidence, usage, comparison, and agreement. The generated
`cpc_jev_findings_v3.md` feeds `logbook/2026-09-19-jev-evidence.md` through Make.
Failed requests retain all report/field rows as missing. The evidence CSV holds
selected text, source/PDF page/offset/hash, and separate selection/check scores.
The comparison preserves each original coder, the nonconflicting original
reference, and the AI-assisted working reference. A post-run consistency flag
records contradictions between broad topics and detailed components without
changing any returned labels. The combined topic questions still miss substantive
passages and disagree with their own details; these outputs are development
observations, not replacement production codes or population accuracy estimates.

## Jev development trial

The September 19 Jev preparation reuses the exact 30 v3 source bundles and
asks 31 non-count questions from `code/cpc_jev_codebook_v1.json`. Issue details,
Council-member positions, and civic-group support/opposition/requests are
included. The broader CPC-or-local issue definition is a working development
choice; reconciled AI-assisted labels are not an independent human benchmark.
Original human labels, regex predictions, and Sol responses remain unchanged.

Normal `make` also produces `cpc_jev_requests_v1.jsonl`,
`cpc_jev_sample_v1.csv`, and `cpc_jev_preparation_v1.md`, without inference.
Every source page is retained. The 90,000-character state limit is a planning
rule, not an exact token count. Long bundles are split at page boundaries and
must be reviewed jointly before report-level comparison. This first trial
collects classifications and probabilities, not verified supporting quotations.

TypeSafe signup was waitlisted. Vercel offers a
[TypeSafe-compatible endpoint](https://vercel.com/docs/ai-gateway/sdks-and-apis/typesafe)
using its own `AI_GATEWAY_API_KEY`, with no TypeSafe key required. Save that
key in the existing ignored project `.env` or process environment. The
[model listing](https://vercel.com/ai-gateway/models/jev) advertised free
promotional pricing through September 25, 2026 when checked on September 19.
Access and actual billing still require a successful authenticated request.
`typesafe-ai/jev` is a gateway alias, so even a returned alias cannot establish
an immutable model version.

The first live attempt on September 19 returned HTTP 403 with
`customer_verification_required`: Vercel requires a credit card on file to
unlock free credits. The request snapshot and rejected response are preserved
in the acquisition directory. No Jev answers were received. The acquisition
guard stopped that run. After Jacob verified his card, a read-only balance
check confirmed $5 available and $0 used. The authorized trial then used
`data_raw/cpc_jev_pilot/20260919_vercel_v1_verified/`, preserving the rejected
attempt in the original directory.

From `code/`, `make acquire-jev` explicitly submits at most one request.
After inspecting that response, `make acquire-jev JEV_REQUEST_LIMIT=35` can
complete the prepared batch. The runner archives exact requests and an
append-only attempt/response log under
`data_raw/cpc_jev_pilot/20260919_vercel_v1_verified/`. It reuses only completed,
matching requests. Changed inputs, unfinished calls, authentication errors,
and unexpected schemas stop acquisition. After observed HTTP 503 errors,
bounded retries were added for HTTP 429/502/503/504, preserving each attempt.
The default maximum is three attempts per request. Persistent service failures
remain missing while other reports proceed. No source is dropped or shortened.
The completed trial used a final explicit `JEV_MAX_ATTEMPTS=5` pass for the
six requests still unavailable after three attempts. Five then succeeded;
`C 030300 ZMK` remained unavailable. The sample contains all 30 reports:
25 whole bundles enter agreement tables, four fully received split bundles
await joint review, and the unavailable report has an explicit missing status.

The $0.25 model-charge planning budget reserves 64,000 input tokens per attempt
at the published $0.042/million non-promotional rate. It is not an enforced
gateway billing limit; use an account/key budget for that. Acquisition is
explicit and never a prerequisite of ordinary builds. Credit balances are
recorded before and after acquisition; their change measures account-level
spend at those timestamps, not a per-request invoice.

`compare_cpc_jev_pilot.py` reads the archived requests, responses, and credit
observations. Normal `make` rebuilds the part-level label and attempt/usage
tables, the report/field comparison, field-level agreement, and a findings
document. All 30 reports remain in the comparison table. Missing responses
and split bundles have explicit status and no report-level Jev value.
Agreement uses identical complete-bundle cases for Jev, Sol, and regex, with
Jacob, Tyler, completed nonconflicting human labels, and AI-assisted reconciled
working values kept as separate references. Probabilities and confidence are
saved without treating them as calibrated accuracy.

## Jev repeated-reading experiment

The follow-up uses `cpc_jev_codebook_v2.json` to distinguish concrete character
issues from descriptions/routine findings, and adopted revisions or substantive
commitments from requests, ordinary features, and procedural requirements.
It preserves the broad legacy definitions: CPC discussion can count, and an
undertaken or imposed commitment need not be new during review. A separate
timing question asks about new changes without redefining the legacy outcome.

`prepare_cpc_jev_repeats.py` freezes 46 complete bundles: the 26 unsplit original
reports (including the previously unavailable one) plus 20 additional reports.
The latter contain five reports in each original human character/revision
combination. Both original codes must be complete and nonconflicting. The new
bundles share no source with earlier pilots, source reconciliation, or each
other and must fit the 90,000-character state limit without truncation.
The selection roster retains all 340 human-coded reports with reasons,
including the four original split bundles. This is a deliberately balanced
development test, not a representative sample of the full CPC universe.

Each bundle receives five identical request bodies. Each request contains the
two original questions, three clarified phrasings of each, four diagnostic
questions, and two candidate source-page choices. Human values, notes, and
previous model answers never enter requests. The original question definitions
and source states are retained, though the question batch differs from v1.
[TypeSafe documents](https://docs.typesafe.ai/primitives) that questions are
evaluated independently. Identical answers do not distinguish model determinism
from provider caching, and repeated probabilities are not independent samples
of correctness. Candidate pages do not establish that their interpretation is
correct or provide verified quotations.

From `code/`, `make acquire-jev-repeats` explicitly submits the frozen 230
requests with bounded retries. This uses the same runner with experiment `v2`
and a zero-charge requirement: balances are recorded before, after, and every
ten successes, with acquisition stopping if observed usage increases. This is
an observation guard, not a gateway-enforced billing limit. Public-source
verification established that all 73 included PDFs come from NYC government
URLs and every outgoing page matches the extracted public text. The first
interrupted run had one 120-second timeout; that uncertain outcome was recorded
before resumption without replacing any earlier observation. Subsequent free
requests use a 60-second read timeout and retain/retry transport failures as
uncertain attempts, checking credits before continuing. V1 transport failures
still stop. Requests and raw attempts are archived in
`data_raw/cpc_jev_pilot/20260919_vercel_v2_repeats/`.

Ordinary `make` only prepares requests and analyzes saved responses. The v2
label, usage, comparison, stability, agreement, and findings outputs separate
one reading, majority across five identical requests, majority across three
phrasings, all fifteen clarified votes, and diagnostic-category classification.
A majority needs more than half of all planned votes; incomplete readings are
explicitly missing, and summaries use the same complete cases across methods.
Original human and AI-assisted working references remain separate. The analysis
counts both corrected earlier mismatches and new disagreements on earlier
matches. No production or human label is replaced.

The completed v2 run received all 230 planned responses (3,220 question
answers) over 289 recorded attempts, with $0 observed credit use. The two
readings still unavailable after three attempts succeeded in a final bounded
`JEV_MAX_ATTEMPTS=5` pass. Only two of the 92 original-question report/field
sets changed choices across repeats; majority voting repaired no additional
v1 mismatch beyond the first v2 reading. Clearer wording helped some revision
comparisons but did not improve character on the additional balanced sample.
See `logbook/2026-09-19-jev-repeats.md` and the generated v2 findings.

## Sample and source text

`prepare_cpc_subagent_pilot.py` selects 30 additional retained narratives:
ten with completed Jacob coding only, ten with completed Tyler coding only,
and ten with completed coding by both. Within each group it cycles through
available decades using a fixed hash ranking. These strata refer to completed
coding; a report can also have a provisional reading from the other researcher.
The old 20-report Sol API pilot is recorded in `cpc_subagent_exclusions_v3.csv`.
Its source bundles are excluded, and selected reports cannot share source
reports with one another. Selection does not depend on regex detections,
model performance, report length, or the content of the human answers.

Each packet contains the focal report and the companions whose text is included
in the current narrative bundle. It supplies their full extracted text, including
resolution and attachment pages, with source-specific PDF page numbers and
verified source hashes. Whitespace is collapsed within each page. No keyword
filter shortens the reading. This gives the readers more context than the
current regex's bounded narratives; a performance difference can reflect both
the method and available context. The full ZAP project sample is unaffected.

Three readers each receive ten primary packets and one packet assigned to
another reader. The three repeated reports are selected before reading. They
provide a small check on repeatability within this Codex setup. They cannot
establish a stable population error rate or equivalence with the API.

## Reading and preserved judgments

The frozen v3 prompt retains the 22 existing fields and adds seven issue details
and six separate councilmember/civic-group stance or request fields. The new
fields have no direct human reference. Evidence can support several fields and
records the exact quote, source, PDF page, actor, stance, and explanation.
The codebook requires evidence for positive labels, substantive categories, and
non-null counts. Missing counts stay null. Abstentions remain separate from
literal negative votes. Confidence is self-reported, not calibrated accuracy.

Readers start without this conversation's history and are instructed to read
only their assigned packets. Human answers, notes, and regex predictions are
not in those packets. This is instruction-based blinding in a shared workspace,
not a separate filesystem sandbox. Readers may use code to serialize responses
and verify quotations; substantive labels must come from reading the reports.
Their submitted, self-checked judgments remain in
`data_raw/cpc_subagent_pilot/20260915_sol_medium_v3/reader_a.jsonl`, `reader_b.jsonl`,
and `reader_c.jsonl`. These are recorded model observations with no Make producer.
The source snapshot also preserves the exact dispatch messages and shared worker
instructions. No submitted response is replaced after comparison with human coding.
All three readers revised saved citations during self-checking; a and b also corrected request hashes,
despite the instruction to preserve first saved responses. They report that
labels and confidence did not change. Original drafts were not retained (a later manager checkpoint exists for c), so the
files must not be described as untouched first responses. `self_check_notes.json`
records these provenance limits. Quote validity refers to submitted evidence.

The packets use an API-compatible request shape so their explicit prompt,
source input, and schema can be inspected and reused later. The archived API
`max_output_tokens` field is not a Codex execution cap. Codex's additional
instructions, tools, and accumulated reader context mean this is not an API
experiment. Exact token usage is unavailable from the collaboration tool.
Saving the raw responses makes the downstream dataset reproducible; it does
not make future model readings deterministic.

## Rebuilding the comparison

Run `make` from `code/` after the recorded readings are present. Make prepares
the packets and rebuilds the following files without launching agents or APIs:

- `cpc_subagent_sample_v3.csv` and the three reader packet JSONL files.
- `cpc_subagent_labels_v3.csv`: every model field, including repeats, confidence,
  required-evidence status, verified-quote coverage, and request hash.
- `cpc_subagent_evidence_v3.csv`: original quotations and source-page checks.
- `cpc_subagent_comparison_v3.csv`: unchanged human values and model/regex values
  by report, field, and reference. Nonstandard and provisional human values
  remain visible but are excluded from completed-reference summaries.
- `cpc_subagent_agreement_v3.csv`: agreement, availability, and binary precision
  and recall against Jacob, Tyler, and completed nonconflicting human values.
- `cpc_subagent_repeat_v3.csv`: paired model readings for the three repeats.
- `cpc_subagent_findings_v3.md`: the generated comparison used by the logbook.
  It also separates the direction of character/revision disagreements and
  summarizes these two fields across all completed, jointly coded human reports.

Identifier, request-hash, response-schema, and source-list errors stop the
comparison. Missing or mismatched quotations are flagged and retained. A quote
match checks traceability, not the correctness of its interpretation. Likewise,
a source-list declaration is not independent proof that every page was read.
Human disagreements are not settled by choosing a preferred coder, and none of
these pilot outputs replaces a production label. Data reports are saved by the
CSV producers as ordinary side effects.

## Character and revision diagnosis

The September 15 follow-up inspected all 17 disagreements in these two fields
against completed, nonconflicting human values (12 distinct reports), using the
saved evidence, source pages, original codebook, and human notes. Two existing
Sol-medium readers helped inspect passages after access to the human answers;
this was unblinded development, not a new validation run. The research record
and source-page examples are in `logbook/2026-09-15-sol-subagent-pilot.md`.

The original human issue codebook says "substantive local issue," while v3
explicitly includes CPC consideration. The broad revision field also includes
commitments and mitigation, without requiring proof of a new concession during
review. These differences matter when interpreting agreement. Saved discussion
notes are useful context, not replacements for the original workbook values.

`code/cpc_context_rules_v4_candidate.txt` is an unrun proposal clarifying only
these fields. It retains the existing schema and broad legacy definitions,
requires a concrete review issue for character, and distinguishes adoption,
timing, and application scope in revision evidence. It is intentionally not a
prerequisite of the frozen v3 packets. No v4 performance improvement is claimed,
and no human value or submitted v3 response has been revised. A later test should
use other already-coded reports before drawing conclusions about improvement.

## Interim corpus quality check

`audit_cpc_jev_quality.py` compares completed reports with the preserved human benchmark, distinguishing concern-only topics from the broader concern-or-commitment mapping. It also checks twelve additional positive reports using blinded Sol-medium source readings, verifies their source quotes and frozen selection, and reports internal evidence flags and completion by report length. Targeted checks and random-audit results remain separate; none is presented as verified overall accuracy. The fixed audit observations and sampling population are in `data_raw/cpc_jev_corpus_audit/20260921_quality/`.

Build `../output/cpc_jev_quality_findings_v1.md` from `code/`, or build `output/2026-09-21-jev-quality.pdf` from `logbook/`. The report does not alter any Jev question, model answer, original human label, or sample inclusion rule.

The quality audit also saves `cpc_jev_quality_link_checks_v1.csv`: included context companions sharing a nonempty title and vote date but having nonoverlapping recorded ZAP project IDs. This is a review screen, not an automatic exclusion rule. The directly checked C 920197 PPQ example confirms wrong-application attribution; other screened links are not presumed wrong. Original source packets, model answers and blind-reader judgments remain unchanged.

## Source-attribution repair

`audit_cpc_jev_scope.py` compares corrected v3 source packets with the preserved
v2 run, verifies that previously represented applications remain represented,
checks false and genuine companion examples, and writes the recovery plan,
source-link changes and `cpc_jev_scope_findings_v3.md`. Build that findings target
from `code/`, or the September 21 source-repair PDF from `logbook/`. Historical
v2 quality comparisons use the archived v2 human-reference and source-link
snapshots; they do not silently incorporate the repair. No target here calls Jev.

Historical pilot inputs are pinned to the preserved pre-repair source snapshot.
The original 30-report subagent packets reproduce the saved response hashes.
The default build runs the current corpus, quality and source-repair audits;
older pilot targets remain available explicitly. The repaired live source links
use a separate v3 input. Output pattern rules are limited to the local output
directory so a historical pilot cannot intercept an upstream corpus filename.

### Source vintage for completed trials

The v3 corpus inputs used by the completed Jev and Sol trials are pinned to
`data_raw/cpc_text_repair/20260922_before/`. That snapshot preserves the exact
roster, segments, requests, page scopes, and source links available before the
attachment OCR repair. Rebuilding these comparisons must not substitute newly
recovered passages for text the readers actually received. New readings should
use the current corpus producer in `extract_ulurp_cpc_jev_labels`.
