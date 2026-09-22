# CPC narrative extraction with Jev

**Bulk acquisition paused September 21 at Jacob's request.** The active first pass and queued retries have been stopped. Both acquisition commands now exit before reading credentials or contacting the API. Do not restart until companion relationships and within-PDF application attribution have been repaired and audited, and Jacob authorizes resumption. Saved requests, responses and original labels remain intact; normal `make` can still rebuild from saved observations. The interim quality report documents the confirmed wrong-application attribution and the candidate links requiring review.

This task owns provisional corpus labels. It preserves every narrative in `summarize_text_cpc_trends/output/ulurp_cpc_text_labels.csv` and every source in the CPC report manifest. Questions, answers, and original human labels remain separate. The audit task `audits/pilot_ulurp_cpc_llm_labels` compares these labels with independent source readings and the existing human coding, and links coverage back to the full ZAP project universe.

Run from `code/`:

```sh
make                         # saved responses only; never calls Jev
make acquire                 # explicitly acquire previously unattempted requests
make acquire REQUEST_LIMIT=5 # bounded acquisition, also skipping attempted requests
make acquire-retries         # temporary errors only, at most six total attempts
```

The September 21 repair adds v3 preparation to normal `make`; it does not acquire
answers. `cpc_jev_roster_v3.csv` preserves every corrected narrative and marks
reports with unresolved page scope as `needs_scope_review`. Those reports have
no prepared requests and are never implicitly coded negative. The page-scope
ledger records every source page, decision, reason and source hash; only scoped
text from ready reports enters v3 requests and segments. Reviewed page overrides
come from `record_ulurp_cpc_source_corrections`. Distinct full PDFs with repeated
narrative prefixes remain available when their attachments differ. The previously
token-rejected document uses a smaller 22,000-character state cap if ready.

The v2 roster, segments, request index, source links, source manifest and human
reference snapshot are archived with the original request vintage. Historical
v2 builds read that snapshot; v3 uses corrected live sources. The audit's
`cpc_jev_recovery_plan_v3.csv` identifies successful request hashes that match
exactly, changed inputs and held reports. It does not create new model answers.
Source preparation is not substantive model validation; actor and adoption
questions remain provisional, and all acquisition commands remain paused.

The ignored root `.env` contains `AI_GATEWAY_API_KEY`; it is read without shell evaluation. Only public CPC source text is sent. API input excludes human labels, private notes, and this repository's instructions. Acquisition freezes the exact JSON request bodies and codebook in `data_raw/cpc_jev_corpus/20260919_v2/` and appends started/received records and credit checks. A lock prevents concurrent acquisition runs. `make acquire` skips every previously started request.

Jacob authorized recovery of temporary service failures on September 20 and confirmed on September 21 that steady progress matters more than finishing by the next day. `make acquire-retries` waits for the active acquisition to release its lock, then makes attempts 2 through 6 only for requests whose immediately preceding attempt returned HTTP 429, 502, 503, 504, or 529. Each pass waits at least 15 minutes before submitting work. Successes, invalid answers, and unknown outcomes are excluded. The same frozen request body is used. Attempts append to the existing journal under `(request_id, attempt)`; records without an attempt number belong to attempt 1. No old observation is edited or deleted. The first valid response is used, with its attempt number recorded in the answer and usage tables. Without success, the latest failure remains unavailable. Reports enter the working dataset only when every part succeeds. This is failure recovery, not repeated voting or selection among competing model judgments.

The frozen codebook is `code/cpc_jev_codebook_v1.json`: eight topics with discussion, concern/request, and adopted commitment; five Council fields; three civic fields. Council involvement includes a member's concerns motivating a rezoning. Endorsement remains separate. Topic adoption is provisional and does not establish a post-certification revision or a causal response to opposition. The task does not replace the earlier regex vote counts or original human judgments.

Packet v2 corrects the first corpus packet format's token overflow without changing the questions. It preserves complete source text in page segments, with short segment identifiers, at most 20 segments and 45,000 state characters per request. It never truncates a long report: 2,230 of 8,905 narratives span multiple requests. Split context is flagged. The three fully successful, single-part v1 reports are carried forward using their archived requests and segment map. Original v1 outputs and observations remain unchanged.

`cpc_jev_successful_labels_v2.csv` is the preserved provisional result of the paused run, not the corrected analysis dataset. Following Jacob's September 20 decision, it keeps a report only when every requested part returned a valid response. Partial, failed, and unattempted reports are excluded from these working results and agreement comparisons. A successful response is not proof of correct coding, and exact 0.5 ties remain missing.

`cpc_jev_labels_v2.csv` retains the complete roster and provisional values for coverage. `cpc_jev_answers_v2.csv`, `cpc_jev_usage_v2.csv`, and `cpc_jev_source_coverage_v2.csv` retain the detailed evidence and processing history. The answer table records page evidence, hashes and offsets; it does not convert an evidence disagreement into a new answer. Negative labels require all parts to be available and negative. A positive on any part yields a provisional positive in the full ledger, but a partial report is still excluded from the working dataset. Exact ties, failures, unattempted requests, and unknown outcomes remain explicit. Legacy topic comparisons use concern/request OR adopted; the concern-only values are also retained.

Acquisition uses one worker and starts at least ten seconds apart. A rate limit pauses submissions for at least 60 seconds or the provider's longer Retry-After interval, with increasing cooldowns for repeated limits. Ten consecutive transient failures trigger a 15-minute pause, followed by 30- and 60-minute pauses if the next attempts also fail. Further service failures now continue at one attempt per hour instead of terminating the batch. A successful answer resets the outage count. Restarts honor the remaining saved outage cooldown. Failed requests stay saved; subsequent work moves to unattempted requests. Six rate limits without a successful answer, unexpected answers, or observed account charges still stop the run. In-flight outcomes are saved. Credits are account-wide observations rather than per-request invoices. No automatic repeat-voting is used.

After the September 21 stops, a temporary failure of the read-only credit check pauses new submissions and retries up to five times, waiting 5, 15, 60, 120, and 300 seconds or a longer Retry-After interval. The runner does not proceed without a successful balance check. Authentication failures, observed charges, and persistent credit-check failures still stop acquisition. A known HTTP 400 `max_tokens_exceeded` response is saved and left unresolved without stopping unrelated requests; it is not retried unchanged. This preserves the requirement that every part succeed before a report is usable.

Build the research record with `make output/2026-09-19-jev-corpus.pdf` in `logbook/`. The stopped background run's session log is `/private/tmp/cpc_jev_corpus_full_run.log`; the underlying raw observations are the durable record.

The queued recovery passes were also stopped on September 21. Their session log is `/private/tmp/cpc_jev_corpus_recovery.log`; the attempt-numbered response journal remains the durable record. No recurring scheduled check was created.

`code/cpc_jev_codebook_v2_candidate.json` preserves v1 and records the candidate
Council-review concern, civic actor, and adoption-boilerplate refinements. It is
not used in v3 preparation, which isolates the source repair using unchanged
questions. The September 21 repair-test logbook records the bounded test and remaining
failures; this candidate is not adopted for bulk acquisition.

Jacob subsequently authorized the separate v12 repair-validation pilot in the
audit task. This bounded test does not authorize restarting the bulk runner.
