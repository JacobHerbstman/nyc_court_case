---
title: "CPC reading pilots, September 15--22: what we learned and why we changed the design"
date: "2026-09-22"
---

Between September 15 and 22 we tested three language models for coding CPC
reports. Each read a report and answered a fixed checklist: 27 topic questions
(for example, "is there a traffic/parking concern?" and "is there a
traffic/parking obligation?") plus Council and civic-group position questions.
None of these procedures was ready for the full corpus. The main obstacles were
the source text, the checklist format and the reference labels, more than model
capability. On September 22 Jacob decided to replace the checklist with
statement records, described below. The pilot code, raw model answers and 21
earlier logbook entries were then removed; the code remains in commit `970b2dc`.

**Jev** is an inexpensive classifier reached through the Vercel gateway. Its bulk
run over 8,905 narratives was paused on September 21 with 2,620 complete and
5,697 never attempted. Service failures dominated the later pilots: in a fixed
twenty-report comparison, 62 of 85 requests still returned HTTP 503 after one
retry, so only three reports could be compared across question versions. Where
answers returned, Jev confused requests with adopted conditions. In Commerce
Avenue (C 190426 PCX), Bronx Community Board 9 asked that sanitation vehicles
park on site (page 4), and the CPC approved without adopting that condition
(page 6). Jev nevertheless recorded a traffic/parking obligation, and it gave
three different answers about whether one tree-planting promise was a
character, design or environmental obligation.

**GPT-6 Luna and Sol**, at medium reasoning, ran as Codex subagents reading full
report packets. On four difficult reports they matched 75 and 76 of 79 earlier
reference decisions. On twenty further human-coded reports, Sol matched 84 of 98
broad issue codes, found 43 of 47 human-coded positives, and matched Council
involvement in 19 of 20 reports. A separate short pass that listed every actor
recovered participants the combined reading missed, such as the Building and
Construction Trades Council's support for the New York Wheel (C 150447 ZSR).
Every quotation matched the source. These were targeted samples, not accuracy
estimates, and the subagents processed two reports per job; the twenty-report
run took about 80 minutes.

Four problems recurred. First, the source text was wrong in two ways. Links
based only on a generic title and date (for example C-O-P dispositions) attached
one project's testimony to another; the September 21 repair requires a shared
ZAP project, an explicit application reference or a recorded companion
decision, which raised the narrative count from 8,905 to 9,063. Separately, OCR
had stopped at the CPC resolution, dropping scanned recommendations in four of
Sol's twenty reports (see the attachment-repair entry). Second, the checklist
made the model decide actor, status and topic again for every question, and
errors clustered there: request versus adoption, applicant versus independent
organization, concern versus opposition. Third, the human reference is coarser
than the checklist (five issue families on 340 reports), and Jacob and Tyler
disagree on 19 of 55 jointly coded reports for both character/preservation and
revisions. The pilots therefore could not validate the detailed fields, and
definitions were revised after each round. Fourth, interactive subagents are
not a practical way to process 9,063 narratives.

Jacob's decision is to extract one row per statement or action: the actor as
written, every role they hold, whether they belong to the project team, whether
they speak for an organization, the statement type (concern, request, applicant
promise, requirement by a deciding body, change to the proposal, project
description, finding), stance toward the project, whether the report says the
request was adopted, timing when stated, topic tags, exact quote, page and
application. Report-level topic and actor measures are derived from these rows
in code, so later definition changes do not require rereading reports. The
extraction runs once on frozen, repaired text and the rows are then treated as
fixed raw data. Jacob accepts the model's judgment of statement status; a small
spot check will report its error rate rather than adjudicate every row.

Two constraints carry forward. The page-scope step, now
`build_ulurp_cpc_reading_text`, holds any report with an attachment page it
cannot attribute to the focal application; on the repaired text that is 5,068 of
9,063 narratives. Pilot samples drawn after September 21 came only from ready
reports, and the new extraction needs a rule for attachments rather than
excluding most of the corpus. Original human coding and the full ZAP project universe are unchanged.
