---
title: "CPC attachment text repair"
date: "2026-09-22"
---

The expanded Sol test exposed scanned recommendations that were present in the
saved CPC PDFs but absent from the text supplied to readers. This can create
false negatives for local concerns, requests, and official positions. Four of
the twenty focal reports had confirmed missing recommendation attachments.
That selected pilot is not a random sample for estimating the corpus-wide rate.

The cause was in text extraction: partial-page OCR stopped at the first CPC
resolution. The repair checks every page with fewer than 50 embedded words,
including pages after that resolution. It reuses the saved PDFs. Short pages
remain visible in the manifest, and a timeout remains an explicit failure flag.
The builder stages text and publishes it only after checking all source results.

The accompanying audit distinguishes short-page candidates, pages now yielding
at least 50 OCR words, and automatic recommendation markers. None of those
screens alone proves that a substantive recommendation was missing. Visual
checks found that photographs can produce many meaningless OCR words. Maps,
forms, and blank sheets must not be counted as missing narratives merely because
their original extracted text was short.

Additional source checks found newly extracted Borough President material for
St. Francis Preparatory School (C 130170 ZMQ, page 8), 551 West 21st Street
(C 150110 ZSM, pages 13--18), the New York Wheel (C 150447 ZSR, pages 50--51),
and the NYPD vehicle-storage facility (C 160249 PCQ, page 9). The West 21st
Street recommendation specifies bicycle-parking and street-signage requests.
These targeted checks establish examples of substantive recovery; they do not
estimate a population error rate. The Wheel PDF also illustrates the need to
separate readable recommendations from extensive plans and noisy image OCR.

The newly extracted committee minutes for 27-24 College Point Boulevard
(C 220185 ZMQ, page 12) also report Councilwoman Palladino’s support through her
representative. That is additional actor evidence for a future reading, not a
change to the historical model answers.

The recovered BP report for 809 Atlantic Avenue (C 190072 ZSK, page 32)
records three opposing speakers, including 32BJ. The main CPC narrative records
later support by that union. This changes the evidence available for substantive
opposition and the legacy civic-position summary. Both stages must be retained;
a broad "any opposition" summary is not the union's final position. The earlier
review is preserved in the snapshot. No coding ruling was changed in this repair;
the recovered passage is evidence for a later review.

The pre-repair manifest, complete extracted text, page fingerprints, source links,
processing roster, and manual source reviews are preserved in
`data_raw/cpc_text_repair/20260922_before/`. Source-link and page-scope reviews
are reanchored only after checking their evidence in the repaired text.

Many recovered pages cannot be attributed to an application from their text.
Under the earlier rule, `build_ulurp_cpc_reading_text` held any report with such
a page out of reading: 5,068 of 9,063 narratives on the repaired text, compared
with 4,720 before the repair (353 newly held, 5 released). Most of the hold
therefore predated the repair. Jacob then decided to supply these pages to the
reader, flagged as unresolved, and to record the application on each extracted
statement; all 9,063 narratives are now ready.

The corpus builder owns extraction, and the text-measurement audit owns the
before/after counts below.
