# Record ULURP CPC Corrections

This record-only production task owns the reviewed corrections used to build
and summarize the Department of City Planning CPC report corpus.

- `ulurp_cpc_source_corrections.csv` corrects verified identifier, URL, date,
  and source-availability errors in indexed rows. These corrections prevent the
  corpus builder from attaching the wrong document or treating a documented
  source failure as a download failure. It feeds directly into
  `build_ulurp_cpc_report_corpus`.
- `ulurp_cpc_index_additions.csv` records certified reports and related lead
  narratives verified to be absent from the official index. It is needed to
  preserve reviewed omissions in the corpus universe and also feeds directly
  into `build_ulurp_cpc_report_corpus`.
- `ulurp_cpc_narrative_boundary_exceptions.csv` records hash-locked boundaries
  and exclusions for reports whose OCR or structure defeats the general
  narrative parser. It feeds `summarize_text_cpc_trends`.
- `ulurp_cpc_community_district_corrections.csv` fixes one reviewed official
  community-district value used by the homeowner-tercile summary. It feeds
  `summarize_text_cpc_trends`.
- `ulurp_cpc_companion_reports.csv` records explicit referrals from certified
  reports to N companions. Each pair carries the certified source-text hash,
  the companion PDF URL, review date, and evidence. The corpus builder includes
  those indexed N sources, and the narrative producer attaches them as context.
  The first 20 pairs come from the September 14 selection audit; the two reviewed
  passages that did not establish referrals were not promoted. This ledger does
  not mark N reports as independent observations or change their official lead
  flags. Other possible referrals still require review.
- `ulurp_cpc_page_scope_reviews.csv` records hash-locked page ranges resolving
  application identity for `build_ulurp_cpc_reading_text`. The initial decisions retain
  the Queens report and local recommendations, exclude its unrelated Manhattan
  testimony, and preserve the Bay Ridge attachment blocks despite OCR-corrupted
  dockets. These are model-assisted source reviews, not new human topic labels. Every
  decision is validated against the source hash and applied page range.

The Makefile only verifies that the committed decisions exist; it does not
regenerate them.

### Attachment OCR source update, 2026-09-22

The complete pre-repair tables and text are saved in
`data_raw/cpc_text_repair/20260922_before/`. Three companion links were checked
against the repaired text; their recorded referral passages are unchanged, so
the full-text fingerprints and review dates were updated. Eight page-scope rows
were rechecked. Their application-scope decisions are unchanged; the reason
column records whether the reviewed pages were identical or newly extracted
continuations, maps, or photographs. The original review versions remain in the
snapshot.

Four additional page-scope records verify recovered attachments that page images
showed were missing before the repair: Bronx
Special Districts (38--52), 3276 Jerome Avenue (8--11), 19 East 72nd Street
(13--21), and Variety Boys and Girls Club (12--18). They document explicit project
identity, paired actions, OCR errors in docket headers, and contextual references
to other projects. These are Codex source checks, not new human coding labels.
