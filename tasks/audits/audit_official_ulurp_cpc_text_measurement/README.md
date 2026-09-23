# Audit Official ULURP CPC Text Measurement

This task audits the text boundaries and sample restrictions used by the CPC
label producer. Both tasks use `tasks/_lib/cpc_narratives.py` to find the CPC
resolution boundary, allowing for an earlier quoted board resolution. This retains the project
background, ULURP review, CB and BP recommendations, CPC hearing, and CPC
consideration while excluding resolution boilerplate and scanned appendices.

The narrative manifest preserves the source text path and exact character
boundary. It also defines three transparent universes:

- every distinct official certified CPC report;
- one comparable narrative per DCP-designated lead-report group, with exact
  duplicate narratives counted once;
- non-`PP` and `ZM`/`ZR`/`ZS` flags for planned robustness analyses.

No fuzzy matching or model judgment collapses reports. Related actions are
collapsed only when DCP marks one report as the lead for the same named project
and vote date, or when normalized narrative text is exactly identical.

Twenty-four page-rendered boundary exceptions are read from the record-only
validation task and locked to exact source-text hashes. They cover OCR-garbled
resolution cuts, visibly incomplete scans, a commissioner statement without
the majority report, and one report that explicitly delegates its analysis to
a companion. A changed extraction invalidates the recorded decision.

The deterministic decade sample exposes the narrative tail and the first text
excluded by the boundary. Its manual columns are intentionally blank pending
source review.

`audit_cpc_selection.py` checks the N-report exclusions and exact duplicate
groups against the saved index, source manifest, narrative manifest, and actual
label output. It asserts agreement in retained identifiers and narrative hashes.
It produces:

- `cpc_excluded_n_reports.csv`: every excluded indexed N row, with name/date
  matches, references from certified reports, and recorded review decisions.
  The key includes application number, date, and URL because an identifier can
  occur on more than one index row.
- `cpc_n_report_references.csv`: one row per certified-report/N-identifier pair,
  with source pages, an excerpt, and whether the N source is included, excluded
  from the index-based corpus, or absent from the saved index. Nearby referral
  language is only a screening flag; it does not authorize automatic inclusion.
- `cpc_duplicate_narrative_groups.csv`: one row per repeated narrative, retaining
  all member applications and project IDs, full-text and raw-prefix comparisons,
  and discrepancies in the representative's action classification and metadata.
- `cpc_selection_findings.md`: the audit findings included in the logbook.

The September 14 review found explicit referrals to excluded N reports and loss
of member metadata when choosing a duplicate representative. The subsequent
production correction adds the 20 reviewed N sources as context and preserves
represented applications in `ulurp_cpc_narrative_sources.csv`. The audit checks
that every collapsed application has a source link, metadata matches the corpus,
represented project IDs and action flags are preserved, and all measured text
hashes reconstruct from the included sources in their recorded order. Verified
N sources must be attached wherever their referring certified source is used.
The original narrative observations remain; these checks do not certify complete
historical coverage or validate every contextual judgment.
Reviewed passages and attachment differences are preserved in
`record_ulurp_cpc_source_validation/output/cpc_selection_reviews.csv` and checked
against source-text hashes on each build. Standard data reports accompany all
audit datasets. Run `make` from this task's `code/` directory.


## Attachment recovery

`audit_cpc_attachments.py` compares the preserved September 22 pre-repair
manifest and page fingerprints with the rebuilt source corpus. It preserves all
source rows and verifies document IDs, source URLs, source availability and PDF
page counts. Its page, report and summary outputs distinguish short-page
candidates, entirely empty extracted pages, pages recovered to at least 50
words, and automatic recommendation markers. Maps and blank sheets remain
visible; the screening counts are not manual classifications.

The former processing roster supplies an explicit mapping from source PDFs to
focal narrative bundles. A source can contribute to multiple bundles, so the
reported affected-bundle count uses distinct focal IDs. Four reports whose page
images showed missing recommendation attachments are checked separately. The
audit does not infer that newly extracted attachments belong to the focal
project; that is the page-scope check in `build_ulurp_cpc_reading_text`.
