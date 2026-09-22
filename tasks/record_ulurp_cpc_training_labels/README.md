# Record ULURP CPC Training Labels

This task owns the human-coded CPC report labels used to evaluate
and train later text classifiers. The CSV preserves coding decisions outside
the ignored audit workbooks, so rebuilding a workbook cannot erase completed
labels.

`ulurp_cpc_training_labels_jacob.csv` records Jacob's completed and in-progress
coding. It feeds label agreement checks and the future CPC text-labeling task.
`ulurp_cpc_training_labels_tyler.csv` preserves the Coding sheet of Tyler's
existing workbook, including all 300 assignments. Two hundred rows contain
labels; 179 are marked complete and 21 remain provisional. The remaining 100
assignments stay blank. Together with Jacob's 200 completed rows, these contain
400 readings of 340 distinct reports. The overlap is 60 reports in the current
files, so the old ten-report disagreement note is not a current agreement count.

The unchanged workbook and discussion notes were recovered from saved
`origin/main` commit `f1fadd339867b70c82dd574b2a8f1c5e0ef1c09a`, under
`hand_coding_tyler/`, into `data_raw/cpc_human_coding/20260915/`. That snapshot's
README records byte hashes and recovery instructions. `import_tyler_coding.R`
converts the sheet to CSV without renaming columns or revising values. The
workbook retains the original formulas, codebook, and formatting. Discussion
notes and proposed corrections are preserved as source material, not executed
as instructions or silently applied to the coding.

The current Jacob CSV remains his source; the older workbook on main, with
only ten completed rows, does not replace it. These human sources now feed
`summarize_text_cpc_trends/output/ulurp_cpc_human_coding.csv` and the regex audit.

`zone_change` records the literal zoning action. `dev_direction` records the
dominant practical development effect and can classify a non-zoning approval
as `more` only when it materially changes capacity or enables a substantial
redevelopment. Routine or merely legal approvals remain `none`. When the
fields were split in August 2026, existing direction codes were preserved in
`zone_change` and mapped mechanically to `dev_direction`. Four legacy mixed
cases were set to lower because their existing coder notes explicitly
described a dominant downzoning; no report was retrospectively reread.

The Makefile treats Jacob's committed CSV as a source and converts Tyler's
saved workbook through a concrete input link. Neither recipe creates human
judgments. Run `make` from `code/`; R's `readxl` is listed in environment setup.

`ulurp_cpc_coding_adjudications.csv` records source reviews of all 204 differing
field values across 51 overlapping reports. These are AI-assisted judgments,
not a third human coding round. Each row preserves both original values, local
and broad issue readings, the ruling status, confidence, reason, and quotations
with PDF page numbers and source-text hashes. The submitted reviews and protocol
are frozen in `data_raw/cpc_human_coding/reconciliation_20260915/`. Manager notes
identify subsequent decisions in the source ledger.

This ledger is a recorded source with no automatic judgment-producing recipe.
The summarization task applies resolved rulings to a separate reconciled table
and verifies the source hashes and quotations first. Five differences remain
unresolved. Original coding, including provisional entries and nonstandard
categories, is retained. These reviews do not provide independent validation
for models used to help produce them.
