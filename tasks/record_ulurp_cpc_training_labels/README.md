# Record ULURP CPC Training Labels

This record-only task owns the human-coded CPC report labels used to evaluate
and train later text classifiers. The CSV preserves coding decisions outside
the ignored audit workbooks, so rebuilding a workbook cannot erase completed
labels.

`ulurp_cpc_training_labels_jacob.csv` records Jacob's completed and in-progress
coding. It feeds label agreement checks and the future CPC text-labeling task.
Tyler's labels will be stored alongside it when they are ready.

`zone_change` records the literal zoning action. `dev_direction` records the
dominant practical development effect and can classify a non-zoning approval
as `more` only when it materially changes capacity or enables a substantial
redevelopment. Routine or merely legal approvals remain `none`. When the
fields were split in August 2026, existing direction codes were preserved in
`zone_change` and mapped mechanically to `dev_direction`. Four legacy mixed
cases were set to lower because their existing coder notes explicitly
described a dominant downzoning; no report was retrospectively reread.

The Makefile only verifies that the committed decisions exist; it does not
regenerate them.
