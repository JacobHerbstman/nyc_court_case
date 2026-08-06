# Record ULURP CPC Training Labels

This record-only task owns the human-coded CPC report labels used to evaluate
and train later text classifiers. The CSV preserves coding decisions outside
the ignored audit workbooks, so rebuilding a workbook cannot erase completed
labels.

`ulurp_cpc_training_labels_jacob.csv` records Jacob's completed and in-progress
coding. It feeds label agreement checks and the future CPC text-labeling task.
Tyler's labels will be stored alongside it when they are ready.

The Makefile only verifies that the committed decisions exist; it does not
regenerate them.
