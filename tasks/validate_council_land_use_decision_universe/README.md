# Validate Council Land-Use Decision Universe

This task validates the main Council land-use decision panel against the
underlying Legistar broad-recall fetch outputs.

The validation object is the 1998-2025 Legistar matter universe used by
`tasks/build_council_land_use_decision_panel`. It checks that the broad recall
queries reconcile to Legistar-reported record counts, that the decision panel is
unique by matter, and that vote-detail and geography coverage are counted by
year. This does not validate that each Legistar matter is a unique land-use
project; project bundling remains a separate research decision.
