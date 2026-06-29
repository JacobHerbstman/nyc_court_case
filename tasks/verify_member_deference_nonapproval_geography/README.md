# Verify Member-Deference Non-Approval Geography

This production-support task verifies geography leads for unresolved first-pass
non-approval land-use matters against official records.

The upstream review queue contains land-use decisions with no clear affected
Council district that were fed into ChatGPT for a first-pass district inference.
This task treats those ChatGPT responses as leads only. A row is verified only when an
official Legistar record directly lists a Council district for the same matter
or a related LU/application record, or when the exact official matter text gives
BBLs that can be mapped to current MapPLUTO Council districts. Current MapPLUTO
matches remain backup geography and are flagged separately from direct official
district statements.

The task also writes a conservative queue for the downstream non-approval
vote-detail fetch. That queue keeps original affected districts first, then
uses deterministic geography recovery, then uses first-pass official
verification. The second-pass ChatGPT-only leads are not incorporated. The
current conservative queue covers 491 core non-approval matters. It keeps 236
original Legistar/ZAP geography assignments, adds 126 deterministic recovery
assignments, adds 37 official-verification assignments, and leaves 92 rows blank
pending manual review.
