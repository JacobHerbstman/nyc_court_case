# Verify Member-Deference Non-Approval Geography

This audit task verifies ChatGPT geography leads for unresolved first-pass
non-approval land-use matters against official records.

The task treats ChatGPT as a lead source only. A row is verified only when an
official Legistar record directly lists a Council district for the same matter
or a related LU/application record, or when the exact official matter text gives
BBLs that can be mapped to current MapPLUTO Council districts. Current MapPLUTO
matches remain backup geography and are flagged separately from direct official
district statements.

The task also writes a conservative queue for the downstream non-approval
vote-detail audit. That queue keeps original affected districts first, then
uses deterministic geography recovery, then uses first-pass official
verification. The second-pass ChatGPT-only leads are not incorporated. The
current conservative queue promotes 194 of 220 first-pass non-approval matters
and leaves 26 rows blank pending manual review.
