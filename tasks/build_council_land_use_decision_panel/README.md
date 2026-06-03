# Build Council Land-Use Decision Panel

This task builds the main matter-level Council land-use decision panel from the
audited Legistar/member-deference workflow.

The output unit is a Legistar matter. The panel keeps the full land-use-recalled
matter universe from 1998-2010 and attaches final-action vote evidence where the
upstream audit tasks have parsed it: adopted matters from the approval vote
panel and core City Council non-approval matters from the conservative
non-approval queue. The local-member vote output is one row per affected local
Council member when the matter can be assigned to affected districts and a
roster member.

This task is deliberately narrow. It does not emit source-fetch logs,
manual-review queues, prompt batches, official-source ledgers, or QC sidecars.
Those remain in `tasks/audits/` as provenance for the construction choices.
