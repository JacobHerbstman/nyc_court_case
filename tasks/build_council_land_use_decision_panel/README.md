# Build Council Land-Use Decision Panel

This task builds the main matter-level Council land-use decision panel from the
audited Legistar/member-deference workflow.

The output unit is a Legistar matter. The panel keeps the full land-use-recalled
matter universe from 1998-2025 and attaches final-action vote evidence where the
upstream production-support tasks have parsed it: adopted matters from the
approval vote panel and core City Council non-approval matters from the
conservative non-approval queue. The local-member vote output is one row per
affected local Council member when the matter can be assigned to affected
districts and a roster member.

Some affected-district assignments enter through accepted AI-assisted repair
ledgers upstream. In those cases, land-use decisions with no clear affected
Council district were fed into ChatGPT as a first-pass document-reading aid, and
only the reviewed ledger decisions with source notes enter this panel.

This task is deliberately narrow. It does not emit source-fetch logs,
manual-review queues, prompt batches, official-source ledgers, or diagnostic files.
Those remain in upstream provenance tasks or in `tasks/audits/` as construction
records.
