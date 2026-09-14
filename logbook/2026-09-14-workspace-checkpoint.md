---
title: "Checkpoint: community districts and CPC regex validation"
date: "September 14, 2026"
author: "Codex research record"
---

This checkpoint preserves the pending workspace on `cpc_llm_training`, following Jacob's instruction to stay on this branch and commit and push. The pending paper changes use New York City's 59 community districts, DCP's 1990 community-district homeownership counts, and the 25v4 MapPLUTO surviving-stock construction proxy. The 2010 Council-district alternatives move into the audit area. These substantive changes were already in the workspace when the checkpoint was requested; this record does not establish a new causal interpretation or a new decision about the preferred geography.

The checkpoint also preserves the pending CPC regex revisions and their validation code. Two existing Codex-coded samples, development and holdout, each contain 100 reports. Their source ledgers are now explicitly included in version control: these judgments cannot be recreated by rerunning the deterministic parser. The holdout is a model-coded benchmark, not an independent human gold standard. A separate comparison with Tyler's August 28 extraction description follows this checkpoint.

The main community-district source, panel, summary, and event-study tasks built successfully, as did the paper, CPC text labels, regex audit, community-district grouping audit, CB-opposition approval audit, anatomy summary, source registry, and task graph. A subsequent `make -n` proposed no producer commands in these 16 locations. The task graph was regenerated from the current dependencies. `git diff --check HEAD` passed before committing.

The broader sweep ran `make` in all 59 changed task-code directories, plus the paper and task graph: 17 calls succeeded and 44 failed. Fourteen failures were archived workflows with stale include or prerequisite paths. Thirty were auxiliary audit workflows, including Council-district alternatives. Their errors included unresolved NHGIS GIS and DCP profile paths stored in manifests, a missing housing-database catalog entry, and a file target incorrectly handled as a directory in the older ZAP source-integrity audit. These auxiliary failures remain unresolved in this checkpoint; successful paper and regex builds do not establish repository-wide replication. Repairing those unrelated workflows was not added to the requested parser comparison.

Reproduction uses the following Make entry points:

```
make -C paper
make -C tasks/summarize_text_cpc_trends/code
make -C tasks/audits/audit_ulurp_cpc_regex_labels/code
```

The full ZAP recovery preceding this checkpoint is recorded separately in `2026-09-14-zap-universe.md` and commit `d0cbb82`.
