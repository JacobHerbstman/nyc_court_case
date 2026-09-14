The following appendix is frozen at commit `94bd806`, before the extraction revisions.

# Checks from our parser and existing labels

These checks execute our parser only. They do not estimate agreement with Tyler's implementation, which has not been located. The two application examples are identified in his August 28, 2026 PDF, page 12.

Current output: 8,905 unique analysis narratives, 1976–2025. 2,906 rows list companion applications. These are not all ZAP projects and are not directly comparable with Tyler's 11,188 usable-report denominator.

| Count pair | Both counts present | Share of our narratives |
|---|---:|---:|
| Community-board votes | 6,324 | 71.0% |
| CPC hearing speakers | 6,902 | 77.5% |

## The two actual report examples

Counts below are read from our production CSV, not inferred from the snippets.

| Application | Votes for | Votes against | Our CB opposition flag |
|---|---:|---:|---:|
| C 780349 TCM | 2 | 21 | 1 |
| C 160174 ZSR | 17 | 14 | 0 |

Tyler reports the same two count pairs, but labels both formal board positions as opposition. Our second flag differs because it compares votes against with votes for whenever both counts exist. The second source report explicitly says that five abstentions counted as disapproval votes.

## Existing Codex holdout benchmark

This benchmark has 100 reports whose own action codes are ZM, ZR, or ZS. The reference labels were read by Codex after rule development. They are not independent human gold labels. Unclear reference values are excluded. The first rate counts an unparsed regex value as a miss; the second conditions on the regex returning a value.

| Field | Reference values | Exact / reference | Exact / parsed |
|---|---:|---:|---:|
| Supporting speakers | 96 | 75/96 (78.1%) | 75/91 (82.4%) |
| Opposing speakers | 95 | 82/95 (86.3%) | 82/89 (92.1%) |
| Supporting board votes | 90 | 79/90 (87.8%) | 79/86 (91.9%) |
| Opposing board votes | 90 | 79/90 (87.8%) | 79/86 (91.9%) |
| Revision or concession | 100 | 65/100 (65.0%) | 65/100 (65.0%) |
| Procedural response | 99 | 64/99 (64.6%) | 64/99 (64.6%) |
| Explicit local response | 100 | 77/100 (77.0%) | 77/100 (77.0%) |

\newpage

## Controlled snippets

These are synthetic diagnostic inputs to our actual counting functions. They reveal possible failure mechanisms, not their prevalence in the corpus. They bypass section selection and companion-document handling. No Tyler function is executed.

**An unreported side becomes zero.**

> Four speakers in favor appeared. The hearing was closed.

Our result (for, against): 4, 0.

**An explicitly absent side is also zero.**

> Four speakers in favor appeared. There were no other speakers. The hearing was closed.

Our result (for, against): 4, 0.

**A repeated vote is summed twice.**

> The Board recommended approval by a vote of 19 in favor, 3 opposed. The recommendation was approval by a vote of 19 in favor, 3 opposed.

Our result (for, against): 38, 6.

**An unsupported number word falls through to a singular-speaker rule.**

> Seventy speakers in favor and two in opposition appeared.

Our result (for, against): 1, 2.

**Spaced OCR number words do not resolve.**

> The Board recommended approval by a vote of t w e n t y - o n e in favor, 2 opposed.

Our result (for, against): missing, missing.

The parser source fingerprint is SHA-256:

```
4e86a23f0880cbc8cd3f37ff10c2efd2a17ab22ebc8b319dccdf43fc28a08193
```
