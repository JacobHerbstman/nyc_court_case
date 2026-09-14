# Audit ULURP CPC Regex Labels

This audit measures extraction coverage and agreement with existing coding.
Jacob's 200 reports provide human development comparisons. Two 100-report
Codex-coded samples are model-coded regression benchmarks. The second sample
was originally held out; it has now been inspected during rule development and
must not be described as a fresh test. Existing filenames retain `holdout` for
continuity. No manual labels are changed by the extraction program.

The Codex samples contain reports whose own action code is ZM, ZR, or ZS.
`unclear` reference values are excluded. Agreement outputs distinguish an
unparsed count from an incorrect parsed count and check reference source hashes.
Binary contextual measures are also compared, but count improvements do not
establish that regex understands revisions, conditions, or concessions.

Run `make` from `code/` to produce:

- The five existing training/validation/holdout agreement and disagreement CSVs.
- `ulurp_cpc_regex_coverage.csv` and `.md`: coverage by field and decade, reasons
  for review, and agreement with existing human development labels.
- `ulurp_cpc_regex_review_queue.csv`: narrative identifiers, status, source
  evidence, and document links for subsequent human or AI reading.
- `ulurp_cpc_regex_human_review_sample.csv`: 150 previously uncoded narratives,
  stratified by decade, action family, and extraction route. The sheet omits
  predictions and includes stratum population/sample sizes and sampling weights.

The new review sheet excludes every report in the three earlier samples and
those inspected during rule development, recorded in `code/rule_development_reports.csv`.
Related report bundles are excluded as well to prevent reusing the same hearing
through a different application number. Treat
it as an unfilled hand-coding source: copy completed judgments into the existing
manual-ledger task before rebuilding or altering sampling choices. Review the
whole focal report and related reports when needed; record evidence pages,
formal recommendation, literal motion votes, proposal-aligned votes, abstentions,
the abstention rule, hearing counts, and reasons a quantity cannot be determined.
Leave unavailable counts blank. Zero requires evidence. Use the weights for
population summaries, with separate results for extraction route and decade.
A weighted accuracy estimate and uncertainty interval require actual independent
human coding; this task does not invent those results.

`python3 test_cpc_counts.py` checks source-derived edge cases and controlled
ambiguity examples against the shared extraction functions. These tests catch
regressions; they are not an accuracy sample.

The prior comparison with Tyler is frozen in
`logbook/2026-09-14-regex-comparison.md` and its checks appendix, at the rules
from commit `94bd806`. Tyler's implementation and row-level output were not
available. The current parser adopts described features independently; it does
not claim a measured comparison against his actual code.
