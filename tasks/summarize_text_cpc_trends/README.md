# Summarize CPC Text Trends

This production task creates two initial motivating summaries of deterministic
text signals in readable CPC reports from 1975 through 2025. It uses the
reviewed section-aware rules developed in the audit work, removes repeated
boilerplate and known mechanical false positives, and reports document shares
rather than raw mention counts.

- `ulurp_cpc_text_signal_trends.pdf` compares all reports, non-PP reports, and
  ZM/ZR/ZS reports.
- `ulurp_cpc_text_signal_homeowner_tercile_trends.pdf` splits the same signals
  by the 1990 homeownership terciles used in Figure 2.

These are descriptive rule-based measures, not the planned hand-labeled or
LLM-trained outcomes. The reviewed narrative and district corrections are
preserved in `record_ulurp_cpc_source_corrections`.
