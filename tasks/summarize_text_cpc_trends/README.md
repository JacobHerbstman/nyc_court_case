# Summarize CPC Text Trends

This production task measures deterministic text signals in readable CPC
reports from 1975 through 2025 and creates two initial motivating summaries.
It uses conservative report sections and local context, removes repeated
boilerplate and known mechanical false positives, and reports document shares
rather than raw mentions.

Reports decided on the same date are bundled when they share a ZAP project ID
or cite one another as related applications. The focal narrative hash remains
unchanged; the output separately records the bundled-text hash, word count,
and contributing companion applications. Historical prose transitions are
used when older reports lack modern CB, BP, hearing, or consideration headings.

The document-level file follows the human coding sheet where regex can make a
defensible measurement: substantial opposition, local requests, revisions or
concessions, responses, unresolved objections, CB opposition, broader CB/BP
activity, councilmember and civic-group positions, and five issue families.
Actor-specific events require an actor and stance or request in the same
sentence. Adjacent sentences are joined only when the second begins with an
explicit continuation such as a pronoun or "in response."
An observed CB vote determines the opposition indicator; textual stance
language is used only when the report does not provide a vote count. These
remain rule-based proxies rather than replacements for the hand coding. Narrow
response and revision rules favor precision over recall.

The same file records narrative word count and exact reported counts of CPC
speakers in support and opposition and Community Board votes supporting
approval and disapproval. A blank means that an exact count was not
established; zero is used only when the report establishes zero. The plots show
count-reporting coverage separately from mean counts among reports with an
exact count, because reporting completeness changes sharply over time.

- `ulurp_cpc_text_labels.csv` contains one row per analysis narrative.
- `ulurp_cpc_text_signal_trends.pdf` compares all reports, non-PP reports, and
  ZM/ZR/ZS reports.
- `ulurp_cpc_text_signal_homeowner_tercile_trends.pdf` splits the same signals
  by within-borough terciles of 1990 community-district homeownership.

The reviewed narrative and district corrections are preserved in
`record_ulurp_cpc_source_corrections`.
