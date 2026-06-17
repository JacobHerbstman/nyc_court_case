# Recover Member-Deference Non-Approval Geography

This production-support task recovers geography for first-pass non-approval City
Council land-use actions that do not already have affected Council districts in
the Legistar matter index.

The task keeps source flags explicit:

- existing Legistar affected Council district, when present
- ZAP application-number crosswalk to project Council districts, if available
- ZAP application-number crosswalk to project BBLs, then current MapPLUTO Council district
- BBLs parsed from the Legistar title, then current MapPLUTO Council district
- exact normalized title address and borough match to current MapPLUTO
- deterministic title-address variants, mainly small numeric address ranges,
  matched to current MapPLUTO

Current MapPLUTO matches are locational backups, not direct historical district
statements. They are carried with source flags and reviewed before entering the
main member-deference decision panel.

The unresolved rows are also exported as a plain-text review queue. Those prompts
are for research assistance only; any recovered geography should be entered back
with source URLs or official-document references before becoming final.
