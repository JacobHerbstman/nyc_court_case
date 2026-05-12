# Build Member-Deference Overrule Candidates

Builds an audit-ready project-bundle series of Council approvals that overruled
local-member opposition in ULURP-related actions, including UDAAP. The first
pass uses the 2025 Charter land-use history table as a seed validation set and
official Council records for hard case checks; it does not claim complete
Legistar recall yet.

Outputs:

- `overrule_project_bundle_candidates.csv`: broad seed-backed candidate universe.
- `overrule_project_bundle_audited.csv`: analysis-ready labels and evidence tier.
- `overrule_action_crosswalk.csv`: bundle-to-LU/Resolution/ULURP/ZAP mapping.
- `overrule_time_series_year.csv`: annual confirmed, candidate, unresolved, and rejected counts with coverage flags.
- `overrule_residential_mixed_case_anatomy.csv`: residential/mixed-use seed cases compared against current ZAP housing flags.
- `overrule_candidate_qc.csv`: acceptance checks for known cases and join safety.

The design is intentionally modular: broad Legistar recall and pre-1998 archival
OCR can be added upstream without changing the downstream output contracts.
