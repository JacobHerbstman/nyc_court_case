# Summarize Council Land-Use Decision Trends

This task produces the canonical descriptive member-deference plot from the
1998-2025 Council land-use decision panel: the annual adoption rate when the
affected local member opposed adoption, with a trailing 5-year rate overlaid.

## Production Input Chain

The rolling-average member-deference figure is built from this production chain:

1. `tasks/fetch_council_land_use_records/` downloads Legistar recalled land-use
   matter, action, history, and member-vote records for 1998-2025.
2. `tasks/build_council_member_roster/` builds the member-to-district roster
   used to identify the affected local member.
3. `tasks/build_zap_datasets/` builds ZAP project and project-BBL data used as
   secondary geography when Legistar text does not list affected districts.
4. `tasks/build_mappluto_lots/` builds current MapPLUTO lots used only for BBL
   and address-based geography recovery.
5. `tasks/create_council_land_use_ai_geography_repairs/` creates reviewed and
   accepted manual/AI geography repairs.
6. `tasks/build_member_deference_vote_panel/` builds the approval-side matter
   universe, local-member vote panel, and queue of non-approval final actions.
7. `tasks/recover_member_deference_nonapproval_geography/` recovers geography
   for non-approval matters from application numbers, BBLs, addresses, and
   reviewed repair leads.
8. `tasks/verify_member_deference_nonapproval_geography/` applies conservative
   official-source verification and writes the non-approval queue used
   downstream.
9. `tasks/fetch_member_deference_nonapproval_action_votes/` fetches final-action
   vote details for the verified non-approval queue.
10. `tasks/build_council_land_use_decision_panel/` combines approval and
    non-approval evidence into `council_land_use_decision_panel.csv` and
    `council_land_use_local_member_votes.csv`.
11. `tasks/validate_council_land_use_decision_universe/` summarizes decision
    universe coverage.
12. This task produces
    `council_land_use_adoption_over_local_member_rollcall_opposition_rolling5_with_raw_clean.pdf`.

The following remain outside the production chain: prompt-batch construction,
ChatGPT response workspaces, split-vote investigations, and broader exploratory
audits. Those tasks document how repair candidates were developed, but the
production graph consumes only reviewed/conservative outputs.
