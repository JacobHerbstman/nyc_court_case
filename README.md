# NYC Court Case

This repository builds the data and draft paper for a project on New York City housing production, homeownership exposure, and Council land-use decision making.

The workflow is task-based. Each production task lives in `tasks/<task_name>/` with `code/`, `input/`, and `output/` folders. Run a task from its `code/` folder with `make`. Run the paper from `paper/` with `make`.

`tasks/audits/` contains diagnostics, exploratory work, review queues, and validation exercises. The production task graph does not depend on audit tasks.

## Data Collection and Extraction

- `setup_environment`: records the R package environment.
- `source_registry`: maintains the source catalog used by fetching tasks.
- `fetch_mappluto_archive`: downloads MapPLUTO release files.
- `build_dcp_boundaries`: standardizes DCP boundary files.
- `build_dcp_cd_profiles_1990_2000`: standardizes DCP community district profile files.
- `build_dcp_housing_database`: standardizes DCP Housing Database records.
- `build_dob_permit_issuance_harmonized`: builds harmonized DOB permit issuance records.
- `build_nhgis_extracts`: standardizes 1980 and 1990 NHGIS tract extracts.
- `build_zap_datasets`: standardizes ZAP project and project-BBL files.
- `fetch_council_land_use_records`: fetches and parses NYC Council Legistar land-use matter, action, history, and member-vote records.
- `fetch_council_member_roster_sources`: fetches source pages for Council member rosters.

## Cleaning and Intermediate Data

- `build_cd_homeownership_1990_measure` and `build_ccd2010_homeownership_1990_measure`: build 1990 homeownership exposure measures.
- `build_cd_baseline_1990_controls`: builds baseline community-district controls.
- `build_mappluto_current_lookup`: builds the current parcel lookup used for BBL and address joins.
- `build_mappluto_construction_proxy` and `build_ccd2010_mappluto_construction_proxy`: build MapPLUTO-based construction proxies.
- `build_cd_homeownership_long_units_series` and `build_ccd2010_homeownership_long_units_series`: build annual housing production series.
- `build_zap_housing_cohort_base`, `build_zap_housing_pipeline_from_raw`, and `build_zap_housing_hdb_link`: build ZAP housing project cohorts and ZAP-to-housing links.
- `cd_homeownership_dcp_supply_panel` and `cd_homeownership_permit_nb_panel`: build auxiliary community-district housing supply panels.
- `build_council_member_roster`: builds the Council member roster used to identify local members.
- `create_council_land_use_ai_geography_repairs`: stores accepted geography repairs for Council land-use matters whose affected districts were not clear from source tables. These rows were reviewed with ChatGPT using matter text, application identifiers, source links, and geography clues, then promoted only through a committed decision ledger.
- `create_member_deference_nonapproval_geography_review`: stores the review queue and structured ChatGPT responses for non-approval land-use matters with unclear affected districts. Downstream tasks use these responses as review leads, not as final evidence by themselves.
- `build_member_deference_vote_panel`, `recover_member_deference_nonapproval_geography`, `verify_member_deference_nonapproval_geography`, `fetch_member_deference_nonapproval_action_votes`, and `build_council_land_use_decision_panel`: build the Council land-use decision and local-member vote series.

## Paper and Summary Outputs

- `build_ccd2010_homeownership_1990_measure`: creates the 2010 Council district homeownership map used in the paper.
- `summarize_ccd2010_homeownership_long_units_series`: creates raw-unit descriptive housing production plots.
- `estimate_ccd2010_homeownership_long_units_event_study`: creates raw-unit event-study and long-difference outputs.
- `summarize_council_land_use_decision_trends`: creates the member-deference land-use decision trend plot.
- `summarize_citywide_ulurp_application_trends`: creates annual citywide ULURP application counts.
- `task_graph`: creates the production task graph and task inventories.

The current paper entry point is:

```sh
cd paper
make
```
