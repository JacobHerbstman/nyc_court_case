# NYC Court Case

This repository builds the data and draft paper for a project on New York City housing production, homeownership exposure, and Council land-use decision making.

The workflow is task-based. Each production task lives in `tasks/<task_name>/` with `code/`, `input/`, and `output/` folders. Run a task from its `code/` folder with `make`. Run the paper from `paper/` with `make`.

`tasks/audits/` contains diagnostics, exploratory work, review queues, and validation exercises. The production task graph does not depend on audit tasks.

## Data Collection and Extraction

- `setup_environment`: records the R package environment.
- `source_registry`: copies the source catalog used by fetching tasks.
- `fetch_mappluto_archive`: downloads MapPLUTO release files.
- `build_nhgis_extracts`: standardizes NHGIS tract inputs for the 1990 homeownership measure.
- `build_zap_datasets`: standardizes ZAP project and project-BBL files.
- `fetch_council_land_use_records`: fetches and parses NYC Council Legistar land-use matter, action, history, and member-vote records.
- `fetch_council_member_roster_sources`: fetches source pages for Council member rosters.

## Cleaning and Intermediate Data

- `build_ccd2010_homeownership_1990_measure`: builds 1990 homeownership exposure for 2010 Council districts.
- `build_mappluto_current_lookup`: builds the current parcel lookup used for BBL and address joins.
- `build_ccd2010_mappluto_construction_proxy`: builds MapPLUTO-based construction proxies.
- `build_ccd2010_homeownership_long_units_series`: builds annual housing production series.
- `build_council_member_roster`: builds the Council member roster used to identify local members.
- `create_council_land_use_ai_geography_repairs`: stores accepted geography repairs for Council land-use matters whose affected districts were not clear from source tables. These rows were reviewed in ChatGPT using matter text, application identifiers, source links, and geography clues, then accepted only through a version-controlled decision ledger.
- `create_member_deference_nonapproval_geography_review`: stores the review queue and structured ChatGPT responses for non-approval land-use matters with unclear affected districts. Downstream tasks use these responses as review leads, not as final evidence by themselves.
- `build_member_deference_vote_panel`, `recover_member_deference_nonapproval_geography`, `verify_member_deference_nonapproval_geography`, `fetch_council_land_use_nonapproval_votes`, and `build_council_land_use_decision_panel`: build the Council land-use decision and local-member vote series.

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
