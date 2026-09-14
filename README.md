# NYC Court Case

This repository builds the data and draft paper for a project on New York City housing production, homeownership exposure, and Council land-use decision making.

The workflow is task-based. Each main task lives in `tasks/<task_name>/` with `code/`, `input/`, and `output/` folders. Run a task from its `code/` folder with `make`. Run the paper from `paper/` with `make`.

Task Makefiles declare literal inputs and outputs. `tasks/generic.make` owns
recursive upstream checks; input recipes are plain symlinks. Coupled outputs
use shared pattern rules supported by GNU Make 3.81. Missing actual outputs
rebuild their producer. Dataset reports in `report/` are save-time side effects,
not build targets. Committed human coding tables have no automated producer.

## Task Graph

The graph below is generated from the concrete dependencies declared in the
main task Makefiles. Run `make task-graph` from the repository root to update it.

![Main task dependency graph](tasks/task_graph/output/task_flow.png)

## Running the Project

After downloading the repo, install the system tools used by the pipeline:

- GNU Make
- R
- Python 3 with `pip`
- LaTeX with `pdflatex` and `bibtex`

The root Makefile runs `tasks/setup_environment` before building the paper. That
task checks command-line tools, installs missing R and Python packages, and
prints the exact Homebrew or apt command to run if a compiled R package such as
`sf` needs geospatial system libraries.

For a full rebuild when the NHGIS files are not already saved in `data_raw/`,
set an IPUMS API key first:

```sh
export IPUMS_API_KEY="your-ipums-key"
make
```

Equivalently, from R you can run
`ipumsr::set_ipums_api_key("<your key>", save = TRUE)` and then restart R
before running `make`.

The pipeline downloads public source files as needed into task outputs or
`data_raw/`. The paper's community-district treatment uses DCP's exact 1990
profiles and standardized community-district boundaries. The IPUMS key is used
only by tasks that require NHGIS extracts.

## Data Collection and Extraction

- `setup_environment`: installs and records the R and Python package environment.
- `source_registry`: copies the source catalog for paper and member-deference inputs.
- `build_dcp_cd_profiles_1990_2000`: downloads and parses DCP's 1990 community-district profiles.
- `build_dcp_boundaries`: downloads and standardizes DCP community-district boundaries.
- `fetch_mappluto_archive`: downloads the pinned DCP MapPLUTO 25v4 archive ZIP used by the paper construction proxy.
- `build_nhgis_extracts`: standardizes NHGIS tract inputs for the 1990 homeownership measure.
- `build_zap_datasets`: standardizes ZAP project and project-BBL files.
- `fetch_council_land_use_records`: fetches and parses NYC Council Legistar land-use matter, action, history, and member-vote records.

## Cleaning and Intermediate Data

- `build_cd_homeownership_1990_measure`: builds exact 1990 homeownership exposure for the 59 community districts.
- `build_mappluto_current_lookup`: builds the current parcel lookup used for BBL and address joins.
- `build_mappluto_construction_proxy`: builds community-district MapPLUTO construction proxies.
- `build_cd_homeownership_long_units_series`: builds annual community-district housing production series.
- `build_council_member_roster`: builds the Council member roster used to identify local members.
- `create_council_land_use_geography_review_ledgers`: stores reviewed geography corrections for Council land-use matters with unclear affected districts.
- `build_member_deference_vote_panel`, `recover_member_deference_nonapproval_geography`, `verify_member_deference_nonapproval_geography`, `fetch_council_land_use_nonapproval_votes`, and `build_council_land_use_decision_panel`: build the Council land-use decision and local-member vote series.

## Paper and Summary Outputs

- `build_cd_homeownership_1990_measure`: creates the community-district homeownership map used in the paper.
- `summarize_cd_homeownership_long_units_series`: creates raw-unit descriptive housing production plots.
- `estimate_cd_homeownership_long_units_event_study`: creates raw-unit event-study and long-difference outputs.
- `summarize_council_land_use_decision_trends`: creates the member-deference land-use decision trend plot.
- `summarize_citywide_ulurp_application_trends`: creates annual citywide ULURP application counts.
- `summarize_text_cpc_trends`: creates initial rule-based CPC text-signal trends citywide and by community-district homeowner tercile.
- `task_graph`: creates the main task graph and task list.

The paper can also be rebuilt from the paper folder:

```sh
cd paper
make
```

## ULURP universe and text extraction

`build_zap_project_universe` retains projects regardless of whether they reached
a CPC report, including withdrawals and terminations. The CPC corpus and its
selected narratives are a linked document sample, not the universe of projects.

The counting codebook is in
[`summarize_text_cpc_trends`](tasks/summarize_text_cpc_trends/README.md).
The [regex audit](tasks/audits/audit_ulurp_cpc_regex_labels/README.md) produces
coverage by field and decade, evidence for unresolved cases, and a fresh human
review sheet. Count extraction status is separate from validated accuracy.
Current research decisions and build limitations are recorded in `logbook/`.
