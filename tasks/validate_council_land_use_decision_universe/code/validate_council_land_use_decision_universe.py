# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/validate_council_land_use_decision_universe/code")

from __future__ import annotations

from pathlib import Path

import pandas as pd


RECALL_YEARS = list(range(1998, 2026))


def write_csv(path: str, df: pd.DataFrame) -> None:
    temp_path = Path(path).with_suffix(Path(path).suffix + ".tmp")
    df.to_csv(temp_path, index=False)
    temp_path.replace(path)


decision_panel = pd.read_csv("../input/council_land_use_decision_panel.csv", dtype=str, keep_default_na=False)

count_checks = []
fetch_qc = []
action_vote_qc = []
for year in RECALL_YEARS:
    year_count_check = pd.read_csv(
        f"../input/legistar_{year}_broad_recall_count_check.csv", dtype=str, keep_default_na=False
    )
    year_count_check["query_year"] = str(year)
    count_checks.append(year_count_check)

    year_fetch_qc = pd.read_csv(f"../input/legistar_{year}_broad_recall_qc.csv", dtype=str, keep_default_na=False)
    year_fetch_qc["query_year"] = str(year)
    fetch_qc.append(year_fetch_qc)

    year_action_vote_qc = pd.read_csv(
        f"../input/legistar_{year}_broad_recall_action_vote_qc.csv", dtype=str, keep_default_na=False
    )
    year_action_vote_qc["query_year"] = str(year)
    action_vote_qc.append(year_action_vote_qc)

count_checks = pd.concat(count_checks, ignore_index=True)
fetch_qc = pd.concat(fetch_qc, ignore_index=True)
action_vote_qc = pd.concat(action_vote_qc, ignore_index=True)

for col in ["parsed_rows", "reported_records"]:
    count_checks[col] = pd.to_numeric(count_checks[col], errors="raise")

if decision_panel["matter_id"].duplicated().any():
    raise RuntimeError("Council land-use decision panel must be unique by matter_id.")

decision_panel["matter_in_main_vote_sample_bool"] = decision_panel["matter_in_main_vote_sample"].str.lower().eq("true")
decision_panel["has_affected_council_district_bool"] = decision_panel["has_affected_council_district"].str.lower().eq(
    "true"
)
decision_panel["has_local_member_from_roster_bool"] = decision_panel["has_local_member_from_roster"].str.lower().eq(
    "true"
)
decision_panel["has_local_member_vote_observed_bool"] = decision_panel["has_local_member_vote_observed"].str.lower().eq(
    "true"
)

fetch_query_year = (
    count_checks.assign(matches_reported_records_bool=count_checks["matches_reported_records"].str.lower().eq("true"))
    .loc[
        :,
        [
            "query_year",
            "query_matter_type",
            "parsed_rows",
            "reported_records",
            "matches_reported_records_bool",
        ],
    ]
    .rename(columns={"matches_reported_records_bool": "matches_reported_records"})
    .sort_values(["query_year", "query_matter_type"])
)

recall_year = (
    count_checks.groupby("query_year", as_index=False)
    .agg(
        broad_recall_parsed_rows=("parsed_rows", "sum"),
        broad_recall_reported_records=("reported_records", "sum"),
        broad_recall_queries_reconciled=("matches_reported_records", lambda x: bool(x.str.lower().eq("true").all())),
    )
)

decision_year = (
    decision_panel.groupby("query_year", as_index=False)
    .agg(
        matter_rows=("matter_id", "size"),
        adopted_rows=("disposition_group", lambda x: int((x == "adopted").sum())),
        disapproved_rows=("disposition_group", lambda x: int((x == "disapproved").sum())),
        filed_or_withdrawn_rows=("disposition_group", lambda x: int((x != "adopted").sum())),
        main_vote_sample_rows=("matter_in_main_vote_sample_bool", "sum"),
        not_fetched_rows=("vote_source", lambda x: int((x == "not_fetched").sum())),
        affected_district_rows=("has_affected_council_district_bool", "sum"),
        local_member_roster_rows=("has_local_member_from_roster_bool", "sum"),
        local_member_vote_rows=("has_local_member_vote_observed_bool", "sum"),
    )
)

year = recall_year.merge(decision_year, on="query_year", how="outer", validate="one_to_one").sort_values("query_year")
for col in [
    "broad_recall_parsed_rows",
    "broad_recall_reported_records",
    "matter_rows",
    "adopted_rows",
    "disapproved_rows",
    "filed_or_withdrawn_rows",
    "main_vote_sample_rows",
    "not_fetched_rows",
    "affected_district_rows",
    "local_member_roster_rows",
    "local_member_vote_rows",
]:
    year[col] = pd.to_numeric(year[col], errors="coerce").fillna(0).astype(int)
year["matter_rows_share_of_broad_recall"] = year["matter_rows"] / year["broad_recall_reported_records"]
year["main_vote_sample_share_of_matters"] = year["main_vote_sample_rows"] / year["matter_rows"]
year["affected_district_share_of_matters"] = year["affected_district_rows"] / year["matter_rows"]
year["local_member_vote_share_of_matters"] = year["local_member_vote_rows"] / year["matter_rows"]

disposition_year = (
    decision_panel.groupby(["query_year", "disposition_group"], as_index=False)
    .agg(matter_rows=("matter_id", "size"))
    .sort_values(["query_year", "disposition_group"])
)
disposition_year = disposition_year.merge(
    decision_year[["query_year", "matter_rows"]].rename(columns={"matter_rows": "year_matter_rows"}),
    on="query_year",
    how="left",
    validate="many_to_one",
)
disposition_year["matter_share"] = disposition_year["matter_rows"] / disposition_year["year_matter_rows"]

vote_source_year = (
    decision_panel.groupby(["query_year", "vote_source"], as_index=False)
    .agg(matter_rows=("matter_id", "size"))
    .sort_values(["query_year", "vote_source"])
)
vote_source_year = vote_source_year.merge(
    decision_year[["query_year", "matter_rows"]].rename(columns={"matter_rows": "year_matter_rows"}),
    on="query_year",
    how="left",
    validate="many_to_one",
)
vote_source_year["matter_share"] = vote_source_year["matter_rows"] / vote_source_year["year_matter_rows"]

fetch_qc["passed_bool"] = fetch_qc["passed"].str.lower().eq("true")
action_vote_qc["passed_bool"] = action_vote_qc["passed"].str.lower().eq("true")

summary = pd.DataFrame(
    [
        {
            "check_name": "decision_panel_unique_by_matter_id",
            "passed": not decision_panel["matter_id"].duplicated().any(),
            "detail": f"{len(decision_panel)} decision rows and {decision_panel['matter_id'].nunique()} unique matter IDs.",
        },
        {
            "check_name": "decision_panel_year_range",
            "passed": sorted(decision_panel["query_year"].unique().tolist()) == [str(year) for year in RECALL_YEARS],
            "detail": "Decision panel covers 1998-2025 exactly.",
        },
        {
            "check_name": "legistar_recall_counts_reconcile",
            "passed": bool(count_checks["matches_reported_records"].str.lower().eq("true").all()),
            "detail": (
                f"All {len(count_checks)} query-year count checks match Legistar-reported record counts."
            ),
        },
        {
            "check_name": "legistar_fetch_qc_passed",
            "passed": bool(fetch_qc["passed_bool"].all()),
            "detail": f"{int(fetch_qc['passed_bool'].sum())} of {len(fetch_qc)} broad-recall fetch QC checks passed.",
        },
        {
            "check_name": "approval_vote_qc_passed",
            "passed": bool(action_vote_qc["passed_bool"].all()),
            "detail": (
                f"{int(action_vote_qc['passed_bool'].sum())} of {len(action_vote_qc)} approval-vote QC checks passed."
            ),
        },
        {
            "check_name": "main_vote_sample_coverage",
            "passed": True,
            "detail": (
                f"{int(decision_panel['matter_in_main_vote_sample_bool'].sum())} of {len(decision_panel)} "
                "matters have parsed final-action vote details in the current main sample."
            ),
        },
        {
            "check_name": "not_fetched_rows_counted",
            "passed": True,
            "detail": (
                f"{int((decision_panel['vote_source'] == 'not_fetched').sum())} matters remain outside "
                "the parsed vote-detail sample, mostly end-of-session and lower-information final actions."
            ),
        },
        {
            "check_name": "affected_district_rows_counted",
            "passed": True,
            "detail": (
                f"{int(decision_panel['has_affected_council_district_bool'].sum())} of {len(decision_panel)} "
                "matters have affected Council district geography."
            ),
        },
    ]
)

write_csv("../output/council_land_use_decision_universe_validation_summary.csv", summary)
write_csv("../output/council_land_use_decision_universe_year.csv", year)
write_csv("../output/council_land_use_decision_universe_fetch_query_year.csv", fetch_query_year)
write_csv("../output/council_land_use_decision_universe_disposition_year.csv", disposition_year)
write_csv("../output/council_land_use_decision_universe_vote_source_year.csv", vote_source_year)

if not summary["passed"].all():
    failed_checks = ", ".join(summary.loc[~summary["passed"], "check_name"].astype(str))
    raise RuntimeError(f"Council land-use universe validation failed: {failed_checks}.")
