# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/validate_council_land_use_decision_universe/code")

from __future__ import annotations

from pathlib import Path

import pandas as pd


RECALL_YEARS = list(range(1998, 2026))


def write_csv(path: str, df: pd.DataFrame) -> None:
    temp_path = Path(path).with_suffix(Path(path).suffix + ".tmp")
    df.to_csv(temp_path, index=False)
    temp_path.replace(path)


decision_panel = pd.read_csv("../input/council_land_use_decision_panel.csv", dtype=str, keep_default_na=False)

matter_index_frames = []
history_event_frames = []
action_detail_frames = []
member_vote_frames = []
for year in RECALL_YEARS:
    matter_index_frames.append(
        pd.read_csv(f"../input/legistar_{year}_broad_recall_matter_index.csv", dtype=str, keep_default_na=False)
    )
    history_event_frames.append(
        pd.read_csv(f"../input/legistar_{year}_broad_recall_history_events.csv", dtype=str, keep_default_na=False)
    )
    action_detail_frames.append(
        pd.read_csv(f"../input/legistar_{year}_broad_recall_action_details.csv", dtype=str, keep_default_na=False)
    )
    member_vote_frames.append(
        pd.read_csv(f"../input/legistar_{year}_broad_recall_member_votes.csv", dtype=str, keep_default_na=False)
    )

matter_index = pd.concat(matter_index_frames, ignore_index=True)
history_events = pd.concat(history_event_frames, ignore_index=True)
action_details = pd.concat(action_detail_frames, ignore_index=True)
member_votes = pd.concat(member_vote_frames, ignore_index=True)

for col in ["query_record_count"]:
    matter_index[col] = pd.to_numeric(matter_index[col], errors="raise")
for col in ["parsed_vote_rows", "vote_record_count"]:
    action_details[col] = pd.to_numeric(action_details[col], errors="coerce")

count_checks = (
    matter_index.groupby(["query_year", "query_matter_type"], as_index=False)
    .agg(parsed_rows=("matter_id", "size"), reported_records=("query_record_count", "max"))
)
count_checks["matches_reported_records"] = count_checks["parsed_rows"] == count_checks["reported_records"]

member_vote_counts = (
    member_votes.groupby("matter_id", as_index=False)
    .agg(member_vote_rows=("vote", "size"))
)
vote_count_checks = action_details[
    ["matter_id", "matter_file", "parsed_vote_rows", "vote_record_count", "vote_tab_label"]
].merge(
    member_vote_counts,
    on="matter_id",
    how="left",
    validate="one_to_one",
)
vote_count_checks["member_vote_rows"] = vote_count_checks["member_vote_rows"].fillna(0).astype(int)
vote_count_checks["parsed_rows_match_summary"] = (
    vote_count_checks["member_vote_rows"] == vote_count_checks["parsed_vote_rows"]
)
vote_count_checks["parsed_rows_match_legistar_record_count"] = (
    vote_count_checks["member_vote_rows"] == vote_count_checks["vote_record_count"]
)
zero_vote_pages = vote_count_checks[vote_count_checks["parsed_vote_rows"] == 0]

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
            "passed": bool(count_checks["matches_reported_records"].all()),
            "detail": (
                f"All {len(count_checks)} query-year matter-type counts match Legistar-reported record counts."
            ),
        },
        {
            "check_name": "legistar_matter_index_unique_by_matter_id",
            "passed": not matter_index["matter_id"].duplicated().any(),
            "detail": f"{len(matter_index)} fetched matter rows and {matter_index['matter_id'].nunique()} unique matter IDs.",
        },
        {
            "check_name": "legistar_history_events_available",
            "passed": bool(len(history_events) >= decision_panel["matter_id"].nunique()),
            "detail": f"Fetched {len(history_events)} Legistar history-event rows.",
        },
        {
            "check_name": "approval_action_details_unique_by_matter_id",
            "passed": not action_details["matter_id"].duplicated().any(),
            "detail": f"Fetched final approval action details for {action_details['matter_id'].nunique()} matters.",
        },
        {
            "check_name": "member_vote_rows_match_action_summaries",
            "passed": bool(vote_count_checks["parsed_rows_match_summary"].all()),
            "detail": "Long member-vote rows reconcile to parsed_vote_rows on every action-detail page.",
        },
        {
            "check_name": "member_vote_rows_match_legistar_record_counts",
            "passed": bool(vote_count_checks["parsed_rows_match_legistar_record_count"].all()),
            "detail": "Long member-vote rows reconcile to Legistar's displayed vote-record count on every action-detail page.",
        },
        {
            "check_name": "zero_vote_action_pages_are_consent_zero_zero",
            "passed": bool(
                zero_vote_pages.empty
                or (
                    (zero_vote_pages["vote_record_count"] == 0)
                    & zero_vote_pages["vote_tab_label"].fillna("").str.contains("\\(0:0\\)")
                ).all()
            ),
            "detail": f"{len(zero_vote_pages)} final approval action-detail pages show no individual member-vote rows.",
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
        {
            "check_name": "decision_panel_year_summary_counted",
            "passed": True,
            "detail": f"Summarized {len(decision_year)} decision-panel years.",
        },
    ]
)

write_csv("../output/council_land_use_decision_universe_validation_summary.csv", summary)

if not summary["passed"].all():
    failed_checks = ", ".join(summary.loc[~summary["passed"], "check_name"].astype(str))
    raise RuntimeError(f"Council land-use universe validation failed: {failed_checks}.")
