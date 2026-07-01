from __future__ import annotations

from pathlib import Path

import pandas as pd


def write_csv(path: str, df: pd.DataFrame) -> None:
    temp_path = f"{path}.tmp"
    df.to_csv(temp_path, index=False)
    Path(temp_path).replace(path)


def add_metric(rows: list[dict[str, object]], metric: str, value: object) -> None:
    rows.append({"metric": metric, "value": value})


def summarize_by_year(df: pd.DataFrame, metric_prefix: str, columns: list[str]) -> pd.DataFrame:
    rows = [{"summary_group": "overall", "query_year": "", "metric": f"{metric_prefix}_rows", "value": len(df)}]
    if "query_year" in df.columns:
        for query_year, group in df.groupby("query_year", dropna=False):
            rows.append(
                {
                    "summary_group": "query_year",
                    "query_year": query_year,
                    "metric": f"{metric_prefix}_rows",
                    "value": len(group),
                }
            )
    for column in columns:
        if column not in df.columns:
            continue
        for value, group in df.groupby(column, dropna=False):
            rows.append(
                {
                    "summary_group": column,
                    "query_year": "",
                    "metric": "" if pd.isna(value) else str(value),
                    "value": len(group),
                }
            )
    return pd.DataFrame(rows)


panel = pd.read_csv("../input/member_deference_vote_panel.csv", dtype=str, keep_default_na=False)
matter_universe = pd.read_csv("../input/member_deference_matter_universe.csv", dtype=str, keep_default_na=False)
final_action_vote_queue = pd.read_csv(
    "../input/member_deference_final_action_vote_queue.csv", dtype=str, keep_default_na=False
)
ai_geo_repairs = pd.read_csv(
    "../input/council_land_use_ai_geography_accepted_repairs.csv", dtype=str, keep_default_na=False
)

summary_rows: list[dict[str, object]] = []
add_metric(summary_rows, "approved_matter_rows", len(panel))
add_metric(summary_rows, "matter_rows_with_application_key", int(panel["application_keys"].ne("").sum()))
add_metric(summary_rows, "matter_rows_with_affected_district", int(panel["affected_council_districts"].ne("").sum()))
add_metric(summary_rows, "local_member_negative_rows", int(panel["local_member_negative"].eq("True").sum()))
add_metric(summary_rows, "local_member_abstain_rows", int(panel["local_member_abstain"].eq("True").sum()))
add_metric(
    summary_rows,
    "strong_exception_candidate_rows",
    int(panel["vote_evidence_strength"].eq("strong_exception_candidate").sum()),
)
add_metric(summary_rows, "unresolved_rows", int(panel["vote_evidence_strength"].eq("unresolved").sum()))
add_metric(
    summary_rows,
    "excluded_inverted_disapproval_motion_rows",
    int(panel["excluded_inverted_disapproval_motion"].eq("True").sum()),
)
add_metric(summary_rows, "matter_universe_rows", len(matter_universe))
add_metric(summary_rows, "final_action_vote_queue_rows", len(final_action_vote_queue))
add_metric(summary_rows, "accepted_ai_geography_repair_rows", len(ai_geo_repairs))
write_csv("../output/member_deference_vote_panel_summary.csv", pd.DataFrame(summary_rows))

qc = pd.DataFrame(
    [
        {
            "check_name": "vote_panel_unique_by_matter_id",
            "passed": str(not panel["matter_id"].duplicated().any()),
            "detail": "Approval vote panel is unique by matter_id.",
        },
        {
            "check_name": "matter_universe_unique_by_matter_id",
            "passed": str(not matter_universe["matter_id"].duplicated().any()),
            "detail": "Matter universe is unique by matter_id.",
        },
        {
            "check_name": "final_action_vote_queue_unique_by_matter_id",
            "passed": str(not final_action_vote_queue["matter_id"].duplicated().any()),
            "detail": "Final-action nonapproval vote queue is unique by matter_id.",
        },
        {
            "check_name": "accepted_ai_geography_repair_unique_by_key",
            "passed": str(not ai_geo_repairs.duplicated(["query_year", "vote_date", "matter_file"]).any()),
            "detail": "Accepted AI/manual geography repairs are unique by query_year, vote_date, and matter_file.",
        },
    ]
)
write_csv("../output/member_deference_vote_panel_qc.csv", qc)

write_csv(
    "../output/member_deference_vote_panel_exception_candidates.csv",
    panel[panel["vote_evidence_strength"] == "strong_exception_candidate"].copy(),
)
write_csv(
    "../output/member_deference_vote_panel_unresolved.csv",
    panel[panel["vote_evidence_strength"] == "unresolved"].copy(),
)

filed_matter_audit_columns = [
    "query_year",
    "matter_file",
    "matter_file_year",
    "matter_age_years",
    "query_matter_type",
    "matter_status",
    "disposition_group",
    "filed_age_group",
    "final_history_date",
    "final_history_action_by",
    "final_history_action",
    "affected_council_districts",
    "affected_district_source",
    "ai_geography_repair_applied",
    "ai_geography_repair_signature_review_id",
    "ai_geography_repair_source",
    "ai_geography_repair_confidence",
    "local_members_from_roster",
    "application_keys",
    "title",
    "matter_url",
]
write_csv(
    "../output/member_deference_filed_matter_audit.csv",
    matter_universe.loc[
        matter_universe["disposition_group"].eq("filed"),
        [column for column in filed_matter_audit_columns if column in matter_universe.columns],
    ].copy(),
)

write_csv(
    "../output/member_deference_matter_universe_summary.csv",
    summarize_by_year(
        matter_universe,
        "matter",
        ["disposition_group", "query_matter_type", "affected_district_source", "matter_status"],
    ),
)
write_csv(
    "../output/member_deference_final_action_vote_queue_summary.csv",
    summarize_by_year(
        final_action_vote_queue,
        "queue",
        ["disposition_group", "final_action_vote_fetch_tier", "affected_district_source"],
    ),
)
