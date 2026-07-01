from __future__ import annotations

import sys

import pandas as pd

sys.path.append("../../_lib")
from member_deference_utils import write_csv


def approval_vote_status(value: object) -> str:
    if value == "approved_with_all_local_members_affirmative":
        return "local_member_affirmative_only"
    if value in {"approved_with_local_member_negative", "approved_with_local_member_abstain"}:
        return "local_member_negative_or_abstain"
    if value == "approved_with_local_member_other_nonaffirmative":
        return "local_member_excused_nonvoting_absent_or_other"
    if value == "unresolved_no_affected_district":
        return "no_affected_district"
    if value == "unresolved_missing_roster":
        return "missing_roster"
    if value == "unresolved_no_member_vote_rows":
        return "no_member_vote_rows"
    if value == "unresolved_no_local_member_vote_match":
        return "local_member_missing_from_vote_rows"
    if value == "unresolved_partial_local_member_vote_match":
        return "partial_local_member_vote_match"
    return "not_classified"

matter_universe = pd.read_csv("../input/member_deference_matter_universe.csv", dtype=str, keep_default_na=False)
approval_panel = pd.read_csv("../input/member_deference_vote_panel.csv", dtype=str, keep_default_na=False)
nonapproval_queue = pd.read_csv(
    "../input/member_deference_nonapproval_geography_conservative_queue.csv", dtype=str, keep_default_na=False
)
nonapproval_actions = pd.read_csv("../input/member_deference_nonapproval_action_details.csv", dtype=str, keep_default_na=False)
nonapproval_local_vote_status = pd.read_csv(
    "../input/member_deference_nonapproval_local_member_vote_status.csv", dtype=str, keep_default_na=False
)

for name, df in [
    ("matter_universe", matter_universe),
    ("approval_panel", approval_panel),
    ("nonapproval_queue", nonapproval_queue),
    ("nonapproval_actions", nonapproval_actions),
    ("nonapproval_local_vote_status", nonapproval_local_vote_status),
]:
    if df["matter_id"].duplicated().any():
        raise RuntimeError(f"{name} must be unique by matter_id.")

approval_panel["approval_source_row"] = "true"
approval_panel["approval_vote_status_standardized"] = approval_panel["vote_evidence_status"].map(approval_vote_status)
approval_panel = approval_panel[
    [
        "matter_id",
        "approval_source_row",
        "vote_date",
        "vote_margin",
        "affirmative_count",
        "negative_count",
        "abstain_count",
        "affected_council_districts",
        "affected_district_source",
        "local_members_from_roster",
        "local_member_votes",
        "local_member_negative",
        "local_member_abstain",
        "local_member_other_nonaffirmative",
        "missing_roster_districts",
        "vote_evidence_status",
        "vote_evidence_strength",
        "approval_vote_status_standardized",
        "history_detail_url",
    ]
].rename(
    columns={
        "vote_date": "approval_vote_date",
        "vote_margin": "approval_vote_margin",
        "affirmative_count": "approval_affirmative_count",
        "negative_count": "approval_negative_count",
        "abstain_count": "approval_abstain_count",
        "affected_council_districts": "approval_affected_council_districts",
        "affected_district_source": "approval_affected_district_source",
        "local_members_from_roster": "approval_local_members_from_roster",
        "local_member_votes": "approval_local_member_votes",
        "local_member_negative": "approval_local_member_negative",
        "local_member_abstain": "approval_local_member_abstain",
        "local_member_other_nonaffirmative": "approval_local_member_other_nonaffirmative",
        "missing_roster_districts": "approval_missing_roster_districts",
        "vote_evidence_status": "approval_vote_evidence_status",
        "vote_evidence_strength": "approval_vote_evidence_strength",
        "history_detail_url": "approval_history_detail_url",
    }
)

nonapproval_actions = nonapproval_actions.merge(
    nonapproval_queue[
        [
            "matter_id",
            "geography_incorporation_status",
            "affected_district_confidence_conservative",
            "affected_district_source_detail_conservative",
            "affected_council_districts_original",
            "affected_district_source_original",
            "local_members_from_roster_original",
        ]
    ],
    on="matter_id",
    how="left",
    validate="one_to_one",
)
nonapproval_actions = nonapproval_actions.merge(
    nonapproval_local_vote_status[
        [
            "matter_id",
            "local_member_rows",
            "local_member_vote_rows_found",
            "matched_vote_person_names",
            "local_member_final_action_votes",
            "local_member_final_action_vote_categories",
            "local_member_final_action_vote_status",
        ]
    ],
    on="matter_id",
    how="left",
    validate="one_to_one",
)
for col in [
    "geography_incorporation_status",
    "affected_district_confidence_conservative",
    "affected_district_source_detail_conservative",
    "affected_council_districts_original",
    "affected_district_source_original",
    "local_members_from_roster_original",
    "local_member_rows",
    "local_member_vote_rows_found",
    "matched_vote_person_names",
    "local_member_final_action_votes",
    "local_member_final_action_vote_categories",
    "local_member_final_action_vote_status",
]:
    nonapproval_actions[col] = nonapproval_actions[col].fillna("")

nonapproval_actions["nonapproval_source_row"] = "true"
nonapproval_actions = nonapproval_actions[
    [
        "matter_id",
        "nonapproval_source_row",
        "final_history_date",
        "final_history_action",
        "final_history_result",
        "final_history_detail_url",
        "vote_margin",
        "affirmative_count",
        "negative_count",
        "abstain_count",
        "excused_count",
        "non_voting_count",
        "parsed_vote_rows",
        "affected_council_districts",
        "affected_district_source",
        "local_members_from_roster",
        "local_member_rows",
        "local_member_vote_rows_found",
        "matched_vote_person_names",
        "local_member_final_action_votes",
        "local_member_final_action_vote_categories",
        "local_member_final_action_vote_status",
        "geography_incorporation_status",
        "affected_district_confidence_conservative",
        "affected_district_source_detail_conservative",
        "affected_council_districts_original",
        "affected_district_source_original",
        "local_members_from_roster_original",
    ]
].rename(
    columns={
        "final_history_date": "nonapproval_vote_date",
        "final_history_action": "nonapproval_final_action",
        "final_history_result": "nonapproval_final_result",
        "final_history_detail_url": "nonapproval_history_detail_url",
        "vote_margin": "nonapproval_vote_margin",
        "affirmative_count": "nonapproval_affirmative_count",
        "negative_count": "nonapproval_negative_count",
        "abstain_count": "nonapproval_abstain_count",
        "excused_count": "nonapproval_excused_count",
        "non_voting_count": "nonapproval_non_voting_count",
        "parsed_vote_rows": "nonapproval_parsed_vote_rows",
        "affected_council_districts": "nonapproval_affected_council_districts",
        "affected_district_source": "nonapproval_affected_district_source",
        "local_members_from_roster": "nonapproval_local_members_from_roster",
        "local_member_rows": "nonapproval_local_member_rows",
        "local_member_vote_rows_found": "nonapproval_local_member_vote_rows_found",
        "matched_vote_person_names": "nonapproval_matched_vote_person_names",
        "local_member_final_action_votes": "nonapproval_local_member_final_action_votes",
        "local_member_final_action_vote_categories": "nonapproval_local_member_final_action_vote_categories",
        "local_member_final_action_vote_status": "nonapproval_local_member_final_action_vote_status",
    }
)

decision_panel = matter_universe.merge(approval_panel, on="matter_id", how="left", validate="one_to_one")
decision_panel = decision_panel.merge(nonapproval_actions, on="matter_id", how="left", validate="one_to_one")
decision_panel = decision_panel.fillna("").copy()

decision_panel["has_approval_vote_detail"] = decision_panel["approval_source_row"].eq("true")
decision_panel["has_nonapproval_vote_detail"] = decision_panel["nonapproval_source_row"].eq("true")
decision_panel["vote_source"] = "not_fetched"
decision_panel.loc[decision_panel["has_approval_vote_detail"], "vote_source"] = "approval_action_detail"
decision_panel.loc[
    decision_panel["has_approval_vote_detail"] & ~decision_panel["disposition_group"].eq("adopted"),
    "vote_source",
] = "approval_action_detail_nonfinal_disposition"
decision_panel.loc[decision_panel["has_nonapproval_vote_detail"], "vote_source"] = "nonapproval_action_detail"

decision_panel["decision_date"] = decision_panel["final_history_date"]
decision_panel["decision_action_by"] = decision_panel["final_history_action_by"]
decision_panel["decision_action"] = decision_panel["final_history_action"]
decision_panel["decision_result"] = decision_panel["final_history_result"]
decision_panel["history_detail_url"] = decision_panel["final_history_detail_url"]

decision_panel["vote_date"] = ""
decision_panel["vote_margin"] = ""
decision_panel["affirmative_count"] = ""
decision_panel["negative_count"] = ""
decision_panel["abstain_count"] = ""
decision_panel["parsed_vote_rows"] = ""
decision_panel["local_member_final_action_vote_status"] = "not_fetched"
decision_panel["local_member_final_action_votes"] = ""
decision_panel["local_member_final_action_vote_categories"] = ""
decision_panel["member_deference_vote_signal"] = "not_observed"
decision_panel["geography_incorporation_status_main"] = "matter_universe"

approval_rows = decision_panel["vote_source"].isin(
    ["approval_action_detail", "approval_action_detail_nonfinal_disposition"]
)
decision_panel.loc[approval_rows, "vote_date"] = decision_panel.loc[approval_rows, "approval_vote_date"]
decision_panel.loc[approval_rows, "vote_margin"] = decision_panel.loc[approval_rows, "approval_vote_margin"]
decision_panel.loc[approval_rows, "affirmative_count"] = decision_panel.loc[approval_rows, "approval_affirmative_count"]
decision_panel.loc[approval_rows, "negative_count"] = decision_panel.loc[approval_rows, "approval_negative_count"]
decision_panel.loc[approval_rows, "abstain_count"] = decision_panel.loc[approval_rows, "approval_abstain_count"]
decision_panel.loc[approval_rows, "affected_council_districts"] = decision_panel.loc[
    approval_rows, "approval_affected_council_districts"
]
decision_panel.loc[approval_rows, "affected_district_source"] = decision_panel.loc[
    approval_rows, "approval_affected_district_source"
]
decision_panel.loc[approval_rows, "local_members_from_roster"] = decision_panel.loc[
    approval_rows, "approval_local_members_from_roster"
]
decision_panel.loc[approval_rows, "local_member_final_action_vote_status"] = decision_panel.loc[
    approval_rows, "approval_vote_status_standardized"
]
decision_panel.loc[approval_rows, "local_member_final_action_votes"] = decision_panel.loc[
    approval_rows, "approval_local_member_votes"
]
decision_panel.loc[approval_rows, "member_deference_vote_signal"] = decision_panel.loc[
    approval_rows, "approval_vote_evidence_strength"
]
decision_panel.loc[approval_rows, "geography_incorporation_status_main"] = "approval_panel"

nonapproval_rows = decision_panel["vote_source"].eq("nonapproval_action_detail")
decision_panel.loc[nonapproval_rows, "vote_date"] = decision_panel.loc[nonapproval_rows, "nonapproval_vote_date"]
decision_panel.loc[nonapproval_rows, "vote_margin"] = decision_panel.loc[nonapproval_rows, "nonapproval_vote_margin"]
decision_panel.loc[nonapproval_rows, "affirmative_count"] = decision_panel.loc[
    nonapproval_rows, "nonapproval_affirmative_count"
]
decision_panel.loc[nonapproval_rows, "negative_count"] = decision_panel.loc[
    nonapproval_rows, "nonapproval_negative_count"
]
decision_panel.loc[nonapproval_rows, "abstain_count"] = decision_panel.loc[
    nonapproval_rows, "nonapproval_abstain_count"
]
decision_panel.loc[nonapproval_rows, "parsed_vote_rows"] = decision_panel.loc[
    nonapproval_rows, "nonapproval_parsed_vote_rows"
]
decision_panel.loc[nonapproval_rows, "affected_council_districts"] = decision_panel.loc[
    nonapproval_rows, "nonapproval_affected_council_districts"
]
decision_panel.loc[nonapproval_rows, "affected_district_source"] = decision_panel.loc[
    nonapproval_rows, "nonapproval_affected_district_source"
]
decision_panel.loc[nonapproval_rows, "local_members_from_roster"] = decision_panel.loc[
    nonapproval_rows, "nonapproval_local_members_from_roster"
]
decision_panel.loc[nonapproval_rows, "local_member_final_action_vote_status"] = decision_panel.loc[
    nonapproval_rows, "nonapproval_local_member_final_action_vote_status"
]
decision_panel.loc[nonapproval_rows, "local_member_final_action_votes"] = decision_panel.loc[
    nonapproval_rows, "nonapproval_local_member_final_action_votes"
]
decision_panel.loc[nonapproval_rows, "local_member_final_action_vote_categories"] = decision_panel.loc[
    nonapproval_rows, "nonapproval_local_member_final_action_vote_categories"
]
decision_panel.loc[nonapproval_rows, "member_deference_vote_signal"] = decision_panel.loc[
    nonapproval_rows, "nonapproval_local_member_final_action_vote_status"
]
decision_panel.loc[nonapproval_rows, "geography_incorporation_status_main"] = decision_panel.loc[
    nonapproval_rows, "geography_incorporation_status"
]

decision_panel["has_affected_council_district"] = decision_panel["affected_council_districts"].ne("")
decision_panel["has_local_member_from_roster"] = decision_panel["local_members_from_roster"].ne("")
decision_panel["has_local_member_vote_observed"] = decision_panel["local_member_final_action_votes"].ne("")
decision_panel["matter_in_main_vote_sample"] = decision_panel["vote_source"].ne("not_fetched")

decision_panel = decision_panel[
    [
        "query_year",
        "matter_id",
        "matter_file",
        "matter_file_year",
        "matter_age_years",
        "query_matter_type",
        "matter_type",
        "matter_status",
        "disposition_group",
        "filed_age_group",
        "decision_date",
        "decision_action_by",
        "decision_action",
        "decision_result",
        "vote_source",
        "matter_in_main_vote_sample",
        "vote_date",
        "vote_margin",
        "parsed_vote_rows",
        "affirmative_count",
        "negative_count",
        "abstain_count",
        "affected_council_districts",
        "affected_district_source",
        "geography_incorporation_status_main",
        "has_affected_council_district",
        "local_members_from_roster",
        "has_local_member_from_roster",
        "local_member_final_action_vote_status",
        "local_member_final_action_votes",
        "local_member_final_action_vote_categories",
        "has_local_member_vote_observed",
        "member_deference_vote_signal",
        "application_keys",
        "zap_matched_application_keys",
        "zap_project_ids",
        "zap_project_names",
        "zap_cc_districts",
        "borough",
        "committee",
        "land_use_recall_reason",
        "title",
        "matter_url",
        "history_detail_url",
    ]
].sort_values(["query_year", "matter_file", "matter_id"])

if decision_panel["matter_id"].duplicated().any():
    raise RuntimeError("Council land-use decision panel must be unique by matter_id.")
if len(decision_panel) != len(matter_universe):
    raise RuntimeError("Council land-use decision panel must keep every matter-universe row.")

write_csv("../output/council_land_use_decision_panel.csv", decision_panel)
