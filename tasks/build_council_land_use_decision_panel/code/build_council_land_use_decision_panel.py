from __future__ import annotations

import sys

import pandas as pd

sys.path.append("../../_lib")
from member_deference_utils import save_frame, split_semicolon

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
        "local_member_vote_match_methods",
        "local_member_votes",
        "local_member_abstain",
        "local_member_vote_status",
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
        "local_member_vote_match_methods": "approval_local_member_vote_match_methods",
        "local_member_votes": "approval_local_member_votes",
        "local_member_abstain": "approval_local_member_abstain",
        "local_member_vote_status": "approval_local_member_vote_status",
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
            "local_member_vote_match_methods",
            "local_member_final_action_votes",
            "local_member_abstain",
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
    "local_member_vote_match_methods",
    "local_member_final_action_votes",
    "local_member_abstain",
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
        "local_member_vote_match_methods",
        "local_member_final_action_votes",
        "local_member_abstain",
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
        "local_member_vote_match_methods": "nonapproval_local_member_vote_match_methods",
        "local_member_final_action_votes": "nonapproval_local_member_final_action_votes",
        "local_member_abstain": "nonapproval_local_member_abstain",
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
decision_panel["local_member_abstain"] = ""
decision_panel["local_member_vote_match_methods"] = ""
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
    approval_rows, "approval_local_member_vote_status"
]
decision_panel.loc[approval_rows, "local_member_final_action_votes"] = decision_panel.loc[
    approval_rows, "approval_local_member_votes"
]
decision_panel.loc[approval_rows, "local_member_abstain"] = decision_panel.loc[approval_rows, "approval_local_member_abstain"]
decision_panel.loc[approval_rows, "local_member_vote_match_methods"] = decision_panel.loc[
    approval_rows, "approval_local_member_vote_match_methods"
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
decision_panel.loc[nonapproval_rows, "local_member_abstain"] = decision_panel.loc[
    nonapproval_rows, "nonapproval_local_member_abstain"
]
decision_panel.loc[nonapproval_rows, "local_member_vote_match_methods"] = decision_panel.loc[
    nonapproval_rows, "nonapproval_local_member_vote_match_methods"
]
decision_panel.loc[nonapproval_rows, "geography_incorporation_status_main"] = decision_panel.loc[
    nonapproval_rows, "geography_incorporation_status"
]

decision_panel["has_affected_council_district"] = decision_panel["affected_council_districts"].ne("")
decision_panel["has_local_member_from_roster"] = decision_panel["local_members_from_roster"].ne("")
decision_panel["has_local_member_vote_observed"] = decision_panel["local_member_final_action_votes"].ne("")
decision_panel["matter_in_main_vote_sample"] = decision_panel["vote_source"].ne("not_fetched")
decision_panel["n_affected_districts"] = decision_panel["affected_council_districts"].map(lambda x: len(split_semicolon(x)))

# Project outcome comes from the matter's Legistar disposition, not from which vote page
# was fetched. A "Resolution disapproving ..." that is adopted means the project was
# disapproved.
disapproval_resolution = decision_panel["title"].str.match(r"(?i)\s*resolution\s+disapprov")
decision_panel["project_outcome"] = ""
decision_panel.loc[decision_panel["disposition_group"].eq("adopted"), "project_outcome"] = "approved"
decision_panel.loc[
    decision_panel["disposition_group"].eq("disapproved")
    | (decision_panel["disposition_group"].eq("adopted") & disapproval_resolution),
    "project_outcome",
] = "disapproved"

# Direction of the roll call. A yes vote supports the project on an approval vote and
# opposes it on a vote to disapprove, file, or override a veto of a disapproval. A
# Council approval of a disapproved matter or of a disapproval resolution is a vote to
# disapprove (for example LU 0468-2005 and LU 0470-2005, the 2005 marine transfer stations).
decision_panel["rollcall_direction"] = ""
decision_panel.loc[approval_rows, "rollcall_direction"] = "approve_project"
decision_panel.loc[
    (approval_rows & (decision_panel["disposition_group"].eq("disapproved") | disapproval_resolution)) | nonapproval_rows,
    "rollcall_direction",
] = "reject_project"

local_negative = decision_panel["local_member_final_action_vote_status"].eq("local_member_negative")
local_affirmative = decision_panel["local_member_final_action_vote_status"].eq("local_member_affirmative_only")
approve_vote = decision_panel["rollcall_direction"].eq("approve_project")
reject_vote = decision_panel["rollcall_direction"].eq("reject_project")
decision_panel["local_member_project_position"] = ""
decision_panel.loc[(approve_vote & local_affirmative) | (reject_vote & local_negative), "local_member_project_position"] = "supports"
decision_panel.loc[(approve_vote & local_negative) | (reject_vote & local_affirmative), "local_member_project_position"] = "opposes"
decision_panel.loc[decision_panel["project_outcome"].eq(""), "local_member_project_position"] = ""

# Events. Companion matters for one project (the LU application, its resolution, and
# related M items) share ZAP project ids or application keys. Matters in the same query
# year that share any id or key are one event (connected components).
parent = {matter_id: matter_id for matter_id in decision_panel["matter_id"]}


def find(matter_id: str) -> str:
    while parent[matter_id] != matter_id:
        parent[matter_id] = parent[parent[matter_id]]
        matter_id = parent[matter_id]
    return matter_id


first_matter_by_token = {}
for row in decision_panel[["query_year", "matter_id", "zap_project_ids", "application_keys"]].to_dict("records"):
    tokens = [f"zap:{x}" for x in split_semicolon(row["zap_project_ids"])]
    tokens += [f"application:{x}" for x in split_semicolon(row["application_keys"])]
    for token in tokens:
        key = (row["query_year"], token)
        if key in first_matter_by_token:
            parent[find(row["matter_id"])] = find(first_matter_by_token[key])
        else:
            first_matter_by_token[key] = row["matter_id"]
decision_panel["event_root"] = decision_panel["matter_id"].map(find)
decision_panel["event_id"] = decision_panel.groupby("event_root")["matter_id"].transform(
    lambda ids: "event_" + min(ids, key=int)
)
decision_panel["event_matter_count"] = decision_panel.groupby("event_id")["matter_id"].transform("size")

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
        "n_affected_districts",
        "local_member_final_action_vote_status",
        "local_member_final_action_votes",
        "local_member_abstain",
        "local_member_vote_match_methods",
        "has_local_member_vote_observed",
        "rollcall_direction",
        "local_member_project_position",
        "project_outcome",
        "event_id",
        "event_matter_count",
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
if not decision_panel.loc[decision_panel["vote_date"].ne(""), "vote_date"].str.fullmatch(r"\d{4}-\d{2}-\d{2}").all():
    raise RuntimeError("Vote dates must be ISO dates.")

save_frame(decision_panel, "../output/council_land_use_decision_panel.csv", ["matter_id"])
