from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.append("../../../_lib")
from legistar_utils import parse_action_detail
from member_deference_utils import write_csv


action_details = pd.read_csv(
    "../input/member_deference_nonapproval_action_details.csv", dtype=str, keep_default_na=False
)

production_code_dir = Path("../../../fetch_council_land_use_nonapproval_votes/code")
member_vote_rows = []

for row in action_details.sort_values(["query_year", "matter_file", "matter_id"]).to_dict("records"):
    raw_path = Path(row["raw_path"])
    if not raw_path.is_absolute():
        raw_path = production_code_dir / raw_path
    if not raw_path.exists():
        raise FileNotFoundError(f"Missing cached Legistar action-detail page: {raw_path}")

    _, votes = parse_action_detail(raw_path.read_text(encoding="utf-8"))
    for vote_sequence, vote in enumerate(votes, start=1):
        member_vote_rows.append(
            {
                "query_year": row["query_year"],
                "matter_id": row["matter_id"],
                "matter_file": row["matter_file"],
                "matter_status": row["matter_status"],
                "disposition_group": row["disposition_group"],
                "final_history_date": row["final_history_date"],
                "final_history_action": row["final_history_action"],
                "affected_council_districts": row["affected_council_districts"],
                "local_members_from_roster": row["local_members_from_roster"],
                "vote_sequence": vote_sequence,
                **vote,
            }
        )

member_votes = pd.DataFrame(
    member_vote_rows,
    columns=[
        "query_year",
        "matter_id",
        "matter_file",
        "matter_status",
        "disposition_group",
        "final_history_date",
        "final_history_action",
        "affected_council_districts",
        "local_members_from_roster",
        "vote_sequence",
        "person_name",
        "person_id",
        "person_guid",
        "vote",
    ],
)

vote_count_check = action_details[
    ["matter_id", "matter_file", "parsed_vote_rows", "vote_record_count"]
].merge(
    member_votes.groupby("matter_id", dropna=False).agg(vote_rows=("vote", "size")).reset_index(),
    on="matter_id",
    how="left",
    validate="one_to_one",
)
vote_count_check["vote_rows"] = vote_count_check["vote_rows"].fillna(0).astype(int)
vote_count_check["parsed_vote_rows"] = pd.to_numeric(vote_count_check["parsed_vote_rows"], errors="coerce")
vote_count_check["vote_record_count"] = pd.to_numeric(vote_count_check["vote_record_count"], errors="coerce")

if not (vote_count_check["vote_rows"] == vote_count_check["parsed_vote_rows"]).all():
    bad_matters = ", ".join(
        vote_count_check.loc[
            vote_count_check["vote_rows"] != vote_count_check["parsed_vote_rows"], "matter_file"
        ]
        .head(10)
        .astype(str)
    )
    raise RuntimeError(f"Member-vote rows do not reconcile to parsed_vote_rows for: {bad_matters}")

if not (
    vote_count_check["vote_record_count"].isna()
    | (vote_count_check["vote_rows"] == vote_count_check["vote_record_count"])
).all():
    bad_matters = ", ".join(
        vote_count_check.loc[
            ~(
                vote_count_check["vote_record_count"].isna()
                | (vote_count_check["vote_rows"] == vote_count_check["vote_record_count"])
            ),
            "matter_file",
        ]
        .head(10)
        .astype(str)
    )
    raise RuntimeError(f"Member-vote rows do not reconcile to Legistar vote-record counts for: {bad_matters}")

write_csv("../output/member_deference_nonapproval_member_votes.csv", member_votes)
