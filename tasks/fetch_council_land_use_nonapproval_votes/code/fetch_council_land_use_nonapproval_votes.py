# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_council_land_use_nonapproval_votes/code")

from __future__ import annotations

import sys
import time
from pathlib import Path

import pandas as pd
import requests

sys.path.append("../../_lib")
from legistar_utils import parse_action_detail, request_with_retries, safe_stub, save_text, sha256
from member_deference_utils import collapse_values, edge_name, split_semicolon, write_csv


queue = pd.read_csv(
    "../input/member_deference_nonapproval_geography_conservative_queue.csv", dtype=str, keep_default_na=False
)
target_queue = queue[queue["fetch_vote_detail_first_pass"].str.lower().eq("true")].copy()
target_queue = target_queue.sort_values(["query_year", "matter_file", "matter_id"]).reset_index(drop=True)

if target_queue.empty:
    raise RuntimeError("The first-pass non-approval action-detail queue is empty.")
if target_queue["matter_id"].duplicated().any():
    raise RuntimeError("The first-pass non-approval queue must be unique by matter_id.")
if target_queue["final_history_detail_url"].eq("").any():
    raise RuntimeError("Every first-pass non-approval queue row must have a final action-detail URL.")
if target_queue["final_history_detail_url"].duplicated().any():
    raise RuntimeError("The first-pass non-approval action-detail URLs must be unique.")

session = requests.Session()
session.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Safari/537.36",
        "Referer": "https://legistar.council.nyc.gov/Legislation.aspx",
    }
)

raw_dir = Path("../output/source_files/member_deference_nonapproval_action_pages")
action_rows = []
member_vote_rows = []
fetch_failures = []

for i, row in enumerate(target_queue.to_dict("records"), start=1):
    raw_path = raw_dir / f"{safe_stub(row['matter_file'])}_{row['matter_id']}.html"

    if not raw_path.exists() or raw_path.stat().st_size == 0:
        try:
            response = request_with_retries(session, row["final_history_detail_url"])
            save_text(raw_path, response.text)
            time.sleep(0.03)
        except requests.RequestException as exc:
            fetch_failures.append(
                {
                    "matter_id": row["matter_id"],
                    "matter_file": row["matter_file"],
                    "final_history_detail_url": row["final_history_detail_url"],
                    "fetch_error": str(exc),
                }
            )
            continue

    summary, votes = parse_action_detail(raw_path.read_text(encoding="utf-8"))
    action_rows.append(
        {
            "query_year": row["query_year"],
            "matter_id": row["matter_id"],
            "matter_file": row["matter_file"],
            "query_matter_type": row["query_matter_type"],
            "matter_status": row["matter_status"],
            "disposition_group": row["disposition_group"],
            "filed_age_group": row["filed_age_group"],
            "final_action_vote_fetch_tier": row["final_action_vote_fetch_tier"],
            "final_history_date": row["final_history_date"],
            "final_history_action_by": row["final_history_action_by"],
            "final_history_action": row["final_history_action"],
            "final_history_result": row["final_history_result"],
            "final_history_detail_url": row["final_history_detail_url"],
            "affected_council_districts": row["affected_council_districts"],
            "affected_district_source": row["affected_district_source"],
            "local_members_from_roster": row["local_members_from_roster"],
            "application_keys": row["application_keys"],
            "title": row["title"],
            "raw_path": str(raw_path),
            "file_size_bytes": raw_path.stat().st_size,
            "checksum_sha256": sha256(raw_path),
            **summary,
        }
    )

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

    if i == 1 or i % 50 == 0 or i == len(target_queue):
        print(f"Processed first-pass non-approval action-detail page {i} of {len(target_queue)}", flush=True)

if not action_rows:
    raise RuntimeError("No first-pass non-approval action-detail pages were fetched or parsed.")

action_details = pd.DataFrame(action_rows)
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
fetch_failures_df = pd.DataFrame(
    fetch_failures,
    columns=["matter_id", "matter_file", "final_history_detail_url", "fetch_error"],
)
if not fetch_failures_df.empty or len(action_details) != len(target_queue):
    raise RuntimeError(
        "Expected one parsed action-detail page for every queued non-approval matter; "
        f"parsed {len(action_details)} pages for {len(target_queue)} queued matters with "
        f"{len(fetch_failures_df)} fetch failures."
    )

vote_count_check = action_details[
    ["matter_id", "matter_file", "vote_tab_label", "parsed_vote_rows", "vote_record_count"]
].merge(
    member_votes.groupby("matter_id", dropna=False)
    .agg(vote_rows=("vote", "size"))
    .reset_index(),
    on="matter_id",
    how="left",
    validate="one_to_one",
)
vote_count_check["vote_rows"] = vote_count_check["vote_rows"].fillna(0).astype(int)
vote_count_check["vote_record_count"] = pd.to_numeric(vote_count_check["vote_record_count"], errors="coerce")
vote_count_check["parsed_rows_match_summary"] = vote_count_check["vote_rows"] == vote_count_check["parsed_vote_rows"]
vote_count_check["parsed_rows_match_legistar_record_count"] = (
    vote_count_check["vote_record_count"].isna()
    | (vote_count_check["vote_rows"] == vote_count_check["vote_record_count"])
)
if not vote_count_check["parsed_rows_match_summary"].all():
    bad_matters = ", ".join(
        vote_count_check.loc[~vote_count_check["parsed_rows_match_summary"], "matter_file"].head(10).astype(str)
    )
    raise RuntimeError(f"Member-vote rows do not reconcile to parsed_vote_rows for: {bad_matters}")
if not vote_count_check["parsed_rows_match_legistar_record_count"].all():
    bad_matters = ", ".join(
        vote_count_check.loc[
            ~vote_count_check["parsed_rows_match_legistar_record_count"], "matter_file"
        ].head(10).astype(str)
    )
    raise RuntimeError(f"Member-vote rows do not reconcile to Legistar vote-record counts for: {bad_matters}")

member_votes_for_join = member_votes.copy()
if member_votes_for_join.empty:
    member_votes_by_person = pd.DataFrame(
        columns=["matter_id", "local_member_key", "matched_vote_person_names", "local_member_final_action_votes"]
    )
else:
    member_votes_for_join["local_member_key"] = member_votes_for_join["person_name"].map(edge_name)
    member_votes_by_person = (
        member_votes_for_join.groupby(["matter_id", "local_member_key"], as_index=False)
        .agg(
            matched_vote_person_names=("person_name", collapse_values),
            local_member_final_action_votes=("vote", collapse_values),
        )
    )

if member_votes_by_person.duplicated(["matter_id", "local_member_key"]).any():
    raise RuntimeError("Non-approval member-vote rows must be unique by matter_id and normalized person key.")

local_member_rows = []
for row in action_details.to_dict("records"):
    for local_member_name in split_semicolon(row["local_members_from_roster"]):
        local_member_rows.append(
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
                "local_member_name": local_member_name,
                "local_member_key": edge_name(local_member_name),
                "parsed_vote_rows": row["parsed_vote_rows"],
            }
        )

local_member_votes = pd.DataFrame(
    local_member_rows,
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
        "local_member_name",
        "local_member_key",
        "parsed_vote_rows",
    ],
)
if not local_member_votes.empty:
    local_member_votes = local_member_votes.merge(
        member_votes_by_person,
        on=["matter_id", "local_member_key"],
        how="left",
        validate="many_to_one",
    )
else:
    local_member_votes["matched_vote_person_names"] = pd.Series(dtype="object")
    local_member_votes["local_member_final_action_votes"] = pd.Series(dtype="object")

local_member_votes["local_member_vote_found"] = local_member_votes["local_member_final_action_votes"].fillna("").ne("")


def local_member_vote_category(value: object) -> str:
    votes = set(split_semicolon(value))
    if not votes:
        return "missing_from_vote_rows"
    if any(vote in {"Negative", "Abstain"} for vote in votes):
        return "negative_or_abstain"
    if votes == {"Affirmative"}:
        return "affirmative"
    if votes.issubset({"Excused", "Non-voting", "Absent", "Maternity"}):
        return "excused_nonvoting_absent"
    return "mixed_or_other"


local_member_votes["local_member_final_action_vote_category"] = local_member_votes[
    "local_member_final_action_votes"
].map(local_member_vote_category)

local_member_base = action_details[
    [
        "query_year",
        "matter_id",
        "matter_file",
        "matter_status",
        "disposition_group",
        "final_history_date",
        "final_history_action",
        "affected_council_districts",
        "local_members_from_roster",
        "parsed_vote_rows",
    ]
].copy()
if local_member_votes.empty:
    local_member_matter = pd.DataFrame(columns=["matter_id"])
else:
    local_member_matter = (
        local_member_votes.groupby("matter_id", as_index=False)
        .agg(
            local_member_rows=("local_member_name", "size"),
            local_member_vote_rows_found=("local_member_vote_found", "sum"),
            matched_vote_person_names=("matched_vote_person_names", collapse_values),
            local_member_final_action_votes=("local_member_final_action_votes", collapse_values),
            local_member_final_action_vote_categories=("local_member_final_action_vote_category", collapse_values),
        )
    )

local_member_summary = local_member_base.merge(local_member_matter, on="matter_id", how="left", validate="one_to_one")
local_member_summary["local_member_rows"] = local_member_summary["local_member_rows"].fillna(0).astype(int)
local_member_summary["local_member_vote_rows_found"] = (
    local_member_summary["local_member_vote_rows_found"].fillna(0).astype(int)
)
for col in [
    "matched_vote_person_names",
    "local_member_final_action_votes",
    "local_member_final_action_vote_categories",
]:
    local_member_summary[col] = local_member_summary[col].fillna("")


def matter_vote_status(row: pd.Series) -> str:
    categories = split_semicolon(row["local_member_final_action_vote_categories"])
    if not split_semicolon(row["local_members_from_roster"]):
        return "no_local_member_from_roster"
    if int(row["parsed_vote_rows"]) == 0:
        return "zero_vote_page"
    if not categories or set(categories) == {"missing_from_vote_rows"}:
        return "local_member_missing_from_vote_rows"
    if "negative_or_abstain" in categories:
        return "local_member_negative_or_abstain"
    if set(categories) == {"affirmative"}:
        return "local_member_affirmative_only"
    if set(categories) == {"excused_nonvoting_absent"}:
        return "local_member_excused_nonvoting_absent_only"
    return "local_member_mixed_or_other"


local_member_summary["local_member_final_action_vote_status"] = local_member_summary.apply(matter_vote_status, axis=1)

write_csv("../output/member_deference_nonapproval_action_details.csv", action_details)
write_csv("../output/member_deference_nonapproval_local_member_vote_status.csv", local_member_summary)
