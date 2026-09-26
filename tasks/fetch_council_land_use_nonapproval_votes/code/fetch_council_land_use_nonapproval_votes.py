from __future__ import annotations

import sys
import time
from pathlib import Path

import pandas as pd
import requests

sys.path.append("../../_lib")
from legistar_utils import check_cached_history_page, parse_action_detail, request_with_retries, safe_stub, save_text, sha256
from member_deference_utils import (
    collapse_values,
    local_member_votes,
    local_roster_rows,
    local_vote_status,
    read_roster_by_district,
    save_frame,
    split_semicolon,
)


queue = pd.read_csv(
    "../input/member_deference_nonapproval_geography_conservative_queue.csv", dtype=str, keep_default_na=False
)
roster_by_district = read_roster_by_district("../input/council_member_roster_master.csv")
target_queue = queue[queue["fetch_vote_detail_first_pass"].str.lower().eq("true")].copy()
target_queue = target_queue.sort_values(["query_year", "matter_file", "matter_id"]).reset_index(drop=True)

if target_queue.empty:
    raise RuntimeError("The first-pass nonapproval action-detail list is empty.")
if target_queue["matter_id"].duplicated().any():
    raise RuntimeError("The first-pass nonapproval list must be unique by matter_id.")
if target_queue["final_history_detail_url"].eq("").any():
    raise RuntimeError("Every first-pass nonapproval list row must have a final action-detail URL.")
if target_queue["final_history_detail_url"].duplicated().any():
    raise RuntimeError("The first-pass nonapproval action-detail URLs must be unique.")

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

    if raw_path.exists() and raw_path.stat().st_size > 0:
        check_cached_history_page(raw_path, row["final_history_detail_url"])
    else:
        try:
            response = request_with_retries(session, row["final_history_detail_url"])
            save_text(raw_path, response.text)
            time.sleep(0.25)
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
                "vote_sequence": vote_sequence,
                **vote,
            }
        )

    if i == 1 or i % 50 == 0 or i == len(target_queue):
        print(f"Processed first-pass nonapproval action-detail page {i} of {len(target_queue)}", flush=True)

if not action_rows:
    raise RuntimeError("No first-pass nonapproval action-detail pages were fetched or parsed.")

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
        "Expected one parsed action-detail page for every listed nonapproval matter; "
        f"parsed {len(action_details)} pages for {len(target_queue)} listed matters with "
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

# Local members are read from the roster on the final-action date and matched to the
# roll call by Legistar person_id, with the same rule as the approval-side vote panel.
votes_by_matter = {matter_id: rows.to_dict("records") for matter_id, rows in member_votes.groupby("matter_id")}
local_rows_out = []
for row in action_details.to_dict("records"):
    districts = split_semicolon(row["affected_council_districts"])
    local_rows, missing_roster_districts = local_roster_rows(
        roster_by_district, districts, pd.to_datetime(row["final_history_date"], format="%Y-%m-%d")
    )
    matter_votes = votes_by_matter.get(row["matter_id"], [])
    local_votes = local_member_votes(local_rows, matter_votes)
    local_rows_out.append(
        {
            "matter_id": row["matter_id"],
            "local_members_from_roster": collapse_values([local["member_name"] for local in local_votes]),
            "local_member_person_ids": collapse_values([local["person_id"] for local in local_votes]),
            "local_member_vote_match_methods": collapse_values([local["match_method"] for local in local_votes]),
            "missing_roster_districts": collapse_values(missing_roster_districts),
            "local_member_rows": len(local_votes),
            "local_member_vote_rows_found": sum(bool(local["vote"]) for local in local_votes),
            "matched_vote_person_names": collapse_values([local["member_name"] for local in local_votes if local["vote"]]),
            "local_member_final_action_votes": collapse_values(
                [f"{local['member_name']}: {local['vote']}" for local in local_votes if local["vote"]]
            ),
            "local_member_abstain": collapse_values(
                [local["member_name"] for local in local_votes if local["vote"] == "Abstain"]
            ),
            "local_member_final_action_vote_status": local_vote_status(
                districts, missing_roster_districts, local_votes, len(matter_votes)
            ),
        }
    )

local_member_summary = action_details[
    [
        "query_year",
        "matter_id",
        "matter_file",
        "matter_status",
        "disposition_group",
        "final_history_date",
        "final_history_action",
        "affected_council_districts",
        "parsed_vote_rows",
    ]
].merge(pd.DataFrame(local_rows_out), on="matter_id", how="left", validate="one_to_one")
action_details = action_details.merge(
    local_member_summary[["matter_id", "local_members_from_roster"]], on="matter_id", how="left", validate="one_to_one"
)

save_frame(action_details, "../output/member_deference_nonapproval_action_details.csv", ["matter_id"])
save_frame(local_member_summary, "../output/member_deference_nonapproval_local_member_vote_status.csv", ["matter_id"])
