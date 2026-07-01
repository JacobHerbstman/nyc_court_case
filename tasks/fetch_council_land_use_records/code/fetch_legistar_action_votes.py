from __future__ import annotations

import re
import sys
import time
from pathlib import Path

import pandas as pd
import requests

sys.path.append("../../_lib")
from legistar_utils import (
    check_saved_html,
    parse_action_detail,
    request_with_retries,
    safe_stub,
    save_text,
    sha256,
)

SOURCE_ID = "nyc_council_legistar_land_use_broad_recall"
if len(sys.argv) != 2 or not re.fullmatch(r"\d{4}", sys.argv[1]):
    raise RuntimeError("Usage: python3 fetch_legistar_action_votes.py <year>")

QUERY_YEAR = sys.argv[1]
ACTION_DETAILS_OUTPUT = Path(f"../output/legistar_{QUERY_YEAR}_broad_recall_action_details.csv")
MEMBER_VOTES_OUTPUT = Path(f"../output/legistar_{QUERY_YEAR}_broad_recall_member_votes.csv")


history_events = pd.read_csv(
    f"../output/legistar_{QUERY_YEAR}_broad_recall_history_events.csv",
    dtype=str,
    keep_default_na=False,
)
pull_dates = sorted(set(history_events["pull_date"]) - {""})
if len(pull_dates) != 1:
    raise RuntimeError(f"Expected exactly one pull_date in the {QUERY_YEAR} history-event table.")

approved_by_council = (
    history_events["history_action"]
    .str.replace(",", "", regex=False)
    .str.lower()
    .eq("approved by council")
)
target_events = history_events[
    (history_events["history_action_by"] == "City Council")
    & approved_by_council
    & (history_events["history_detail_url"] != "")
].copy()
target_events["history_date_parsed"] = pd.to_datetime(target_events["history_date"], errors="coerce")
target_events["history_sequence_int"] = pd.to_numeric(target_events["history_sequence"], errors="coerce")
target_events = (
    target_events.sort_values(["matter_id", "history_date_parsed", "history_sequence_int"])
    .drop_duplicates("matter_id", keep="last")
    .drop(columns=["history_date_parsed", "history_sequence_int"])
)

if target_events.empty:
    raise RuntimeError(f"No final Council approval action-detail URLs found for the {QUERY_YEAR} pull.")
if target_events["history_detail_url"].duplicated().any():
    raise RuntimeError("Final Council approval action-detail URLs are not unique.")
if target_events["matter_id"].duplicated().any():
    raise RuntimeError("Final Council approval action-detail events are not unique by matter_id.")

session = requests.Session()
session.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Safari/537.36",
        "Referer": "https://legistar.council.nyc.gov/Legislation.aspx",
    }
)

raw_dir = (
    Path("../output/source_files")
    / SOURCE_ID
    / pull_dates[0]
    / f"year_{QUERY_YEAR}"
    / "action_detail_pages"
)

action_rows = []
member_vote_rows = []
for i, row in enumerate(target_events.sort_values(["history_date", "matter_file"]).to_dict("records"), start=1):
    raw_path = raw_dir / f"{safe_stub(row['matter_file'])}_{row['matter_id']}.html"
    check_saved_html([raw_path], f"{QUERY_YEAR} Legistar action-detail page")
    if raw_path.exists() and raw_path.stat().st_size > 0:
        page_html = raw_path.read_text(encoding="utf-8")
        fetch_status = "cached"
    else:
        response = request_with_retries(session, row["history_detail_url"])
        page_html = response.text
        save_text(raw_path, page_html)
        fetch_status = "downloaded"
    summary, votes = parse_action_detail(page_html)

    action_rows.append(
        {
            "source_id": row["source_id"],
            "pull_date": row["pull_date"],
            "query_year": row["query_year"],
            "query_matter_type": row["query_matter_type"],
            "matter_id": row["matter_id"],
            "matter_guid": row["matter_guid"],
            "matter_file": row["matter_file"],
            "matter_url": row["matter_url"],
            "history_sequence": row["history_sequence"],
            "history_date": row["history_date"],
            "history_action_by": row["history_action_by"],
            "history_action": row["history_action"],
            "history_result": row["history_result"],
            "history_detail_url": row["history_detail_url"],
            "fetch_status": fetch_status,
            "raw_path": str(raw_path),
            "file_size_bytes": raw_path.stat().st_size,
            "checksum_sha256": sha256(raw_path),
            **summary,
        }
    )

    for vote_sequence, vote in enumerate(votes, start=1):
        member_vote_rows.append(
            {
                "source_id": row["source_id"],
                "pull_date": row["pull_date"],
                "query_year": row["query_year"],
                "query_matter_type": row["query_matter_type"],
                "matter_id": row["matter_id"],
                "matter_guid": row["matter_guid"],
                "matter_file": row["matter_file"],
                "history_sequence": row["history_sequence"],
                "history_date": row["history_date"],
                "history_detail_url": row["history_detail_url"],
                "vote_sequence": vote_sequence,
                **vote,
            }
        )

    if fetch_status == "downloaded":
        time.sleep(0.03)
    if i == 1 or i % 100 == 0 or i == len(target_events):
        print(f"Fetched action-detail page {i} of {len(target_events)}", flush=True)

action_details = pd.DataFrame(action_rows)
member_votes = pd.DataFrame(member_vote_rows)
if action_details.empty:
    raise RuntimeError("No action detail pages were downloaded.")
if member_votes.empty:
    raise RuntimeError("No member votes were parsed from action detail pages.")

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
vote_count_check["parsed_rows_match_summary"] = vote_count_check["vote_rows"] == vote_count_check["parsed_vote_rows"]
vote_count_check["parsed_rows_match_legistar_record_count"] = (
    vote_count_check["vote_rows"] == vote_count_check["vote_record_count"]
)
zero_vote_pages = vote_count_check[vote_count_check["parsed_vote_rows"] == 0]

if len(action_details) != len(target_events):
    raise RuntimeError("Every target approval event must have an action-detail page.")
if action_details["history_detail_url"].duplicated().any():
    raise RuntimeError("Action-detail URLs must be unique.")
if not vote_count_check["parsed_rows_match_summary"].all():
    raise RuntimeError("Member-vote rows must reconcile to parsed_vote_rows on every action-detail page.")
if not vote_count_check["parsed_rows_match_legistar_record_count"].all():
    raise RuntimeError("Member-vote rows must reconcile to Legistar's displayed vote-record counts.")
if not (
    zero_vote_pages.empty
    or (
        (zero_vote_pages["vote_record_count"] == 0)
        & zero_vote_pages["vote_tab_label"].fillna("").str.contains("\\(0:0\\)")
    ).all()
):
    raise RuntimeError("Zero-row action-detail pages must be displayed as consent-style (0:0) vote tabs.")
if action_details["matter_id"].duplicated().any():
    raise RuntimeError("Action details must be unique by matter_id after duplicate approval-event handling.")

if QUERY_YEAR == "2001":
    laguardia_negative = member_votes[
        (member_votes["matter_file"] == "Res 1939-2001")
        & (member_votes["person_name"] == "Helen M. Marshall")
        & (member_votes["vote"] == "Negative")
    ]
    if laguardia_negative.empty:
        raise RuntimeError("Res 1939-2001 must record Helen M. Marshall voting Negative.")

action_details.to_csv(ACTION_DETAILS_OUTPUT, index=False)
member_votes.to_csv(MEMBER_VOTES_OUTPUT, index=False)
