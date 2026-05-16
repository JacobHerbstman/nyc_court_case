# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_council_land_use_records/code")

from __future__ import annotations

import re
import sys

import pandas as pd

if len(sys.argv) != 2 or not re.fullmatch(r"\d{4}", sys.argv[1]):
    raise RuntimeError("Usage: python3 build_legistar_vote_signals.py <year>")

QUERY_YEAR = sys.argv[1]


def application_keys(value: object) -> list[str]:
    if pd.isna(value) or str(value).strip() == "":
        return []

    keys = []
    for part in str(value).split(";"):
        key = re.sub(r"[^A-Za-z0-9]", "", part).upper()
        key = re.sub(r"^[CNM](?=\d)", "", key)
        if key:
            keys.append(key)

    return list(dict.fromkeys(keys))


matter_index = pd.read_csv(
    f"../output/legistar_{QUERY_YEAR}_broad_recall_matter_index.csv",
    dtype=str,
    keep_default_na=False,
)
split_vote_signals = pd.read_csv(
    f"../output/legistar_{QUERY_YEAR}_broad_recall_split_vote_signals.csv",
    dtype=str,
    keep_default_na=False,
)
action_details = pd.read_csv(
    f"../output/legistar_{QUERY_YEAR}_broad_recall_action_details.csv",
    dtype=str,
    keep_default_na=False,
)
member_votes = pd.read_csv(
    f"../output/legistar_{QUERY_YEAR}_broad_recall_member_votes.csv",
    dtype=str,
    keep_default_na=False,
)

callup_key_rows = []
for row in matter_index[matter_index["query_matter_type"] == "Land Use Call-Up"].to_dict("records"):
    for application_key in application_keys(row["application_numbers_in_title"]):
        callup_key_rows.append(
            {
                "application_key": application_key,
                "callup_matter_id": row["matter_id"],
                "callup_matter_file": row["matter_file"],
                "callup_prime_sponsor": row["prime_sponsor"],
                "callup_title": row["title"],
            }
        )

callup_keys = pd.DataFrame(callup_key_rows).drop_duplicates()
if callup_keys.empty:
    raise RuntimeError(f"No {QUERY_YEAR} Land Use Call-Up application keys were parsed.")

callup_links = []
for row in split_vote_signals.to_dict("records"):
    negative_members = [member.strip() for member in row["negative_members"].split(";") if member.strip()]
    abstain_members = [member.strip() for member in row["abstain_members"].split(";") if member.strip()]

    for application_key in application_keys(row["application_numbers_in_title"]):
        matches = callup_keys[callup_keys["application_key"] == application_key]
        for callup in matches.to_dict("records"):
            sponsor = callup["callup_prime_sponsor"].strip()
            callup_links.append(
                {
                    "matter_id": row["matter_id"],
                    "matter_file": row["matter_file"],
                    "query_matter_type": row["query_matter_type"],
                    "history_date": row["history_date"],
                    "vote_margin": row["vote_margin"],
                    "application_key": application_key,
                    "negative_members": row["negative_members"] or None,
                    "abstain_members": row["abstain_members"] or None,
                    "callup_matter_id": callup["callup_matter_id"],
                    "callup_matter_file": callup["callup_matter_file"],
                    "callup_prime_sponsor": sponsor or None,
                    "callup_sponsor_in_negative_members": sponsor in negative_members,
                    "callup_sponsor_in_abstain_members": sponsor in abstain_members,
                    "callup_title": callup["callup_title"],
                    "title": row["title"],
                }
            )

callup_link_df = pd.DataFrame(callup_links)
if callup_link_df.empty:
    callup_link_df = pd.DataFrame(
        columns=[
            "matter_id",
            "matter_file",
            "query_matter_type",
            "history_date",
            "vote_margin",
            "application_key",
            "negative_members",
            "abstain_members",
            "callup_matter_id",
            "callup_matter_file",
            "callup_prime_sponsor",
            "callup_sponsor_in_negative_members",
            "callup_sponsor_in_abstain_members",
            "callup_title",
            "title",
        ]
    )

signal_profile = pd.DataFrame(
    [
        {"metric": "final_council_approval_action_pages", "value": len(action_details)},
        {"metric": "member_vote_rows", "value": len(member_votes)},
        {
            "metric": "zero_row_consent_vote_action_pages",
            "value": int((action_details["parsed_vote_rows"].astype(int) == 0).sum()),
        },
        {"metric": "split_or_abstain_matter_signals", "value": len(split_vote_signals)},
        {"metric": "split_or_abstain_callup_links", "value": len(callup_link_df)},
        {
            "metric": "callup_sponsor_negative_matter_signals",
            "value": int(callup_link_df["callup_sponsor_in_negative_members"].sum()) if not callup_link_df.empty else 0,
        },
        {
            "metric": "callup_sponsor_abstain_matter_signals",
            "value": int(callup_link_df["callup_sponsor_in_abstain_members"].sum()) if not callup_link_df.empty else 0,
        },
    ]
)

qc_rows = [
    {
        "check_name": "callup_application_keys_present",
        "passed": not callup_keys.empty,
        "detail": f"Parsed {len(callup_keys)} application-key rows from {QUERY_YEAR} Land Use Call-Up matters.",
    },
    {
        "check_name": "split_vote_callup_links_counted",
        "passed": True,
        "detail": f"Linked {len(callup_link_df)} split-vote matter rows to call-up matters by normalized application key.",
    },
]

if QUERY_YEAR == "2001":
    laguardia_callup_link = callup_link_df[
        (callup_link_df["matter_file"] == "Res 1939-2001")
        & (callup_link_df["callup_prime_sponsor"] == "Helen M. Marshall")
        & (callup_link_df["callup_sponsor_in_negative_members"])
    ]
    qc_rows.append(
        {
            "check_name": "laguardia_callup_sponsor_negative_link_found",
            "passed": not laguardia_callup_link.empty,
            "detail": "Res 1939-2001 links to M 1141-2001, sponsored by Helen M. Marshall, and Marshall is in the negative vote list.",
        }
    )

qc = pd.DataFrame(qc_rows)

callup_link_df.sort_values(
    ["callup_sponsor_in_negative_members", "callup_sponsor_in_abstain_members", "history_date", "matter_file"],
    ascending=[False, False, True, True],
).to_csv(f"../output/legistar_{QUERY_YEAR}_broad_recall_split_vote_callup_links.csv", index=False)
signal_profile.to_csv(f"../output/legistar_{QUERY_YEAR}_broad_recall_behavior_profile.csv", index=False)
qc.to_csv(f"../output/legistar_{QUERY_YEAR}_broad_recall_vote_signal_qc.csv", index=False)

if not qc["passed"].all():
    raise RuntimeError(f"Legistar {QUERY_YEAR} vote-signal build failed QC.")
