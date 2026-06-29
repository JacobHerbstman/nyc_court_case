# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/build_member_deference_legistar_signals/code")
# query_year = "1998"

from __future__ import annotations

import re
import sys

import pandas as pd

if len(sys.argv) == 2:
    query_year = sys.argv[1]
elif "query_year" not in globals():
    raise RuntimeError("Usage: python3 build_member_deference_legistar_signals.py <year>")

if not re.fullmatch(r"\d{4}", query_year):
    raise RuntimeError("query_year must be a four-digit year.")


def norm_name(value: object) -> str:
    value = re.sub(r"[^A-Za-z0-9 ]", " ", "" if pd.isna(value) else str(value))
    value = re.sub(r"\s+", " ", value).strip().lower()
    return value


def edge_name(value: object) -> str:
    parts = norm_name(value).split()
    if len(parts) < 2:
        return norm_name(value)
    return f"{parts[0]} {parts[-1]}"


def split_semicolon(value: object) -> list[str]:
    if pd.isna(value) or str(value).strip() == "":
        return []
    return [part.strip() for part in str(value).split(";") if part.strip()]


def collapse_values(values: object) -> str | None:
    clean_values = []
    for value in values:
        if pd.isna(value) or str(value).strip() == "":
            continue
        if str(value) not in clean_values:
            clean_values.append(str(value))
    return "; ".join(clean_values) if clean_values else None


district_patterns = [
    re.compile(
        r"Council District(?:s)?(?:\s*(?:No\.?|Nos\.?|no\.?|nos\.?))?\s*([0-9,\sand]+)",
        flags=re.IGNORECASE,
    ),
    re.compile(r"\bCD'?s?\.?\s*([0-9,\sand]+)", flags=re.IGNORECASE),
]
application_re = re.compile(
    r"\b(?:[CNM]\s*)?\d{6}\s*(?:\([A-Z0-9]+\)\s*)?[A-Z]{2,4}\b|"
    r"\b\d{8}\s*[A-Z]{2,4}\b",
    flags=re.IGNORECASE,
)


def districts_from_text(value: object) -> list[str]:
    text = "" if pd.isna(value) else str(value)
    districts = []
    for pattern in district_patterns:
        for match in pattern.finditer(text):
            districts.extend(re.findall(r"\d{1,2}", match.group(1)))
    return list(dict.fromkeys(districts))


def application_keys(value: object) -> list[str]:
    text = "" if pd.isna(value) else str(value)
    keys = []
    for match in application_re.finditer(text):
        key = re.sub(r"[^A-Za-z0-9]", "", match.group(0)).upper()
        key = re.sub(r"^[CNM](?=\d)", "", key)
        keys.append(key)
    return list(dict.fromkeys(keys))


action_details = pd.read_csv(
    f"../input/legistar_{query_year}_broad_recall_action_details.csv",
    dtype=str,
    keep_default_na=False,
)
matter_index = pd.read_csv(
    f"../input/legistar_{query_year}_broad_recall_matter_index.csv",
    dtype=str,
    keep_default_na=False,
)
roster = pd.read_csv(
    "../input/council_member_roster_master.csv",
    dtype=str,
    keep_default_na=False,
)

matter_columns = [
    "matter_id",
    "matter_file",
    "matter_type",
    "status",
    "committee",
    "title",
    "borough",
    "affected_council_districts",
    "application_numbers_in_title",
    "land_use_recall_reason",
    "laguardia_hotel_seed_flag",
]
action_details["negative_count_int"] = pd.to_numeric(
    action_details["negative_count"], errors="coerce"
).fillna(0)
action_details["abstain_count_int"] = pd.to_numeric(
    action_details["abstain_count"], errors="coerce"
).fillna(0)
split_votes = action_details.merge(
    matter_index[matter_columns],
    on=["matter_id", "matter_file"],
    how="left",
    validate="one_to_one",
)
split_votes = split_votes[
    (split_votes["negative_count_int"] > 0)
    | (split_votes["abstain_count_int"] > 0)
].sort_values(["history_date", "matter_file"])

split_ids = set(split_votes["matter_id"])
action_ids = set(action_details["matter_id"])
if not split_ids.issubset(action_ids):
    raise RuntimeError("Every split-vote matter must appear in the action-detail table.")

roster["term_start_date_parsed"] = pd.to_datetime(roster["term_start_date"], errors="coerce")
roster["term_end_date_parsed"] = pd.to_datetime(roster["term_end_date"], errors="coerce").fillna(
    pd.Timestamp("2100-01-01")
)
roster["member_name_norm"] = roster["member_name"].map(norm_name)
roster["member_name_edge"] = roster["member_name"].map(edge_name)
roster = roster[roster["member_name_norm"] != "vacant"].copy()

matter_rows = []
for row in split_votes.sort_values(["history_date", "matter_file"]).to_dict("records"):
    vote_date = pd.to_datetime(row["history_date"], errors="coerce")
    text_for_parse = " ".join(
        [
            row.get("title", ""),
            row.get("action_detail_title", ""),
            row.get("action_detail_text", ""),
        ]
    )
    ulurp_like_flag = bool(
        re.search(
            r"\bULURP\b|Uniform Land Use|Section s?197-[cd]|Sections 197-[cd]|ZRM|ZMM|ZRY|ZMK|ZMQ|ZSR|ZSM|ZSK",
            text_for_parse,
            flags=re.IGNORECASE,
        )
    )
    udaap_flag = bool(
        re.search(r"UDAAP|Urban Development Action Area", text_for_parse, flags=re.IGNORECASE)
    )
    affected_districts = districts_from_text(text_for_parse)
    negative_members = split_semicolon(row.get("negative_members", ""))
    abstain_members = split_semicolon(row.get("abstain_members", ""))
    negative_names_norm = {norm_name(member) for member in negative_members}
    negative_names_edge = {edge_name(member) for member in negative_members}
    abstain_names_norm = {norm_name(member) for member in abstain_members}
    abstain_names_edge = {edge_name(member) for member in abstain_members}

    roster_matches = []
    missing_districts = []
    for district in affected_districts:
        matches = roster[
            (roster["district"] == district)
            & (roster["term_start_date_parsed"] <= vote_date)
            & (vote_date <= roster["term_end_date_parsed"])
        ]
        if matches.empty:
            missing_districts.append(district)
            continue
        for match in matches.to_dict("records"):
            roster_matches.append(match)

    local_members = [match["member_name"] for match in roster_matches]
    local_negative = [
        match["member_name"]
        for match in roster_matches
        if match["member_name_norm"] in negative_names_norm or match["member_name_edge"] in negative_names_edge
    ]
    local_abstain = [
        match["member_name"]
        for match in roster_matches
        if match["member_name_norm"] in abstain_names_norm or match["member_name_edge"] in abstain_names_edge
    ]

    if local_negative:
        screen_status = "candidate_local_member_negative"
    elif local_abstain:
        screen_status = "candidate_local_member_abstain"
    elif missing_districts:
        screen_status = "unresolved_missing_roster"
    elif not affected_districts:
        screen_status = "unresolved_no_district_in_record"
    else:
        screen_status = "rejected_no_local_member_in_negative_or_abstain_vote"

    matter_rows.append(
        {
            "query_year": query_year,
            "matter_id": row["matter_id"],
            "matter_file": row["matter_file"],
            "query_matter_type": row["query_matter_type"],
            "vote_date": vote_date.strftime("%Y-%m-%d") if not pd.isna(vote_date) else None,
            "vote_margin": row["vote_margin"],
            "negative_members": collapse_values(negative_members),
            "abstain_members": collapse_values(abstain_members),
            "affected_council_districts": collapse_values(affected_districts),
            "local_members_from_roster": collapse_values(local_members),
            "local_member_negative": collapse_values(local_negative),
            "local_member_abstain": collapse_values(local_abstain),
            "missing_roster_districts": collapse_values(missing_districts),
            "screen_status": screen_status,
            "ulurp_like_flag": ulurp_like_flag,
            "udaap_flag": udaap_flag,
            "application_keys": collapse_values(application_keys(text_for_parse)),
            "source_tiers": collapse_values(match["source_tier"] for match in roster_matches),
            "district_sources": collapse_values(match["district_source"] for match in roster_matches),
            "title": row["action_detail_title"] or row["title"],
            "history_detail_url": row["history_detail_url"],
        }
    )

matter_signals = pd.DataFrame(matter_rows)

summary_rows = [
    {"metric": "split_or_abstain_matter_rows", "value": len(matter_signals)},
    {
        "metric": "candidate_local_member_negative_rows",
        "value": int((matter_signals["screen_status"] == "candidate_local_member_negative").sum()),
    },
    {
        "metric": "candidate_local_member_negative_ulurp_like_rows",
        "value": int(
            (
                (matter_signals["screen_status"] == "candidate_local_member_negative")
                & matter_signals["ulurp_like_flag"]
            ).sum()
        ),
    },
    {
        "metric": "candidate_local_member_abstain_rows",
        "value": int((matter_signals["screen_status"] == "candidate_local_member_abstain").sum()),
    },
    {
        "metric": "unresolved_missing_roster_rows",
        "value": int((matter_signals["screen_status"] == "unresolved_missing_roster").sum()),
    },
    {
        "metric": "unresolved_no_district_rows",
        "value": int((matter_signals["screen_status"] == "unresolved_no_district_in_record").sum()),
    },
    {
        "metric": "rejected_no_local_member_rows",
        "value": int(
            (matter_signals["screen_status"] == "rejected_no_local_member_in_negative_or_abstain_vote").sum()
        ),
    },
]
summary = pd.DataFrame(summary_rows)

qc = pd.DataFrame(
    [
        {
            "check_name": "split_vote_ids_unique",
            "passed": not split_votes["matter_id"].duplicated().any(),
            "detail": "Each split-vote matter row appears once before local-member screening.",
        },
        {
            "check_name": "split_vote_ids_present_in_action_details",
            "passed": split_ids.issubset(action_ids),
            "detail": "Split-vote matter IDs are a subset of final Council action-detail matter IDs.",
        },
        {
            "check_name": "screen_status_assigned",
            "passed": bool(matter_signals["screen_status"].notna().all()),
            "detail": "Every split-vote matter receives a candidate, rejected, or unresolved screen status.",
        },
    ]
)

matter_signals.to_csv(f"../output/legistar_{query_year}_member_override_matter_signals.csv", index=False)
summary.to_csv(f"../output/legistar_{query_year}_member_override_summary.csv", index=False)
qc.to_csv(f"../output/legistar_{query_year}_member_override_qc.csv", index=False)

if not qc["passed"].all():
    raise RuntimeError("Member-deference Legistar signal build failed QC.")
