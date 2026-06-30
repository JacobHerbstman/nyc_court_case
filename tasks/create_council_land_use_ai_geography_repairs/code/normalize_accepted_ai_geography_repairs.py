#!/usr/bin/env python3

import re
import sys

import pandas as pd

sys.path.append("../../_lib")
from member_deference_utils import split_semicolon, write_csv


# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/create_council_land_use_ai_geography_repairs/code")


def valid_districts(value):
    districts = []
    for part in split_semicolon(value):
        if not re.fullmatch(r"\d{1,2}", part):
            return False
        district = int(part)
        if district < 1 or district > 51:
            return False
        districts.append(district)
    return len(districts) > 0


repairs = pd.read_csv("accepted_ai_geography_repair_ledger.csv", dtype=str, keep_default_na=False)

required_columns = [
    "query_year",
    "vote_date",
    "matter_file",
    "signature_review_id",
    "accepted_council_districts",
    "repair_source",
    "repair_confidence",
    "repair_promotion_decision",
    "repair_evidence_type",
    "repair_note",
    "signature_matter_files",
    "application_keys",
    "zap_project_ids",
    "zap_project_names",
    "title_examples",
    "matter_urls",
    "history_detail_urls",
]
missing_columns = [col for col in required_columns if col not in repairs.columns]
if missing_columns:
    raise RuntimeError(f"Repair ledger is missing required columns: {', '.join(missing_columns)}")
repairs = repairs[required_columns].copy()

blocked_sources = {
    "remaining_split_vote_geography_ai_review_researcher_accepted": (
        "The remaining split-vote ChatGPT review pass is excluded from production until "
        "its cited source records are reverified against the queued Council matter rows."
    )
}
blocked_mask = repairs["repair_source"].isin(blocked_sources)
if blocked_mask.any():
    blocked_counts = repairs.loc[blocked_mask, "repair_source"].value_counts()
    blocked_detail = "; ".join(f"{source}: {count}" for source, count in blocked_counts.items())
    raise RuntimeError(f"Repair ledger contains blocked repair sources: {blocked_detail}")

if repairs.duplicated(["query_year", "vote_date", "matter_file"]).any():
    duplicate_rows = repairs[
        repairs.duplicated(["query_year", "vote_date", "matter_file"], keep=False)
    ][["query_year", "vote_date", "matter_file", "signature_review_id"]]
    raise RuntimeError(f"Repair ledger is not unique by query_year, vote_date, matter_file:\n{duplicate_rows}")
if repairs["signature_review_id"].str.strip().eq("").any():
    raise RuntimeError("Repair ledger rows must have signature_review_id.")
if repairs["accepted_council_districts"].map(valid_districts).eq(False).any():
    raise RuntimeError("Repair ledger rows must have valid accepted_council_districts.")
for col in ["repair_source", "repair_confidence", "repair_promotion_decision", "repair_evidence_type", "repair_note"]:
    if repairs[col].str.strip().eq("").any():
        raise RuntimeError(f"Repair ledger rows must have nonempty {col}.")

signature_consistency = (
    repairs.groupby("signature_review_id", as_index=False)
    .agg(
        district_values=("accepted_council_districts", "nunique"),
        source_values=("repair_source", "nunique"),
    )
)
if signature_consistency["district_values"].gt(1).any() or signature_consistency["source_values"].gt(1).any():
    raise RuntimeError("Repair ledger signatures must have internally consistent districts and sources.")

repairs["vote_date"] = pd.to_datetime(repairs["vote_date"], format="mixed", errors="raise").dt.strftime("%Y-%m-%d")
repairs = repairs.sort_values(["query_year", "vote_date", "matter_file"]).reset_index(drop=True)

write_csv("../output/council_land_use_ai_geography_accepted_repairs.csv", repairs)
