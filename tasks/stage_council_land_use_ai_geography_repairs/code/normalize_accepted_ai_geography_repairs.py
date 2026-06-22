#!/usr/bin/env python3

from pathlib import Path
import re

import pandas as pd


# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/stage_council_land_use_ai_geography_repairs/code")


def split_semicolon(value):
    if pd.isna(value) or str(value).strip() == "":
        return []
    return [part.strip() for part in str(value).split(";") if part.strip()]


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


def write_csv(path, df):
    temp_path = Path(path).with_suffix(Path(path).suffix + ".tmp")
    df.to_csv(temp_path, index=False)
    temp_path.replace(path)


repairs = pd.read_csv("accepted_ai_geography_repair_ledger.csv", dtype=str, keep_default_na=False)
remaining_bundle_queue = pd.read_csv(
    "../input/council_land_use_remaining_split_geography_bundle_queue.csv", dtype=str, keep_default_na=False
)
remaining_responses = pd.read_csv(
    "../input/council_land_use_remaining_split_geography_responses_combined.csv", dtype=str, keep_default_na=False
)

if repairs.duplicated(["query_year", "vote_date", "matter_file"]).any():
    duplicate_rows = repairs[
        repairs.duplicated(["query_year", "vote_date", "matter_file"], keep=False)
    ][["query_year", "vote_date", "matter_file", "signature_review_id"]]
    raise RuntimeError(f"Repair ledger is not unique by query_year, vote_date, matter_file:\n{duplicate_rows}")
if repairs["signature_review_id"].str.strip().eq("").any():
    raise RuntimeError("Repair ledger rows must have signature_review_id.")
if repairs["accepted_council_districts"].map(valid_districts).eq(False).any():
    raise RuntimeError("Repair ledger rows must have valid accepted_council_districts.")

if remaining_bundle_queue["review_id"].duplicated().any():
    raise RuntimeError("Remaining split-geography bundle queue must be unique by review_id.")
if remaining_responses["review_id"].duplicated().any():
    raise RuntimeError("Remaining split-geography responses must be unique by review_id.")

remaining_repairs = remaining_bundle_queue.merge(
    remaining_responses,
    on="review_id",
    how="inner",
    validate="one_to_one",
)
remaining_repairs = remaining_repairs[
    remaining_repairs["needs_human_review"].eq("false")
    & remaining_repairs["status"].isin(["project_geography", "mixed_bundle"])
].copy()
if remaining_repairs["affected_council_districts"].map(valid_districts).eq(False).any():
    raise RuntimeError("Accepted remaining split-geography repairs must have valid districts.")

remaining_repair_rows = []
for row in remaining_repairs.to_dict("records"):
    for matter_file in split_semicolon(row["matter_files"]):
        remaining_repair_rows.append(
            {
                "query_year": row["query_year"],
                "vote_date": row["vote_date"],
                "matter_file": matter_file,
                "signature_review_id": row["review_id"],
                "accepted_council_districts": row["affected_council_districts"],
                "repair_source": "remaining_split_vote_geography_ai_review_researcher_accepted",
                "repair_confidence": row["confidence"],
                "repair_promotion_decision": "accepted_from_remaining_split_vote_review",
                "repair_evidence_type": row["evidence_basis"],
                "repair_note": row["short_explanation"],
                "signature_matter_files": row["matter_files"],
                "application_keys": row["application_keys"],
                "zap_project_ids": row["zap_project_ids"],
                "zap_project_names": row["zap_project_names"],
                "title_examples": row["title_examples"],
                "matter_urls": row["matter_urls"],
                "history_detail_urls": row["history_detail_urls"],
            }
        )

if remaining_repair_rows:
    repairs = pd.concat([repairs, pd.DataFrame(remaining_repair_rows)], ignore_index=True)

duplicate_keys = repairs[repairs.duplicated(["query_year", "vote_date", "matter_file"], keep=False)].copy()
if not duplicate_keys.empty:
    district_counts = (
        duplicate_keys.groupby(["query_year", "vote_date", "matter_file"], as_index=False)
        .agg(district_values=("accepted_council_districts", "nunique"))
    )
    if district_counts["district_values"].gt(1).any():
        raise RuntimeError("Accepted repair sources disagree on districts for at least one matter key.")
    repairs["existing_ledger_source"] = repairs["repair_source"].ne(
        "remaining_split_vote_geography_ai_review_researcher_accepted"
    )
    repairs = (
        repairs.sort_values(
            ["query_year", "vote_date", "matter_file", "existing_ledger_source"],
            ascending=[True, True, True, False],
        )
        .drop_duplicates(["query_year", "vote_date", "matter_file"], keep="first")
        .drop(columns=["existing_ledger_source"])
    )

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
