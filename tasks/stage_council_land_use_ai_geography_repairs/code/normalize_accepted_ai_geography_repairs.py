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

if repairs.duplicated(["query_year", "vote_date", "matter_file"]).any():
    duplicate_rows = repairs[
        repairs.duplicated(["query_year", "vote_date", "matter_file"], keep=False)
    ][["query_year", "vote_date", "matter_file", "signature_review_id"]]
    raise RuntimeError(f"Repair ledger is not unique by query_year, vote_date, matter_file:\n{duplicate_rows}")
if repairs["signature_review_id"].str.strip().eq("").any():
    raise RuntimeError("Repair ledger rows must have signature_review_id.")
if repairs["accepted_council_districts"].map(valid_districts).eq(False).any():
    raise RuntimeError("Repair ledger rows must have valid accepted_council_districts.")

signature_consistency = (
    repairs.groupby("signature_review_id", as_index=False)
    .agg(
        district_values=("accepted_council_districts", "nunique"),
        source_values=("repair_source", "nunique"),
    )
)
if signature_consistency["district_values"].gt(1).any() or signature_consistency["source_values"].gt(1).any():
    raise RuntimeError("Repair ledger signatures must have internally consistent districts and sources.")

repairs["vote_date"] = pd.to_datetime(repairs["vote_date"], errors="raise").dt.strftime("%Y-%m-%d")
repairs = repairs.sort_values(["query_year", "vote_date", "matter_file"]).reset_index(drop=True)

write_csv("../output/council_land_use_ai_geography_accepted_repairs.csv", repairs)
