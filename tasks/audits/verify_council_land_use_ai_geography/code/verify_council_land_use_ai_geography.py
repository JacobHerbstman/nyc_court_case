#!/usr/bin/env python3

import csv
import io
import json
import os
import re
from functools import lru_cache

import geopandas as gpd
import pandas as pd
from shapely import wkt




TARGET_CATEGORIES = {
    "ai_adjudicated_official_project_geography",
    "official_location_strong_district_inferred",
    "ai_adjudicated_project_geography_needs_spot_check",
}

BOROUGH_CODES = {
    "MANHATTAN": 1,
    "NEW YORK": 1,
    "MN": 1,
    "BRONX": 2,
    "BX": 2,
    "BROOKLYN": 3,
    "KINGS": 3,
    "BK": 3,
    "QUEENS": 4,
    "QN": 4,
    "STATEN ISLAND": 5,
    "RICHMOND": 5,
    "SI": 5,
}

STREET_REPLACEMENTS = {
    " ST ": " STREET ",
    " AVE ": " AVENUE ",
    " RD ": " ROAD ",
    " BLVD ": " BOULEVARD ",
    " DR ": " DRIVE ",
    " PL ": " PLACE ",
    " LN ": " LANE ",
    " PKWY ": " PARKWAY ",
    " CT ": " COURT ",
    " TER ": " TERRACE ",
    " SQ ": " SQUARE ",
}

ADDRESS_PATTERN = re.compile(
    r"(?<![-\d])\b\d{1,5}(?:-\d{1,5})?\s+"
    r"(?:[A-Za-z0-9]+\.?\s+){0,6}?"
    r"(?:Avenue\s+of\s+the\s+Americas|Ave\.?\s+of\s+the\s+Americas|Street|St\.?|Avenue|Ave\.?|Road|Rd\.?|Boulevard|Blvd\.?|Drive|Dr\.?|Place|Pl\.?|"
    r"Lane|Ln\.?|Parkway|Pkwy\.?|Court|Ct\.?|Terrace|Ter\.?|Square|Sq\.?|Highway|"
    r"Broadway|Bowery|Concourse)\b",
    re.IGNORECASE,
)

BLOCK_PATTERN = re.compile(r"\bblocks?\s+(\d{1,5})\b", re.IGNORECASE)


def write_csv_if_changed(rows, fieldnames, path):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    new_text = output.getvalue()

    try:
        with open(path, "r", encoding="utf-8", newline="") as old_file:
            old_text = old_file.read()
    except FileNotFoundError:
        old_text = None

    if old_text != new_text:
        with open(path, "w", encoding="utf-8", newline="") as output_file:
            output_file.write(new_text)


def parse_jsonl_if_exists(path):
    rows = []
    errors = []
    if not os.path.exists(path):
        return rows, errors
    with open(path, "r", encoding="utf-8") as input_file:
        for line_number, line in enumerate(input_file, start=1):
            line = line.strip()
            if line == "":
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                errors.append(
                    {
                        "line_number": line_number,
                        "signature_review_id": "",
                        "validation_errors": f"json_decode_error:{exc.msg}",
                    }
                )
                continue
            row["line_number"] = line_number
            rows.append(row)
    return rows, errors


def clean_text(value):
    if value is None or pd.isna(value):
        return ""
    return " ".join(str(value).split())


def split_semicolon_values(value):
    parts = []
    for part in re.split(r"[;|]", clean_text(value)):
        part = part.strip()
        if part and part not in parts:
            parts.append(part)
    return parts


def split_districts(value):
    districts = []
    for part in re.split(r"[;,|]", clean_text(value)):
        part = part.strip()
        if part.isdigit() and 1 <= int(part) <= 51 and part not in districts:
            districts.append(part)
    return districts


def normalize_bbl(value):
    text = clean_text(value)
    if text == "":
        return ""
    try:
        text = str(int(float(text)))
    except ValueError:
        text = re.sub(r"\D", "", text)
    if len(text) == 10 and text[0] in "12345":
        return text
    return ""


def normalize_address(value):
    text = clean_text(value).upper()
    text = re.sub(r"[,.;:()]", " ", text)

    def normalize_house_number(match):
        left = match.group(1)
        right = match.group(2)
        if len(left) == 2 and len(right) == 2:
            return f"{left}-{right}"
        return left

    text = re.sub(r"\b(\d{1,5})-(\d{1,5})\b", normalize_house_number, text)
    text = re.sub(r"\b(\d+)(ST|ND|RD|TH)\b", r"\1", text)
    text = re.sub(r"\bAVE\b\s+OF\s+THE\s+AMERICAS\b", "AVENUE OF THE AMERICAS", text)
    text = re.sub(r"\s+", " ", text)
    text = f" {text} "
    for old, new in STREET_REPLACEMENTS.items():
        text = text.replace(old, new)
    return re.sub(r"\s+", " ", text).strip()


def borough_code_from_text(*values):
    text = " ".join(clean_text(value).upper() for value in values)
    for name, code in BOROUGH_CODES.items():
        if re.search(rf"\b{re.escape(name)}\b", text):
            return code
    return None


def bbl_from_block_lot(borough_code, block, lot):
    if borough_code is None:
        return ""
    try:
        return f"{borough_code}{int(block):05d}{int(lot):04d}"
    except ValueError:
        return ""


def extract_bbls_from_text(text, borough_code):
    cleaned = clean_text(text)
    bbls = []

    for match in re.finditer(r"\b[1-5]\d{9}\b", cleaned):
        bbl = normalize_bbl(match.group(0))
        if bbl and bbl not in bbls:
            bbls.append(bbl)

    if borough_code is None:
        return bbls

    block_matches = list(BLOCK_PATTERN.finditer(cleaned))
    for index, match in enumerate(block_matches):
        start = match.end()
        end = block_matches[index + 1].start() if index + 1 < len(block_matches) else min(len(cleaned), start + 180)
        segment = cleaned[start:end]
        if not re.search(r"\blots?\b", segment, re.IGNORECASE):
            continue
        segment = re.split(
            r"\b(?:and beds?|beds?|bounded|community|council|borough|street|st\.?|avenue|ave\.?|road|"
            r"boulevard|drive|place|highway|railroad|expressway)\b",
            segment,
            flags=re.IGNORECASE,
        )[0]
        for lot in re.findall(r"\b\d{1,4}\b", segment):
            bbl = bbl_from_block_lot(borough_code, match.group(1), lot)
            if bbl and bbl not in bbls:
                bbls.append(bbl)

    return bbls


def extract_addresses_from_text(text):
    addresses = []
    for match in ADDRESS_PATTERN.finditer(clean_text(text)):
        address = normalize_address(match.group(0))
        if address and address not in addresses:
            addresses.append(address)
    return addresses


def selected_boundary_release(year, boundary_index):
    eligible = boundary_index[boundary_index["archive_year"] <= year].copy()
    boundary_relation = "nearest_prior_or_same_year"
    if eligible.empty:
        eligible = boundary_index.copy()
        boundary_relation = "earliest_available_post_year"
    max_year = eligible["archive_year"].max()
    release_row = eligible[eligible["archive_year"] == max_year].sort_values("release").tail(1).iloc[0]
    return str(release_row["release"]).lower(), int(release_row["archive_year"]), boundary_relation


@lru_cache(maxsize=None)
def read_boundary_release(release):
    df = pd.read_parquet(f"../input/dcp_council_boundary_archive_output/dcp_boundary_city_council_districts_archive_{release}.parquet")
    gdf = gpd.GeoDataFrame(
        df[["district_id", "coundist", "geometry_wkt"]].copy(),
        geometry=df["geometry_wkt"].map(wkt.loads),
        crs="EPSG:2263",
    )
    return gdf.rename(columns={"coundist": "computed_council_district"})[["computed_council_district", "geometry"]]


def districts_for_bbls(bbls, lot_gdf, boundary_gdf):
    if not bbls:
        return [], []
    matched = lot_gdf[lot_gdf["bbl"] .isin(bbls)].copy()
    if matched.empty:
        return [], []
    matched["geometry"] = matched.geometry.representative_point()
    joined = gpd.sjoin(
        matched[["bbl", "address", "geometry"]],
        boundary_gdf,
        how="left",
        predicate="within",
    )
    districts = sorted(
        {
            str(int(value))
            for value in joined["computed_council_district"].dropna().tolist()
            if 1 <= int(value) <= 51
        },
        key=int,
    )
    found_bbls = sorted(joined["bbl"].dropna().astype(str).unique().tolist())
    return districts, found_bbls


def districts_for_addresses(addresses, borough_code, lot_gdf, boundary_gdf):
    if not addresses:
        return [], [], []

    found_bbls = []
    matched_addresses = []
    for address in addresses:
        candidates = lot_gdf[lot_gdf["address_normalized"] == address]
        if borough_code is not None:
            candidates = candidates[candidates["boro_code"] == borough_code]
        if 0 < len(candidates) <= 5:
            matched_addresses.append(address)
            for bbl in candidates["bbl"].dropna().astype(str).tolist():
                if bbl not in found_bbls:
                    found_bbls.append(bbl)

    districts, geocoded_bbls = districts_for_bbls(found_bbls, lot_gdf, boundary_gdf)
    return districts, geocoded_bbls, matched_addresses


def classify_verification(claimed_districts, computed_districts, boundary_relation):
    claimed = set(claimed_districts)
    computed = set(computed_districts)

    if not computed:
        return "not_verified_no_deterministic_match"
    if boundary_relation == "earliest_available_post_year":
        if claimed == computed:
            return "tentative_match_pre_boundary_archive"
        if claimed & computed:
            return "tentative_partial_match_pre_boundary_archive"
        return "conflict_pre_boundary_archive"
    if claimed == computed:
        return "verified_exact_match"
    if claimed and claimed.issubset(computed):
        return "verified_claimed_subset_of_computed"
    if computed and computed.issubset(claimed):
        return "verified_computed_subset_of_claimed"
    if claimed & computed:
        return "partial_conflict"
    return "conflict"


candidate_df = pd.read_csv("../input/council_land_use_missing_geography_adjudication_ai_repair_candidates.csv", dtype=str).fillna("")
zap_bbl_df = pd.read_csv("../input/zap_bbl.csv", dtype=str).fillna("")
boundary_index = pd.read_csv("../input/dcp_council_boundary_archive_index.csv")
boundary_index["archive_year"] = pd.to_numeric(boundary_index["archive_year"], errors="coerce").astype(int)

lot_gdf = gpd.read_file(
    "zip://../input/nyc_mappluto_25v4_shp.zip",
    columns=["BBL", "Borough", "Block", "Lot", "Address", "Council"],
).rename(
    columns={
        "BBL": "bbl",
        "Borough": "borough_abbrev",
        "Block": "block",
        "Lot": "lot",
        "Address": "address",
        "Council": "current_council_district",
    }
)
lot_gdf["bbl"] = lot_gdf["bbl"].map(normalize_bbl)
lot_gdf["address_normalized"] = lot_gdf["address"].map(normalize_address)
lot_gdf["boro_code"] = lot_gdf["bbl"].str.slice(0, 1).replace("", pd.NA).astype("Int64")
lot_gdf = lot_gdf[lot_gdf["bbl"] != ""].copy()

zap_bbl_df["bbl"] = zap_bbl_df["bbl"].map(normalize_bbl)
zap_bbl_df["project_id_normalized"] = zap_bbl_df["project_id"].map(lambda value: clean_text(value).upper())
zap_bbl_df = zap_bbl_df[(zap_bbl_df["project_id_normalized"] != "") & (zap_bbl_df["bbl"] != "")].copy()

verification_rows = []
for _, row in candidate_df.iterrows():
    candidate_category = clean_text(row["adjudication_candidate_category"])
    vote_year = int(str(row["vote_date"])[:4]) if str(row["vote_date"])[:4].isdigit() else int(row["query_year"])
    release, boundary_year, boundary_relation = selected_boundary_release(vote_year, boundary_index)
    boundary_gdf = read_boundary_release(release)

    claimed_districts = split_districts(row["adjudication_affected_council_districts"])
    text_for_location = " ".join(
        [
            clean_text(row["adjudication_project_area"]),
            clean_text(row["adjudication_project_name"]),
            clean_text(row["title_examples"]),
            clean_text(row["adjudication_official_sources_used"]),
            clean_text(row["adjudication_source_check_summary"]),
            clean_text(row["adjudication_evidence_limitations"]),
        ]
    )
    borough_code = borough_code_from_text(row["adjudication_borough"], text_for_location)

    project_ids = [value.upper() for value in split_semicolon_values(row["zap_project_ids"])]
    zap_bbls = []
    if project_ids:
        matched_zap = zap_bbl_df[zap_bbl_df["project_id_normalized"].isin(project_ids)]
        zap_bbls = sorted(matched_zap["bbl"].dropna().unique().tolist())

    text_bbls = extract_bbls_from_text(text_for_location, borough_code)
    addresses = extract_addresses_from_text(text_for_location)

    source_rows = []
    for source_name, bbls in [("zap_bbl", zap_bbls), ("text_bbl_or_block_lot", text_bbls)]:
        districts, found_bbls = districts_for_bbls(bbls, lot_gdf, boundary_gdf)
        if bbls or districts:
            source_rows.append(
                {
                    "source_name": source_name,
                    "candidate_bbls": bbls,
                    "found_bbls": found_bbls,
                    "districts": districts,
                    "matched_addresses": [],
                }
            )

    address_districts, address_bbls, matched_addresses = districts_for_addresses(addresses, borough_code, lot_gdf, boundary_gdf)
    if addresses or address_districts:
        source_rows.append(
            {
                "source_name": "mappluto_address_exact",
                "candidate_bbls": [],
                "found_bbls": address_bbls,
                "districts": address_districts,
                "matched_addresses": matched_addresses,
            }
        )

    preferred_source = ""
    preferred_districts = []
    preferred_found_bbls = []
    preferred_matched_addresses = []
    for source_name in ["zap_bbl", "text_bbl_or_block_lot", "mappluto_address_exact"]:
        source_match = next((item for item in source_rows if item["source_name"] == source_name and item["districts"]), None)
        if source_match is not None:
            preferred_source = source_name
            preferred_districts = source_match["districts"]
            preferred_found_bbls = source_match["found_bbls"]
            preferred_matched_addresses = source_match["matched_addresses"]
            break

    verification_status = classify_verification(claimed_districts, preferred_districts, boundary_relation)
    if candidate_category not in TARGET_CATEGORIES:
        verification_status = "not_in_deterministic_verification_sample"

    source_summary = []
    for item in source_rows:
        source_summary.append(
            f"{item['source_name']}:districts={';'.join(item['districts'])};"
            f"bbls={';'.join(item['found_bbls'][:20])};"
            f"addresses={';'.join(item['matched_addresses'][:10])}"
        )

    verification_rows.append(
        {
            "signature_review_id": row["signature_review_id"],
            "query_year": row["query_year"],
            "vote_date": row["vote_date"],
            "adjudication_candidate_category": candidate_category,
            "adjudication_project_name": clean_text(row["adjudication_project_name"]),
            "claimed_council_districts": "; ".join(claimed_districts),
            "deterministic_council_districts": "; ".join(preferred_districts),
            "deterministic_verification_status": verification_status,
            "deterministic_verification_basis": preferred_source,
            "boundary_release": release.upper(),
            "boundary_year": str(boundary_year),
            "boundary_relation_to_vote_year": boundary_relation,
            "zap_project_ids": clean_text(row["zap_project_ids"]),
            "zap_bbl_count": str(len(zap_bbls)),
            "text_bbl_count": str(len(text_bbls)),
            "address_candidate_count": str(len(addresses)),
            "matched_bbl_count": str(len(preferred_found_bbls)),
            "matched_addresses": "; ".join(preferred_matched_addresses),
            "matched_bbls": "; ".join(preferred_found_bbls[:50]),
            "all_source_district_summaries": " | ".join(source_summary),
            "adjudication_project_area": clean_text(row["adjudication_project_area"]),
            "adjudication_source_check_summary": clean_text(row["adjudication_source_check_summary"]),
            "matter_files": clean_text(row["matter_files"]),
            "application_keys": clean_text(row["application_keys"]),
            "matter_urls": clean_text(row["matter_urls"]),
        }
    )

repair_statuses = {
    "verified_exact_match",
    "verified_claimed_subset_of_computed",
    "verified_computed_subset_of_claimed",
}
repair_rows = [
    {
        **row,
        "manual_verdict_status": "deterministically_verified_project_geography",
        "manual_verdict_districts": row["claimed_council_districts"],
        "manual_verdict_note": (
            f"AI-adjudicated geography matched deterministic {row['deterministic_verification_basis']} "
            f"assignment using DCP Council boundary release {row['boundary_release']}."
        ),
    }
    for row in verification_rows
    if row["deterministic_verification_status"] in repair_statuses
]

manual_queue_rows = [
    row
    for row in verification_rows
    if row["adjudication_candidate_category"] in TARGET_CATEGORIES
    and row["deterministic_verification_status"] not in repair_statuses
]

conflict_rows = [
    row
    for row in verification_rows
    if row["deterministic_verification_status"] in {"conflict", "partial_conflict"}
]
expected_conflict_ids = {row["signature_review_id"] for row in conflict_rows}
conflict_response_rows, conflict_response_errors = parse_jsonl_if_exists("chatgpt_conflict_review_responses.jsonl")
seen_conflict_ids = set()
duplicate_conflict_ids = set()
conflict_review_rows = []
conflict_review_errors = []
allowed_conflict_statuses = {
    "ai_claim_correct",
    "deterministic_claim_correct",
    "both_partly_correct",
    "neither_resolved",
    "not_enough_evidence",
}
allowed_conflict_confidence = {"high", "medium", "low"}
allowed_conflict_basis = {
    "official_district_field",
    "official_bbl_or_block_lot",
    "official_address_to_district",
    "project_area_interpretation",
    "insufficient",
}

verification_by_id = {row["signature_review_id"]: row for row in verification_rows}
for response in conflict_response_rows:
    signature_review_id = clean_text(response.get("signature_review_id", ""))
    errors = []
    if signature_review_id in seen_conflict_ids:
        duplicate_conflict_ids.add(signature_review_id)
        continue
    seen_conflict_ids.add(signature_review_id)
    if signature_review_id not in expected_conflict_ids:
        errors.append("signature_review_id_not_in_conflict_frame")
    if clean_text(response.get("conflict_review_status", "")) not in allowed_conflict_statuses:
        errors.append("invalid_conflict_review_status")
    if clean_text(response.get("confidence", "")) not in allowed_conflict_confidence:
        errors.append("invalid_confidence")
    if clean_text(response.get("evidence_basis", "")) not in allowed_conflict_basis:
        errors.append("invalid_evidence_basis")
    if clean_text(response.get("manual_followup_needed", "")) not in {"yes", "no"}:
        errors.append("invalid_manual_followup_needed")

    source_row = verification_by_id.get(signature_review_id, {})
    conflict_review_rows.append(
        {
            "signature_review_id": signature_review_id,
            "line_number": str(response.get("line_number", "")),
            "chatgpt_conflict_review_status": clean_text(response.get("conflict_review_status", "")),
            "chatgpt_accepted_council_districts": clean_text(response.get("accepted_council_districts", "")),
            "chatgpt_confidence": clean_text(response.get("confidence", "")),
            "chatgpt_evidence_basis": clean_text(response.get("evidence_basis", "")),
            "chatgpt_official_sources_checked": json.dumps(response.get("official_sources_checked", []), ensure_ascii=True),
            "chatgpt_short_explanation": clean_text(response.get("short_explanation", "")),
            "chatgpt_manual_followup_needed": clean_text(response.get("manual_followup_needed", "")),
            "original_claimed_council_districts": source_row.get("claimed_council_districts", ""),
            "deterministic_council_districts": source_row.get("deterministic_council_districts", ""),
            "deterministic_verification_status": source_row.get("deterministic_verification_status", ""),
            "deterministic_verification_basis": source_row.get("deterministic_verification_basis", ""),
            "validation_status": "fail" if errors else "pass",
            "validation_errors": "|".join(errors),
        }
    )
    if errors:
        conflict_review_errors.append(
            {
                "line_number": str(response.get("line_number", "")),
                "signature_review_id": signature_review_id,
                "validation_errors": "|".join(errors),
            }
        )

missing_conflict_ids = sorted(expected_conflict_ids - seen_conflict_ids)
for row in conflict_response_errors:
    conflict_review_errors.append(
        {
            "line_number": str(row.get("line_number", "")),
            "signature_review_id": row.get("signature_review_id", ""),
            "validation_errors": row.get("validation_errors", ""),
        }
    )

qc_rows = []
for category, group in pd.DataFrame(verification_rows).groupby("adjudication_candidate_category", dropna=False):
    qc_rows.append(
        {
            "metric": f"category_count:{category}",
            "value": str(len(group)),
            "status": "info",
            "detail": "Rows by AI adjudication category.",
        }
    )

for status, group in pd.DataFrame(verification_rows).groupby("deterministic_verification_status", dropna=False):
    qc_rows.append(
        {
            "metric": f"verification_status_count:{status}",
            "value": str(len(group)),
            "status": "info",
            "detail": "Rows by deterministic verification status.",
        }
    )

target_rows = [row for row in verification_rows if row["adjudication_candidate_category"] in TARGET_CATEGORIES]
qc_rows.extend(
    [
        {
            "metric": "target_category_count",
            "value": str(len(target_rows)),
            "status": "pass" if len(target_rows) == 82 else "warn",
            "detail": "Rows eligible for deterministic verification: 20 official, 57 official-location inferred, and 5 weaker spot-check rows.",
        },
        {
            "metric": "deterministically_verified_repair_count",
            "value": str(len(repair_rows)),
            "status": "info",
            "detail": "Rows where deterministic source districts agree with the AI-claimed districts under the strict repair-ready rule.",
        },
        {
            "metric": "manual_queue_count",
            "value": str(len(manual_queue_rows)),
            "status": "info",
            "detail": "Target rows still requiring human review after deterministic verification.",
        },
        {
            "metric": "chatgpt_conflict_review_expected_count",
            "value": str(len(expected_conflict_ids)),
            "status": "pass" if len(expected_conflict_ids) == 5 else "warn",
            "detail": "Rows with deterministic conflict or partial conflict sent to ChatGPT for adjudication.",
        },
        {
            "metric": "chatgpt_conflict_review_response_count",
            "value": str(len(conflict_review_rows)),
            "status": "pass" if len(conflict_review_rows) == len(expected_conflict_ids) else "warn",
            "detail": "Parsed ChatGPT conflict-review responses.",
        },
        {
            "metric": "chatgpt_conflict_review_missing_count",
            "value": str(len(missing_conflict_ids)),
            "status": "pass" if len(missing_conflict_ids) == 0 else "warn",
            "detail": "; ".join(missing_conflict_ids),
        },
        {
            "metric": "chatgpt_conflict_review_error_count",
            "value": str(len(conflict_review_errors)),
            "status": "pass" if len(conflict_review_errors) == 0 else "warn",
            "detail": "JSON parse or controlled-vocabulary validation errors in ChatGPT conflict-review responses.",
        },
        {
            "metric": "mappluto_lot_count",
            "value": str(len(lot_gdf)),
            "status": "pass" if len(lot_gdf) > 800000 else "warn",
            "detail": "Current MapPLUTO lots loaded from local DCP 25v4 shapefile.",
        },
        {
            "metric": "boundary_release_count",
            "value": str(len(boundary_index)),
            "status": "pass" if len(boundary_index) >= 60 else "warn",
            "detail": "DCP archived Council boundary releases available locally.",
        },
    ]
)

verification_fields = [
    "signature_review_id",
    "query_year",
    "vote_date",
    "adjudication_candidate_category",
    "adjudication_project_name",
    "claimed_council_districts",
    "deterministic_council_districts",
    "deterministic_verification_status",
    "deterministic_verification_basis",
    "boundary_release",
    "boundary_year",
    "boundary_relation_to_vote_year",
    "zap_project_ids",
    "zap_bbl_count",
    "text_bbl_count",
    "address_candidate_count",
    "matched_bbl_count",
    "matched_addresses",
    "matched_bbls",
    "all_source_district_summaries",
    "adjudication_project_area",
    "adjudication_source_check_summary",
    "matter_files",
    "application_keys",
    "matter_urls",
]

write_csv_if_changed(
    verification_rows,
    verification_fields,
    "../output/council_land_use_ai_geography_deterministic_verification.csv",
)
write_csv_if_changed(
    repair_rows,
    verification_fields + ["manual_verdict_status", "manual_verdict_districts", "manual_verdict_note"],
    "../output/council_land_use_ai_geography_deterministic_repairs.csv",
)
write_csv_if_changed(
    manual_queue_rows,
    verification_fields,
    "../output/council_land_use_ai_geography_deterministic_manual_queue.csv",
)
write_csv_if_changed(
    conflict_review_rows,
    [
        "signature_review_id",
        "line_number",
        "chatgpt_conflict_review_status",
        "chatgpt_accepted_council_districts",
        "chatgpt_confidence",
        "chatgpt_evidence_basis",
        "chatgpt_official_sources_checked",
        "chatgpt_short_explanation",
        "chatgpt_manual_followup_needed",
        "original_claimed_council_districts",
        "deterministic_council_districts",
        "deterministic_verification_status",
        "deterministic_verification_basis",
        "validation_status",
        "validation_errors",
    ],
    "../output/council_land_use_ai_geography_conflict_chatgpt_review.csv",
)
write_csv_if_changed(
    conflict_review_errors,
    ["line_number", "signature_review_id", "validation_errors"],
    "../output/council_land_use_ai_geography_conflict_chatgpt_review_errors.csv",
)
write_csv_if_changed(
    qc_rows,
    ["metric", "value", "status", "detail"],
    "../output/council_land_use_ai_geography_deterministic_qc.csv",
)
