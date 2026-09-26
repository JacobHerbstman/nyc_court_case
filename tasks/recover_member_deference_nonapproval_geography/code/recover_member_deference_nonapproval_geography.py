from __future__ import annotations

import re
import sys

import pandas as pd

sys.path.append("../../_lib")
from member_deference_utils import (
    application_keys,
    borough_code_from_text,
    collapse_districts,
    district_from_scalar,
    lot_numbers_from_text,
    normalize_space,
    save_frame,
    split_semicolon,
)

ordinal_words = {
    "FIRST": "1",
    "SECOND": "2",
    "THIRD": "3",
    "FOURTH": "4",
    "FIFTH": "5",
    "SIXTH": "6",
    "SEVENTH": "7",
    "EIGHTH": "8",
    "NINTH": "9",
    "TENTH": "10",
    "ELEVENTH": "11",
    "TWELFTH": "12",
}

def title_bbls(title: object, matter_application_keys: object) -> tuple[list[str], str, str]:
    borough_code, borough_source = borough_code_from_text(title, matter_application_keys)
    if borough_code == "":
        return [], "", ""

    text = normalize_space(title).replace("–", "-").replace("—", "-")
    bbls = []
    for match in re.finditer(
        r"Block\s+(\d{1,5})\s*(?:/|,|\s+)\s*Lots?\s+(.+?)(?=[);.]|,\s*(?:Manhattan|Brooklyn|Queens|Bronx|Staten Island)\b|$)",
        text,
        flags=re.IGNORECASE,
    ):
        block = int(match.group(1))
        for lot in lot_numbers_from_text(match.group(2), max_range=200):
            bbls.append(f"{int(borough_code)}{block:05d}{lot:04d}")

    for match in re.findall(r"\b[1-5]\d{9}\b", text):
        bbls.append(match)

    return list(dict.fromkeys(bbls)), borough_code, borough_source


def normalize_address(value: object) -> str:
    text = normalize_space(value).upper()
    text = re.sub(
        r",?\s*BOROUGH OF\s+(MANHATTAN|BROOKLYN|QUEENS|THE BRONX|BRONX|STATEN ISLAND)\b",
        " ",
        text,
    )
    text = re.sub(r"\b(\d+)(ST|ND|RD|TH)\b", r"\1", text)
    for word, number in ordinal_words.items():
        text = re.sub(rf"\b{word}\b", number, text)
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def title_address_candidate(title: object) -> str:
    text = normalize_space(title)
    match = re.search(
        r"located at\s+(.+?)(?:\s*\(|,\s*(?:Borough of\s+)?(?:Manhattan|Brooklyn|Queens|the Bronx|Bronx|Staten Island)\b|\.\s*$)",
        text,
        flags=re.IGNORECASE,
    )
    if match is None:
        return ""

    candidate = match.group(1).strip(" ,")
    if re.search(
        r"\b(between|from| to | and |through|properties|parcels|sites|blocks?|a\.k\.a\.)\b|/",
        candidate,
        flags=re.IGNORECASE,
    ):
        return ""
    return candidate


def address_variants(value: object) -> list[str]:
    candidate = normalize_space(value)
    if candidate == "":
        return []

    candidate = re.sub(
        r",?\s*Borough of\s+(Manhattan|Brooklyn|Queens|the Bronx|Bronx|Staten Island)\b",
        "",
        candidate,
        flags=re.IGNORECASE,
    ).strip(" ,")

    variants = [candidate]
    match = re.match(r"^(\d{1,5})-(\d{1,5})\s+(.+)$", candidate)
    if match is not None:
        start = int(match.group(1))
        end = int(match.group(2))
        street = match.group(3)
        if start <= end and end - start <= 40:
            step = 2 if start % 2 == end % 2 else 1
            variants.extend(f"{number} {street}" for number in range(start, end + 1, step))

    return list(dict.fromkeys(variants))

queue = pd.read_csv("../input/member_deference_final_action_vote_queue.csv", dtype=str, keep_default_na=False)
zap_project_data = pd.read_parquet("../input/zap_project_data.parquet")
zap_project_bbl = pd.read_parquet("../input/zap_project_bbl.parquet")
mappluto = pd.read_parquet(
    "../input/mappluto_current_lot_lookup.parquet",
    columns=["bbl", "borough", "address", "council"],
)

target_queue = queue[queue["fetch_vote_detail_first_pass"].str.lower().eq("true")].copy()
target_queue = target_queue.sort_values(["query_year", "matter_file", "matter_id"]).reset_index(drop=True)

if target_queue.empty:
    raise RuntimeError("First-pass nonapproval list is empty.")
if target_queue["matter_id"].duplicated().any():
    raise RuntimeError("First-pass nonapproval list must be unique by matter_id.")
if zap_project_data["project_id"].astype(str).duplicated().any():
    raise RuntimeError("Staged ZAP project data must be unique by project_id.")

zap_project_bbl = (
    zap_project_bbl.assign(
        project_id=zap_project_bbl["project_id"].astype(str),
        bbl_standardized=zap_project_bbl["bbl_standardized"].astype(str),
    )
    .loc[:, ["project_id", "bbl_standardized"]]
    .query("project_id != '' and bbl_standardized != ''")
    .drop_duplicates()
)
if zap_project_bbl.duplicated(["project_id", "bbl_standardized"]).any():
    raise RuntimeError("Staged ZAP project-BBL rows must be unique by project_id and BBL after deduplication.")

mappluto["bbl"] = mappluto["bbl"].astype(str)
mappluto["borough"] = mappluto["borough"].astype(str)
mappluto["mappluto_council_district"] = mappluto["council"].map(district_from_scalar)
mappluto["address_normalized"] = mappluto["address"].map(normalize_address)

mappluto_bbl_council = (
    mappluto.groupby("bbl", as_index=False)
    .agg(mappluto_council_districts=("mappluto_council_district", collapse_districts))
)
if mappluto_bbl_council["bbl"].duplicated().any():
    raise RuntimeError("MapPLUTO BBL lookup must be unique by BBL.")

mappluto_address_council = (
    mappluto[(mappluto["address_normalized"] != "") & (mappluto["mappluto_council_district"] != "")]
    .groupby(["borough", "address_normalized"], as_index=False)
    .agg(title_address_current_mappluto_council_districts=("mappluto_council_district", collapse_districts))
)
mappluto_address_council["unique_single_district_address_match"] = mappluto_address_council[
    "title_address_current_mappluto_council_districts"
].map(lambda x: len(split_semicolon(x)) == 1)
mappluto_address_council = mappluto_address_council[
    mappluto_address_council["unique_single_district_address_match"]
].drop(columns=["unique_single_district_address_match"])
if mappluto_address_council.duplicated(["borough", "address_normalized"]).any():
    raise RuntimeError("MapPLUTO exact address lookup must be unique by borough and normalized address.")

zap_application_rows = []
for row in zap_project_data[["project_id", "ulurp_numbers", "cc_district"]].to_dict("records"):
    for key in application_keys(row["ulurp_numbers"]):
        zap_application_rows.append(
            {
                "application_key": key,
                "project_id": str(row["project_id"]),
                "zap_project_cc_districts": district_from_scalar(row["cc_district"]),
            }
        )

zap_application_project = pd.DataFrame(zap_application_rows)
if zap_application_project.empty:
    raise RuntimeError("No application keys were parsed from staged ZAP project data.")
zap_application_project = zap_application_project.drop_duplicates()

zap_project_bbl_council = (
    zap_project_bbl.merge(
        mappluto_bbl_council[["bbl", "mappluto_council_districts"]],
        left_on="bbl_standardized",
        right_on="bbl",
        how="left",
        validate="many_to_one",
    )
    .groupby("project_id", as_index=False)
    .agg(zap_project_bbl_current_mappluto_council_districts=("mappluto_council_districts", collapse_districts))
)
if zap_project_bbl_council["project_id"].duplicated().any():
    raise RuntimeError("ZAP project-BBL Council lookup must be unique by project_id.")

zap_application_project = zap_application_project.merge(
    zap_project_bbl_council,
    on="project_id",
    how="left",
    validate="many_to_one",
)
zap_application_crosswalk = (
    zap_application_project.groupby("application_key", as_index=False)
    .agg(
        zap_project_cc_districts=("zap_project_cc_districts", collapse_districts),
        zap_project_bbl_current_mappluto_council_districts=(
            "zap_project_bbl_current_mappluto_council_districts",
            collapse_districts,
        ),
    )
)

matter_application = pd.DataFrame(
    [
        {"matter_id": row["matter_id"], "application_key": key}
        for row in target_queue[["matter_id", "application_keys"]].to_dict("records")
        for key in split_semicolon(row["application_keys"])
    ],
    columns=["matter_id", "application_key"],
)
matter_application_crosswalk = (
    matter_application.merge(zap_application_crosswalk, on="application_key", how="left", validate="many_to_one")
    .groupby("matter_id", as_index=False)
    .agg(
        zap_project_cc_districts=("zap_project_cc_districts", collapse_districts),
        zap_project_bbl_current_mappluto_council_districts=(
            "zap_project_bbl_current_mappluto_council_districts",
            collapse_districts,
        ),
    )
)

title_location_rows = []
for row in target_queue[["matter_id", "matter_file", "application_keys", "title"]].to_dict("records"):
    parsed_bbls, title_borough_code, _ = title_bbls(row["title"], row["application_keys"])
    title_location_rows.append(
        {
            "matter_id": row["matter_id"],
            "matter_file": row["matter_file"],
            "title_borough_code_for_location_parse": title_borough_code,
            "title_bbls": "; ".join(parsed_bbls),
            "title_address_candidate": title_address_candidate(row["title"]),
        }
    )

title_location = pd.DataFrame(title_location_rows)
title_location["title_address_normalized"] = title_location["title_address_candidate"].map(normalize_address)

title_address_variants = pd.DataFrame(
    [
        {
            "matter_id": row["matter_id"],
            "title_borough_code_for_location_parse": row["title_borough_code_for_location_parse"],
            "title_address_variant_normalized": normalize_address(variant),
        }
        for row in title_location.to_dict("records")
        for variant in address_variants(row["title_address_candidate"])
    ],
    columns=["matter_id", "title_borough_code_for_location_parse", "title_address_variant_normalized"],
)

title_bbl_long = (
    title_location.assign(title_bbl=title_location["title_bbls"].str.split("; "))
    .explode("title_bbl")
    .loc[:, ["matter_id", "title_bbl"]]
)
title_bbl_long = title_bbl_long[title_bbl_long["title_bbl"].fillna("") != ""].drop_duplicates()
title_bbl_summary = (
    title_bbl_long.merge(
        mappluto_bbl_council[["bbl", "mappluto_council_districts"]],
        left_on="title_bbl",
        right_on="bbl",
        how="left",
        validate="many_to_one",
    )
    .groupby("matter_id", as_index=False)
    .agg(title_bbl_current_mappluto_council_districts=("mappluto_council_districts", collapse_districts))
)

title_location = title_location.merge(title_bbl_summary, on="matter_id", how="left", validate="one_to_one")
title_location = title_location.merge(
    mappluto_address_council,
    left_on=["title_borough_code_for_location_parse", "title_address_normalized"],
    right_on=["borough", "address_normalized"],
    how="left",
    validate="many_to_one",
)
title_location = title_location.drop(columns=["borough", "address_normalized"])

title_address_variant_summary = (
    title_address_variants.merge(
        mappluto_address_council,
        left_on=["title_borough_code_for_location_parse", "title_address_variant_normalized"],
        right_on=["borough", "address_normalized"],
        how="left",
        validate="many_to_one",
    )
    .groupby("matter_id", as_index=False)
    .agg(
        title_address_variant_current_mappluto_council_districts=(
            "title_address_current_mappluto_council_districts",
            collapse_districts,
        )
    )
)
title_location = title_location.merge(title_address_variant_summary, on="matter_id", how="left", validate="one_to_one")

recovery = target_queue.merge(matter_application_crosswalk, on="matter_id", how="left", validate="one_to_one")
recovery = recovery.merge(title_location, on=["matter_id", "matter_file"], how="left", validate="one_to_one")
for col in [
    "zap_project_cc_districts",
    "zap_project_bbl_current_mappluto_council_districts",
    "title_bbls",
    "title_bbl_current_mappluto_council_districts",
    "title_address_current_mappluto_council_districts",
    "title_address_variant_current_mappluto_council_districts",
]:
    recovery[col] = recovery[col].fillna("")


def recovered_districts(row: pd.Series) -> tuple[str, str, str, str]:
    if row["affected_council_districts"] != "":
        return row["affected_council_districts"], "legistar_existing", "official", "legistar"
    if row["zap_project_cc_districts"] != "":
        return row["zap_project_cc_districts"], "zap_application_project_cc_district", "official_crosswalk", "zap_project_data"
    if row["zap_project_bbl_current_mappluto_council_districts"] != "":
        return (
            row["zap_project_bbl_current_mappluto_council_districts"],
            "zap_application_project_bbl_current_mappluto",
            "current_geography_backup",
            "zap_bbl_to_current_mappluto_25v4",
        )
    if row["title_bbl_current_mappluto_council_districts"] != "":
        return (
            row["title_bbl_current_mappluto_council_districts"],
            "title_bbl_current_mappluto",
            "current_geography_backup",
            "title_bbl_to_current_mappluto_25v4",
        )
    if row["title_address_current_mappluto_council_districts"] != "":
        return (
            row["title_address_current_mappluto_council_districts"],
            "title_address_exact_current_mappluto",
            "current_geography_backup_fragile",
            "title_address_exact_to_current_mappluto_25v4",
        )
    if row["title_address_variant_current_mappluto_council_districts"] != "":
        return (
            row["title_address_variant_current_mappluto_council_districts"],
            "title_address_variant_current_mappluto",
            "current_geography_backup_fragile",
            "title_address_variant_to_current_mappluto_25v4",
        )
    return "", "unresolved", "unresolved", ""


recovered_columns = recovery.apply(recovered_districts, axis=1, result_type="expand")
recovery["recovered_affected_council_districts"] = recovered_columns[0]
recovery["geography_recovery_method"] = recovered_columns[1]
recovery["geography_recovery_confidence"] = recovered_columns[2]
recovery["geography_recovery_source"] = recovered_columns[3]
recovery["original_affected_district_missing"] = recovery["affected_council_districts"] == ""
recovery["recovered_affected_district_missing"] = recovery["recovered_affected_council_districts"] == ""
recovery["recovered_district_count"] = recovery["recovered_affected_council_districts"].map(lambda x: len(split_semicolon(x)))

recovery = recovery[
    [
        "query_year",
        "matter_id",
        "matter_file",
        "matter_status",
        "disposition_group",
        "final_history_date",
        "final_history_action",
        "affected_council_districts",
        "affected_district_source",
        "recovered_affected_council_districts",
        "geography_recovery_method",
        "geography_recovery_confidence",
        "geography_recovery_source",
        "original_affected_district_missing",
        "recovered_affected_district_missing",
        "recovered_district_count",
        "application_keys",
        "title_bbls",
        "title",
    ]
]

if len(recovery) != len(target_queue):
    raise RuntimeError("Recovery output must keep every first-pass nonapproval matter.")
if recovery["matter_id"].duplicated().any():
    raise RuntimeError("Recovery output must be unique by matter_id.")

save_frame(recovery, "../output/member_deference_nonapproval_geography_recovery.csv", ["matter_id"])
