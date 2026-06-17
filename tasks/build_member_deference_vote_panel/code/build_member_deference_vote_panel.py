# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_member_deference_vote_panel/code")

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


RECALL_YEARS = list(range(1998, 2026))


def normalize_space(value: object) -> str:
    return re.sub(r"\s+", " ", "" if pd.isna(value) else str(value)).strip()


def norm_name(value: object) -> str:
    value = re.sub(r"[^A-Za-z0-9 ]", " ", "" if pd.isna(value) else str(value))
    return re.sub(r"\s+", " ", value).strip().lower()


def edge_name(value: object) -> str:
    parts = norm_name(value).split()
    if len(parts) < 2:
        return norm_name(value)
    return f"{parts[0]} {parts[-1]}"


def split_semicolon(value: object) -> list[str]:
    if pd.isna(value) or str(value).strip() == "":
        return []
    return [part.strip() for part in str(value).split(";") if part.strip()]


def collapse_values(values: object) -> str:
    clean_values = []
    for value in values:
        if pd.isna(value) or str(value).strip() == "":
            continue
        if str(value) not in clean_values:
            clean_values.append(str(value))
    return "; ".join(clean_values)


def collapse_int_strings(values: object) -> str:
    clean_values = []
    for value in values:
        if pd.isna(value) or str(value).strip() == "":
            continue
        for match in re.findall(r"\d{1,2}", str(value)):
            district = str(int(match))
            if district not in clean_values and 1 <= int(district) <= 51:
                clean_values.append(district)
    return "; ".join(clean_values)


district_patterns = [
    re.compile(
        r"Council District(?:s)?(?:\s*(?:No\.?|Nos\.?|no\.?|nos\.?))?\s*([0-9,\sand-]+)",
        flags=re.IGNORECASE,
    ),
    re.compile(r"\bCD'?s?\.?\s*([0-9,\sand-]+)", flags=re.IGNORECASE),
]
application_re = re.compile(
    r"\b(?:[CNM]\s*)?\d{6,8}\s*(?:\([A-Z0-9]+\)\s*)?[A-Z]{2,4}\b",
    flags=re.IGNORECASE,
)


def districts_from_text(value: object) -> list[str]:
    text = "" if pd.isna(value) else str(value)
    districts = []
    for pattern in district_patterns:
        for match in pattern.finditer(text):
            districts.extend(re.findall(r"\d{1,2}", match.group(1)))
    return [district for district in dict.fromkeys(districts) if 1 <= int(district) <= 51]


def application_keys(value: object) -> list[str]:
    text = "" if pd.isna(value) else str(value)
    keys = []
    for match in application_re.finditer(text):
        key = re.sub(r"[^A-Za-z0-9]", "", match.group(0)).upper()
        key = re.sub(r"^[CNM](?=\d)", "", key)
        keys.append(key)
    return list(dict.fromkeys(keys))


def disposition_group(status: object, final_action: object, title: object) -> str:
    status_text = normalize_space(status).lower()
    action_text = normalize_space(final_action).lower()
    title_text = normalize_space(title).lower()

    if status_text == "adopted":
        return "adopted"
    if status_text == "disapproved":
        return "disapproved"
    if status_text == "withdrawn":
        return "withdrawn"
    if "filed" in status_text:
        if "end of session" in status_text or "end of session" in action_text:
            return "filed_end_of_session"
        if re.search(r"withdrawal|motion\s+to\s+file", title_text):
            return "filed_withdrawal_or_motion"
        if "filed by council" in action_text:
            return "filed_by_council_other"
        if "filed by committee" in action_text or "filed by subcommittee" in action_text:
            return "filed_by_committee_or_subcommittee"
        return "filed_other"
    return "other_status"


def filed_age_group(status: object, query_year: object, matter_file_year: object) -> str:
    if "filed" not in normalize_space(status).lower():
        return ""

    query_year_num = pd.to_numeric(pd.Series([query_year]), errors="coerce").iloc[0]
    matter_year_num = pd.to_numeric(pd.Series([matter_file_year]), errors="coerce").iloc[0]
    if pd.isna(query_year_num) or pd.isna(matter_year_num) or matter_year_num < 1900 or matter_year_num > 2100:
        return "filed_unknown_matter_year"
    if int(query_year_num) == int(matter_year_num):
        return "filed_same_year_matter"
    if int(query_year_num) > int(matter_year_num):
        return "filed_older_matter"
    return "filed_future_matter_year"


def read_year_stack(file_suffix: str) -> pd.DataFrame:
    rows = []
    for year in RECALL_YEARS:
        df = pd.read_csv(f"../input/legistar_{year}_broad_recall_{file_suffix}.csv", dtype=str, keep_default_na=False)
        df["query_year_int"] = year
        rows.append(df)
    return pd.concat(rows, ignore_index=True)


def write_csv(path: str, df: pd.DataFrame) -> None:
    new_path = Path(path)
    temp_path = new_path.with_suffix(new_path.suffix + ".tmp")
    df.to_csv(temp_path, index=False)
    temp_path.replace(new_path)


def repair_lookup_key(query_year: object, date_value: object, matter_file: object) -> tuple[str, str, str] | None:
    query_year_num = pd.to_numeric(pd.Series([query_year]), errors="coerce").iloc[0]
    date_parsed = pd.to_datetime(date_value, errors="coerce")
    matter_file_clean = normalize_space(matter_file)
    if pd.isna(query_year_num) or pd.isna(date_parsed) or matter_file_clean == "":
        return None
    return (str(int(query_year_num)), date_parsed.strftime("%Y-%m-%d"), matter_file_clean)


action_details = read_year_stack("action_details")
member_votes = read_year_stack("member_votes")
matter_index = read_year_stack("matter_index")
history_events = read_year_stack("history_events")
roster = pd.read_csv("../input/council_member_roster_master.csv", dtype=str, keep_default_na=False)
zap_projects = pd.read_parquet("../input/zap_project_data.parquet")
ai_geo_repairs = pd.read_csv("../input/council_land_use_ai_geography_accepted_repairs.csv", dtype=str, keep_default_na=False)

if action_details["matter_id"].duplicated().any():
    raise RuntimeError("Legistar action-detail rows must be unique by matter_id.")
if matter_index["matter_id"].duplicated().any():
    raise RuntimeError("Legistar matter-index rows must be unique by matter_id.")
if not set(member_votes["matter_id"]).issubset(set(action_details["matter_id"])):
    raise RuntimeError("Every member-vote matter_id must appear in action details.")
if not set(action_details["matter_id"]).issubset(set(matter_index["matter_id"])):
    raise RuntimeError("Every action-detail matter_id must appear in the matter index.")
if zap_projects["project_id"].astype(str).duplicated().any():
    raise RuntimeError("Staged ZAP project data must be unique by project_id.")
if ai_geo_repairs.duplicated(["query_year", "vote_date", "matter_file"]).any():
    raise RuntimeError("Accepted AI geography repairs must be unique by query_year, vote_date, matter_file.")
if ai_geo_repairs["accepted_council_districts"].map(lambda x: len(split_semicolon(x)) == 0).any():
    raise RuntimeError("Accepted AI geography repairs must have nonempty district assignments.")

ai_geo_repair_lookup = {}
for row in ai_geo_repairs.to_dict("records"):
    key = repair_lookup_key(row["query_year"], row["vote_date"], row["matter_file"])
    if key is None:
        raise RuntimeError("Accepted AI geography repair has an unusable lookup key.")
    ai_geo_repair_lookup[key] = row

matter_index_join = matter_index[
    [
        "matter_id",
        "matter_type",
        "status",
        "committee",
        "title",
        "borough",
        "affected_council_districts",
        "application_numbers_in_title",
        "land_use_recall_reason",
    ]
].rename(
    columns={
        "matter_type": "matter_index_type",
        "status": "matter_index_status",
        "committee": "matter_index_committee",
        "title": "matter_index_title",
        "borough": "matter_index_borough",
        "affected_council_districts": "matter_index_affected_council_districts",
        "application_numbers_in_title": "matter_index_application_numbers",
        "land_use_recall_reason": "matter_index_land_use_recall_reason",
    }
)
action_details = action_details.merge(matter_index_join, on="matter_id", how="left")
action_details["vote_date"] = pd.to_datetime(action_details["history_date"], errors="coerce")
action_details["text_for_parse"] = (
    action_details["matter_index_title"].fillna("")
    + " "
    + action_details["matter_index_application_numbers"].fillna("")
    + " "
    + action_details["action_detail_title"].fillna("")
    + " "
    + action_details["action_detail_text"].fillna("")
    + " "
    + action_details["agenda_note"].fillna("")
    + " "
    + action_details["minutes_note"].fillna("")
)
action_details["legistar_text_districts"] = action_details["text_for_parse"].map(
    lambda x: collapse_values(districts_from_text(x))
)
action_details["matter_index_districts"] = action_details["matter_index_affected_council_districts"].map(
    lambda x: collapse_int_strings(split_semicolon(x))
)
action_details["application_keys"] = action_details["text_for_parse"].map(lambda x: collapse_values(application_keys(x)))

zap_projects["project_id"] = zap_projects["project_id"].astype(str)
zap_projects["zap_text_for_app_key"] = zap_projects["ulurp_numbers"].fillna("").astype(str)
zap_app_rows = []
for row in zap_projects[
    [
        "project_id",
        "project_name",
        "ulurp_numbers",
        "cc_district",
        "council_district_first",
        "project_reference_year",
    ]
].fillna("").to_dict("records"):
    for key in application_keys(row["ulurp_numbers"]):
        zap_app_rows.append(
            {
                "application_key": key,
                "zap_project_id": row["project_id"],
                "zap_project_name": normalize_space(row["project_name"]),
                "zap_cc_districts": collapse_int_strings([row["cc_district"], row["council_district_first"]]),
                "zap_project_reference_year": row["project_reference_year"],
            }
        )
zap_app_key = pd.DataFrame(zap_app_rows)
if zap_app_key.empty:
    raise RuntimeError("No ZAP application keys were parsed.")

zap_app_key_base = (
    zap_app_key.sort_values(["application_key", "zap_project_id"])
    .groupby("application_key", as_index=False)
    .agg(
        zap_project_ids=("zap_project_id", collapse_values),
        zap_project_names=("zap_project_name", collapse_values),
        zap_cc_districts=("zap_cc_districts", collapse_int_strings),
        zap_project_reference_years=("zap_project_reference_year", collapse_values),
        zap_project_count=("zap_project_id", "nunique"),
    )
)

matter_app_rows = []
for row in action_details[["matter_id", "application_keys"]].to_dict("records"):
    for key in split_semicolon(row["application_keys"]):
        matter_app_rows.append({"matter_id": row["matter_id"], "application_key": key})
matter_app_key = pd.DataFrame(matter_app_rows)

if matter_app_key.empty:
    matter_zap = pd.DataFrame(
        columns=[
            "matter_id",
            "zap_matched_application_keys",
            "zap_project_ids",
            "zap_project_names",
            "zap_cc_districts",
            "zap_project_reference_years",
            "zap_project_count",
        ]
    )
else:
    matter_zap_long = matter_app_key.merge(zap_app_key_base, on="application_key", how="left")
    matter_zap = (
        matter_zap_long[matter_zap_long["zap_project_ids"].notna()]
        .sort_values(["matter_id", "application_key"])
        .groupby("matter_id", as_index=False)
        .agg(
            zap_matched_application_keys=("application_key", collapse_values),
            zap_project_ids=("zap_project_ids", collapse_values),
            zap_project_names=("zap_project_names", collapse_values),
            zap_cc_districts=("zap_cc_districts", collapse_int_strings),
            zap_project_reference_years=("zap_project_reference_years", collapse_values),
            zap_project_count=("zap_project_count", "sum"),
        )
    )

history_events["history_date_parsed"] = pd.to_datetime(history_events["history_date"], errors="coerce")
history_events["history_sequence_int"] = pd.to_numeric(history_events["history_sequence"], errors="coerce")
final_history = (
    history_events.sort_values(["matter_id", "history_date_parsed", "history_sequence_int"])
    .drop_duplicates("matter_id", keep="last")
    [
        [
            "matter_id",
            "history_date",
            "history_action_by",
            "history_action",
            "history_result",
            "history_detail_url",
        ]
    ]
    .rename(
        columns={
            "history_date": "final_history_date",
            "history_action_by": "final_history_action_by",
            "history_action": "final_history_action",
            "history_result": "final_history_result",
            "history_detail_url": "final_history_detail_url",
        }
    )
)

city_council_history = history_events[history_events["history_action_by"] == "City Council"].copy()
city_council_history["approved_action_flag"] = city_council_history["history_action"].str.contains(
    r"approved.*council|approved,\s*by council",
    case=False,
    na=False,
    regex=True,
)
city_council_history["disapproved_action_flag"] = city_council_history["history_action"].str.contains(
    "disapproved by council",
    case=False,
    na=False,
)
city_council_history["filed_action_flag"] = city_council_history["history_action"].str.contains(
    "filed",
    case=False,
    na=False,
)
city_council_history["modified_action_flag"] = city_council_history["history_action"].str.contains(
    "modification|modifications",
    case=False,
    na=False,
)
city_council_summary = (
    city_council_history.groupby("matter_id", as_index=False)
    .agg(
        council_history_action_count=("history_action", "size"),
        council_approved_action_count=("approved_action_flag", "sum"),
        council_disapproved_action_count=("disapproved_action_flag", "sum"),
        council_filed_action_count=("filed_action_flag", "sum"),
        council_modified_action_count=("modified_action_flag", "sum"),
    )
)

matter_universe_base = matter_index[
    matter_index["land_use_recall_flag"].astype(str).str.lower().eq("true")
].copy()
matter_universe_base["matter_text_for_parse"] = (
    matter_universe_base["title"].fillna("")
    + " "
    + matter_universe_base["application_numbers_in_title"].fillna("")
)
matter_universe_base["matter_index_districts"] = matter_universe_base["affected_council_districts"].map(
    lambda x: collapse_int_strings(split_semicolon(x))
)
matter_universe_base["legistar_text_districts"] = matter_universe_base["matter_text_for_parse"].map(
    lambda x: collapse_values(districts_from_text(x))
)
matter_universe_base["application_keys"] = matter_universe_base["matter_text_for_parse"].map(
    lambda x: collapse_values(application_keys(x))
)

universe_app_rows = []
for row in matter_universe_base[["matter_id", "application_keys"]].to_dict("records"):
    for key in split_semicolon(row["application_keys"]):
        universe_app_rows.append({"matter_id": row["matter_id"], "application_key": key})
universe_app_key = pd.DataFrame(universe_app_rows)

if universe_app_key.empty:
    universe_matter_zap = pd.DataFrame(
        columns=[
            "matter_id",
            "zap_matched_application_keys",
            "zap_project_ids",
            "zap_project_names",
            "zap_cc_districts",
            "zap_project_reference_years",
            "zap_project_count",
        ]
    )
else:
    universe_zap_long = universe_app_key.merge(zap_app_key_base, on="application_key", how="left")
    universe_matter_zap = (
        universe_zap_long[universe_zap_long["zap_project_ids"].notna()]
        .sort_values(["matter_id", "application_key"])
        .groupby("matter_id", as_index=False)
        .agg(
            zap_matched_application_keys=("application_key", collapse_values),
            zap_project_ids=("zap_project_ids", collapse_values),
            zap_project_names=("zap_project_names", collapse_values),
            zap_cc_districts=("zap_cc_districts", collapse_int_strings),
            zap_project_reference_years=("zap_project_reference_years", collapse_values),
            zap_project_count=("zap_project_count", "sum"),
        )
    )

matter_universe_base = (
    matter_universe_base.merge(final_history, on="matter_id", how="left")
    .merge(city_council_summary, on="matter_id", how="left")
    .merge(universe_matter_zap, on="matter_id", how="left")
)
for column in [
    "council_history_action_count",
    "council_approved_action_count",
    "council_disapproved_action_count",
    "council_filed_action_count",
    "council_modified_action_count",
]:
    matter_universe_base[column] = matter_universe_base[column].fillna(0).astype(int)

vote_lookup: dict[str, dict[str, str]] = {}
for row in member_votes.to_dict("records"):
    matter_id = row["matter_id"]
    vote_lookup.setdefault(matter_id, {})
    person_name = row.get("person_name", "")
    vote_value = row.get("vote", "")
    vote_lookup[matter_id][norm_name(person_name)] = vote_value
    vote_lookup[matter_id][edge_name(person_name)] = vote_value

roster["term_start_date_parsed"] = pd.to_datetime(roster["term_start_date"], errors="coerce")
roster["term_end_date_parsed"] = pd.to_datetime(roster["term_end_date"], errors="coerce").fillna(
    pd.Timestamp("2100-01-01")
)
roster["member_name_norm"] = roster["member_name"].map(norm_name)
roster["member_name_edge"] = roster["member_name"].map(edge_name)
roster = roster[roster["member_name_norm"] != "vacant"].copy()

matter_universe_rows = []
matter_universe_ai_geo_repair_keys_used = set()
for row in matter_universe_base.sort_values(["query_year_int", "matter_file"]).to_dict("records"):
    matter_index_districts = split_semicolon(row.get("matter_index_districts", ""))
    legistar_districts = split_semicolon(row.get("legistar_text_districts", ""))
    zap_districts = split_semicolon(row.get("zap_cc_districts", ""))
    final_date = pd.to_datetime(row.get("final_history_date", ""), errors="coerce")
    ai_geo_repair_key = repair_lookup_key(row.get("query_year_int", ""), final_date, row.get("matter_file", ""))
    ai_geo_repair = ai_geo_repair_lookup.get(ai_geo_repair_key, {}) if ai_geo_repair_key is not None else {}
    ai_geo_repair_districts = split_semicolon(ai_geo_repair.get("accepted_council_districts", ""))

    if matter_index_districts:
        affected_districts = matter_index_districts
        affected_district_source = "legistar_matter_index"
    elif legistar_districts:
        affected_districts = legistar_districts
        affected_district_source = "legistar_text"
    elif zap_districts:
        affected_districts = zap_districts
        affected_district_source = "zap_application_key"
    elif ai_geo_repair_districts:
        affected_districts = ai_geo_repair_districts
        affected_district_source = "ai_geography_repair"
        matter_universe_ai_geo_repair_keys_used.add(ai_geo_repair_key)
    else:
        affected_districts = []
        affected_district_source = "missing"

    local_members = []
    missing_roster_districts = []
    if not pd.isna(final_date):
        for district in affected_districts:
            matches = roster[
                (roster["district"].astype(str) == str(int(district)))
                & (roster["term_start_date_parsed"] <= final_date)
                & (final_date <= roster["term_end_date_parsed"])
            ]
            if matches.empty:
                missing_roster_districts.append(str(district))
                continue
            local_members.extend(matches["member_name"].tolist())

    matter_file_year_num = pd.to_numeric(pd.Series([row.get("matter_file_year", "")]), errors="coerce").iloc[0]
    query_year_num = pd.to_numeric(pd.Series([row.get("query_year_int", "")]), errors="coerce").iloc[0]
    if pd.isna(matter_file_year_num) or matter_file_year_num < 1900 or matter_file_year_num > 2100:
        matter_file_year_clean = ""
        matter_age_years = ""
    else:
        matter_file_year_clean = str(int(matter_file_year_num))
        matter_age_years = str(int(query_year_num - matter_file_year_num)) if not pd.isna(query_year_num) else ""

    disposition = disposition_group(row.get("status", ""), row.get("final_history_action", ""), row.get("title", ""))
    matter_universe_rows.append(
        {
            "query_year": row["query_year"],
            "matter_id": row["matter_id"],
            "matter_file": row["matter_file"],
            "matter_file_year": matter_file_year_clean,
            "matter_age_years": matter_age_years,
            "query_matter_type": row["query_matter_type"],
            "matter_type": row["matter_type"],
            "matter_status": row["status"],
            "disposition_group": disposition,
            "filed_age_group": filed_age_group(row.get("status", ""), row.get("query_year_int", ""), row.get("matter_file_year", "")),
            "final_history_date": row.get("final_history_date", ""),
            "final_history_action_by": row.get("final_history_action_by", ""),
            "final_history_action": row.get("final_history_action", ""),
            "final_history_result": row.get("final_history_result", ""),
            "council_history_action_count": row["council_history_action_count"],
            "council_approved_action_count": row["council_approved_action_count"],
            "council_disapproved_action_count": row["council_disapproved_action_count"],
            "council_filed_action_count": row["council_filed_action_count"],
            "council_modified_action_count": row["council_modified_action_count"],
            "application_keys": row["application_keys"],
            "zap_matched_application_keys": row.get("zap_matched_application_keys", ""),
            "zap_project_ids": row.get("zap_project_ids", ""),
            "zap_project_names": row.get("zap_project_names", ""),
            "zap_cc_districts": row.get("zap_cc_districts", ""),
            "matter_index_districts": row.get("matter_index_districts", ""),
            "legistar_text_districts": row.get("legistar_text_districts", ""),
            "affected_council_districts": collapse_values(affected_districts),
            "affected_district_source": affected_district_source,
            "ai_geography_repair_applied": "true" if affected_district_source == "ai_geography_repair" else "false",
            "ai_geography_repair_signature_review_id": ai_geo_repair.get("signature_review_id", ""),
            "ai_geography_repair_source": ai_geo_repair.get("repair_source", ""),
            "ai_geography_repair_confidence": ai_geo_repair.get("repair_confidence", ""),
            "ai_geography_repair_note": ai_geo_repair.get("repair_note", ""),
            "local_members_from_roster": collapse_values(local_members),
            "missing_roster_districts": collapse_values(missing_roster_districts),
            "borough": row.get("borough", ""),
            "committee": row.get("committee", ""),
            "land_use_recall_reason": row.get("land_use_recall_reason", ""),
            "title": row["title"],
            "matter_url": row["matter_url"],
            "final_history_detail_url": row.get("final_history_detail_url", ""),
        }
    )
matter_universe = pd.DataFrame(matter_universe_rows)

panel_base = action_details.merge(matter_zap, on="matter_id", how="left")
panel_rows = []
panel_ai_geo_repair_keys_used = set()
for row in panel_base.sort_values(["query_year_int", "history_date", "matter_file"]).to_dict("records"):
    matter_index_districts = split_semicolon(row.get("matter_index_districts", ""))
    legistar_districts = split_semicolon(row.get("legistar_text_districts", ""))
    zap_districts = split_semicolon(row.get("zap_cc_districts", ""))
    ai_geo_repair_key = repair_lookup_key(row.get("query_year_int", ""), row.get("vote_date", ""), row.get("matter_file", ""))
    ai_geo_repair = ai_geo_repair_lookup.get(ai_geo_repair_key, {}) if ai_geo_repair_key is not None else {}
    ai_geo_repair_districts = split_semicolon(ai_geo_repair.get("accepted_council_districts", ""))

    if matter_index_districts:
        affected_districts = matter_index_districts
        affected_district_source = "legistar_matter_index"
    elif legistar_districts:
        affected_districts = legistar_districts
        affected_district_source = "legistar_text"
    elif zap_districts:
        affected_districts = zap_districts
        affected_district_source = "zap_application_key"
    elif ai_geo_repair_districts:
        affected_districts = ai_geo_repair_districts
        affected_district_source = "ai_geography_repair"
        panel_ai_geo_repair_keys_used.add(ai_geo_repair_key)
    else:
        affected_districts = []
        affected_district_source = "missing"

    vote_date = row["vote_date"]
    local_rows = []
    missing_roster_districts = []
    for district in affected_districts:
        matches = roster[
            (roster["district"].astype(str) == str(int(district)))
            & (roster["term_start_date_parsed"] <= vote_date)
            & (vote_date <= roster["term_end_date_parsed"])
        ]
        if matches.empty:
            missing_roster_districts.append(str(district))
            continue
        local_rows.extend(matches.to_dict("records"))

    local_member_names = []
    local_member_votes = []
    local_member_negative = []
    local_member_abstain = []
    local_member_other_nonaffirmative = []
    matter_votes = vote_lookup.get(row["matter_id"], {})
    for local in local_rows:
        name = local["member_name"]
        vote_value = matter_votes.get(local["member_name_norm"]) or matter_votes.get(local["member_name_edge"]) or ""
        local_member_names.append(name)
        if vote_value:
            local_member_votes.append(f"{name}: {vote_value}")
        if vote_value == "Negative":
            local_member_negative.append(name)
        elif vote_value == "Abstain":
            local_member_abstain.append(name)
        elif vote_value and vote_value != "Affirmative":
            local_member_other_nonaffirmative.append(f"{name}: {vote_value}")

    local_vote_count = len(local_member_votes)
    local_affirmative_count = sum(vote.endswith(": Affirmative") for vote in local_member_votes)

    if not affected_districts:
        vote_evidence_status = "unresolved_no_affected_district"
        vote_evidence_strength = "unresolved"
    elif missing_roster_districts:
        vote_evidence_status = "unresolved_missing_roster"
        vote_evidence_strength = "unresolved"
    elif not matter_votes:
        vote_evidence_status = "unresolved_no_member_vote_rows"
        vote_evidence_strength = "unresolved"
    elif local_vote_count == 0:
        vote_evidence_status = "unresolved_no_local_member_vote_match"
        vote_evidence_strength = "unresolved"
    elif local_member_negative:
        vote_evidence_status = "approved_with_local_member_negative"
        vote_evidence_strength = "strong_exception_candidate"
    elif local_member_abstain:
        vote_evidence_status = "approved_with_local_member_abstain"
        vote_evidence_strength = "strong_exception_candidate"
    elif local_member_other_nonaffirmative:
        vote_evidence_status = "approved_with_local_member_other_nonaffirmative"
        vote_evidence_strength = "ambiguous_nonaffirmative"
    elif local_affirmative_count == len(local_rows):
        vote_evidence_status = "approved_with_all_local_members_affirmative"
        vote_evidence_strength = "weakly_deference_consistent"
    else:
        vote_evidence_status = "unresolved_partial_local_member_vote_match"
        vote_evidence_strength = "unresolved"

    panel_rows.append(
        {
            "query_year": row["query_year"],
            "matter_id": row["matter_id"],
            "matter_file": row["matter_file"],
            "query_matter_type": row["query_matter_type"],
            "vote_date": vote_date.strftime("%Y-%m-%d") if not pd.isna(vote_date) else "",
            "vote_margin": row["vote_margin"],
            "affirmative_count": row["affirmative_count"],
            "negative_count": row["negative_count"],
            "abstain_count": row["abstain_count"],
            "application_keys": row["application_keys"],
            "zap_matched_application_keys": row.get("zap_matched_application_keys", ""),
            "zap_project_ids": row.get("zap_project_ids", ""),
            "zap_project_names": row.get("zap_project_names", ""),
            "zap_cc_districts": row.get("zap_cc_districts", ""),
            "matter_index_districts": row.get("matter_index_districts", ""),
            "legistar_text_districts": row.get("legistar_text_districts", ""),
            "affected_council_districts": collapse_values(affected_districts),
            "affected_district_source": affected_district_source,
            "ai_geography_repair_applied": "true" if affected_district_source == "ai_geography_repair" else "false",
            "ai_geography_repair_signature_review_id": ai_geo_repair.get("signature_review_id", ""),
            "ai_geography_repair_source": ai_geo_repair.get("repair_source", ""),
            "ai_geography_repair_confidence": ai_geo_repair.get("repair_confidence", ""),
            "ai_geography_repair_note": ai_geo_repair.get("repair_note", ""),
            "local_members_from_roster": collapse_values(local_member_names),
            "local_member_votes": collapse_values(local_member_votes),
            "local_member_negative": collapse_values(local_member_negative),
            "local_member_abstain": collapse_values(local_member_abstain),
            "local_member_other_nonaffirmative": collapse_values(local_member_other_nonaffirmative),
            "missing_roster_districts": collapse_values(missing_roster_districts),
            "vote_evidence_status": vote_evidence_status,
            "vote_evidence_strength": vote_evidence_strength,
            "title": row["matter_index_title"] or row["action_detail_title"],
            "history_detail_url": row["history_detail_url"],
        }
    )

panel = pd.DataFrame(panel_rows)

summary_rows = [
    {"metric": "approved_matter_rows", "value": len(panel)},
    {
        "metric": "matter_rows_with_application_key",
        "value": int(panel["application_keys"].fillna("").ne("").sum()),
    },
    {
        "metric": "matter_rows_with_zap_application_match",
        "value": int(panel["zap_project_ids"].fillna("").ne("").sum()),
    },
    {
        "metric": "matter_rows_with_matter_index_district",
        "value": int(panel["matter_index_districts"].fillna("").ne("").sum()),
    },
    {
        "metric": "matter_rows_with_action_text_district",
        "value": int(panel["legistar_text_districts"].fillna("").ne("").sum()),
    },
    {
        "metric": "matter_rows_with_zap_district_fallback",
        "value": int((panel["affected_district_source"] == "zap_application_key").sum()),
    },
    {
        "metric": "matter_rows_with_ai_geography_repair",
        "value": int(panel["ai_geography_repair_applied"].eq("true").sum()),
    },
    {
        "metric": "strong_exception_candidate_rows",
        "value": int((panel["vote_evidence_strength"] == "strong_exception_candidate").sum()),
    },
    {
        "metric": "weakly_deference_consistent_rows",
        "value": int((panel["vote_evidence_strength"] == "weakly_deference_consistent").sum()),
    },
    {
        "metric": "unresolved_rows",
        "value": int((panel["vote_evidence_strength"] == "unresolved").sum()),
    },
]
summary_rows.extend(
    {"metric": f"status_{status}", "value": int(count)}
    for status, count in panel["vote_evidence_status"].value_counts().sort_index().items()
)
summary_rows.extend(
    {"metric": f"district_source_{source}", "value": int(count)}
    for source, count in panel["affected_district_source"].value_counts().sort_index().items()
)
summary = pd.DataFrame(summary_rows)

universe_summary_rows = [
    {"summary_group": "overall", "query_year": "", "metric": "matter_rows", "value": len(matter_universe)},
    {
        "summary_group": "overall",
        "query_year": "",
        "metric": "matter_rows_missing_final_history",
        "value": int(matter_universe["final_history_action"].fillna("").eq("").sum()),
    },
    {
        "summary_group": "overall",
        "query_year": "",
        "metric": "matter_rows_with_affected_district",
        "value": int((matter_universe["affected_district_source"] != "missing").sum()),
    },
    {
        "summary_group": "overall",
        "query_year": "",
        "metric": "matter_rows_with_ai_geography_repair",
        "value": int(matter_universe["ai_geography_repair_applied"].eq("true").sum()),
    },
    {
        "summary_group": "overall",
        "query_year": "",
        "metric": "matter_rows_with_local_member_from_roster",
        "value": int(matter_universe["local_members_from_roster"].fillna("").ne("").sum()),
    },
]
universe_summary_rows.extend(
    {
        "summary_group": "status",
        "query_year": "",
        "metric": status,
        "value": int(count),
    }
    for status, count in matter_universe["matter_status"].value_counts().sort_index().items()
)
universe_summary_rows.extend(
    {
        "summary_group": "disposition_group",
        "query_year": "",
        "metric": disposition,
        "value": int(count),
    }
    for disposition, count in matter_universe["disposition_group"].value_counts().sort_index().items()
)
universe_summary_rows.extend(
    {
        "summary_group": "filed_age_group",
        "query_year": "",
        "metric": age_group,
        "value": int(count),
    }
    for age_group, count in matter_universe.loc[
        matter_universe["filed_age_group"].fillna("").ne(""), "filed_age_group"
    ].value_counts().sort_index().items()
)
for (query_year, disposition), count in (
    matter_universe.groupby(["query_year", "disposition_group"]).size().sort_index().items()
):
    universe_summary_rows.append(
        {
            "summary_group": "year_by_disposition_group",
            "query_year": query_year,
            "metric": disposition,
            "value": int(count),
        }
    )
for (query_year, age_group), count in (
    matter_universe.loc[matter_universe["filed_age_group"].fillna("").ne("")]
    .groupby(["query_year", "filed_age_group"])
    .size()
    .sort_index()
    .items()
):
    universe_summary_rows.append(
        {
            "summary_group": "year_by_filed_age_group",
            "query_year": query_year,
            "metric": age_group,
            "value": int(count),
        }
    )
universe_summary = pd.DataFrame(universe_summary_rows)

filed_matter_audit = matter_universe[matter_universe["matter_status"].str.contains("Filed", case=False, na=False)][
    [
        "query_year",
        "matter_file",
        "matter_file_year",
        "matter_age_years",
        "query_matter_type",
        "matter_status",
        "disposition_group",
        "filed_age_group",
        "final_history_date",
        "final_history_action_by",
        "final_history_action",
        "affected_council_districts",
        "affected_district_source",
        "ai_geography_repair_applied",
        "ai_geography_repair_signature_review_id",
        "ai_geography_repair_source",
        "ai_geography_repair_confidence",
        "local_members_from_roster",
        "application_keys",
        "title",
        "matter_url",
    ]
].sort_values(["query_year", "filed_age_group", "matter_file"])

queue_disposition_groups = {
    "disapproved",
    "filed_by_council_other",
    "filed_withdrawal_or_motion",
    "filed_by_committee_or_subcommittee",
    "filed_other",
    "withdrawn",
}
final_action_vote_queue = matter_universe[
    matter_universe["disposition_group"].isin(queue_disposition_groups)
].copy()
final_action_vote_queue["final_action_vote_fetch_tier"] = "not_targeted"
final_action_vote_queue.loc[
    final_action_vote_queue["final_history_detail_url"].fillna("").eq(""),
    "final_action_vote_fetch_tier",
] = "no_final_history_detail_url"
final_action_vote_queue.loc[
    final_action_vote_queue["disposition_group"].isin(
        ["disapproved", "filed_by_council_other", "filed_withdrawal_or_motion"]
    )
    & final_action_vote_queue["final_history_action_by"].fillna("").eq("City Council")
    & final_action_vote_queue["final_history_detail_url"].fillna("").ne(""),
    "final_action_vote_fetch_tier",
] = "core_city_council_nonapproval"
final_action_vote_queue.loc[
    final_action_vote_queue["disposition_group"].isin(["filed_by_committee_or_subcommittee"])
    & final_action_vote_queue["final_history_detail_url"].fillna("").ne(""),
    "final_action_vote_fetch_tier",
] = "committee_or_subcommittee_nonapproval"
final_action_vote_queue.loc[
    final_action_vote_queue["disposition_group"].isin(["filed_other", "withdrawn"])
    & final_action_vote_queue["final_history_detail_url"].fillna("").ne(""),
    "final_action_vote_fetch_tier",
] = "low_information_final_action"
final_action_vote_queue["fetch_vote_detail_first_pass"] = final_action_vote_queue[
    "final_action_vote_fetch_tier"
].isin(["core_city_council_nonapproval"])
final_action_vote_queue["fetch_vote_detail_second_pass"] = final_action_vote_queue[
    "final_action_vote_fetch_tier"
].isin(["committee_or_subcommittee_nonapproval", "low_information_final_action"])
final_action_vote_queue = final_action_vote_queue[
    [
        "query_year",
        "matter_id",
        "matter_file",
        "query_matter_type",
        "matter_status",
        "disposition_group",
        "filed_age_group",
        "final_action_vote_fetch_tier",
        "fetch_vote_detail_first_pass",
        "fetch_vote_detail_second_pass",
        "final_history_date",
        "final_history_action_by",
        "final_history_action",
        "final_history_result",
        "final_history_detail_url",
        "affected_council_districts",
        "affected_district_source",
        "ai_geography_repair_applied",
        "ai_geography_repair_signature_review_id",
        "ai_geography_repair_source",
        "ai_geography_repair_confidence",
        "local_members_from_roster",
        "application_keys",
        "title",
    ]
].sort_values(["final_action_vote_fetch_tier", "query_year", "matter_file"])

queue_summary_rows = [
    {
        "summary_group": "overall",
        "query_year": "",
        "metric": "queue_rows",
        "value": len(final_action_vote_queue),
    },
    {
        "summary_group": "overall",
        "query_year": "",
        "metric": "first_pass_fetch_rows",
        "value": int(final_action_vote_queue["fetch_vote_detail_first_pass"].sum()),
    },
    {
        "summary_group": "overall",
        "query_year": "",
        "metric": "second_pass_fetch_rows",
        "value": int(final_action_vote_queue["fetch_vote_detail_second_pass"].sum()),
    },
]
for fetch_tier, count in (
    final_action_vote_queue["final_action_vote_fetch_tier"].value_counts().sort_index().items()
):
    queue_summary_rows.append(
        {
            "summary_group": "fetch_tier",
            "query_year": "",
            "metric": fetch_tier,
            "value": int(count),
        }
    )
for (query_year, fetch_tier), count in (
    final_action_vote_queue.groupby(["query_year", "final_action_vote_fetch_tier"]).size().sort_index().items()
):
    queue_summary_rows.append(
        {
            "summary_group": "year_by_fetch_tier",
            "query_year": query_year,
            "metric": fetch_tier,
            "value": int(count),
        }
    )
for (disposition, fetch_tier), count in (
    final_action_vote_queue.groupby(["disposition_group", "final_action_vote_fetch_tier"])
    .size()
    .sort_index()
    .items()
):
    queue_summary_rows.append(
        {
            "summary_group": "disposition_by_fetch_tier",
            "query_year": "",
            "metric": f"{disposition}: {fetch_tier}",
            "value": int(count),
        }
    )
final_action_vote_queue_summary = pd.DataFrame(queue_summary_rows)

accepted_ai_geo_repair_keys = set(ai_geo_repair_lookup)
panel_keys = {
    repair_lookup_key(row["query_year"], row["vote_date"], row["matter_file"])
    for row in panel[["query_year", "vote_date", "matter_file"]].to_dict("records")
}
panel_keys.discard(None)
matter_universe_keys = {
    repair_lookup_key(row["query_year"], row["final_history_date"], row["matter_file"])
    for row in matter_universe[["query_year", "final_history_date", "matter_file"]].to_dict("records")
}
matter_universe_keys.discard(None)

qc = pd.DataFrame(
    [
        {
            "check_name": "action_detail_unique_by_matter_id",
            "passed": not action_details["matter_id"].duplicated().any(),
            "detail": "Legistar final approval action-detail rows are unique by matter_id before panel construction.",
        },
        {
            "check_name": "matter_index_unique_by_matter_id",
            "passed": not matter_index["matter_id"].duplicated().any(),
            "detail": "Legistar matter-index rows are unique by matter_id before joining to final approval rows.",
        },
        {
            "check_name": "zap_project_unique_by_project_id",
            "passed": not zap_projects["project_id"].astype(str).duplicated().any(),
            "detail": "Staged ZAP project data are unique by project_id before application-key aggregation.",
        },
        {
            "check_name": "panel_unique_by_matter_id",
            "passed": not panel["matter_id"].duplicated().any(),
            "detail": "The output panel is one row per final Council approval matter.",
        },
        {
            "check_name": "matter_universe_unique_by_matter_id",
            "passed": not matter_universe["matter_id"].duplicated().any(),
            "detail": "The matter-universe output is one row per recalled Legistar matter.",
        },
        {
            "check_name": "matter_universe_history_coverage",
            "passed": int(matter_universe["final_history_action"].fillna("").eq("").sum()) <= 5,
            "detail": (
                f"{int(matter_universe['final_history_action'].fillna('').eq('').sum())} land-use-recalled "
                "matter rows have no parsed final history action."
            ),
        },
        {
            "check_name": "vote_status_assigned",
            "passed": bool(panel["vote_evidence_status"].fillna("").ne("").all()),
            "detail": "Every panel row receives a vote-evidence status.",
        },
        {
            "check_name": "universe_disposition_assigned",
            "passed": bool(matter_universe["disposition_group"].fillna("").ne("").all()),
            "detail": "Every recalled matter receives a broad disposition group.",
        },
        {
            "check_name": "final_action_vote_queue_tier_assigned",
            "passed": bool(final_action_vote_queue["final_action_vote_fetch_tier"].fillna("").ne("").all()),
            "detail": "Every non-adopted final-action queue row receives a fetch tier.",
        },
        {
            "check_name": "accepted_ai_geography_repair_unique_by_key",
            "passed": not ai_geo_repairs.duplicated(["query_year", "vote_date", "matter_file"]).any(),
            "detail": "Accepted AI/manual geography repairs are unique by query_year, vote_date, and matter_file.",
        },
        {
            "check_name": "accepted_ai_geography_repair_panel_or_universe_key_coverage",
            "passed": len(accepted_ai_geo_repair_keys - panel_keys - matter_universe_keys) == 0,
            "detail": (
                f"{len(panel_ai_geo_repair_keys_used)} accepted AI/manual repair keys were applied in the approval panel; "
                f"{len(accepted_ai_geo_repair_keys - panel_keys)} accepted repair keys are non-approval or otherwise outside "
                "the approval-panel rows."
            ),
        },
        {
            "check_name": "accepted_ai_geography_repair_universe_key_coverage",
            "passed": len(accepted_ai_geo_repair_keys - matter_universe_keys) == 0,
            "detail": (
                f"{len(matter_universe_ai_geo_repair_keys_used)} accepted AI/manual repair keys were applied in the matter universe; "
                f"{len(accepted_ai_geo_repair_keys - matter_universe_keys)} accepted repair keys do not match matter-universe rows."
            ),
        },
    ]
)

write_csv("../output/member_deference_vote_panel.csv", panel)
write_csv("../output/member_deference_vote_panel_summary.csv", summary)
write_csv("../output/member_deference_vote_panel_qc.csv", qc)
write_csv("../output/member_deference_matter_universe.csv", matter_universe)
write_csv("../output/member_deference_matter_universe_summary.csv", universe_summary)
write_csv("../output/member_deference_filed_matter_audit.csv", filed_matter_audit)
write_csv("../output/member_deference_final_action_vote_queue.csv", final_action_vote_queue)
write_csv("../output/member_deference_final_action_vote_queue_summary.csv", final_action_vote_queue_summary)
write_csv(
    "../output/member_deference_vote_panel_exception_candidates.csv",
    panel[panel["vote_evidence_strength"] == "strong_exception_candidate"].copy(),
)
write_csv(
    "../output/member_deference_vote_panel_unresolved.csv",
    panel[panel["vote_evidence_strength"] == "unresolved"].copy(),
)
