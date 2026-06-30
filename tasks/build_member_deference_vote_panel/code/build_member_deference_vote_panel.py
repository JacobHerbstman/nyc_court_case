# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_member_deference_vote_panel/code")

from __future__ import annotations

import re
import sys
import time

import pandas as pd

sys.path.append("../../_lib")
from member_deference_utils import (
    application_keys,
    collapse_districts as collapse_int_strings,
    collapse_values,
    council_districts_from_text,
    edge_name,
    norm_name,
    normalize_space,
    split_semicolon,
)

RECALL_YEARS = list(range(1998, 2026))

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


def read_legistar_csv(path: str) -> pd.DataFrame:
    last_error = None
    for attempt in range(1, 4):
        try:
            return pd.read_csv(path, dtype=str, keep_default_na=False)
        except TimeoutError as error:
            last_error = error
            if attempt == 3:
                break
            time.sleep(2 * attempt)
    raise last_error


def read_year_stack(file_suffix: str) -> pd.DataFrame:
    rows = []
    for year in RECALL_YEARS:
        df = read_legistar_csv(f"../input/legistar_{year}_broad_recall_{file_suffix}.csv")
        df["query_year_int"] = year
        rows.append(df)
    return pd.concat(rows, ignore_index=True)


def repair_lookup_key(query_year: object, date_value: object, matter_file: object) -> tuple[str, str, str] | None:
    query_year_num = pd.to_numeric(pd.Series([query_year]), errors="coerce").iloc[0]
    date_parsed = pd.to_datetime(date_value, errors="coerce")
    matter_file_clean = normalize_space(matter_file)
    if pd.isna(query_year_num) or pd.isna(date_parsed) or matter_file_clean == "":
        return None
    return (str(int(query_year_num)), date_parsed.strftime("%Y-%m-%d"), matter_file_clean)


def build_matter_zap_lookup(matter_application_keys: pd.DataFrame, zap_application_lookup: pd.DataFrame) -> pd.DataFrame:
    app_rows = []
    for row in matter_application_keys[["matter_id", "application_keys"]].to_dict("records"):
        for key in split_semicolon(row["application_keys"]):
            app_rows.append({"matter_id": row["matter_id"], "application_key": key})

    if not app_rows:
        return pd.DataFrame(
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

    matter_app_key = pd.DataFrame(app_rows)
    matter_zap_long = matter_app_key.merge(
        zap_application_lookup,
        on="application_key",
        how="left",
        validate="many_to_one",
    )
    return (
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


def affected_district_assignment(
    row: dict[str, object],
    date_value: object,
    repair_lookup: dict[tuple[str, str, str], dict[str, object]],
) -> tuple[list[str], str, tuple[str, str, str] | None, dict[str, object]]:
    matter_index_districts = split_semicolon(row.get("matter_index_districts", ""))
    legistar_districts = split_semicolon(row.get("legistar_text_districts", ""))
    zap_districts = split_semicolon(row.get("zap_cc_districts", ""))
    repair_key = repair_lookup_key(row.get("query_year_int", ""), date_value, row.get("matter_file", ""))
    repair = repair_lookup.get(repair_key, {}) if repair_key is not None else {}
    repair_districts = split_semicolon(repair.get("accepted_council_districts", ""))

    if matter_index_districts:
        return matter_index_districts, "legistar_matter_index", repair_key, repair
    if legistar_districts:
        return legistar_districts, "legistar_text", repair_key, repair
    if zap_districts:
        return zap_districts, "zap_application_key", repair_key, repair
    if repair_districts:
        return repair_districts, "ai_geography_repair", repair_key, repair
    return [], "missing", repair_key, repair


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
action_details = action_details.merge(matter_index_join, on="matter_id", how="left", validate="one_to_one")
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
    lambda x: collapse_values(council_districts_from_text(x))
)
action_details["matter_index_districts"] = action_details["matter_index_affected_council_districts"].map(
    lambda x: collapse_int_strings(split_semicolon(x))
)
action_details["application_keys"] = action_details["text_for_parse"].map(lambda x: collapse_values(application_keys(x)))

zap_projects["project_id"] = zap_projects["project_id"].astype(str)
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

matter_zap = build_matter_zap_lookup(action_details[["matter_id", "application_keys"]], zap_app_key_base)

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
    lambda x: collapse_values(council_districts_from_text(x))
)
matter_universe_base["application_keys"] = matter_universe_base["matter_text_for_parse"].map(
    lambda x: collapse_values(application_keys(x))
)

universe_matter_zap = build_matter_zap_lookup(
    matter_universe_base[["matter_id", "application_keys"]],
    zap_app_key_base,
)

matter_universe_base = (
    matter_universe_base.merge(final_history, on="matter_id", how="left", validate="one_to_one")
    .merge(city_council_summary, on="matter_id", how="left", validate="one_to_one")
    .merge(universe_matter_zap, on="matter_id", how="left", validate="one_to_one")
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
roster["district_key"] = roster["district"].map(lambda x: str(int(x)) if normalize_space(x) else "")

roster_by_district = {
    district: rows.to_dict("records")
    for district, rows in roster.sort_values(["district_key", "term_start_date_parsed", "member_name"]).groupby("district_key")
}


def local_roster_rows(affected_districts: list[str], target_date: object) -> tuple[list[dict[str, object]], list[str]]:
    if pd.isna(target_date):
        return [], []

    local_rows = []
    missing_roster_districts = []
    for district in affected_districts:
        district_key = str(int(district))
        matches = [
            row
            for row in roster_by_district.get(district_key, [])
            if row["term_start_date_parsed"] <= target_date <= row["term_end_date_parsed"]
        ]
        if matches:
            local_rows.extend(matches)
        else:
            missing_roster_districts.append(district_key)

    return local_rows, missing_roster_districts

matter_universe_rows = []
for row in matter_universe_base.sort_values(["query_year_int", "matter_file"]).to_dict("records"):
    final_date = pd.to_datetime(row.get("final_history_date", ""), errors="coerce")
    affected_districts, affected_district_source, _, ai_geo_repair = affected_district_assignment(
        row,
        final_date,
        ai_geo_repair_lookup,
    )

    local_rows, missing_roster_districts = local_roster_rows(affected_districts, final_date)
    local_members = [local["member_name"] for local in local_rows]

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

panel_base = action_details.merge(matter_zap, on="matter_id", how="left", validate="one_to_one")
panel_rows = []
panel_ai_geo_repair_keys_used = set()
for row in panel_base.sort_values(["query_year_int", "history_date", "matter_file"]).to_dict("records"):
    affected_districts, affected_district_source, ai_geo_repair_key, ai_geo_repair = affected_district_assignment(
        row,
        row.get("vote_date", ""),
        ai_geo_repair_lookup,
    )
    if affected_district_source == "ai_geography_repair":
        panel_ai_geo_repair_keys_used.add(ai_geo_repair_key)

    vote_date = row["vote_date"]
    local_rows, missing_roster_districts = local_roster_rows(affected_districts, vote_date)

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
    # Hamilton Avenue transfer station was a motion to disapprove; a local no vote means project-side support.
    hamilton_transfer_disapproval = str(row["matter_id"]) in {"450009", "444462"}

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
    elif hamilton_transfer_disapproval:
        vote_evidence_status = "excluded_inverted_disapproval_motion"
        vote_evidence_strength = "excluded"
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
            "excluded_inverted_disapproval_motion": "true" if hamilton_transfer_disapproval else "false",
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

if panel["matter_id"].duplicated().any():
    raise RuntimeError("Vote panel must be unique by matter_id.")
if matter_universe["matter_id"].duplicated().any():
    raise RuntimeError("Matter universe must be unique by matter_id.")
if int(matter_universe["final_history_action"].fillna("").eq("").sum()) > 5:
    raise RuntimeError("Too many recalled matter rows lack a parsed final history action.")
if not panel["vote_evidence_status"].fillna("").ne("").all():
    raise RuntimeError("Every vote-panel row must receive a vote-evidence status.")
if not matter_universe["disposition_group"].fillna("").ne("").all():
    raise RuntimeError("Every recalled matter must receive a disposition group.")
if not final_action_vote_queue["final_action_vote_fetch_tier"].fillna("").ne("").all():
    raise RuntimeError("Every non-adopted final-action queue row must receive a fetch tier.")
if accepted_ai_geo_repair_keys - panel_keys - matter_universe_keys:
    raise RuntimeError("Every accepted geography repair key must appear in the panel or matter universe.")

panel.to_csv("../output/member_deference_vote_panel.csv", index=False)
matter_universe.to_csv("../output/member_deference_matter_universe.csv", index=False)
final_action_vote_queue.to_csv("../output/member_deference_final_action_vote_queue.csv", index=False)
