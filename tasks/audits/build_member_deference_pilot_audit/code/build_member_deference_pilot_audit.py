from __future__ import annotations

import hashlib
import re
from collections import defaultdict, deque

import pandas as pd


PILOT_YEARS = [str(year) for year in range(1998, 2011)]
CONTROL_SAMPLE_PER_YEAR = 20


def clean(value: object) -> str:
    return re.sub(r"\s+", " ", "" if pd.isna(value) else str(value)).strip()


def collapse_values(values: object) -> str:
    out = []
    for value in values:
        value = clean(value)
        if value and value not in out:
            out.append(value)
    return "; ".join(out)


def split_values(value: object) -> list[str]:
    return [part.strip() for part in clean(value).split(";") if part.strip()]


def norm_name(value: object) -> str:
    value = re.sub(r"[^A-Za-z0-9 ]", " ", clean(value))
    return re.sub(r"\s+", " ", value).strip().lower()


def edge_name(value: object) -> str:
    parts = norm_name(value).split()
    if len(parts) < 2:
        return norm_name(value)
    return f"{parts[0]} {parts[-1]}"


application_re = re.compile(
    r"\b(?:[CNM]\s*)?\d{6}\s*(?:\([A-Z0-9]+\)\s*)?[A-Z]{2,4}\b|"
    r"\b\d{8}\s*[A-Z]{2,4}\b",
    flags=re.IGNORECASE,
)


def application_keys(value: object) -> list[str]:
    keys = []
    for match in application_re.finditer(clean(value)):
        key = re.sub(r"[^A-Za-z0-9]", "", match.group(0)).upper()
        key = re.sub(r"^[CNM](?=\d)", "", key)
        if key and key not in keys:
            keys.append(key)
    return keys


def action_code(key: str) -> str:
    match = re.search(r"([A-Z]{2,4})$", key)
    return match.group(1) if match else ""


def action_code_root(code: str) -> str:
    code = re.sub(r"^A(?=[A-Z]{2,3}$)", "", code)
    if len(code) >= 3 and code[-1] in {"K", "M", "Q", "R", "X"}:
        return code[:-1]
    return code


def application_number_values(keys: list[str]) -> list[int]:
    numbers = []
    for key in keys:
        match = re.match(r"(\d+)", key)
        if match:
            numbers.append(int(match.group(1)))
    return numbers


def min_gap(values_a: list[int], values_b: list[int]) -> int | None:
    gaps = [abs(value_a - value_b) for value_a in values_a for value_b in values_b]
    return min(gaps) if gaps else None


def district_values(value: object) -> list[str]:
    districts = []
    for match in re.findall(r"\d{1,2}", clean(value)):
        district = str(int(match))
        if 1 <= int(district) <= 51 and district not in districts:
            districts.append(district)
    return districts


district_patterns = [
    re.compile(
        r"Council District(?:s)?(?:\s*(?:No\.?|Nos\.?|no\.?|nos\.?))?\s*([0-9,\sand]+)",
        flags=re.IGNORECASE,
    ),
    re.compile(r"\bCD'?s?\.?\s*([0-9,\sand]+)", flags=re.IGNORECASE),
]


def districts_from_record_text(value: object) -> list[str]:
    districts = []
    for pattern in district_patterns:
        for match in pattern.finditer(clean(value)):
            districts.extend(district_values(match.group(1)))
    return list(dict.fromkeys(districts))


def matter_file_number(value: object, prefix: str) -> str:
    match = re.search(rf"\b{prefix}\s+0*([0-9]+)-[0-9]{{4}}\b", clean(value), flags=re.IGNORECASE)
    return match.group(1) if match else ""


def lu_reference_numbers(value: object) -> list[str]:
    text = clean(value)
    refs = []
    for match in re.finditer(r"L\.?\s*U\.?\s+Nos?\.?\s*([0-9,;\sand-]+)", text, flags=re.IGNORECASE):
        for number in re.findall(r"\d+", match.group(1)):
            normalized = str(int(number))
            if normalized not in refs:
                refs.append(normalized)
    return refs


def bool_any(values: object) -> bool:
    return any(str(value).lower() == "true" for value in values)


def safe_date(value: object) -> pd.Timestamp:
    date = pd.to_datetime(value, errors="coerce")
    return date if not pd.isna(date) else pd.NaT


def source_key_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def text_has_any(text: str, terms: list[str]) -> bool:
    return any(re.search(term, text, flags=re.IGNORECASE) for term in terms)


roster = pd.read_csv("../input/council_member_roster_master.csv", dtype=str, keep_default_na=False)
roster["term_start_date_parsed"] = pd.to_datetime(roster["term_start_date"], errors="coerce")
roster["term_end_date_parsed"] = pd.to_datetime(roster["term_end_date"], errors="coerce").fillna(
    pd.Timestamp("2100-01-01")
)
roster["member_name_norm"] = roster["member_name"].map(norm_name)
roster["member_name_edge"] = roster["member_name"].map(edge_name)
roster = roster[roster["member_name_norm"] != "vacant"].copy()

matter_index_parts = []
history_parts = []
action_detail_parts = []
override_signal_parts = []
for year in PILOT_YEARS:
    matter_index_parts.append(
        pd.read_csv(f"../input/legistar_{year}_broad_recall_matter_index.csv", dtype=str, keep_default_na=False)
    )
    history_parts.append(
        pd.read_csv(f"../input/legistar_{year}_broad_recall_history_events.csv", dtype=str, keep_default_na=False)
    )
    action_detail_parts.append(
        pd.read_csv(f"../input/legistar_{year}_broad_recall_action_details.csv", dtype=str, keep_default_na=False)
    )
    override_signal_parts.append(
        pd.read_csv(f"../input/legistar_{year}_member_override_matter_signals.csv", dtype=str, keep_default_na=False)
    )

matter_index = pd.concat(matter_index_parts, ignore_index=True)
history_events = pd.concat(history_parts, ignore_index=True)
action_details = pd.concat(action_detail_parts, ignore_index=True)
override_signals = pd.concat(override_signal_parts, ignore_index=True)

if matter_index[["query_year", "matter_id"]].duplicated().any():
    raise RuntimeError("Matter index is not unique by query_year and matter_id.")
if action_details[["query_year", "matter_id"]].duplicated().any():
    raise RuntimeError("Final Council action details are not unique by query_year and matter_id.")
if override_signals[["query_year", "matter_id"]].duplicated().any():
    raise RuntimeError("Override signal rows are not unique by query_year and matter_id.")

history_events["history_date_parsed"] = pd.to_datetime(history_events["history_date"], errors="coerce")
history_events["history_action_norm"] = history_events["history_action"].str.replace(",", "", regex=False).str.lower()

history_rows = []
for (query_year, matter_id), group in history_events.groupby(["query_year", "matter_id"], dropna=False):
    group = group.copy()
    approved = group[
        (group["history_action_by"] == "City Council")
        & group["history_action_norm"].eq("approved by council")
    ]
    disapproved = group[group["history_action_norm"].str.contains("disapproved", na=False)]
    filed = group[group["history_action_norm"].str.contains("filed", na=False)]
    re_referred = group[group["history_action_norm"].str.contains("re-referred|referred pursuant", regex=True, na=False)]
    modified = group[group["history_action_norm"].str.contains("modification|modifications|modified", regex=True, na=False)]
    history_rows.append(
        {
            "query_year": query_year,
            "matter_id": matter_id,
            "first_history_date": group["history_date_parsed"].min(),
            "last_history_date": group["history_date_parsed"].max(),
            "final_council_approval_date": approved["history_date_parsed"].max(),
            "council_disapproval_date": disapproved["history_date_parsed"].max(),
            "filed_date": filed["history_date_parsed"].max(),
            "final_council_approval_flag": not approved.empty,
            "council_disapproval_flag": not disapproved.empty,
            "filed_no_final_flag": not filed.empty,
            "re_referred_flag": not re_referred.empty,
            "approved_with_modification_flag": not modified.empty,
            "history_actions": collapse_values(group["history_action"]),
            "history_action_bodies": collapse_values(group["history_action_by"]),
        }
    )

history_summary = pd.DataFrame(history_rows)
matter = matter_index.merge(history_summary, on=["query_year", "matter_id"], how="left", validate="one_to_one")

action_detail_fields = [
    "query_year",
    "matter_id",
    "history_date",
    "history_detail_url",
    "vote_margin",
    "affirmative_count",
    "negative_count",
    "abstain_count",
    "negative_members",
    "abstain_members",
]
matter = matter.merge(
    action_details[action_detail_fields].rename(
        columns={
            "history_date": "final_vote_date",
            "history_detail_url": "final_vote_detail_url",
            "vote_margin": "final_vote_margin",
            "affirmative_count": "final_vote_affirmative_count",
            "negative_count": "final_vote_negative_count",
            "abstain_count": "final_vote_abstain_count",
            "negative_members": "final_vote_negative_members",
            "abstain_members": "final_vote_abstain_members",
        }
    ),
    on=["query_year", "matter_id"],
    how="left",
    validate="one_to_one",
)

signal_fields = [
    "query_year",
    "matter_id",
    "screen_status",
    "local_member_negative",
    "local_member_abstain",
    "local_members_from_roster",
    "missing_roster_districts",
    "source_tiers",
    "district_sources",
]
matter = matter.merge(
    override_signals[signal_fields].rename(
        columns={
            "screen_status": "override_vote_screen_status",
            "local_member_negative": "final_vote_local_member_negative",
            "local_member_abstain": "final_vote_local_member_abstain",
            "local_members_from_roster": "final_vote_local_members_from_roster",
            "missing_roster_districts": "final_vote_missing_roster_districts",
            "source_tiers": "final_vote_roster_source_tiers",
            "district_sources": "final_vote_district_sources",
        }
    ),
    on=["query_year", "matter_id"],
    how="left",
    validate="one_to_one",
)

for flag_column in [
    "final_council_approval_flag",
    "council_disapproval_flag",
    "filed_no_final_flag",
    "re_referred_flag",
    "approved_with_modification_flag",
]:
    matter[flag_column] = matter[flag_column].map(lambda value: value is True or str(value).lower() == "true")

matter["text_for_parse"] = (
    matter["title"].map(clean)
    + " "
    + matter["detail_title"].map(clean)
    + " "
    + matter["application_numbers_in_title"].map(clean)
)
matter["application_keys_list"] = matter["text_for_parse"].map(application_keys)
matter["application_keys"] = matter["application_keys_list"].map(collapse_values)
matter["action_code_families"] = matter["application_keys_list"].map(lambda keys: collapse_values(action_code(key) for key in keys))
matter["application_number_values"] = matter["application_keys_list"].map(application_number_values)
matter["action_code_family_list"] = matter["application_keys_list"].map(
    lambda keys: list(dict.fromkeys(action_code(key) for key in keys if action_code(key)))
)
matter["action_code_root_list"] = matter["action_code_family_list"].map(
    lambda codes: list(dict.fromkeys(action_code_root(code) for code in codes if action_code_root(code)))
)
matter["affected_districts_list"] = (
    matter["affected_council_districts"].map(district_values)
    + matter["title"].map(districts_from_record_text)
    + matter["detail_title"].map(districts_from_record_text)
)
matter["affected_districts_list"] = matter["affected_districts_list"].map(lambda values: list(dict.fromkeys(values)))
matter["affected_council_districts_clean"] = matter["affected_districts_list"].map(collapse_values)
matter["lu_number"] = matter["matter_file"].map(lambda value: matter_file_number(value, "LU"))
matter["lu_number_int"] = pd.to_numeric(matter["lu_number"], errors="coerce")
matter["resolution_number"] = matter["matter_file"].map(lambda value: matter_file_number(value, "Res"))
matter["referenced_lu_numbers_list"] = matter["text_for_parse"].map(lu_reference_numbers)
matter["ulurp_like_flag"] = matter["ulurp_text_flag"].str.lower().eq("true") | matter["text_for_parse"].str.contains(
    r"\bULURP\b|Uniform Land Use|Section s?197-[cd]|Sections 197-[cd]|ZRM|ZMM|ZRY|ZMK|ZMQ|ZSR|ZSM|ZSK",
    flags=re.IGNORECASE,
    regex=True,
    na=False,
)
matter["udaap_flag"] = matter["text_for_parse"].str.contains(
    r"UDAAP|Urban Development Action Area", flags=re.IGNORECASE, regex=True, na=False
)
matter["documented_withdrawal_flag"] = matter["text_for_parse"].str.contains(
    r"\bmotion\s+to\s+file\s+pursuant\s+to\s+withdrawal\b|\bfile\s+pursuant\s+to\s+withdrawal\b",
    flags=re.IGNORECASE,
    regex=True,
    na=False,
)
matter["documented_hpd_udaap_withdrawal_flag"] = matter["documented_withdrawal_flag"] & matter["udaap_flag"]
matter["withdrawal_source_code_auto"] = ""
matter.loc[matter["documented_withdrawal_flag"], "withdrawal_source_code_auto"] = (
    "official_resolution_motion_to_file_pursuant_to_withdrawal"
)
matter.loc[matter["documented_hpd_udaap_withdrawal_flag"], "withdrawal_source_code_auto"] = (
    "admin_withdrawal_documented_hpd_udaap_resolution"
)
matter["callup_flag"] = matter["query_matter_type"].eq("Land Use Call-Up")
matter["substantive_matter_flag"] = ~matter["callup_flag"]
matter["split_final_approval_vote_flag"] = (
    pd.to_numeric(matter["final_vote_negative_count"], errors="coerce").fillna(0).gt(0)
    | pd.to_numeric(matter["final_vote_abstain_count"], errors="coerce").fillna(0).gt(0)
)
matter["local_member_final_negative_flag"] = matter["override_vote_screen_status"].eq("candidate_local_member_negative")
matter["local_member_final_abstain_flag"] = matter["override_vote_screen_status"].eq("candidate_local_member_abstain")
matter["council_adverse_outcome_flag"] = matter["council_disapproval_flag"] | matter["filed_no_final_flag"]
matter["event_date"] = matter["final_council_approval_date"]
matter.loc[matter["event_date"].isna(), "event_date"] = matter.loc[matter["event_date"].isna(), "council_disapproval_date"]
matter.loc[matter["event_date"].isna(), "event_date"] = matter.loc[matter["event_date"].isna(), "filed_date"]
matter.loc[matter["event_date"].isna(), "event_date"] = matter.loc[matter["event_date"].isna(), "last_history_date"]

matter_local_members = []
for row in matter.to_dict("records"):
    event_date = row["event_date"]
    if pd.isna(event_date):
        event_date = pd.Timestamp(f"{row['query_year']}-12-31")
    matches = []
    missing_districts = []
    for district in row["affected_districts_list"]:
        district_matches = roster[
            (roster["district"] == district)
            & (roster["term_start_date_parsed"] <= event_date)
            & (event_date <= roster["term_end_date_parsed"])
        ]
        if district_matches.empty:
            missing_districts.append(district)
        else:
            matches.extend(district_matches.to_dict("records"))
    local_names = [match["member_name"] for match in matches]
    sponsor_edges = {edge_name(row["prime_sponsor"])}
    local_edges = {match["member_name_edge"] for match in matches}
    matter_local_members.append(
        {
            "query_year": row["query_year"],
            "matter_id": row["matter_id"],
            "local_members_at_event_date": collapse_values(local_names),
            "local_member_source_tiers": collapse_values(match["source_tier"] for match in matches),
            "local_member_district_sources": collapse_values(match["district_source"] for match in matches),
            "missing_roster_districts_at_event_date": collapse_values(missing_districts),
            "prime_sponsor_is_local_member": bool(sponsor_edges & local_edges),
        }
    )

matter = matter.merge(pd.DataFrame(matter_local_members), on=["query_year", "matter_id"], how="left", validate="one_to_one")

key_to_matter_ids = defaultdict(set)
for row in matter.to_dict("records"):
    row_key = f"{row['query_year']}::{row['matter_id']}"
    application_edge_keys = list(row["application_keys_list"])
    if row["callup_flag"]:
        for key in row["application_keys_list"]:
            match = re.match(r"20(\d{6})([A-Z]{2,4})$", key)
            if match:
                application_edge_keys.append(match.group(1) + match.group(2))
    for key in list(dict.fromkeys(application_edge_keys)):
        key_to_matter_ids[f"app:{key}"].add(row_key)
    if row["lu_number"]:
        key_to_matter_ids[f"lu:{row['query_year']}:{row['lu_number']}"].add(row_key)
    for lu_number in row["referenced_lu_numbers_list"]:
        key_to_matter_ids[f"lu:{row['query_year']}:{lu_number}"].add(row_key)

bundle_bridge_review_rows = []
split_vote_package_candidates = matter[
    matter["substantive_matter_flag"]
    & matter["final_council_approval_flag"]
    & matter["split_final_approval_vote_flag"]
    & matter["ulurp_like_flag"].fillna(False)
    & matter["final_council_approval_date"].notna()
].copy()
for bridge_group_key, group in split_vote_package_candidates.groupby(
    [
        "query_year",
        "final_council_approval_date",
        "final_vote_margin",
        "final_vote_negative_members",
        "final_vote_abstain_members",
    ],
    dropna=False,
):
    rows = group.to_dict("records")
    if len(rows) < 2:
        continue

    strong_neighbors = defaultdict(set)
    weak_neighbors = defaultdict(set)
    pair_details = []
    for index, left in enumerate(rows):
        for right in rows[index + 1 :]:
            left_lu = [] if pd.isna(left["lu_number_int"]) else [int(left["lu_number_int"])]
            right_lu = [] if pd.isna(right["lu_number_int"]) else [int(right["lu_number_int"])]
            lu_gap = min_gap(left_lu, right_lu)
            app_gap = min_gap(left["application_number_values"], right["application_number_values"])
            close_lu = lu_gap is not None and lu_gap <= 8
            close_app = app_gap is not None and app_gap <= 8
            if not close_lu and not close_app:
                continue
            left_key = f"{left['query_year']}::{left['matter_id']}"
            right_key = f"{right['query_year']}::{right['matter_id']}"
            pair_details.append(
                {
                    "left_key": left_key,
                    "right_key": right_key,
                    "lu_gap": lu_gap,
                    "application_gap": app_gap,
                    "close_lu": close_lu,
                    "close_application": close_app,
                }
            )
            if close_lu and close_app:
                strong_neighbors[left_key].add(right_key)
                strong_neighbors[right_key].add(left_key)
            else:
                weak_neighbors[left_key].add(right_key)
                weak_neighbors[right_key].add(left_key)

    for neighbors, strength in [(strong_neighbors, "strong"), (weak_neighbors, "weak")]:
        seen = set()
        for row_key in sorted(neighbors):
            if row_key in seen:
                continue
            queue = deque([row_key])
            seen.add(row_key)
            component = []
            while queue:
                current = queue.popleft()
                component.append(current)
                for neighbor in neighbors[current]:
                    if neighbor not in seen:
                        seen.add(neighbor)
                        queue.append(neighbor)
            if len(component) < 2:
                continue

            component_rows = group[
                (group["query_year"] + "::" + group["matter_id"]).isin(component)
            ].copy()
            action_codes = []
            for codes in component_rows["action_code_family_list"]:
                action_codes.extend(codes)
            action_codes = list(dict.fromkeys(action_codes))
            action_roots = []
            for codes in component_rows["action_code_root_list"]:
                action_roots.extend(codes)
            action_roots = list(dict.fromkeys(action_roots))
            auto_merge = strength == "strong" and len(action_roots) >= 2
            bridge_key = "split_vote_package_bridge:" + source_key_hash("|".join(sorted(component)))[:16]
            if auto_merge:
                for row_key in component:
                    key_to_matter_ids[bridge_key].add(row_key)
            component_pairs = [
                pair
                for pair in pair_details
                if pair["left_key"] in component and pair["right_key"] in component
            ]
            bundle_bridge_review_rows.append(
                {
                    "bridge_group_id": bridge_key,
                    "bridge_decision_auto": (
                        "auto_merge_same_split_vote_close_lu_application_multi_action"
                        if auto_merge
                        else "manual_review_same_split_vote_adjacency"
                    ),
                    "adjacency_strength": strength,
                    "query_year": bridge_group_key[0],
                    "vote_date": bridge_group_key[1].strftime("%Y-%m-%d"),
                    "final_vote_margin": bridge_group_key[2],
                    "final_vote_negative_members": bridge_group_key[3],
                    "final_vote_abstain_members": bridge_group_key[4],
                    "matter_keys": collapse_values(component),
                    "matter_files": collapse_values(component_rows["matter_file"]),
                    "application_keys": collapse_values(component_rows["application_keys"]),
                    "action_code_families": collapse_values(action_codes),
                    "action_code_roots": collapse_values(action_roots),
                    "affected_council_districts": collapse_values(
                        component_rows["affected_council_districts_clean"]
                    ),
                    "local_members": collapse_values(component_rows["local_members_at_event_date"]),
                    "titles": collapse_values(component_rows["title"]),
                    "min_lu_gap_in_component": min(
                        [pair["lu_gap"] for pair in component_pairs if pair["lu_gap"] is not None],
                        default="",
                    ),
                    "min_application_gap_in_component": min(
                        [pair["application_gap"] for pair in component_pairs if pair["application_gap"] is not None],
                        default="",
                    ),
                    "hand_check_status": "not_started",
                    "hand_check_notes": "",
                }
            )

for _, group in matter[
    matter["local_member_final_negative_flag"]
    & matter["final_council_approval_flag"]
    & matter["ulurp_like_flag"].fillna(False)
].groupby(
    [
        "query_year",
        "event_date",
        "affected_council_districts_clean",
        "final_vote_local_member_negative",
    ],
    dropna=False,
):
    if len(group) > 1:
        bundle_key = "local_member_negative_same_vote:" + source_key_hash(
            "|".join(clean(value) for value in group.iloc[0][
                [
                    "query_year",
                    "event_date",
                    "affected_council_districts_clean",
                    "final_vote_local_member_negative",
                ]
            ].tolist())
        )[:16]
        for row in group.to_dict("records"):
            key_to_matter_ids[bundle_key].add(f"{row['query_year']}::{row['matter_id']}")

all_row_keys = set(f"{row['query_year']}::{row['matter_id']}" for row in matter.to_dict("records"))
neighbors = defaultdict(set)
for members in key_to_matter_ids.values():
    members = sorted(members)
    for member in members:
        neighbors[member].update(set(members) - {member})

component_id_by_key = {}
component_number = 0
for row_key in sorted(all_row_keys):
    if row_key in component_id_by_key:
        continue
    component_number += 1
    component_id = f"prelim_bundle_{component_number:05d}"
    queue = deque([row_key])
    component_id_by_key[row_key] = component_id
    while queue:
        current = queue.popleft()
        for neighbor in neighbors[current]:
            if neighbor not in component_id_by_key:
                component_id_by_key[neighbor] = component_id
                queue.append(neighbor)

matter["row_key"] = matter["query_year"] + "::" + matter["matter_id"]
matter["preliminary_bundle_id"] = matter["row_key"].map(component_id_by_key)

bundle_bridge_review = pd.DataFrame(bundle_bridge_review_rows)
if bundle_bridge_review.empty:
    bundle_bridge_review = pd.DataFrame(
        columns=[
            "bridge_group_id",
            "bridge_decision_auto",
            "adjacency_strength",
            "query_year",
            "vote_date",
            "final_vote_margin",
            "final_vote_negative_members",
            "final_vote_abstain_members",
            "matter_keys",
            "matter_files",
            "application_keys",
            "action_code_families",
            "action_code_roots",
            "affected_council_districts",
            "local_members",
            "titles",
            "min_lu_gap_in_component",
            "min_application_gap_in_component",
            "preliminary_bundle_ids_after_bridge",
            "hand_check_status",
            "hand_check_notes",
        ]
    )
else:
    bundle_bridge_review["preliminary_bundle_ids_after_bridge"] = bundle_bridge_review["matter_keys"].map(
        lambda value: collapse_values(component_id_by_key.get(key, "") for key in split_values(value))
    )
bundle_bridge_review["preliminary_bundle_count_after_bridge"] = bundle_bridge_review[
    "preliminary_bundle_ids_after_bridge"
].map(lambda value: len(split_values(value)))
bundle_bridge_review["bridge_review_scope_auto"] = "manual_review_remaining_multiple_bundles"
bundle_bridge_review.loc[
    bundle_bridge_review["bridge_decision_auto"].eq("auto_merge_same_split_vote_close_lu_application_multi_action"),
    "bridge_review_scope_auto",
] = "auto_merged"
bundle_bridge_review.loc[
    bundle_bridge_review["bridge_decision_auto"].eq("manual_review_same_split_vote_adjacency")
    & bundle_bridge_review["preliminary_bundle_count_after_bridge"].le(1),
    "bridge_review_scope_auto",
] = "already_one_bundle_after_other_edges"

bundle_rows = []
for bundle_id, group in matter.groupby("preliminary_bundle_id", dropna=False):
    substantive = group[group["substantive_matter_flag"]].copy()
    if substantive.empty:
        substantive = group.copy()
    app_keys = []
    for keys in group["application_keys_list"]:
        app_keys.extend(keys)
    app_keys = list(dict.fromkeys(app_keys))
    text_blob = " ".join(group["text_for_parse"].map(clean))
    title_choices = [title for title in substantive["title"].map(clean) if title]
    representative_title = min(title_choices, key=len) if title_choices else clean(group.iloc[0]["title"])

    residential_mixed_flag = (
        text_has_any(
            text_blob,
            [
                r"\bresidential\b",
                r"\bhousing\b",
                r"\bmixed[- ]use\b",
                r"\bdwelling\b",
                r"\bapartment",
                r"\bUDAAP\b",
                r"Urban Development Action Area",
            ],
        )
    )
    city_project_flag = text_has_any(
        text_blob,
        [r"Department of Citywide Administrative Services", r"school", r"police", r"fire", r"public facility"],
    )
    sidewalk_cafe_flag = text_has_any(text_blob, [r"sidewalk caf[eé]"])
    revocable_consent_flag = text_has_any(text_blob, [r"revocable consent"])
    sidewalk_cafe_or_revocable_consent_flag = sidewalk_cafe_flag or revocable_consent_flag
    commercial_flag = text_has_any(text_blob, [r"sidewalk cafe", r"hotel", r"commercial", r"parking garage"])
    if residential_mixed_flag:
        use_category = "residential_mixed"
    elif city_project_flag:
        use_category = "city_project_or_public"
    elif commercial_flag:
        use_category = "commercial"
    else:
        use_category = "other_or_uncoded"

    substantive_approved = bool_any(substantive["final_council_approval_flag"])
    substantive_disapproved = bool_any(substantive["council_disapproval_flag"])
    substantive_filed = bool_any(substantive["filed_no_final_flag"])
    substantive_modified = bool_any(substantive["approved_with_modification_flag"])
    substantive_rereferred = bool_any(substantive["re_referred_flag"])
    documented_withdrawal = bool_any(substantive["documented_withdrawal_flag"])
    documented_hpd_udaap_withdrawal = bool_any(substantive["documented_hpd_udaap_withdrawal_flag"])
    split_approval = bool_any(substantive["split_final_approval_vote_flag"])
    local_member_negative = bool_any(substantive["local_member_final_negative_flag"])
    local_member_abstain = bool_any(substantive["local_member_final_abstain_flag"])
    callup = bool_any(group["callup_flag"])
    local_member_edges = {
        edge_name(member)
        for value in substantive["local_members_at_event_date"]
        for member in split_values(value)
    }
    callup_sponsor_edges = {
        edge_name(sponsor)
        for sponsor in group[group["callup_flag"]]["prime_sponsor"]
        if clean(sponsor)
    }
    callup_local = bool(local_member_edges & callup_sponsor_edges)
    local_member_names = collapse_values(substantive["local_members_at_event_date"])
    adverse_outcome = substantive_disapproved or substantive_filed
    project_approved_outcome = substantive_approved and not adverse_outcome
    confirmed_breach_flag = local_member_negative and project_approved_outcome
    possible_breach_abstain_flag = local_member_abstain and project_approved_outcome and not local_member_negative
    strong_candidate_local_member_callup_council_disapproved_flag = callup_local and substantive_disapproved
    strong_candidate_local_member_callup_filed_no_final_flag = (
        callup_local and substantive_filed and not substantive_disapproved
    )
    strong_candidate_local_member_callup_adverse_flag = (
        strong_candidate_local_member_callup_council_disapproved_flag
        or strong_candidate_local_member_callup_filed_no_final_flag
    )
    modification_with_member_signal_pending_audit_flag = project_approved_outcome and substantive_modified and (
        callup_local or local_member_negative or local_member_abstain
    )
    documented_withdrawal_no_member_pressure_flag = (
        documented_withdrawal
        and substantive_filed
        and not callup_local
        and not local_member_negative
        and not local_member_abstain
    )
    documented_hpd_udaap_withdrawal_no_member_pressure_flag = (
        documented_hpd_udaap_withdrawal and documented_withdrawal_no_member_pressure_flag
    )
    adverse_outcome_geography_only_candidate_flag = (
        adverse_outcome
        and bool(local_member_names)
        and not strong_candidate_local_member_callup_adverse_flag
        and not documented_withdrawal_no_member_pressure_flag
    )
    procedural_pressure_signal_flag = bool(
        confirmed_breach_flag
        or possible_breach_abstain_flag
        or strong_candidate_local_member_callup_adverse_flag
        or modification_with_member_signal_pending_audit_flag
        or adverse_outcome_geography_only_candidate_flag
        or split_approval
        or substantive_rereferred
        or substantive_modified
        or callup
    )
    confirmed_exercised_flag = False
    confirmed_accommodated_flag = False

    signal_flags = []
    if split_approval:
        signal_flags.append("split_final_approval_vote")
    if local_member_negative:
        signal_flags.append("local_member_negative_final_vote")
    if local_member_abstain:
        signal_flags.append("local_member_abstain_final_vote")
    if substantive_disapproved:
        signal_flags.append("council_disapproval")
    if substantive_filed:
        signal_flags.append("filed_or_no_final_action")
    if documented_hpd_udaap_withdrawal:
        signal_flags.append("admin_withdrawal_documented_hpd_udaap_resolution")
    elif documented_withdrawal:
        signal_flags.append("documented_withdrawal_official_resolution")
    if substantive_rereferred:
        signal_flags.append("re_referred")
    if substantive_modified:
        signal_flags.append("approved_with_modification")
    if callup:
        signal_flags.append("land_use_callup")

    if confirmed_breach_flag:
        classification = "confirmed_breach_local_member_no_vote"
        lm_code = "LM5_negative_vote"
        audit_priority = "high"
        deference_status = "confirmed_breach_local_member_no_vote"
        evidence_tier = "O1_local_member_no_vote_council_approval"
    elif possible_breach_abstain_flag:
        classification = "possible_breach_local_member_abstention"
        lm_code = "LM5_abstain_vote"
        audit_priority = "high"
        deference_status = "possible_breach_local_member_abstention"
        evidence_tier = "O1A_local_member_abstain_council_approval"
    elif strong_candidate_local_member_callup_council_disapproved_flag:
        classification = "strong_candidate_local_member_callup_council_disapproved"
        lm_code = "LM2_local_member_callup"
        audit_priority = "high"
        deference_status = "candidate_council_disapproved_after_local_member_callup"
        evidence_tier = "E3a_callup_plus_council_disapproval_pending_audit"
    elif strong_candidate_local_member_callup_filed_no_final_flag:
        classification = "strong_candidate_local_member_callup_filed_or_no_final_pending_validation"
        lm_code = "LM2_local_member_callup"
        audit_priority = "high"
        deference_status = "candidate_filed_or_no_final_after_local_member_callup"
        evidence_tier = "E3b_callup_plus_filed_no_final_pending_project_validation"
    elif documented_hpd_udaap_withdrawal_no_member_pressure_flag:
        classification = "not_deference_admin_withdrawal"
        lm_code = "LM1_local_member_identified" if local_member_names else "LM0_no_member_evidence"
        audit_priority = "high"
        deference_status = "admin_withdrawal_documented_no_member_pressure_evidence"
        evidence_tier = "N1_admin_withdrawal_documented_hpd_udaap_no_member_pressure"
    elif documented_withdrawal_no_member_pressure_flag:
        classification = "not_deference_documented_withdrawal_no_member_pressure"
        lm_code = "LM1_local_member_identified" if local_member_names else "LM0_no_member_evidence"
        audit_priority = "high"
        deference_status = "documented_withdrawal_no_member_pressure_evidence"
        evidence_tier = "N2_official_withdrawal_documented_no_member_pressure"
    elif adverse_outcome_geography_only_candidate_flag:
        classification = "adverse_outcome_geography_only_candidate"
        lm_code = "LM1_local_member_identified" if local_member_names else "LM0_no_member_evidence"
        audit_priority = "high"
        deference_status = "geography_only_adverse_outcome_candidate_unvalidated"
        evidence_tier = "Q3_geography_only_adverse_outcome_candidate"
    elif adverse_outcome:
        classification = "adverse_outcome_without_member_pressure_evidence"
        lm_code = "LM0_no_member_evidence"
        audit_priority = "high"
        deference_status = "adverse_outcome_without_member_pressure_evidence"
        evidence_tier = "Q4_adverse_outcome_without_member_pressure"
    elif modification_with_member_signal_pending_audit_flag:
        classification = "modification_with_member_signal_pending_substantive_audit"
        lm_code = "LM3_or_LM5_pressure_with_modification"
        audit_priority = "high"
        deference_status = "modification_with_member_signal_pending_substantive_audit"
        evidence_tier = "A2_or_A4_modification_pending_substantive_audit"
    elif substantive_modified or substantive_rereferred or callup:
        classification = "procedural_pressure_signal_only"
        lm_code = "LM2_callup_or_LM1_local_member_identified" if callup else "LM1_local_member_identified"
        audit_priority = "medium"
        deference_status = "procedural_pressure_signal_only"
        evidence_tier = "P1_procedural_signal_only"
    elif split_approval:
        classification = "split_vote_no_local_member_pressure_found"
        lm_code = "LM1_local_member_identified" if local_member_names else "LM0_no_member_evidence"
        audit_priority = "medium"
        deference_status = "contested_vote_no_local_member_pressure_found"
        evidence_tier = "P2_contested_vote_without_local_member_signal"
    else:
        classification = "not_candidate_before_control_sample"
        lm_code = "LM1_local_member_identified" if local_member_names else "LM0_no_member_evidence"
        audit_priority = "not_selected"
        deference_status = "no_observed_deference_signal"
        evidence_tier = "T0_no_observed_signal"

    if substantive_disapproved:
        council_project_outcome = "council_disapproved"
        outcome_code = "OUT3_disapproved"
    elif documented_hpd_udaap_withdrawal_no_member_pressure_flag:
        council_project_outcome = "filed_no_final"
        outcome_code = "OUT6_admin_withdrawal_documented_hpd_udaap_no_member_pressure"
    elif documented_withdrawal_no_member_pressure_flag:
        council_project_outcome = "filed_no_final"
        outcome_code = "OUT6_documented_withdrawal_no_member_pressure"
    elif substantive_filed:
        council_project_outcome = "filed_no_final"
        outcome_code = "OUT4_or_OUT5_filed_no_final_project_status_unvalidated"
    elif substantive_approved and substantive_modified:
        council_project_outcome = "approved_with_modification"
        outcome_code = "OUT1_or_OUT2_approved_with_modification_unclassified"
    elif substantive_approved:
        council_project_outcome = "approved_without_detected_modification"
        outcome_code = "OUT0_approved_without_detected_modification"
    else:
        council_project_outcome = "unknown_or_callup_only"
        outcome_code = "OUT7_unknown_or_callup_only"

    evidence_summary = collapse_values(
        [
            f"signals={', '.join(signal_flags)}" if signal_flags else "",
            f"local_negative={collapse_values(substantive['final_vote_local_member_negative'])}",
            f"local_abstain={collapse_values(substantive['final_vote_local_member_abstain'])}",
            f"callup_sponsors={collapse_values(group[group['callup_flag']]['prime_sponsor'])}" if callup else "",
            "ZAP project-status validation pending",
        ]
    )

    event_dates = [date for date in substantive["event_date"] if not pd.isna(date)]
    vote_dates = [date for date in substantive["final_council_approval_date"] if not pd.isna(date)]
    bundle_rows.append(
        {
            "preliminary_bundle_id": bundle_id,
            "query_years": collapse_values(group["query_year"]),
            "vote_year": str(min(vote_dates).year) if vote_dates else str(min(event_dates).year) if event_dates else "",
            "vote_date": min(vote_dates).strftime("%Y-%m-%d") if vote_dates else "",
            "event_date": min(event_dates).strftime("%Y-%m-%d") if event_dates else "",
            "project_name": representative_title,
            "borough": collapse_values(substantive["borough"]),
            "affected_council_districts": collapse_values(substantive["affected_council_districts_clean"]),
            "local_members": local_member_names,
            "local_member_source_tiers": collapse_values(substantive["local_member_source_tiers"]),
            "local_member_district_sources": collapse_values(substantive["local_member_district_sources"]),
            "missing_roster_districts": collapse_values(substantive["missing_roster_districts_at_event_date"]),
            "lu_numbers": collapse_values(substantive["matter_file"][substantive["matter_file"].str.startswith("LU ")]),
            "resolution_numbers": collapse_values(substantive["matter_file"][substantive["matter_file"].str.startswith("Res ")]),
            "callup_numbers": collapse_values(group["matter_file"][group["callup_flag"]]),
            "matter_ids": collapse_values(group["matter_id"]),
            "ulurp_numbers": collapse_values(app_keys),
            "action_code_families": collapse_values(action_code(key) for key in app_keys),
            "zap_project_ids": "",
            "zap_project_names": "",
            "zap_project_statuses": "",
            "zap_public_statuses": "",
            "zap_status_simple": "",
            "primary_applicants": "",
            "applicant_types": "",
            "private_applicant_flag": False,
            "public_applicant_flag": False,
            "zap_validation_status": "pending_not_linked_in_legistar_pilot",
            "residential_mixed_flag": residential_mixed_flag,
            "use_category": use_category,
            "sidewalk_cafe_flag": sidewalk_cafe_flag,
            "revocable_consent_flag": revocable_consent_flag,
            "sidewalk_cafe_or_revocable_consent_flag": sidewalk_cafe_or_revocable_consent_flag,
            "main_non_sidewalk_cafe_scope_flag": not sidewalk_cafe_flag,
            "core_land_use_scope_flag": not sidewalk_cafe_or_revocable_consent_flag,
            "udaap_flag": bool_any(substantive["udaap_flag"]),
            "ulurp_like_flag": bool_any(substantive["ulurp_like_flag"]),
            "split_final_approval_vote_flag": split_approval,
            "local_member_final_negative_flag": local_member_negative,
            "local_member_final_abstain_flag": local_member_abstain,
            "council_approved_flag": substantive_approved,
            "council_disapproved_flag": substantive_disapproved,
            "filed_no_final_flag": substantive_filed,
            "documented_withdrawal_flag": documented_withdrawal,
            "documented_hpd_udaap_withdrawal_flag": documented_hpd_udaap_withdrawal,
            "documented_withdrawal_no_member_pressure_flag": documented_withdrawal_no_member_pressure_flag,
            "withdrawal_source_codes_auto": collapse_values(substantive["withdrawal_source_code_auto"]),
            "council_project_outcome_auto": council_project_outcome,
            "re_referred_flag": substantive_rereferred,
            "approved_with_modification_flag": substantive_modified,
            "callup_flag": callup,
            "callup_sponsors": collapse_values(group[group["callup_flag"]]["prime_sponsor"]),
            "callup_sponsor_is_local_member": callup_local,
            "pressure_status_auto": deference_status,
            "evidence_tier_auto": evidence_tier,
            "confirmed_breach_flag": confirmed_breach_flag,
            "possible_breach_abstain_flag": possible_breach_abstain_flag,
            "confirmed_exercised_flag": confirmed_exercised_flag,
            "confirmed_accommodated_flag": confirmed_accommodated_flag,
            "strong_candidate_local_member_callup_adverse_flag": strong_candidate_local_member_callup_adverse_flag,
            "strong_candidate_local_member_callup_council_disapproved_flag": (
                strong_candidate_local_member_callup_council_disapproved_flag
            ),
            "strong_candidate_local_member_callup_filed_no_final_flag": (
                strong_candidate_local_member_callup_filed_no_final_flag
            ),
            "modification_with_member_signal_pending_audit_flag": modification_with_member_signal_pending_audit_flag,
            "adverse_outcome_geography_only_candidate_flag": adverse_outcome_geography_only_candidate_flag,
            "procedural_pressure_signal_flag": procedural_pressure_signal_flag,
            "candidate_signal_flags": collapse_values(signal_flags),
            "preaudit_classification": classification,
            "audit_priority": audit_priority,
            "local_member_evidence_code_auto": lm_code,
            "outcome_code_auto": outcome_code,
            "evidence_summary": evidence_summary,
            "source_urls": collapse_values(group["matter_url"]),
            "history_detail_urls": collapse_values(group["detail_history_detail_urls"]),
            "manual_review_status": "not_started",
            "manual_classification": "",
            "manual_local_member_position": "",
            "manual_project_outcome": "",
            "manual_substantive_modification": "",
            "exclusion_reason": "",
            "manual_notes": "",
            "ordinary_control_sample_flag": False,
        }
    )

bundles = pd.DataFrame(bundle_rows)
candidate_flag_columns = [
    "split_final_approval_vote_flag",
    "local_member_final_negative_flag",
    "local_member_final_abstain_flag",
    "council_disapproved_flag",
    "filed_no_final_flag",
    "re_referred_flag",
    "approved_with_modification_flag",
    "callup_flag",
]
bundles["high_recall_candidate_flag"] = bundles[candidate_flag_columns].any(axis=1)

controls = bundles[
    (~bundles["high_recall_candidate_flag"])
    & bundles["council_approved_flag"]
    & (bundles["affected_council_districts"] != "")
].copy()
control_ids = []
for year, group in controls.groupby("vote_year", dropna=False):
    group = group[group["vote_year"].isin(PILOT_YEARS)].copy()
    group["control_sort_key"] = group["preliminary_bundle_id"].map(source_key_hash)
    control_ids.extend(group.sort_values("control_sort_key").head(CONTROL_SAMPLE_PER_YEAR)["preliminary_bundle_id"].tolist())

bundles.loc[bundles["preliminary_bundle_id"].isin(control_ids), "ordinary_control_sample_flag"] = True
bundles.loc[bundles["ordinary_control_sample_flag"], "preaudit_classification"] = "ordinary_unanimous_approval_control"
bundles.loc[bundles["ordinary_control_sample_flag"], "audit_priority"] = "control"
bundles.loc[bundles["ordinary_control_sample_flag"], "candidate_signal_flags"] = "ordinary_control_sample"
bundles.loc[bundles["ordinary_control_sample_flag"], "pressure_status_auto"] = "ordinary_unanimous_approval_control"
bundles.loc[bundles["ordinary_control_sample_flag"], "evidence_tier_auto"] = "T0_control"

series_rows = []
for year in PILOT_YEARS:
    year_bundles = bundles[bundles["vote_year"].eq(year)].copy()
    universes = [
        ("all_bundles", year_bundles),
        ("non_sidewalk_cafe", year_bundles[~year_bundles["sidewalk_cafe_flag"]]),
        (
            "core_land_use_non_sidewalk_or_revocable_consent",
            year_bundles[year_bundles["core_land_use_scope_flag"]],
        ),
        ("residential_mixed", year_bundles[year_bundles["residential_mixed_flag"]]),
        (
            "commercial_non_sidewalk_cafe",
            year_bundles[year_bundles["use_category"].eq("commercial") & ~year_bundles["sidewalk_cafe_flag"]],
        ),
        (
            "city_project_or_public_non_sidewalk_cafe",
            year_bundles[year_bundles["use_category"].eq("city_project_or_public") & ~year_bundles["sidewalk_cafe_flag"]],
        ),
    ]
    for universe_name, universe in universes:
        denominator = len(universe)
        confirmed_breach_count = int(universe["confirmed_breach_flag"].sum())
        possible_breach_abstain_count = int(universe["possible_breach_abstain_flag"].sum())
        confirmed_exercised_count = int(universe["confirmed_exercised_flag"].sum())
        confirmed_accommodated_count = int(universe["confirmed_accommodated_flag"].sum())
        strong_candidate_count = int(universe["strong_candidate_local_member_callup_adverse_flag"].sum())
        strong_candidate_disapproved_count = int(
            universe["strong_candidate_local_member_callup_council_disapproved_flag"].sum()
        )
        strong_candidate_filed_count = int(
            universe["strong_candidate_local_member_callup_filed_no_final_flag"].sum()
        )
        modification_candidate_count = int(universe["modification_with_member_signal_pending_audit_flag"].sum())
        geography_only_count = int(universe["adverse_outcome_geography_only_candidate_flag"].sum())
        procedural_signal_count = int(universe["procedural_pressure_signal_flag"].sum())
        documented_withdrawal_count = int(universe["documented_withdrawal_flag"].sum())
        documented_hpd_udaap_withdrawal_count = int(universe["documented_hpd_udaap_withdrawal_flag"].sum())
        documented_withdrawal_no_pressure_count = int(
            universe["documented_withdrawal_no_member_pressure_flag"].sum()
        )
        series_rows.append(
            {
                "vote_year": year,
                "analysis_universe": universe_name,
                "bundle_count": denominator,
                "council_approved_count": int(universe["council_approved_flag"].sum()),
                "council_disapproved_count": int(universe["council_disapproved_flag"].sum()),
                "filed_no_final_count": int(universe["filed_no_final_flag"].sum()),
                "project_approved_outcome_count": int(
                    universe["council_project_outcome_auto"].str.startswith("approved", na=False).sum()
                ),
                "project_adverse_outcome_count": int(
                    universe["council_project_outcome_auto"].isin(["council_disapproved", "filed_no_final"]).sum()
                ),
                "approved_with_modification_count": int(universe["approved_with_modification_flag"].sum()),
                "re_referred_count": int(universe["re_referred_flag"].sum()),
                "callup_count": int(universe["callup_flag"].sum()),
                "local_member_callup_count": int(universe["callup_sponsor_is_local_member"].sum()),
                "confirmed_breach_count": confirmed_breach_count,
                "possible_breach_abstain_count": possible_breach_abstain_count,
                "confirmed_exercised_count": confirmed_exercised_count,
                "confirmed_accommodated_count": confirmed_accommodated_count,
                "strong_candidate_local_member_callup_adverse_count": strong_candidate_count,
                "strong_candidate_local_member_callup_council_disapproved_count": strong_candidate_disapproved_count,
                "strong_candidate_local_member_callup_filed_no_final_count": strong_candidate_filed_count,
                "modification_with_member_signal_pending_audit_count": modification_candidate_count,
                "adverse_outcome_geography_only_candidate_count": geography_only_count,
                "documented_withdrawal_count": documented_withdrawal_count,
                "documented_hpd_udaap_withdrawal_count": documented_hpd_udaap_withdrawal_count,
                "documented_withdrawal_no_member_pressure_count": documented_withdrawal_no_pressure_count,
                "procedural_pressure_signal_count": procedural_signal_count,
                "confirmed_breach_rate": confirmed_breach_count / denominator if denominator else 0,
                "possible_breach_abstain_rate": possible_breach_abstain_count / denominator if denominator else 0,
                "strong_candidate_rate": strong_candidate_count / denominator if denominator else 0,
                "procedural_signal_rate": procedural_signal_count / denominator if denominator else 0,
                "source_coverage": "legistar_broad_recall_pilot",
            }
        )

pressure_candidate_series = pd.DataFrame(series_rows)
pilot_year_bundles = bundles[bundles["vote_year"].isin(PILOT_YEARS)].copy()

selected = bundles[bundles["high_recall_candidate_flag"] | bundles["ordinary_control_sample_flag"]].copy()
selected = selected.sort_values(
    ["vote_year", "audit_priority", "event_date", "preliminary_bundle_id"], na_position="last"
).reset_index(drop=True)
selected["project_bundle_id"] = [f"mdpa_1998_2010_{i:04d}" for i in range(1, len(selected) + 1)]

bundle_id_lookup = dict(zip(selected["preliminary_bundle_id"], selected["project_bundle_id"]))
matter_crosswalk = matter[matter["preliminary_bundle_id"].isin(bundle_id_lookup)].copy()
matter_crosswalk["project_bundle_id"] = matter_crosswalk["preliminary_bundle_id"].map(bundle_id_lookup)
if "preliminary_bundle_ids_after_bridge" in bundle_bridge_review.columns:
    bundle_bridge_review["project_bundle_ids_after_bridge"] = bundle_bridge_review[
        "preliminary_bundle_ids_after_bridge"
    ].map(lambda value: collapse_values(bundle_id_lookup.get(bundle_id, "") for bundle_id in split_values(value)))
else:
    bundle_bridge_review["preliminary_bundle_ids_after_bridge"] = ""
    bundle_bridge_review["project_bundle_ids_after_bridge"] = ""
bundle_bridge_review = bundle_bridge_review[
    [
        "bridge_group_id",
        "bridge_decision_auto",
        "adjacency_strength",
        "query_year",
        "vote_date",
        "final_vote_margin",
        "final_vote_negative_members",
        "final_vote_abstain_members",
        "preliminary_bundle_ids_after_bridge",
        "preliminary_bundle_count_after_bridge",
        "project_bundle_ids_after_bridge",
        "bridge_review_scope_auto",
        "matter_keys",
        "matter_files",
        "application_keys",
        "action_code_families",
        "action_code_roots",
        "affected_council_districts",
        "local_members",
        "titles",
        "min_lu_gap_in_component",
        "min_application_gap_in_component",
        "hand_check_status",
        "hand_check_notes",
    ]
].sort_values(["query_year", "vote_date", "matter_files", "bridge_group_id"])
matter_crosswalk = matter_crosswalk[
    [
        "project_bundle_id",
        "preliminary_bundle_id",
        "query_year",
        "matter_id",
        "matter_file",
        "query_matter_type",
        "status",
        "committee",
        "prime_sponsor",
        "matter_url",
        "application_keys",
        "action_code_families",
        "affected_council_districts_clean",
        "local_members_at_event_date",
        "final_council_approval_flag",
        "council_disapproval_flag",
        "filed_no_final_flag",
        "documented_withdrawal_flag",
        "documented_hpd_udaap_withdrawal_flag",
        "withdrawal_source_code_auto",
        "re_referred_flag",
        "approved_with_modification_flag",
        "callup_flag",
        "split_final_approval_vote_flag",
        "local_member_final_negative_flag",
        "local_member_final_abstain_flag",
        "final_vote_margin",
        "final_vote_negative_members",
        "final_vote_abstain_members",
        "final_vote_local_member_negative",
        "final_vote_local_member_abstain",
        "title",
    ]
].sort_values(["project_bundle_id", "query_matter_type", "matter_file"])

selected = selected[
    ["project_bundle_id"]
    + [column for column in selected.columns if column not in ["project_bundle_id", "high_recall_candidate_flag"]]
    + ["high_recall_candidate_flag"]
]

audit_sheet = selected.copy()
pressure_candidate_audit_targets = selected[
    selected["procedural_pressure_signal_flag"] & ~selected["ordinary_control_sample_flag"]
].copy()
pressure_candidate_audit_targets = pressure_candidate_audit_targets.sort_values(
    [
        "evidence_tier_auto",
        "vote_year",
        "event_date",
        "project_bundle_id",
    ],
    na_position="last",
)

summary_rows = []
summary_rows.append({"metric": "selected_bundle_rows", "value": len(selected)})
summary_rows.append({"metric": "high_recall_candidate_bundles", "value": int(selected["high_recall_candidate_flag"].sum())})
summary_rows.append({"metric": "ordinary_control_sample_bundles", "value": int(selected["ordinary_control_sample_flag"].sum())})
summary_rows.append({"metric": "all_bundle_rows_loaded", "value": len(bundles)})
summary_rows.append(
    {
        "metric": "pilot_year_all_bundles_in_denominator",
        "value": int(
            pressure_candidate_series[pressure_candidate_series["analysis_universe"].eq("all_bundles")][
                "bundle_count"
            ].sum()
        ),
    }
)
summary_rows.append(
    {
        "metric": "confirmed_breach_bundles",
        "value": int(pilot_year_bundles["confirmed_breach_flag"].sum()),
    }
)
summary_rows.append(
    {
        "metric": "possible_breach_abstain_bundles",
        "value": int(pilot_year_bundles["possible_breach_abstain_flag"].sum()),
    }
)
summary_rows.append(
    {
        "metric": "confirmed_exercised_bundles",
        "value": int(pilot_year_bundles["confirmed_exercised_flag"].sum()),
    }
)
summary_rows.append(
    {
        "metric": "confirmed_accommodated_bundles",
        "value": int(pilot_year_bundles["confirmed_accommodated_flag"].sum()),
    }
)
summary_rows.append(
    {
        "metric": "strong_candidate_local_member_callup_adverse_bundles",
        "value": int(pilot_year_bundles["strong_candidate_local_member_callup_adverse_flag"].sum()),
    }
)
summary_rows.append(
    {
        "metric": "strong_candidate_local_member_callup_council_disapproved_bundles",
        "value": int(pilot_year_bundles["strong_candidate_local_member_callup_council_disapproved_flag"].sum()),
    }
)
summary_rows.append(
    {
        "metric": "strong_candidate_local_member_callup_filed_no_final_bundles",
        "value": int(pilot_year_bundles["strong_candidate_local_member_callup_filed_no_final_flag"].sum()),
    }
)
summary_rows.append(
    {
        "metric": "modification_with_member_signal_pending_audit_bundles",
        "value": int(pilot_year_bundles["modification_with_member_signal_pending_audit_flag"].sum()),
    }
)
summary_rows.append(
    {
        "metric": "adverse_outcome_geography_only_candidate_bundles",
        "value": int(pilot_year_bundles["adverse_outcome_geography_only_candidate_flag"].sum()),
    }
)
summary_rows.append(
    {
        "metric": "documented_withdrawal_bundles",
        "value": int(pilot_year_bundles["documented_withdrawal_flag"].sum()),
    }
)
summary_rows.append(
    {
        "metric": "documented_hpd_udaap_withdrawal_bundles",
        "value": int(pilot_year_bundles["documented_hpd_udaap_withdrawal_flag"].sum()),
    }
)
summary_rows.append(
    {
        "metric": "documented_withdrawal_no_member_pressure_bundles",
        "value": int(pilot_year_bundles["documented_withdrawal_no_member_pressure_flag"].sum()),
    }
)
summary_rows.append(
    {
        "metric": "procedural_pressure_signal_bundles",
        "value": int(pilot_year_bundles["procedural_pressure_signal_flag"].sum()),
    }
)
summary_rows.append(
    {
        "metric": "split_vote_package_auto_bridge_groups",
        "value": int(
            bundle_bridge_review["bridge_decision_auto"].eq(
                "auto_merge_same_split_vote_close_lu_application_multi_action"
            ).sum()
        ),
    }
)
summary_rows.append(
    {
        "metric": "split_vote_package_manual_review_groups",
        "value": int(bundle_bridge_review["bridge_decision_auto"].eq("manual_review_same_split_vote_adjacency").sum()),
    }
)
summary_rows.append(
    {
        "metric": "split_vote_package_remaining_manual_review_groups",
        "value": int(
            bundle_bridge_review["bridge_review_scope_auto"].eq("manual_review_remaining_multiple_bundles").sum()
        ),
    }
)
for column in candidate_flag_columns:
    summary_rows.append({"metric": f"{column}_bundles", "value": int(selected[column].sum())})
for label, count in selected["preaudit_classification"].value_counts().sort_index().items():
    summary_rows.append({"metric": f"preaudit_classification__{label}", "value": int(count)})
for label, count in pilot_year_bundles["pressure_status_auto"].value_counts().sort_index().items():
    summary_rows.append({"metric": f"pressure_status__{label}", "value": int(count)})
for year, count in selected["vote_year"].value_counts().sort_index().items():
    summary_rows.append({"metric": f"selected_bundles_vote_year_{year}", "value": int(count)})

summary = pd.DataFrame(summary_rows)

bundle_app_keys = set(key for keys in bundles["ulurp_numbers"].map(split_values) for key in keys)
auto_bridge_rows = bundle_bridge_review[
    bundle_bridge_review["bridge_decision_auto"].eq("auto_merge_same_split_vote_close_lu_application_multi_action")
].copy()
broadway_triangle_rows = selected[
    selected["ulurp_numbers"].map(
        lambda value: all(
            key in split_values(value)
            for key in ["090413ZMK", "090414ZRK", "090415HUK", "090416HAK"]
        )
    )
].copy()
uws_garage_rows = selected[
    selected["ulurp_numbers"].str.contains("010602ZSM|20010602ZSM", regex=True, na=False)
].copy()
nycem_rows = selected[
    selected["ulurp_numbers"].str.contains("030158PSK|20030158PSK", regex=True, na=False)
].copy()
qc = pd.DataFrame(
    [
        {
            "check_name": "matter_index_unique",
            "passed": not matter_index[["query_year", "matter_id"]].duplicated().any(),
            "detail": "Matter index rows are unique by query_year and matter_id.",
        },
        {
            "check_name": "bundle_ids_unique",
            "passed": not selected["project_bundle_id"].duplicated().any(),
            "detail": "Selected pilot audit rows are unique by project_bundle_id.",
        },
        {
            "check_name": "crosswalk_bundle_ids_valid",
            "passed": set(matter_crosswalk["project_bundle_id"]).issubset(set(selected["project_bundle_id"])),
            "detail": "Every matter crosswalk row points to a selected pilot bundle.",
        },
        {
            "check_name": "control_sample_limit",
            "passed": bool(
                selected[selected["ordinary_control_sample_flag"]]
                .groupby("vote_year")
                .size()
                .le(CONTROL_SAMPLE_PER_YEAR)
                .all()
            ),
            "detail": f"Ordinary controls are capped at {CONTROL_SAMPLE_PER_YEAR} per pilot year.",
        },
        {
            "check_name": "zap_validation_deferred",
            "passed": True,
            "detail": f"{len(bundle_app_keys)} parsed bundle application keys are staged for later ZAP/project-status validation.",
        },
        {
            "check_name": "confirmed_breach_vote_screen_has_local_member",
            "passed": bool(
                selected[
                    selected["preaudit_classification"].eq("confirmed_breach_local_member_no_vote")
                    & selected["local_members"].eq("")
                ].empty
            ),
            "detail": "Every local-member no-vote breach candidate has a matched local member.",
        },
        {
            "check_name": "confirmed_series_requires_manual_audit_or_no_vote",
            "passed": bool(
                bundles[
                    (bundles["confirmed_exercised_flag"] | bundles["confirmed_accommodated_flag"])
                    & bundles["manual_review_status"].eq("not_started")
                ].empty
            ),
            "detail": "No exercised/accommodated deference row is auto-confirmed before manual audit.",
        },
        {
            "check_name": "sidewalk_cafe_exclusion_flag_present",
            "passed": bool("sidewalk_cafe_flag" in selected.columns),
            "detail": "Sidewalk-cafe cases are flagged so main housing/non-cafe series can exclude them.",
        },
        {
            "check_name": "documented_withdrawal_no_pressure_downgraded",
            "passed": bool(
                selected[
                    selected["documented_withdrawal_no_member_pressure_flag"]
                    & selected["preaudit_classification"].isin(
                        [
                            "adverse_outcome_geography_only_candidate",
                            "adverse_outcome_without_member_pressure_evidence",
                        ]
                    )
                ].empty
            ),
            "detail": "Official withdrawal records without local-member pressure are separated from geography-only deference candidates.",
        },
        {
            "check_name": "annual_series_complete",
            "passed": bool(set(pressure_candidate_series["vote_year"]) == set(PILOT_YEARS)),
            "detail": "The pressure-candidate annual series covers each pilot year.",
        },
        {
            "check_name": "auto_split_vote_package_bridges_collapsed",
            "passed": bool(
                auto_bridge_rows.empty
                or auto_bridge_rows["preliminary_bundle_ids_after_bridge"].map(lambda value: len(split_values(value))).eq(1).all()
            ),
            "detail": "Each automatic split-vote package bridge collapses to one preliminary bundle.",
        },
        {
            "check_name": "known_broadway_triangle_single_bundle",
            "passed": bool(len(broadway_triangle_rows) == 1),
            "detail": "Broadway Triangle LUs 1227-1230 / apps 090413-090416 are represented as one project bundle.",
        },
        {
            "check_name": "known_uws_garage_callup_linked_to_lu",
            "passed": bool(len(uws_garage_rows) == 1),
            "detail": "Upper West Side garage call-up app 20010602ZSM links to LU app 010602ZSM.",
        },
        {
            "check_name": "known_nycem_callup_linked_to_lu",
            "passed": bool(len(nycem_rows) == 1),
            "detail": "NYCEM Headquarters call-up app 20030158PSK links to LU app 030158PSK.",
        },
    ]
)

selected.to_csv("../output/member_deference_pilot_1998_2010_bundle_candidates.csv", index=False)
audit_sheet.to_csv("../output/member_deference_pilot_1998_2010_audit_sheet.csv", index=False)
matter_crosswalk.to_csv("../output/member_deference_pilot_1998_2010_matter_crosswalk.csv", index=False)
bundle_bridge_review.to_csv("../output/member_deference_pilot_1998_2010_bundle_bridge_review.csv", index=False)
pressure_candidate_audit_targets.to_csv(
    "../output/member_deference_pilot_1998_2010_pressure_candidate_audit_targets.csv",
    index=False,
)
pressure_candidate_series.to_csv("../output/member_deference_pilot_1998_2010_pressure_candidate_series.csv", index=False)
summary.to_csv("../output/member_deference_pilot_1998_2010_summary.csv", index=False)
qc.to_csv("../output/member_deference_pilot_1998_2010_qc.csv", index=False)

if not qc["passed"].all():
    raise RuntimeError("Member-deference pilot audit failed QC.")
