from __future__ import annotations

import re
import unicodedata
from pathlib import Path

import pandas as pd

from data_reports import save_csv


# ULURP numbers have six digits (C 050173 PCM); HPD, landmark, and revocable-consent
# numbers have eight (20015008 TCX). The same pattern is used in every task.
APPLICATION_RE = re.compile(
    r"\b(?:[CNM]\s*)?\d{6,8}\s*(?:\([A-Z0-9]+\)\s*)?[A-Z]{2,4}\b",
    flags=re.IGNORECASE,
)

# District lists such as "5", "33 and 34", "15-17", or "33-37, 41&45". A number is
# at most two digits and cannot be the start of a longer number (CD1 980316 HUM).
DISTRICT_NUMBER = r"\d{1,2}(?!\d)"
DISTRICT_ITEM = rf"{DISTRICT_NUMBER}(?:\s*-\s*{DISTRICT_NUMBER})?"
DISTRICT_LIST = rf"{DISTRICT_ITEM}(?:\s*(?:,\s*and|,|&|and)\s*{DISTRICT_ITEM})*"
COUNCIL_DISTRICT_RE = re.compile(
    rf"Council\s+Districts?,?(?:\s*(?:No|Nos|Number)\.?)?\s*#?\s*({DISTRICT_LIST})",
    flags=re.IGNORECASE,
)
# "22CD" is a Council district abbreviation in Legistar agenda notes.
COUNCIL_DISTRICT_SUFFIX_RE = re.compile(r"\b(\d{1,2})CD\b", flags=re.IGNORECASE)
COUNCIL_DISTRICT_SHORT_RE = re.compile(
    rf"(?<![A-Za-z])CD(?:'?s)?\.?\s*#?\s*({DISTRICT_LIST})", flags=re.IGNORECASE
)
# "Queens CD 3" and "within CD 7" name Community Districts, not Council districts.
COMMUNITY_DISTRICT_CONTEXT_RE = re.compile(
    r"(?:Manhattan|Bronx|Brooklyn|Queens|Staten Island|\bSI|\bin|\bwithin)\s*$", flags=re.IGNORECASE
)
COMMUNITY_BOARD_RE = re.compile(r"\bCB\b|\bCB\s*#|\bCBs?\s*\d|Community\s+Boards?", flags=re.IGNORECASE)
BOROUGH_NAMES = [
    ("MANHATTAN", "1"),
    ("BRONX", "2"),
    ("BROOKLYN", "3"),
    ("QUEENS", "4"),
    ("STATEN ISLAND", "5"),
]


def normalize_space(value: object) -> str:
    return re.sub(r"\s+", " ", "" if value is None or pd.isna(value) else str(value)).strip()


def split_semicolon(value: object) -> list[str]:
    if value is None or pd.isna(value) or str(value).strip() == "":
        return []
    return [part.strip() for part in str(value).split(";") if part.strip()]


def application_keys(value: object) -> list[str]:
    keys = []
    for match in APPLICATION_RE.finditer("" if value is None or pd.isna(value) else str(value)):
        key = re.sub(r"[^A-Za-z0-9]", "", match.group(0)).upper()
        key = re.sub(r"^[CNM](?=\d)", "", key)
        if key not in keys:
            keys.append(key)
    return keys


def districts_from_list(text: str) -> list[str]:
    districts = []
    for start, end in re.findall(rf"({DISTRICT_NUMBER})(?:\s*-\s*({DISTRICT_NUMBER}))?", text):
        if end == "":
            districts.append(int(start))
        elif int(start) < int(end) <= 51:
            districts.extend(range(int(start), int(end) + 1))
    return [str(district) for district in districts if 1 <= district <= 51]


def council_districts_from_text(value: object) -> list[str]:
    """Explicit Council-district statements ("Council District no. 5", "22CD") win. A bare
    "CD" can also mean Community District, so it is read only when the text has no explicit
    statement, the mention is not a Community-district form, and either the text names a
    Community Board separately or the number is above 18 (no Community District is)."""
    text = normalize_space(value)
    districts = []
    for match in COUNCIL_DISTRICT_RE.finditer(text):
        districts.extend(districts_from_list(match.group(1)))
    for match in COUNCIL_DISTRICT_SUFFIX_RE.finditer(text):
        districts.extend(districts_from_list(match.group(1)))
    if not districts:
        names_community_board = bool(COMMUNITY_BOARD_RE.search(text))
        for match in COUNCIL_DISTRICT_SHORT_RE.finditer(text):
            if not COMMUNITY_DISTRICT_CONTEXT_RE.search(text[: match.start()]):
                districts.extend(
                    district for district in districts_from_list(match.group(1)) if names_community_board or int(district) > 18
                )
    return list(dict.fromkeys(districts))


def borough_code_from_text(text: object, keys: object) -> tuple[str, str]:
    """Borough from a unique borough name in the text, else from the application suffix."""
    text_upper = normalize_space(text).upper()
    borough_hits = {code for name, code in BOROUGH_NAMES if name in text_upper}
    if len(borough_hits) == 1:
        return borough_hits.pop(), "unique_text_borough"
    return borough_code_from_application_suffix(keys)


def borough_code_from_application_suffix(keys: object) -> tuple[str, str]:
    suffix_codes = []
    for key in split_semicolon(keys):
        key = key.upper()
        if key.endswith("M"):
            suffix_codes.append("1")
        if key.endswith("X"):
            suffix_codes.append("2")
        if key.endswith("K"):
            suffix_codes.append("3")
        if key.endswith("Q"):
            suffix_codes.append("4")
        if key.endswith("R"):
            suffix_codes.append("5")

    suffix_codes = list(dict.fromkeys(suffix_codes))
    if len(suffix_codes) == 1:
        return suffix_codes[0], "application_suffix"
    return "", ""


def lot_numbers_from_text(value: str, max_range: int) -> list[int]:
    lots = []
    for start, end in re.findall(r"(\d{1,4})\s*-\s*(\d{1,4})", value):
        start_int = int(start)
        end_int = int(end)
        if start_int <= end_int and end_int - start_int <= max_range:
            lots.extend(range(start_int, end_int + 1))

    without_ranges = re.sub(r"\d{1,4}\s*-\s*\d{1,4}", " ", value)
    lots.extend(int(match) for match in re.findall(r"\d{1,4}", without_ranges))
    return list(dict.fromkeys(lots))


def collapse_districts(values: object) -> str:
    districts = []
    for value in values:
        if value is None or pd.isna(value):
            continue
        for match in re.findall(r"\d{1,2}", str(value)):
            district = int(match)
            if 1 <= district <= 51 and str(district) not in districts:
                districts.append(str(district))
    return "; ".join(districts)


def collapse_values(values: object) -> str:
    clean_values = []
    for value in values:
        if value is None or pd.isna(value) or str(value).strip() == "":
            continue
        if str(value) not in clean_values:
            clean_values.append(str(value))
    return "; ".join(clean_values)


def collapse_semicolon_values(values: object) -> str:
    clean_values = []
    for value in values:
        if value is None or pd.isna(value) or str(value).strip() == "":
            continue
        for part in split_semicolon(value):
            if part not in clean_values:
                clean_values.append(part)
    return "; ".join(clean_values)


def collapse_examples(values: object, limit: int = 20) -> str:
    examples = []
    for value in values:
        if value is None or pd.isna(value) or str(value).strip() == "":
            continue
        value_text = str(value)
        if value_text not in examples:
            examples.append(value_text)
    return "; ".join(examples[:limit])


def district_from_scalar(value: object) -> str:
    return collapse_districts([value])


# Roll-call values that record no position on the motion. Abstentions are grouped here:
# Council abstentions are usually recusals, so they are not read as opposition.
NOT_VOTING_VALUES = {
    "Abstain",
    "Absent",
    "Bereavement",
    "Excused",
    "Maternity",
    "Medical",
    "Non-voting",
    "Parental",
    "Paternity",
    "Present",
    "Recused",
    "Suspended",
}


def surname(value: object) -> str:
    text = "".join(c for c in unicodedata.normalize("NFKD", normalize_space(value)) if not unicodedata.combining(c))
    parts = re.sub(r"[^a-z ]+", " ", text.lower()).split()
    parts = [part for part in parts if len(part) > 1 and part not in {"jr", "sr", "ii", "iii", "iv"}]
    return parts[-1] if parts else ""


def read_roster_by_district(path: str) -> dict[str, list[dict[str, object]]]:
    roster = pd.read_csv(path, dtype=str, keep_default_na=False)
    roster["term_start"] = pd.to_datetime(roster["term_start_date"], format="%Y-%m-%d")
    roster["term_end"] = pd.to_datetime(roster["term_end_date"].replace("", "2100-12-31"), format="%Y-%m-%d")
    return {district: rows.to_dict("records") for district, rows in roster.groupby("district")}


def local_roster_rows(
    roster_by_district: dict[str, list[dict[str, object]]], districts: list[str], date: object
) -> tuple[list[dict[str, object]], list[str]]:
    """Members serving each affected district on the vote date, and districts with no member."""
    local_rows = []
    missing_districts = []
    for district in districts:
        district_key = str(int(district))
        matches = [
            row
            for row in roster_by_district.get(district_key, [])
            if not pd.isna(date) and row["term_start"] <= date <= row["term_end"]
        ]
        local_rows.extend(matches)
        if not matches:
            missing_districts.append(district_key)
    return local_rows, missing_districts


def local_member_votes(local_rows: list[dict[str, object]], vote_rows: list[dict[str, object]]) -> list[dict[str, str]]:
    """Match each local member to the roll call by Legistar person_id. Roster rows without a
    person_id (Wikipedia gap fills) match by surname only when exactly one voter shares it."""
    matched = []
    for local in local_rows:
        if local["person_id"]:
            matches = [vote for vote in vote_rows if vote["person_id"] == local["person_id"]]
            method = "person_id"
        else:
            matches = [vote for vote in vote_rows if surname(vote["person_name"]) == surname(local["member_name"])]
            method = "unique_surname"
        if method == "person_id" and len(matches) > 1:
            raise RuntimeError(f"Person {local['person_id']} appears more than once in one roll call.")
        if len(matches) != 1:
            method = "ambiguous_surname" if matches else "not_in_roll_call"
        vote = matches[0]["vote"] if len(matches) == 1 else ""
        if vote and vote not in NOT_VOTING_VALUES | {"Affirmative", "Negative"}:
            raise RuntimeError(f"Unrecognized Legistar vote value: {vote}")
        matched.append(
            {
                "member_name": local["member_name"],
                "person_id": local["person_id"],
                "vote": vote,
                "match_method": method,
            }
        )
    return matched


def local_vote_status(
    districts: list[str], missing_roster_districts: list[str], local_votes: list[dict[str, str]], vote_row_count: int
) -> str:
    """One matter-level reading of the local members' roll-call votes, used on both the
    approval and nonapproval sides. Any local Negative is a local no vote."""
    votes = [local["vote"] for local in local_votes if local["vote"]]
    if not districts:
        return "no_affected_district"
    if missing_roster_districts:
        return "missing_roster"
    if vote_row_count == 0:
        return "no_member_vote_rows"
    if not votes:
        return "local_member_missing_from_vote_rows"
    if "Negative" in votes:
        return "local_member_negative"
    if len(votes) < len(local_votes):
        return "partial_local_member_vote_match"
    if set(votes) == {"Affirmative"}:
        return "local_member_affirmative_only"
    if set(votes) <= NOT_VOTING_VALUES:
        return "local_member_not_voting_only"
    return "local_member_affirmative_and_not_voting"


def save_frame(df: pd.DataFrame, path: str, key: list[str]) -> None:
    """Save a data frame through the shared SaveData routine (CSV plus ../report JSON)."""
    rows = df.astype(object).where(df.notna(), None).to_dict("records")
    save_csv(rows, list(df.columns), path, key)


def write_csv(path: str, df: pd.DataFrame) -> None:
    temp_path = Path(path).with_suffix(Path(path).suffix + ".tmp")
    df.to_csv(temp_path, index=False)
    temp_path.replace(path)
