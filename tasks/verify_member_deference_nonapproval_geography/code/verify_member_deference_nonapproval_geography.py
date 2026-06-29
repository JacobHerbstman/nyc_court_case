# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/verify_member_deference_nonapproval_geography/code")

from __future__ import annotations

import hashlib
import re
import time
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import pandas as pd
import requests
from bs4 import BeautifulSoup


application_re = re.compile(
    r"\b(?:[CNM]\s*)?\d{6,8}\s*(?:\([A-Z0-9]+\)\s*)?[A-Z]{2,4}\b",
    flags=re.IGNORECASE,
)
district_re = re.compile(
    r"Council District(?:s)?(?:\s*(?:No\.?|Nos\.?|no\.?|nos\.?|number)?)?\s*([0-9,\sand-]+)",
    flags=re.IGNORECASE,
)
lu_re = re.compile(r"\bL\.?\s*U\.?\s*(?:No\.?)?\s*(\d{1,4})(?:\s*-\s*\d{4})?\b", flags=re.IGNORECASE)


def normalize_space(value: object) -> str:
    return re.sub(r"\s+", " ", "" if pd.isna(value) else str(value)).strip()


def split_semicolon(value: object) -> list[str]:
    if pd.isna(value) or str(value).strip() == "":
        return []
    return [part.strip() for part in str(value).split(";") if part.strip()]


def collapse_values(values: object) -> str:
    clean_values = []
    for value in values:
        if pd.isna(value) or str(value).strip() == "":
            continue
        for part in split_semicolon(value):
            if part not in clean_values:
                clean_values.append(part)
    return "; ".join(clean_values)


def collapse_districts(values: object) -> str:
    districts = []
    for value in values:
        if pd.isna(value):
            continue
        for match in re.findall(r"\d{1,2}", str(value)):
            district = int(match)
            if 1 <= district <= 51 and str(district) not in districts:
                districts.append(str(district))
    return "; ".join(districts)


def collapse_examples(values: object, limit: int = 20) -> str:
    examples = []
    for value in values:
        if pd.isna(value) or str(value).strip() == "":
            continue
        value_text = str(value)
        if value_text not in examples:
            examples.append(value_text)
    return "; ".join(examples[:limit])


def application_keys(value: object) -> list[str]:
    keys = []
    for match in application_re.finditer("" if pd.isna(value) else str(value)):
        key = re.sub(r"[^A-Za-z0-9]", "", match.group(0)).upper()
        key = re.sub(r"^[CNM](?=\d)", "", key)
        if key not in keys:
            keys.append(key)
    return keys


def lu_numbers(value: object) -> list[str]:
    numbers = []
    for match in lu_re.finditer("" if pd.isna(value) else str(value)):
        number = str(int(match.group(1)))
        if number not in numbers:
            numbers.append(number)
    return numbers


def districts_from_text(value: object) -> str:
    districts = []
    for match in district_re.finditer("" if pd.isna(value) else str(value)):
        districts.extend(re.findall(r"\d{1,2}", match.group(1)))
    return collapse_districts(districts)


def norm_name(value: object) -> str:
    value = re.sub(r"[^A-Za-z0-9 ]", " ", "" if pd.isna(value) else str(value))
    return re.sub(r"\s+", " ", value).strip().lower()


def clean_url(value: object) -> str:
    url = normalize_space(value)
    if url == "":
        return ""
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    query.pop("utm_source", None)
    clean_query = urlencode(query, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, clean_query, parsed.fragment)).replace(
        "|", "%7C"
    )


def source_id_from_url(value: object) -> str:
    query = parse_qs(urlparse(normalize_space(value)).query)
    return (query.get("ID") or [""])[0]


def source_cache_path(source_url: str, source_role: str) -> Path:
    source_id = source_id_from_url(source_url)
    if source_id == "":
        source_id = hashlib.sha1(source_url.encode("utf-8")).hexdigest()[:16]
    return Path("../output/source_files/official_legistar_pages") / source_role / f"{source_id}.html"


def request_with_retries(session: requests.Session, url: str) -> requests.Response:
    last_error = None
    for attempt in range(1, 4):
        try:
            response = session.get(url, timeout=60)
            if response.status_code == 200:
                return response
            last_error = RuntimeError(f"HTTP {response.status_code}")
        except requests.RequestException as exc:
            last_error = exc
        time.sleep(2 * attempt)
    if last_error is not None:
        raise last_error
    raise RuntimeError("Legistar request failed without an exception.")


def fetch_source_text(session: requests.Session, source_url: str, source_role: str) -> tuple[str, str, str]:
    raw_path = source_cache_path(source_url, source_role)
    if raw_path.exists() and raw_path.stat().st_size > 0:
        html = raw_path.read_text(encoding="utf-8")
        return "200", "", normalize_space(BeautifulSoup(html, "html.parser").get_text(" "))

    response = request_with_retries(session, source_url)
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_text(response.text, encoding="utf-8")
    time.sleep(0.05)
    return str(response.status_code), "", normalize_space(BeautifulSoup(response.text, "html.parser").get_text(" "))


def borough_code_from_text(text: object, keys: object) -> tuple[str, str]:
    text_upper = normalize_space(text).upper()
    borough_hits = []
    for name, code in [
        ("MANHATTAN", "1"),
        ("THE BRONX", "2"),
        ("BRONX", "2"),
        ("BROOKLYN", "3"),
        ("QUEENS", "4"),
        ("STATEN ISLAND", "5"),
    ]:
        if name in text_upper and code not in borough_hits:
            borough_hits.append(code)
    if len(borough_hits) == 1:
        return borough_hits[0], "official_text_borough"

    suffix_hits = []
    for key in split_semicolon(keys):
        key = key.upper()
        if key.endswith("M"):
            suffix_hits.append("1")
        if key.endswith("X"):
            suffix_hits.append("2")
        if key.endswith("K"):
            suffix_hits.append("3")
        if key.endswith("Q"):
            suffix_hits.append("4")
        if key.endswith("R"):
            suffix_hits.append("5")
    suffix_hits = list(dict.fromkeys(suffix_hits))
    if len(suffix_hits) == 1:
        return suffix_hits[0], "application_suffix"
    return "", ""


def lot_numbers(value: str) -> list[int]:
    lots = []
    for start, end in re.findall(r"(\d{1,4})\s*-\s*(\d{1,4})", value):
        start_int = int(start)
        end_int = int(end)
        if start_int <= end_int and end_int - start_int <= 250:
            lots.extend(range(start_int, end_int + 1))

    without_ranges = re.sub(r"\d{1,4}\s*-\s*\d{1,4}", " ", value)
    lots.extend(int(match) for match in re.findall(r"\d{1,4}", without_ranges))
    return list(dict.fromkeys(lots))


def bbls_from_text(text: object, keys: object) -> tuple[str, str, str]:
    borough_code, borough_source = borough_code_from_text(text, keys)
    if borough_code == "":
        return "", "", ""

    clean_text = normalize_space(text).replace("–", "-").replace("—", "-")
    bbls = []
    for match in re.finditer(
        (
            r"Block\s+(\d{1,5})\s*(?:/|,|\s+)\s*"
            r"(?:p\.?\s*o\.?\s*|part\s+of\s+)?Lots?\s+"
            r"(.+?)(?=[);.]|,\s*(?:Manhattan|Brooklyn|Queens|Bronx|Staten Island|Borough)\b|$)"
        ),
        clean_text,
        flags=re.IGNORECASE,
    ):
        block = int(match.group(1))
        for lot in lot_numbers(match.group(2)):
            bbls.append(f"{int(borough_code)}{block:05d}{lot:04d}")

    for match in re.findall(r"\b[1-5]\d{9}\b", clean_text):
        bbls.append(match)

    return "; ".join(dict.fromkeys(bbls)), borough_code, borough_source


def source_snippet(text: str, needles: list[str]) -> str:
    for needle in needles:
        if needle == "":
            continue
        match = re.search(re.escape(needle), text, flags=re.IGNORECASE)
        if match is not None:
            start = max(0, match.start() - 180)
            end = min(len(text), match.end() + 320)
            return text[start:end].strip()
    return text[:500].strip()


def write_csv(path: str, df: pd.DataFrame) -> None:
    temp_path = Path(path).with_suffix(Path(path).suffix + ".tmp")
    df.to_csv(temp_path, index=False)
    temp_path.replace(path)


queue = pd.read_csv("../input/member_deference_nonapproval_geography_review_queue.csv", dtype=str, keep_default_na=False)
full_queue = pd.read_csv("../input/member_deference_final_action_vote_queue.csv", dtype=str, keep_default_na=False)
full_queue = full_queue[full_queue["fetch_vote_detail_first_pass"].str.lower().eq("true")].copy()
recovery = pd.read_csv("../input/member_deference_nonapproval_geography_recovery.csv", dtype=str, keep_default_na=False)
recovery["recovered_affected_district_missing_bool"] = recovery["recovered_affected_district_missing"].str.lower().eq("true")
chatgpt = pd.read_csv(
    "../input/member_deference_nonapproval_geography_chatgpt_review_responses.csv", dtype=str, keep_default_na=False
)
matter_universe = pd.read_csv("../input/member_deference_matter_universe.csv", dtype=str, keep_default_na=False)
mappluto = pd.read_parquet("../input/mappluto_current_lot_lookup.parquet", columns=["bbl", "council"])
roster = pd.read_csv("../input/council_member_roster_master.csv", dtype=str, keep_default_na=False)

if queue["matter_id"].duplicated().any():
    raise RuntimeError("Review queue must be unique by matter_id.")
if full_queue["matter_id"].duplicated().any():
    raise RuntimeError("Full final-action queue must be unique by matter_id.")
if recovery["matter_id"].duplicated().any():
    raise RuntimeError("Geography recovery output must be unique by matter_id.")
if chatgpt["matter_file"].duplicated().any():
    raise RuntimeError("ChatGPT review responses must be unique by matter_file.")
if not chatgpt["matter_file"].isin(set(queue["matter_file"])).all():
    raise RuntimeError("Every ChatGPT review response must link to the current review queue.")
if matter_universe["matter_id"].duplicated().any():
    raise RuntimeError("Matter universe must be unique by matter_id.")

queue = queue.merge(
    chatgpt[
        [
            "matter_file",
            "likely_location",
            "likely_current_or_historical_council_district",
            "confidence_high_medium_low",
            "official_source_to_check_or_source_url",
            "reasoning_notes",
            "source_links_found_in_cell",
        ]
    ],
    on="matter_file",
    how="left",
    validate="one_to_one",
)
queue["chatgpt_response_found"] = queue["likely_current_or_historical_council_district"].notna()
for col in [
    "likely_location",
    "likely_current_or_historical_council_district",
    "confidence_high_medium_low",
    "official_source_to_check_or_source_url",
    "reasoning_notes",
    "source_links_found_in_cell",
]:
    queue[col] = queue[col].fillna("")

queue = queue.merge(
    matter_universe[["matter_id", "matter_url", "final_history_detail_url"]],
    on="matter_id",
    how="left",
    validate="one_to_one",
)
if queue["matter_url"].eq("").any():
    raise RuntimeError("Every review-queue row must have a Legistar matter URL.")

mappluto["bbl"] = mappluto["bbl"].astype(str)
mappluto["current_mappluto_council_district"] = mappluto["council"].map(lambda x: collapse_districts([x]))
mappluto_bbl_lookup = (
    mappluto.groupby("bbl", as_index=False)
    .agg(current_mappluto_council_districts=("current_mappluto_council_district", collapse_districts))
)
if mappluto_bbl_lookup["bbl"].duplicated().any():
    raise RuntimeError("Current MapPLUTO BBL lookup must be unique by BBL.")

source_rows = []
session = requests.Session()
session.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Safari/537.36",
        "Referer": "https://legistar.council.nyc.gov/Legislation.aspx",
    }
)
for row in queue.to_dict("records"):
    urls = [
        {
            "source_role": "exact_matter_legistar",
            "source_url": clean_url(row["matter_url"]),
        }
    ]
    for source_url in split_semicolon(str(row["source_links_found_in_cell"]).replace(",", ";")):
        clean_source_url = clean_url(source_url)
        if urlparse(clean_source_url).netloc.lower() == "legistar.council.nyc.gov":
            urls.append({"source_role": "chatgpt_legistar_link", "source_url": clean_source_url})

    seen_urls = set()
    for url_row in urls:
        if url_row["source_url"] == "" or url_row["source_url"] in seen_urls:
            continue
        seen_urls.add(url_row["source_url"])

        fetched_text = ""
        fetch_status = ""
        fetch_error = ""
        try:
            fetch_status, fetch_error, fetched_text = fetch_source_text(
                session,
                url_row["source_url"],
                url_row["source_role"],
            )
        except Exception as exc:
            fetch_status = "error"
            fetch_error = str(exc)

        row_application_keys = set(split_semicolon(row["application_keys"]))
        source_application_keys = set(application_keys(fetched_text))
        row_lu_numbers = set(lu_numbers(row["title"]))
        source_lu_numbers = set(lu_numbers(fetched_text))
        source_id = source_id_from_url(url_row["source_url"])

        if source_id == str(row["matter_id"]):
            source_relation = "same_matter_id"
        elif row_application_keys and source_application_keys and row_application_keys.intersection(source_application_keys):
            source_relation = "application_key_overlap"
        elif row_lu_numbers and source_lu_numbers and row_lu_numbers.intersection(source_lu_numbers):
            source_relation = "lu_number_overlap"
        else:
            source_relation = "no_relation_match"

        official_bbls, official_borough_code, official_borough_source = bbls_from_text(fetched_text, row["application_keys"])
        direct_districts = districts_from_text(fetched_text)
        source_rows.append(
            {
                "matter_id": row["matter_id"],
                "matter_file": row["matter_file"],
                "source_role": url_row["source_role"],
                "source_url": url_row["source_url"],
                "source_path": urlparse(url_row["source_url"]).path,
                "source_legistar_id": source_id,
                "fetch_status": fetch_status,
                "fetch_error": fetch_error,
                "fetched_text_characters": len(fetched_text),
                "source_relation_to_matter": source_relation,
                "row_application_keys": "; ".join(sorted(row_application_keys)),
                "source_application_keys": "; ".join(sorted(source_application_keys)),
                "row_lu_numbers": "; ".join(sorted(row_lu_numbers, key=lambda x: int(x))),
                "source_lu_numbers": "; ".join(sorted(source_lu_numbers, key=lambda x: int(x))),
                "official_direct_council_districts": direct_districts,
                "official_bbls": official_bbls,
                "official_borough_code_for_bbl_parse": official_borough_code,
                "official_borough_source_for_bbl_parse": official_borough_source,
                "official_text_snippet": source_snippet(
                    fetched_text,
                    [
                        "Council District",
                        next(iter(row_application_keys), ""),
                        f"L.U. No. {next(iter(row_lu_numbers), '')}" if row_lu_numbers else "",
                        "Block",
                    ],
                ),
            }
        )

sources = pd.DataFrame(source_rows)
sources["source_legislation_detail"] = sources["source_path"].str.contains("LegislationDetail.aspx", case=False, na=False)
sources["acceptable_related_source"] = sources["source_relation_to_matter"].isin(
    ["same_matter_id", "application_key_overlap", "lu_number_overlap"]
)
sources["acceptable_direct_district_source"] = (
    sources["source_legislation_detail"]
    & sources["acceptable_related_source"]
    & sources["official_direct_council_districts"].ne("")
)

direct_candidates = sources[sources["acceptable_direct_district_source"]].copy()
direct_candidates["source_relation_rank"] = direct_candidates["source_relation_to_matter"].map(
    {"same_matter_id": 1, "application_key_overlap": 2, "lu_number_overlap": 3}
)
direct_candidates = direct_candidates.sort_values(
    ["matter_id", "source_relation_rank", "source_role", "source_url"]
).drop_duplicates("matter_id")
direct_candidates = direct_candidates[
    [
        "matter_id",
        "official_direct_council_districts",
        "source_url",
        "source_relation_to_matter",
        "official_text_snippet",
    ]
].rename(
    columns={
        "official_direct_council_districts": "verified_direct_official_districts",
        "source_url": "verified_direct_official_url",
        "source_relation_to_matter": "verified_direct_official_relation",
        "official_text_snippet": "verified_direct_official_snippet",
    }
)

exact_sources = sources[sources["source_role"].eq("exact_matter_legistar")].copy()
official_bbl_long = (
    exact_sources.assign(official_bbl=exact_sources["official_bbls"].str.split("; "))
    .explode("official_bbl")
    .loc[:, ["matter_id", "official_bbl"]]
)
official_bbl_long = official_bbl_long[official_bbl_long["official_bbl"].fillna("") != ""].drop_duplicates()
if official_bbl_long.empty:
    official_bbl_summary = pd.DataFrame(columns=["matter_id"])
else:
    official_bbl_summary = (
        official_bbl_long.merge(
            mappluto_bbl_lookup,
            left_on="official_bbl",
            right_on="bbl",
            how="left",
            validate="many_to_one",
        )
        .groupby("matter_id", as_index=False)
        .agg(
            official_matter_bbl_count=("official_bbl", "nunique"),
            official_matter_bbl_current_mappluto_match_count=(
                "current_mappluto_council_districts",
                lambda x: int(x.fillna("").ne("").sum()),
            ),
            official_matter_bbl_current_mappluto_districts=("current_mappluto_council_districts", collapse_districts),
            official_matter_bbl_examples=("official_bbl", collapse_values),
        )
    )

verification = queue.merge(direct_candidates, on="matter_id", how="left", validate="one_to_one")
verification = verification.merge(official_bbl_summary, on="matter_id", how="left", validate="one_to_one")

for col in [
    "verified_direct_official_districts",
    "verified_direct_official_url",
    "verified_direct_official_relation",
    "verified_direct_official_snippet",
    "official_matter_bbl_current_mappluto_districts",
    "official_matter_bbl_examples",
]:
    if col not in verification.columns:
        verification[col] = ""
    verification[col] = verification[col].fillna("")
for col in ["official_matter_bbl_count", "official_matter_bbl_current_mappluto_match_count"]:
    if col not in verification.columns:
        verification[col] = 0
    verification[col] = verification[col].fillna(0).astype(int)

verification["chatgpt_suggested_districts_parsed"] = verification[
    "likely_current_or_historical_council_district"
].map(lambda x: collapse_districts([x]))
verification["official_matter_bbl_unmatched_count"] = (
    verification["official_matter_bbl_count"] - verification["official_matter_bbl_current_mappluto_match_count"]
)


def verification_result(row: pd.Series) -> tuple[str, str, str, str, str, str]:
    if row["verified_direct_official_districts"] != "":
        return (
            "verified_direct_official_district",
            row["verified_direct_official_districts"],
            "direct_official_legistar",
            row["verified_direct_official_url"],
            row["verified_direct_official_relation"],
            "Official Legistar legislation page states Council District for the same or related LU/application record.",
        )
    if (
        row["official_matter_bbl_count"] > 0
        and row["official_matter_bbl_unmatched_count"] == 0
        and row["official_matter_bbl_current_mappluto_districts"] != ""
    ):
        return (
            "verified_official_bbl_to_current_mappluto",
            row["official_matter_bbl_current_mappluto_districts"],
            "official_matter_bbl_current_mappluto",
            row["matter_url"],
            "same_matter_id",
            "Exact official matter page gives BBLs and all parsed BBLs match current MapPLUTO Council districts.",
        )
    if row["official_matter_bbl_count"] > 0 and row["official_matter_bbl_current_mappluto_match_count"] > 0:
        return (
            "needs_manual_review_partial_bbl_current_mappluto",
            row["official_matter_bbl_current_mappluto_districts"],
            "partial_official_matter_bbl_current_mappluto",
            row["matter_url"],
            "same_matter_id",
            "Official matter page gives BBLs, but only some parsed BBLs match current MapPLUTO.",
        )
    if row["official_matter_bbl_count"] > 0:
        return (
            "needs_manual_review_bbl_not_in_current_mappluto",
            "",
            "official_matter_bbl_unmatched_current_mappluto",
            row["matter_url"],
            "same_matter_id",
            "Official matter page gives BBLs, but none of the parsed BBLs match current MapPLUTO.",
        )
    return (
        "needs_manual_review_no_verified_geography",
        "",
        "no_accepted_official_geography",
        "",
        "",
        "No accepted official direct district and no exact-matter BBLs that can be mapped to current MapPLUTO.",
    )


result_columns = verification.apply(verification_result, axis=1, result_type="expand")
verification["verification_status"] = result_columns[0]
verification["verified_districts"] = result_columns[1]
verification["verification_evidence_level"] = result_columns[2]
verification["verification_source_url"] = result_columns[3]
verification["verification_source_relation"] = result_columns[4]
verification["verification_notes"] = result_columns[5]
verification["verified_districts_match_chatgpt"] = verification.apply(
    lambda row: row["verified_districts"] != ""
    and set(split_semicolon(row["verified_districts"]))
    == set(split_semicolon(row["chatgpt_suggested_districts_parsed"])),
    axis=1,
)

roster["term_start_date_parsed"] = pd.to_datetime(roster["term_start_date"], errors="coerce")
roster["term_end_date_parsed"] = pd.to_datetime(roster["term_end_date"], errors="coerce").fillna(
    pd.Timestamp("2100-01-01")
)
roster["member_name_norm"] = roster["member_name"].map(norm_name)
roster = roster[roster["member_name_norm"] != "vacant"].copy()

conservative_queue = full_queue.merge(
    recovery[
        [
            "matter_id",
            "recovered_affected_council_districts",
            "geography_recovery_method",
            "geography_recovery_confidence",
            "geography_recovery_source",
            "original_affected_district_missing",
            "recovered_affected_district_missing",
        ]
    ],
    on="matter_id",
    how="left",
    validate="one_to_one",
)
conservative_queue = conservative_queue.merge(
    verification[
        [
            "matter_id",
            "verification_status",
            "verified_districts",
            "verification_evidence_level",
            "verification_source_url",
            "verification_source_relation",
        ]
    ],
    on="matter_id",
    how="left",
    validate="one_to_one",
)
for col in [
    "recovered_affected_council_districts",
    "geography_recovery_method",
    "geography_recovery_confidence",
    "geography_recovery_source",
    "original_affected_district_missing",
    "recovered_affected_district_missing",
    "verification_status",
    "verified_districts",
    "verification_evidence_level",
    "verification_source_url",
    "verification_source_relation",
]:
    conservative_queue[col] = conservative_queue[col].fillna("")


def conservative_geography(row: pd.Series) -> tuple[str, str, str, str, str]:
    if row["affected_council_districts"] != "":
        return (
            row["affected_council_districts"],
            row["affected_district_source"],
            "original_legistar_or_zap_geography",
            row["affected_district_source"],
            "accepted_original_queue_geography",
        )
    if row["recovered_affected_council_districts"] != "":
        return (
            row["recovered_affected_council_districts"],
            row["geography_recovery_method"],
            row["geography_recovery_confidence"],
            row["geography_recovery_source"],
            "accepted_deterministic_recovery",
        )
    if row["verification_status"].startswith("verified_") and row["verified_districts"] != "":
        return (
            row["verified_districts"],
            row["verification_status"],
            row["verification_evidence_level"],
            row["verification_source_url"],
            "accepted_first_pass_official_verification",
        )
    return "", "manual_review_unresolved", "unresolved", "", "excluded_pending_manual_review"


conservative_columns = conservative_queue.apply(conservative_geography, axis=1, result_type="expand")
conservative_queue["affected_council_districts_conservative"] = conservative_columns[0]
conservative_queue["affected_district_source_conservative"] = conservative_columns[1]
conservative_queue["affected_district_confidence_conservative"] = conservative_columns[2]
conservative_queue["affected_district_source_detail_conservative"] = conservative_columns[3]
conservative_queue["geography_incorporation_status"] = conservative_columns[4]
conservative_queue["affected_districts_conservative_missing"] = (
    conservative_queue["affected_council_districts_conservative"] == ""
)

local_members_conservative = []
missing_roster_districts_conservative = []
for row in conservative_queue.to_dict("records"):
    final_date = pd.to_datetime(row["final_history_date"], errors="coerce")
    local_members = []
    missing_roster_districts = []
    if not pd.isna(final_date):
        for district in split_semicolon(row["affected_council_districts_conservative"]):
            matches = roster[
                (roster["district"].astype(str) == str(int(district)))
                & (roster["term_start_date_parsed"] <= final_date)
                & (final_date <= roster["term_end_date_parsed"])
            ]
            if matches.empty:
                missing_roster_districts.append(str(int(district)))
                continue
            local_members.extend(matches["member_name"].tolist())
    local_members_conservative.append(collapse_examples(local_members))
    missing_roster_districts_conservative.append(collapse_examples(missing_roster_districts))

conservative_queue["affected_council_districts_original"] = conservative_queue["affected_council_districts"]
conservative_queue["affected_district_source_original"] = conservative_queue["affected_district_source"]
conservative_queue["local_members_from_roster_original"] = conservative_queue["local_members_from_roster"]
conservative_queue["affected_council_districts"] = conservative_queue["affected_council_districts_conservative"]
conservative_queue["affected_district_source"] = conservative_queue["affected_district_source_conservative"]
conservative_queue["local_members_from_roster"] = local_members_conservative
conservative_queue["missing_roster_districts_conservative"] = missing_roster_districts_conservative

conservative_queue = conservative_queue[
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
        "local_members_from_roster",
        "application_keys",
        "title",
        "affected_council_districts_original",
        "affected_district_source_original",
        "local_members_from_roster_original",
        "affected_district_confidence_conservative",
        "affected_district_source_detail_conservative",
        "geography_incorporation_status",
        "affected_districts_conservative_missing",
        "missing_roster_districts_conservative",
        "recovered_affected_council_districts",
        "geography_recovery_method",
        "geography_recovery_confidence",
        "geography_recovery_source",
        "verification_status",
        "verified_districts",
        "verification_evidence_level",
        "verification_source_url",
        "verification_source_relation",
    ]
]

verification = verification[
    [
        "query_year",
        "matter_id",
        "matter_file",
        "disposition_group",
        "verification_status",
        "verified_districts",
        "verification_evidence_level",
        "verification_source_url",
        "verification_source_relation",
        "verified_districts_match_chatgpt",
        "chatgpt_suggested_districts_parsed",
        "likely_current_or_historical_council_district",
        "confidence_high_medium_low",
        "likely_location",
        "application_keys",
        "title_bbls",
        "official_matter_bbl_count",
        "official_matter_bbl_current_mappluto_match_count",
        "official_matter_bbl_unmatched_count",
        "official_matter_bbl_current_mappluto_districts",
        "official_matter_bbl_examples",
        "verified_direct_official_districts",
        "verified_direct_official_url",
        "verified_direct_official_relation",
        "verified_direct_official_snippet",
        "verification_notes",
        "official_source_to_check_or_source_url",
        "reasoning_notes",
        "source_links_found_in_cell",
        "matter_url",
        "final_history_detail_url",
        "title",
    ]
]

if len(queue) != int(recovery["recovered_affected_district_missing_bool"].sum()):
    raise RuntimeError("Verification input must cover every unresolved recovery row.")
if not queue.loc[queue["chatgpt_response_found"], "matter_file"].isin(set(chatgpt["matter_file"])).all():
    raise RuntimeError("Every marked ChatGPT review response must link to the current review queue.")
if verification["matter_id"].duplicated().any():
    raise RuntimeError("Verification ledger must be unique by matter_id.")
if int(sources["source_role"].eq("exact_matter_legistar").sum()) != len(queue):
    raise RuntimeError("Every review-queue row must have an exact Legistar matter source.")
if not sources.loc[sources["source_role"].eq("exact_matter_legistar"), "fetch_status"].eq("200").all():
    raise RuntimeError("Every exact Legistar matter page must return HTTP 200.")
if not verification.loc[verification["verification_status"].str.startswith("verified_"), "verified_districts"].ne("").all():
    raise RuntimeError("Every verified row must have at least one verified district.")
if not verification.loc[
    verification["verification_status"].eq("verified_direct_official_district"),
    "verification_source_relation",
].isin(["same_matter_id", "application_key_overlap", "lu_number_overlap"]).all():
    raise RuntimeError("Direct official district rows require an accepted relation to the unresolved matter.")
if conservative_queue["matter_id"].duplicated().any():
    raise RuntimeError("Conservative geography queue must be unique by matter_id.")

write_csv("../output/member_deference_nonapproval_geography_conservative_queue.csv", conservative_queue)
