from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse

import pandas as pd
from bs4 import BeautifulSoup

import sys

sys.path.append("../../_lib")
from data_reports import save_csv
from legistar_utils import normalize_space
from member_deference_utils import surname

LEGISTAR_URL = (
    "https://legistar.council.nyc.gov/"
    "DepartmentDetail.aspx?ID=6897&GUID=CDC6E691-8A8C-4F25-97CB-86F31EDAB081&Mode=MainBody"
)

LEGISTAR_OFFICE_RECORD_SEED_SPECS = {
    "33915": {
        "district": 10,
        "member_name": "Guillermo Linares",
        "term_start_date": "1998-01-01",
        "term_end_date": "2001-12-31",
        "person_guid": "738EDDFB-FFD0-4BAB-940B-A0FA90868B6E",
    }
}


def clean_name(value: object) -> str:
    text = re.sub(r"\[[^\]]+\]", "", normalize_space(value))
    text = re.sub(r"\s*\([^)]*\)\s*$", "", text)
    return normalize_space(text)


def compact_name(value: object) -> str:
    text = clean_name(value).lower()
    text = re.sub(r"[^a-z ]+", " ", text)
    parts = [part for part in text.split() if len(part) > 1]
    return " ".join(parts)


def parse_official_date(value: object) -> str | None:
    text = normalize_space(value)
    if not text:
        return None
    return datetime.strptime(text, "%m/%d/%Y").date().isoformat()


def parse_wiki_date(value: object) -> str | None:
    text = re.sub(r"\[[^\]]+\]", "", normalize_space(value))
    text = text.replace("present", "").replace("Present", "").strip()
    if not text:
        return None
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date().isoformat()


def date_value(value: object, fallback: str = "2100-12-31") -> date:
    text = fallback if value is None or pd.isna(value) or value == "" else str(value)
    return datetime.strptime(text[:10], "%Y-%m-%d").date()


def active_rows(rows: list[dict[str, object]], district: int, check_date: str) -> list[dict[str, object]]:
    target = date_value(check_date)
    return [
        row
        for row in rows
        if row["district"] == district
        and date_value(row["term_start_date"], "1900-01-01") <= target <= date_value(row["term_end_date"])
    ]


def overlaps(left: dict[str, object], right: dict[str, object]) -> bool:
    return date_value(left["term_start_date"], "1900-01-01") <= date_value(right["term_end_date"]) and date_value(
        right["term_start_date"], "1900-01-01"
    ) <= date_value(left["term_end_date"])


# A year-only Wikipedia term ("Kathryn E. Freed (1991-2002)") is read as its widest span,
# January 1 of the first year to December 31 of the last. It is only used to test whether
# a member covers a whole gap in the official record, and is then clipped to that gap.
def wiki_year_term(value: str, is_end: bool = False) -> str | None:
    text = normalize_space(value)
    if re.fullmatch(r"\d{4}", text):
        return f"{text}-12-31" if is_end else f"{text}-01-01"
    return parse_wiki_date(text)


def parse_html_tables(html: str) -> list[pd.DataFrame]:
    soup = BeautifulSoup(html, "html.parser")
    out: list[pd.DataFrame] = []

    for table in soup.find_all("table"):
        headers: list[str] = []
        rows: list[dict[str, str]] = []

        for table_row in table.find_all("tr"):
            header_cells = table_row.find_all("th", recursive=False)
            data_cells = table_row.find_all("td", recursive=False)

            if header_cells:
                headers = []
                for cell in header_cells:
                    headers.extend([normalize_space(cell.get_text(" "))] * int(cell.get("colspan") or 1))
                continue

            if not headers or not data_cells:
                continue

            values = []
            for cell in data_cells:
                values.extend([normalize_space(cell.get_text(" "))] * int(cell.get("colspan") or 1))
            values.extend([""] * max(0, len(headers) - len(values)))
            rows.append(dict(zip(headers, values[: len(headers)])))

        if rows:
            out.append(pd.DataFrame(rows))

    return out


source_files = pd.read_csv("../temp/council_member_roster_source_files.csv").fillna("")
source_files_out = source_files.to_dict("records")
official_terms: list[dict[str, object]] = []
wiki_terms: list[dict[str, object]] = []
person_detail_district_by_id: dict[str, int] = {}
person_detail_path_by_id: dict[str, str] = {}
person_detail_url_by_id: dict[str, str] = {}

for source in source_files_out:
    if source["source_role"] != "legistar_person_detail_page" or source["fetch_status"] != "downloaded":
        continue

    person_id_match = re.search(r"legistar_person_detail_(\d+)\.html$", source["raw_path"])
    if not person_id_match:
        continue

    soup = BeautifulSoup(Path(source["raw_path"]).read_text(encoding="utf-8"), "html.parser")
    match = re.search(r"Notes:\s*District\s*(\d{1,2})\b", normalize_space(soup.get_text(" ")), re.IGNORECASE)

    if match:
        person_id = person_id_match.group(1)
        person_detail_district_by_id[person_id] = int(match.group(1))
        person_detail_path_by_id[person_id] = source["raw_path"]
        person_detail_url_by_id[person_id] = source["url"]

for source in source_files_out:
    if source["source_role"] != "legistar_office_records_page" or source["fetch_status"] != "downloaded":
        continue

    soup = BeautifulSoup(Path(source["raw_path"]).read_text(encoding="utf-8"), "html.parser")
    rows = soup.select("#ctl00_ContentPlaceHolder1_gridPeople tr.rgRow, #ctl00_ContentPlaceHolder1_gridPeople tr.rgAltRow")

    for position, row in enumerate(rows, start=1):
        cells = row.find_all("td", recursive=False)
        if len(cells) < 8:
            continue

        person_link = cells[0].find("a")
        person_href = urljoin("https://legistar.council.nyc.gov/", person_link.get("href")) if person_link else ""
        person_query = parse_qs(urlparse(person_href).query)
        person_id = person_query.get("ID", [""])[0]
        website_link = cells[5].find("a")
        district_match = re.search(r"\d{1,2}", normalize_space(cells[1].get_text(" ")))
        district = int(district_match.group(0)) if district_match else person_detail_district_by_id.get(person_id)
        district_source = ""
        source_url = LEGISTAR_URL
        raw_path = source["raw_path"]
        evidence_summary = "Official Legistar City Council office record."

        if district_match:
            district_source = "legistar_office_record_grid"
        elif district is not None:
            district_source = "legistar_person_detail_notes"
            source_url = f"{LEGISTAR_URL}; {person_detail_url_by_id.get(person_id, '')}"
            raw_path = f"{source['raw_path']}; {person_detail_path_by_id.get(person_id, '')}"
            evidence_summary = (
                "Official Legistar City Council office record; district filled from the linked "
                "Legistar PersonDetail Notes field because the all-term grid omits the district."
            )

        official_terms.append(
            {
                "roster_record_id": f"legistar_{source['page_number']}_{position}",
                "source_id": source["source_id"],
                "source_role": source["source_role"],
                "source_tier": "official_legistar",
                "source_precedence": 1,
                "source_url": source_url,
                "raw_path": raw_path,
                "district": district,
                "district_text": normalize_space(cells[1].get_text(" ")) or (f"District {district:02d}" if district else ""),
                "district_source": district_source,
                "member_name": clean_name(cells[0].get_text(" ")),
                "member_name_clean": compact_name(cells[0].get_text(" ")),
                "party": normalize_space(cells[7].get_text(" ")),
                "borough": normalize_space(cells[6].get_text(" ")),
                "person_title": normalize_space(cells[2].get_text(" ")),
                "term_start_date": parse_official_date(cells[3].get_text(" ")),
                "term_end_date": parse_official_date(cells[4].get_text(" ")),
                "term_text": "",
                "person_id": person_id,
                "person_guid": person_query.get("GUID", [""])[0],
                "person_url": person_href,
                "website_url": website_link.get("href") if website_link else "",
                "evidence_summary": evidence_summary,
                "source_review_required": False,
                "source_review_reason": "",
            }
        )

for person_id, spec in LEGISTAR_OFFICE_RECORD_SEED_SPECS.items():
    if person_id not in person_detail_district_by_id:
        raise RuntimeError(f"Seeded Legistar person detail page {person_id} must be downloaded and parsed.")
    if person_detail_district_by_id[person_id] != spec["district"]:
        raise RuntimeError(f"Seeded Legistar person detail page {person_id} district does not match the seed spec.")

    already_parsed = any(
        row["person_id"] == person_id
        and row["term_start_date"] == spec["term_start_date"]
        and row["term_end_date"] == spec["term_end_date"]
        for row in official_terms
    )
    if already_parsed:
        continue

    official_terms.append(
        {
            "roster_record_id": f"legistar_seed_{person_id}",
            "source_id": "nyc_council_legistar_person_details",
            "source_role": "legistar_person_detail_seed",
            "source_tier": "official_legistar",
            "source_precedence": 1,
            "source_url": person_detail_url_by_id[person_id],
            "raw_path": person_detail_path_by_id[person_id],
            "district": spec["district"],
            "district_text": f"District {spec['district']:02d}",
            "district_source": "legistar_person_detail_notes",
            "member_name": spec["member_name"],
            "member_name_clean": compact_name(spec["member_name"]),
            "party": "",
            "borough": "Manhattan",
            "person_title": "Council Member",
            "term_start_date": spec["term_start_date"],
            "term_end_date": spec["term_end_date"],
            "term_text": "",
            "person_id": person_id,
            "person_guid": spec["person_guid"],
            "person_url": person_detail_url_by_id[person_id],
            "website_url": "",
            "evidence_summary": (
                "Official Legistar PersonDetail page used as a deterministic seed because the live "
                "all-term office-record grid intermittently omits this historical council-member row."
            ),
            "source_review_required": False,
            "source_review_reason": "",
        }
    )

for source in source_files_out:
    if source["source_role"] != "wikipedia_district_history_page" or source["fetch_status"] != "downloaded":
        continue

    html = Path(source["raw_path"]).read_text(encoding="utf-8")
    district = int(source["district"])

    tables = parse_html_tables(html)

    member_table = None
    for table in tables:
        table.columns = [normalize_space(col) for col in table.columns]
        if {"Members", "Party", "Years served"}.issubset(set(table.columns)):
            member_table = table
            break

    if member_table is None:
        soup = BeautifulSoup(html, "html.parser")
        parsed_list_rows = []
        for position, item in enumerate(soup.find_all("li"), start=1):
            text = normalize_space(item.get_text(" "))
            match = re.match(r"^(.+?)\s*\((\d{4})(?:\s*[–-]\s*(\d{4}|present|Present))?\)$", text)
            link = item.find("a")
            if not match or link is None:
                continue

            member_name = clean_name(link.get_text(" "))
            if not member_name or "district" in member_name.lower():
                continue

            term_start_date = wiki_year_term(match.group(2))
            term_end_date = wiki_year_term(match.group(3), is_end=True) if match.group(3) else wiki_year_term(match.group(2), is_end=True)
            if not term_start_date:
                continue

            parsed_list_rows.append(
                {
                    "roster_record_id": f"wiki_{district:02d}_list_{position}",
                    "source_id": source["source_id"],
                    "source_role": source["source_role"],
                    "source_tier": "secondary_wikipedia",
                    "source_precedence": 3,
                    "source_url": source["url"],
                    "raw_path": source["raw_path"],
                    "district": district,
                    "district_text": f"District {district:02d}",
                    "district_source": "wikipedia_district_page_list",
                    "member_name": member_name,
                    "member_name_clean": compact_name(member_name),
                    "party": "",
                    "borough": "",
                    "person_title": "Council Member",
                    "term_start_date": term_start_date,
                    "term_end_date": term_end_date,
                    "term_text": match.group(0),
                    "person_id": "",
                    "person_guid": "",
                    "person_url": urljoin(source["url"], link.get("href", "")),
                    "website_url": "",
                    "evidence_summary": "Secondary district-history page list item used as source-documented backfill.",
                    "source_review_required": True,
                    "source_review_reason": "secondary_wikipedia_list_source",
                }
            )

        if parsed_list_rows:
            wiki_terms.extend(parsed_list_rows)
            continue

        wiki_terms.append(
            {
                "roster_record_id": f"wiki_{district:02d}_unparsed",
                "source_id": source["source_id"],
                "source_role": source["source_role"],
                "source_tier": "secondary_wikipedia",
                "source_precedence": 3,
                "source_url": source["url"],
                "raw_path": source["raw_path"],
                "district": district,
                "district_text": f"District {district:02d}",
                "district_source": "wikipedia_district_page",
                "member_name": "",
                "member_name_clean": "",
                "party": "",
                "borough": "",
                "person_title": "Council Member",
                "term_start_date": None,
                "term_end_date": None,
                "term_text": "",
                "person_id": "",
                "person_guid": "",
                "person_url": "",
                "website_url": "",
                "evidence_summary": "District-history page did not contain a parseable Members table.",
                "source_review_required": True,
                "source_review_reason": "wikipedia_members_table_unparsed",
            }
        )
        continue

    for position, row in member_table.iterrows():
        member_name = clean_name(row.get("Members", ""))
        term_text = normalize_space(row.get("Years served", ""))

        if not member_name or "District established" in member_name:
            continue

        date_parts = re.split(r"\s*[–-]\s*", term_text, maxsplit=1)
        term_start_date = wiki_year_term(date_parts[0]) if date_parts else None
        term_end_date = wiki_year_term(date_parts[1], is_end=True) if len(date_parts) > 1 else None

        wiki_terms.append(
            {
                "roster_record_id": f"wiki_{district:02d}_{position + 1}",
                "source_id": source["source_id"],
                "source_role": source["source_role"],
                "source_tier": "secondary_wikipedia",
                "source_precedence": 3,
                "source_url": source["url"],
                "raw_path": source["raw_path"],
                "district": district,
                "district_text": f"District {district:02d}",
                "district_source": "wikipedia_district_page",
                "member_name": member_name,
                "member_name_clean": compact_name(member_name),
                "party": normalize_space(row.get("Party", "")),
                "borough": "",
                "person_title": "Council Member",
                "term_start_date": term_start_date,
                "term_end_date": term_end_date,
                "term_text": term_text,
                "person_id": "",
                "person_guid": "",
                "person_url": "",
                "website_url": "",
                "evidence_summary": "Secondary district-history page used as pre-Legistar backfill.",
                "source_review_required": True,
                "source_review_reason": "secondary_pre_legistar_source",
            }
        )

# Source priority. Official Legistar office records are the roster: every Council title
# (Council Member, Speaker, Majority/Minority Leader, Whip) except Public Advocate, a
# citywide office. Wikipedia district pages only (a) supply the district for official
# rows whose Legistar grid and PersonDetail page omit it and (b) fill a period in
# 1998-2025 that no official row covers, when one Wikipedia member spans the whole gap
# and has no official term in the district, or has official terms on both sides of it.
# Each row records its source in roster_source.
ANALYSIS_START = date(1998, 1, 1)
ANALYSIS_END = date(2025, 12, 31)

official_rows: list[dict[str, object]] = []
seen_official: set[tuple[object, ...]] = set()
for row in official_terms:
    key = (row["person_id"], row["district"], row["term_start_date"], row["term_end_date"])
    if row["person_title"] == "Public Advocate" or not row["member_name"] or not row["term_start_date"] or key in seen_official:
        continue
    seen_official.add(key)
    official_rows.append({**row, "roster_source": "legistar_office_record"})

wiki_members = [
    row
    for row in wiki_terms
    if row["term_start_date"] and len(compact_name(row["member_name"]).split()) >= 2
    and compact_name(row["member_name"]) != "vacant"
]

unassigned_official_rows = []
for row in official_rows:
    if row["district"] is not None:
        continue
    districts = {
        wiki["district"] for wiki in wiki_members if surname(wiki["member_name"]) == surname(row["member_name"]) and overlaps(wiki, row)
    }
    if len(districts) != 1:
        unassigned_official_rows.append(row)
        continue
    row["district"] = districts.pop()
    row["district_text"] = f"District {row['district']:02d}"
    row["district_source"] = "wikipedia_district_page_surname_match"
    row["roster_source"] = "legistar_office_record_wikipedia_district"
    row["source_review_required"] = True
    row["source_review_reason"] = "district_from_wikipedia_surname_match"
official_rows = [row for row in official_rows if row["district"] is not None]

corrections = pd.read_csv("official_roster_corrections.csv", dtype=str, keep_default_na=False)
for correction in corrections.to_dict("records"):
    matches = [
        row
        for row in official_rows
        if row["person_id"] == correction["person_id"]
        and str(row["district"]) == correction["listed_district"]
        and row["term_start_date"] == correction["listed_term_start_date"]
        and row["term_end_date"] == correction["listed_term_end_date"]
    ]
    if len(matches) != 1:
        raise RuntimeError(f"Roster correction must match exactly one official row: {correction}")
    row = matches[0]
    row["district"] = int(correction["corrected_district"])
    row["district_text"] = f"District {row['district']:02d}"
    row["term_start_date"] = correction["corrected_term_start_date"]
    row["term_end_date"] = correction["corrected_term_end_date"]
    row["roster_source"] = "legistar_office_record_corrected"
    row["source_review_required"] = True
    row["source_review_reason"] = "official_roster_correction"
    row["evidence_summary"] = f"{row['evidence_summary']} Corrected: {correction['correction_reason']}"

gap_fill_rows = []
for district in range(1, 52):
    district_official = sorted(
        (row for row in official_rows if row["district"] == district),
        key=lambda row: date_value(row["term_start_date"], "1900-01-01"),
    )
    gaps = []
    cursor = ANALYSIS_START
    for row in district_official:
        start = date_value(row["term_start_date"], "1900-01-01")
        if start > cursor:
            gaps.append((cursor, min(start - timedelta(days=1), ANALYSIS_END)))
        cursor = max(cursor, date_value(row["term_end_date"]) + timedelta(days=1))
    if cursor <= ANALYSIS_END:
        gaps.append((cursor, ANALYSIS_END))

    for gap_start, gap_end in gaps:
        if gap_start > gap_end:
            continue
        covering = [
            wiki
            for wiki in wiki_members
            if wiki["district"] == district
            and date_value(wiki["term_start_date"], "1900-01-01") <= gap_start
            and date_value(wiki["term_end_date"]) >= gap_end
        ]
        if len(covering) != 1:
            continue
        wiki = covering[0]
        same_member = [row for row in district_official if surname(row["member_name"]) == surname(wiki["member_name"])]
        ended_before_gap = any(date_value(row["term_end_date"]) == gap_start - timedelta(days=1) for row in same_member)
        resumed_after_gap = any(date_value(row["term_start_date"], "1900-01-01") == gap_end + timedelta(days=1) for row in same_member)
        if same_member and not (ended_before_gap and resumed_after_gap):
            continue  # Legistar dates this member's own term; the gap is a vacancy.
        person_ids = {row["person_id"] for row in same_member}
        gap_fill_rows.append(
            {
                **wiki,
                "roster_record_id": f"{wiki['roster_record_id']}_gap_{gap_start.isoformat()}",
                "term_start_date": gap_start.isoformat(),
                "term_end_date": gap_end.isoformat(),
                "person_id": person_ids.pop() if len(person_ids) == 1 else "",
                "roster_source": "wikipedia_gap_fill",
                "evidence_summary": (
                    "No official Legistar office record covers this period; the Wikipedia district page lists "
                    "this member for the whole period. Dates are clipped to the official-record gap."
                ),
                "source_review_required": True,
                "source_review_reason": "wikipedia_gap_fill",
            }
        )

master_rows = sorted(
    official_rows + gap_fill_rows,
    key=lambda row: (int(row["district"]), date_value(row["term_start_date"], "1900-01-01"), row["member_name"]),
)

overlap_rows = [
    (left["district"], left["member_name"], left["term_start_date"], right["member_name"], right["term_start_date"])
    for i, left in enumerate(master_rows)
    for right in master_rows[i + 1 :]
    if left["district"] == right["district"] and left["person_id"] != right["person_id"] and overlaps(left, right)
]

known_specs = [
    (21, "2001-05-23", "Helen M. Marshall"),
    (33, "2009-06-10", "David Yassky"),
    (34, "2009-12-21", "Diana Reyna"),
    (1, "2001-12-31", "Kathryn E. Freed"),
    (1, "2002-01-01", "Alan J. Gerson"),
    (1, "2010-01-01", "Margaret S. Chin"),
    (1, "2021-12-31", "Margaret S. Chin"),
    (1, "2022-01-01", "Christopher Marte"),
    (18, "2024-06-01", "Amanda C. Farias"),
    (19, "2025-06-01", "Vickie Paladino"),
    (43, "2019-06-01", "Justin L. Brannan"),
    (47, "2019-06-01", "Mark Treyger"),
    (51, "2020-06-01", "Joseph C. Borelli"),
]
for district, check_date, expected_member_name in known_specs:
    matches = active_rows(master_rows, district, check_date)
    if [surname(row["member_name"]) for row in matches] != [surname(expected_member_name)]:
        raise RuntimeError(f"{expected_member_name} must be the only district {district} member on {check_date}.")

fields = [
    "roster_record_id",
    "roster_source",
    "source_id",
    "source_role",
    "source_tier",
    "source_precedence",
    "source_url",
    "raw_path",
    "district",
    "district_text",
    "district_source",
    "member_name",
    "member_name_clean",
    "party",
    "borough",
    "person_title",
    "term_start_date",
    "term_end_date",
    "term_text",
    "person_id",
    "person_guid",
    "person_url",
    "website_url",
    "evidence_summary",
    "source_review_required",
    "source_review_reason",
]

print(f"Official rows: {len(official_rows)}; Wikipedia gap fills: {len(gap_fill_rows)}")
print(f"Official rows left without a district: {[row['member_name'] for row in unassigned_official_rows]}")
if len(official_terms) == 0:
    raise RuntimeError("Official Legistar office-record rows must be parsed.")
if len(person_detail_district_by_id) == 0:
    raise RuntimeError("Legistar PersonDetail district notes must be parsed.")
if len(wiki_members) < 51:
    raise RuntimeError("Wikipedia district-history member rows must be parsed.")
if overlap_rows:
    raise RuntimeError(f"Master roster must not have overlapping district intervals: {overlap_rows}")

save_csv(master_rows, fields, "../output/council_member_roster_master.csv", ["roster_record_id"])
