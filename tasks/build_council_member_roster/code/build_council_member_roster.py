# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/build_council_member_roster/code")

from __future__ import annotations

import csv
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse

import pandas as pd
from bs4 import BeautifulSoup


LEGISTAR_URL = (
    "https://legistar.council.nyc.gov/"
    "DepartmentDetail.aspx?ID=6897&GUID=CDC6E691-8A8C-4F25-97CB-86F31EDAB081&Mode=MainBody"
)


def normalize_space(value: object) -> str:
    return re.sub(r"\s+", " ", "" if value is None else str(value)).strip()


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


def write_csv(path: str, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    new_path = Path(path)
    temp_path = new_path.with_suffix(new_path.suffix + ".tmp")

    with temp_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    temp_path.replace(new_path)


def active_rows(rows: list[dict[str, object]], district: int, check_date: str) -> list[dict[str, object]]:
    target = date_value(check_date)
    return [
        row
        for row in rows
        if row["district"] == district
        and date_value(row["term_start_date"], "1900-01-01") <= target <= date_value(row["term_end_date"])
    ]


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


source_files = pd.read_csv("../input/council_member_roster_source_files.csv").fillna("")
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
                "manual_audit_required": False,
                "audit_reason": "",
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
                    "evidence_summary": "Secondary district-history page list item; use as broad recall and manual-audit backfill only.",
                    "manual_audit_required": True,
                    "audit_reason": "secondary_wikipedia_list_source",
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
                "manual_audit_required": True,
                "audit_reason": "wikipedia_members_table_unparsed",
            }
        )
        continue

    for position, row in member_table.iterrows():
        member_name = clean_name(row.get("Members", ""))
        term_text = normalize_space(row.get("Years served", ""))

        if not member_name or "District established" in member_name:
            continue

        date_parts = re.split(r"\s+[–-]\s*", term_text, maxsplit=1)
        term_start_date = parse_wiki_date(date_parts[0]) if date_parts else None
        term_end_date = parse_wiki_date(date_parts[1]) if len(date_parts) > 1 else None

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
                "evidence_summary": "Secondary district-history page; use as broad recall and pre-Legistar backfill only.",
                "manual_audit_required": True,
                "audit_reason": "secondary_pre_legistar_source",
            }
        )

official_with_district = [
    row
    for row in official_terms
    if row["district"] is not None
    and row["term_start_date"]
    and row["member_name"]
    and row["person_title"] in {"Council Member", "Speaker"}
]

wiki_terms_for_master: list[dict[str, object]] = []
seen_wiki_terms: set[tuple[object, ...]] = set()
for row in wiki_terms:
    if not row["member_name"] or not row["term_start_date"]:
        continue

    key = (
        row["district"],
        row["member_name_clean"],
        row["term_start_date"],
        row["term_end_date"],
        row["source_url"],
    )
    if key in seen_wiki_terms:
        continue
    seen_wiki_terms.add(key)
    wiki_terms_for_master.append(row)

districts_with_wiki_history = {int(row["district"]) for row in wiki_terms_for_master}

master_rows = list(wiki_terms_for_master)

for official_row in official_with_district:
    if int(official_row["district"]) not in districts_with_wiki_history:
        continue

    official_start = date_value(official_row["term_start_date"], "1900-01-01")
    official_end = date_value(official_row["term_end_date"], "2100-12-31")

    for master_row in master_rows:
        if int(master_row["district"]) != int(official_row["district"]):
            continue
        if master_row["member_name_clean"] != official_row["member_name_clean"]:
            continue

        master_start = date_value(master_row["term_start_date"], "1900-01-01")
        master_end = date_value(master_row["term_end_date"], "2100-12-31")

        if official_start <= master_end + timedelta(days=1) and official_end > master_end and official_end >= master_start:
            master_row["term_end_date"] = official_row["term_end_date"]
            master_row["manual_audit_required"] = True
            master_row["audit_reason"] = (
                f"{master_row['audit_reason']}; official_legistar_extended_secondary_term_end"
                if master_row["audit_reason"]
                else "official_legistar_extended_secondary_term_end"
            )
            master_row["evidence_summary"] = (
                f"{master_row['evidence_summary']} Term end extended using overlapping official Legistar "
                "office-record dates for the same member and district."
            )

for row in official_with_district:
    if int(row["district"]) not in districts_with_wiki_history:
        master_rows.append(row)

master_rows = sorted(
    master_rows,
    key=lambda row: (
        int(row["district"]),
        date_value(row["term_start_date"], "1900-01-01"),
        date_value(row["term_end_date"], "2100-12-31"),
        row["source_precedence"],
        row["member_name"],
    ),
)

trimmed_master_rows: list[dict[str, object]] = []

for district in range(1, 52):
    district_rows = [row for row in master_rows if row["district"] == district]
    district_rows = sorted(
        district_rows,
        key=lambda row: (
            date_value(row["term_start_date"], "1900-01-01"),
            date_value(row["term_end_date"], "2100-12-31"),
            row["source_precedence"],
            row["member_name"],
        ),
    )

    for row in district_rows:
        row = dict(row)

        if trimmed_master_rows and trimmed_master_rows[-1]["district"] == district:
            previous = trimmed_master_rows[-1]
            previous_end = date_value(previous["term_end_date"], "2100-12-31")
            row_start = date_value(row["term_start_date"], "1900-01-01")
            row_end = date_value(row["term_end_date"], "2100-12-31")

            if row_start < previous_end and row["member_name_clean"] != previous["member_name_clean"]:
                new_start = (previous_end + timedelta(days=1)).isoformat()
                row["term_start_date"] = new_start
                row["manual_audit_required"] = True
                row["audit_reason"] = (
                    f"{row['audit_reason']}; trimmed_start_after_prior_member_interval"
                    if row["audit_reason"]
                    else "trimmed_start_after_prior_member_interval"
                )
                row["evidence_summary"] = (
                    f"{row['evidence_summary']} Start date trimmed to avoid overlapping active members "
                    "within a district; verify against official Green Book or election records."
                )

                if date_value(row["term_start_date"], "1900-01-01") > row_end:
                    continue

        trimmed_master_rows.append(row)

master_rows = trimmed_master_rows

term_rows = sorted(
    official_terms + wiki_terms,
    key=lambda row: (
        row["source_precedence"],
        int(row["district"]) if row["district"] is not None and row["district"] != "" else 999,
        date_value(row["term_start_date"], "1900-01-01"),
        row["member_name"],
    ),
)

overlap_rows: list[dict[str, object]] = []
for district in range(1, 52):
    district_rows = [row for row in master_rows if row["district"] == district]
    for i, left in enumerate(district_rows):
        for right in district_rows[i + 1 :]:
            if date_value(left["term_start_date"]) < date_value(right["term_end_date"]) and date_value(
                right["term_start_date"]
            ) < date_value(left["term_end_date"]):
                overlap_rows.append(
                    {
                        "district": district,
                        "left_record_id": left["roster_record_id"],
                        "left_member_name": left["member_name"],
                        "left_start_date": left["term_start_date"],
                        "left_end_date": left["term_end_date"],
                        "right_record_id": right["roster_record_id"],
                        "right_member_name": right["member_name"],
                        "right_start_date": right["term_start_date"],
                        "right_end_date": right["term_end_date"],
                    }
                )

known_specs = [
    {
        "check_name": "helen_marshall_district_21_2001",
        "district": 21,
        "check_date": "2001-05-23",
        "expected_member_name": "Helen Marshall",
    },
    {
        "check_name": "david_yassky_district_33_2009",
        "district": 33,
        "check_date": "2009-06-10",
        "expected_member_name": "David Yassky",
    },
    {
        "check_name": "diana_reyna_district_34_2009",
        "district": 34,
        "check_date": "2009-12-21",
        "expected_member_name": "Diana Reyna",
    },
]
known_date_checks: list[dict[str, object]] = []

for spec in known_specs:
    matches = active_rows(master_rows, spec["district"], spec["check_date"])
    member_names = "; ".join(row["member_name"] for row in matches)
    expected_clean = compact_name(spec["expected_member_name"])
    known_date_checks.append(
        {
            **spec,
            "matched_rows": len(matches),
            "matched_member_names": member_names,
            "matched_source_tiers": "; ".join(row["source_tier"] for row in matches),
            "passed": len(matches) == 1 and compact_name(member_names) == expected_clean,
        }
    )

roster_checks = [
    {
        "check_name": "official_legistar_terms_parsed",
        "passed": len(official_terms) > 0,
        "detail": f"Parsed {len(official_terms)} official Legistar office-record rows.",
    },
    {
        "check_name": "legistar_person_detail_districts_parsed",
        "passed": len(person_detail_district_by_id) > 0,
        "detail": f"Parsed district notes from {len(person_detail_district_by_id)} Legistar PersonDetail pages.",
    },
    {
        "check_name": "wikipedia_district_terms_parsed",
        "passed": len([row for row in wiki_terms if row["member_name"]]) >= 51,
        "detail": f"Parsed {len([row for row in wiki_terms if row['member_name']])} district-history member rows.",
    },
    {
        "check_name": "master_has_no_overlapping_district_intervals",
        "passed": len(overlap_rows) == 0,
        "detail": f"Found {len(overlap_rows)} overlapping master intervals.",
    },
    {
        "check_name": "master_reaches_before_1990",
        "passed": min(row["term_start_date"] for row in master_rows if row["term_start_date"]) <= "1990-01-01",
        "detail": "Master roster includes secondary backfill before 1990.",
    },
    {
        "check_name": "known_member_date_checks_pass",
        "passed": all(row["passed"] for row in known_date_checks),
        "detail": "; ".join(
            f"{row['check_name']}={row['matched_member_names'] or 'missing'}" for row in known_date_checks
        ),
    },
]

fields = [
    "roster_record_id",
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
    "manual_audit_required",
    "audit_reason",
]

write_csv("../output/council_member_roster_master.csv", master_rows, fields)

if any(not row["passed"] for row in roster_checks):
    failed_checks = ", ".join(row["check_name"] for row in roster_checks if not row["passed"])
    raise RuntimeError(f"Council member roster build failed: {failed_checks}.")
