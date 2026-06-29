# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/fetch_council_member_roster_sources/code")

from __future__ import annotations

import csv
import hashlib
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup


PULL_DATE = "20260512"
LEGISTAR_URL = (
    "https://legistar.council.nyc.gov/"
    "DepartmentDetail.aspx?ID=6897&GUID=CDC6E691-8A8C-4F25-97CB-86F31EDAB081&Mode=MainBody"
)
SOURCE_FILE_ROOT = Path("../../fetch_council_member_roster_sources/output/source_files")
OUTPUT_FILE = Path("../output/council_member_roster_source_files.csv")


class CachedResponse:
    def __init__(self, path: Path):
        self.status_code = 200
        self.content = path.read_bytes()
        self.text = self.content.decode("utf-8", errors="replace")
        self.from_cache = True


def normalize_space(value: object) -> str:
    return re.sub(r"\s+", " ", "" if value is None else str(value)).strip()


def compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def cached_file_is_local(path: Path) -> bool:
    if not path.exists():
        return False
    stat_result = path.stat()
    return stat_result.st_size > 0 and getattr(stat_result, "st_blocks", 1) != 0


def remove_bad_cached_file(path: Path) -> None:
    if path.exists() and not cached_file_is_local(path):
        path.unlink()


def parse_form_inputs(html: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    payload: dict[str, str] = {}

    for inp in soup.find_all("input"):
        name = inp.get("name")
        if not name:
            continue

        input_type = (inp.get("type") or "").lower()
        if input_type in {"submit", "button", "image"}:
            continue
        if input_type in {"checkbox", "radio"} and not inp.has_attr("checked"):
            continue

        payload[name] = inp.get("value") or ""

    return payload


def write_csv(path: str, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    new_path = Path(path)
    temp_path = new_path.with_suffix(new_path.suffix + ".tmp")

    with temp_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    temp_path.replace(new_path)


def source_row(
    *,
    source_id: str,
    source_role: str,
    source_label: str,
    url: str,
    raw_path: Path,
    response: requests.Response | None,
    district: int | None = None,
    page_number: int | None = None,
    page_count: int | None = None,
    record_count: int | None = None,
    postback_event_target: str | None = None,
    postback_event_argument: str | None = None,
    notes: str = "",
) -> dict[str, object]:
    file_exists = raw_path.exists()

    if response is None:
        fetch_status = "not_requested"
        http_status = None
    elif response.status_code == 200 and file_exists:
        fetch_status = "downloaded"
        http_status = response.status_code
    else:
        fetch_status = f"http_{response.status_code}"
        http_status = response.status_code

    return {
        "source_id": source_id,
        "source_role": source_role,
        "source_label": source_label,
        "district": district,
        "page_number": page_number,
        "page_count": page_count,
        "record_count": record_count,
        "url": url,
        "postback_event_target": postback_event_target,
        "postback_event_argument": postback_event_argument,
        "expected_filename": raw_path.name,
        "pull_date": PULL_DATE,
        "raw_path": str(raw_path),
        "fetch_status": fetch_status,
        "http_status": http_status,
        "file_exists": file_exists,
        "file_size_bytes": raw_path.stat().st_size if file_exists else None,
        "checksum_sha256": compute_sha256(raw_path) if file_exists else None,
        "notes": notes,
    }


def save_response(response: requests.Response, raw_path: Path) -> None:
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    if response.status_code == 200:
        raw_path.write_bytes(response.content)


def get_or_fetch(session: requests.Session, url: str, raw_path: Path, data: dict[str, str] | None = None):
    remove_bad_cached_file(raw_path)
    if cached_file_is_local(raw_path):
        return CachedResponse(raw_path)

    response = session.post(url, data=data, timeout=60) if data is not None else session.get(url, timeout=60)
    response.from_cache = False
    save_response(response, raw_path)
    return response


def parse_page_info(html: str) -> tuple[int | None, int | None]:
    soup = BeautifulSoup(html, "html.parser")

    for div in soup.select("div.rgInfoPart"):
        match = re.search(
            r"Page\s+\d+\s+of\s+(\d+)\s*,\s*items\s+\d+\s+to\s+\d+\s+of\s+(\d+)",
            normalize_space(div.get_text(" ")),
        )
        if match:
            return int(match.group(1)), int(match.group(2))

    return None, None


def parse_page_links(html: str) -> dict[int, str]:
    soup = BeautifulSoup(html, "html.parser")
    links: dict[int, str] = {}

    for link in soup.select("td.rgPagerCell a"):
        page_text = normalize_space(link.get_text(" "))
        href = link.get("href") or ""
        match = re.search(r"__doPostBack\('([^']+)'", href)

        if page_text.isdigit() and match and int(page_text) not in links:
            links[int(page_text)] = match.group(1)

    return links


def ordinal(value: int) -> str:
    if 10 <= value % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(value % 10, "th")
    return f"{value}{suffix}"


session = requests.Session()
session.headers.update({"User-Agent": "nyc-court-case-roster-research/0.1"})
fetch_rows: list[dict[str, object]] = []

raw_path = (
    SOURCE_FILE_ROOT
    / "nyc_council_legistar_office_records"
    / PULL_DATE
    / "legistar_city_council_current.html"
)
response = get_or_fetch(session, LEGISTAR_URL, raw_path)
fetch_rows.append(
    source_row(
        source_id="nyc_council_legistar_office_records",
        source_role="legistar_city_council_current_page",
        source_label="Legistar City Council current roster page",
        url=LEGISTAR_URL,
        raw_path=raw_path,
        response=response,
        notes="Initial GET page used to obtain ASP.NET form state before the all-term roster postback.",
    )
)

payload = parse_form_inputs(response.text)
payload["__EVENTTARGET"] = "ctl00$ContentPlaceHolder1$menuPeople"
payload["__EVENTARGUMENT"] = "3:2"
raw_path = (
    SOURCE_FILE_ROOT
    / "nyc_council_legistar_office_records"
    / PULL_DATE
    / "legistar_city_council_office_records_all_page_01.html"
)
response = get_or_fetch(session, LEGISTAR_URL, raw_path, data=payload)
page_count, record_count = parse_page_info(response.text)
fetch_rows.append(
    source_row(
        source_id="nyc_council_legistar_office_records",
        source_role="legistar_office_records_page",
        source_label="Legistar City Council all-term office records page 1",
        url=LEGISTAR_URL,
        raw_path=raw_path,
        response=response,
        page_number=1,
        page_count=page_count,
        record_count=record_count,
        postback_event_target="ctl00$ContentPlaceHolder1$menuPeople",
        postback_event_argument="3:2",
        notes="All-term Legistar roster. Telerik RadMenu term selector uses hierarchical index 3:2 for All.",
    )
)

all_page_html = response.text
office_record_html_pages = [all_page_html]
all_page_payload = parse_form_inputs(all_page_html)
page_links = parse_page_links(all_page_html)

for page_number in range(2, (page_count or 1) + 1):
    payload = dict(all_page_payload)
    payload["__EVENTTARGET"] = page_links.get(page_number, "")
    payload["__EVENTARGUMENT"] = ""

    raw_path = (
        SOURCE_FILE_ROOT
        / "nyc_council_legistar_office_records"
        / PULL_DATE
        / f"legistar_city_council_office_records_all_page_{page_number:02d}.html"
    )
    response = get_or_fetch(session, LEGISTAR_URL, raw_path, data=payload)
    office_record_html_pages.append(response.text)
    fetch_rows.append(
        source_row(
            source_id="nyc_council_legistar_office_records",
            source_role="legistar_office_records_page",
            source_label=f"Legistar City Council all-term office records page {page_number}",
            url=LEGISTAR_URL,
            raw_path=raw_path,
            response=response,
            page_number=page_number,
            page_count=page_count,
            record_count=record_count,
            postback_event_target=page_links.get(page_number, ""),
            postback_event_argument="",
            notes="All-term Legistar roster pagination page.",
        )
    )
    if not getattr(response, "from_cache", False):
        time.sleep(0.25)

person_detail_urls: dict[str, str] = {}

for html in office_record_html_pages:
    soup = BeautifulSoup(html, "html.parser")

    for row in soup.select("#ctl00_ContentPlaceHolder1_gridPeople tr.rgRow, #ctl00_ContentPlaceHolder1_gridPeople tr.rgAltRow"):
        cells = row.find_all("td", recursive=False)
        if len(cells) < 2 or normalize_space(cells[1].get_text(" ")):
            continue

        link = cells[0].find("a")
        if not link or not link.get("href"):
            continue

        person_url = requests.compat.urljoin("https://legistar.council.nyc.gov/", link["href"])
        match = re.search(r"[?&]ID=(\d+)", person_url)
        if match:
            person_detail_urls[match.group(1)] = person_url

for person_id, person_url in sorted(person_detail_urls.items(), key=lambda item: int(item[0])):
    raw_path = (
        SOURCE_FILE_ROOT
        / "nyc_council_legistar_person_details"
        / PULL_DATE
        / f"legistar_person_detail_{person_id}.html"
    )
    response = get_or_fetch(session, person_url, raw_path)
    fetch_rows.append(
        source_row(
            source_id="nyc_council_legistar_person_details",
            source_role="legistar_person_detail_page",
            source_label=f"Legistar person detail page {person_id}",
            url=person_url,
            raw_path=raw_path,
            response=response,
            notes="Fetched for official roster rows where the all-term office-record grid omits district; historical district often appears in the PersonDetail Notes field.",
        )
    )
    if not getattr(response, "from_cache", False):
        time.sleep(0.1)

for district in range(1, 52):
    url = f"https://en.wikipedia.org/wiki/New_York_City%27s_{ordinal(district)}_City_Council_district"
    raw_path = (
        SOURCE_FILE_ROOT
        / "wikipedia_nyc_council_district_history"
        / PULL_DATE
        / f"wikipedia_council_district_{district:02d}.html"
    )
    response = get_or_fetch(session, url, raw_path)
    fetch_rows.append(
        source_row(
            source_id="wikipedia_nyc_council_district_history",
            source_role="wikipedia_district_history_page",
            source_label=f"Wikipedia NYC Council District {district} history page",
            district=district,
            url=url,
            raw_path=raw_path,
            response=response,
            notes="Secondary broad-recall source for pre-Legistar district-member history; review against official Green Book or archives before treating as final.",
        )
    )
    if not getattr(response, "from_cache", False):
        time.sleep(0.1)

official_pages = [
    row
    for row in fetch_rows
    if row["source_role"] == "legistar_office_records_page"
    and row["fetch_status"] == "downloaded"
]
person_detail_pages = [
    row
    for row in fetch_rows
    if row["source_role"] == "legistar_person_detail_page"
    and row["fetch_status"] == "downloaded"
]
wiki_pages = [
    row
    for row in fetch_rows
    if row["source_role"] == "wikipedia_district_history_page"
    and row["fetch_status"] == "downloaded"
]

if len(official_pages) != (page_count or 0) or len(official_pages) == 0:
    raise RuntimeError("Every all-term Legistar roster page must be downloaded.")
if (record_count or 0) <= 0:
    raise RuntimeError("All-term Legistar roster must report at least one office record.")
if len(person_detail_pages) != len(person_detail_urls):
    raise RuntimeError("Every required Legistar person-detail page must be downloaded.")
if len(wiki_pages) != 51:
    raise RuntimeError("Every Wikipedia district-history page must be downloaded.")
if not all(row["checksum_sha256"] for row in fetch_rows if row["fetch_status"] == "downloaded"):
    raise RuntimeError("Every downloaded roster source file must have a SHA-256 checksum.")

write_csv(
    "../output/council_member_roster_source_files.csv",
    fetch_rows,
    [
        "source_id",
        "source_role",
        "source_label",
        "district",
        "page_number",
        "page_count",
        "record_count",
        "url",
        "postback_event_target",
        "postback_event_argument",
        "expected_filename",
        "pull_date",
        "raw_path",
        "fetch_status",
        "http_status",
        "file_exists",
        "file_size_bytes",
        "checksum_sha256",
        "notes",
    ],
)
