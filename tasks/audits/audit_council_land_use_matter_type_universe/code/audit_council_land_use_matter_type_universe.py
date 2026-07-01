import csv
import html
import json
import os
import re
import time
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urljoin, urlparse


BASE_URL = "https://legistar.council.nyc.gov/Legislation.aspx"
SOURCE_ID = "nyc_council_legistar_all_matter_type_universe_audit"
PULL_DATE = time.strftime("%Y%m%d")
RECALL_YEARS = list(range(1998, 2026))
CURRENT_QUERY_MATTER_TYPES = {"Land Use Application", "Land Use Call-Up", "Resolution"}
COOKIE_JAR = "../temp/legistar_all_type_universe_cookies.txt"

CURRENT_RECALL_TEXT_RE = re.compile(
    r"("
    r"\bULURP\b|"
    r"uniform land use|"
    r"land use review|"
    r"city planning commission|"
    r"section 197-[cd]|"
    r"§\s*197-[cd]|"
    r"\bUDAAP\b|"
    r"urban development action area|"
    r"\bC\s*\d{6}\s*[A-Z]{2,4}\b|"
    r"\bN\s*\d{6}\s*[A-Z]{2,4}\b|"
    r"\bM\s*\d{6}"
    r")",
    re.IGNORECASE,
)

APPLICATION_RE = re.compile(
    r"\b(?:[CNM]\s*)?\d{6}\s*(?:\([A-Z0-9]+\)\s*)?[A-Z]{2,4}\b",
    re.IGNORECASE,
)

ZONING_RE = re.compile(
    r"("
    r"\bzoning\b|"
    r"zoning resolution|"
    r"zoning map|"
    r"special district|"
    r"special permit|"
    r"text amendment|"
    r"large[- ]scale residential|"
    r"large[- ]scale general development"
    r")",
    re.IGNORECASE,
)

CITY_MAP_RE = re.compile(
    r"("
    r"city map|"
    r"map change|"
    r"mapping|"
    r"demapping|"
    r"discontinuance|"
    r"street closing|"
    r"street opening|"
    r"street name"
    r")",
    re.IGNORECASE,
)

SITE_REAL_PROPERTY_RE = re.compile(
    r"("
    r"site selection|"
    r"acquisition|"
    r"disposition|"
    r"real property|"
    r"urban development action area|"
    r"\bUDAAP\b|"
    r"urban renewal"
    r")",
    re.IGNORECASE,
)

LANDMARK_RE = re.compile(r"landmark|landmarks preservation|historic district", re.IGNORECASE)

SIDEWALK_REVOCABLE_RE = re.compile(
    r"("
    r"sidewalk cafe|"
    r"unenclosed sidewalk|"
    r"enclosed sidewalk|"
    r"revocable consent|"
    r"franchise"
    r")",
    re.IGNORECASE,
)

QUERY_ROW_FIELDS = [
    "source_id",
    "pull_date",
    "query_year",
    "query_matter_type",
    "query_matter_type_value",
    "query_page",
    "query_record_count",
    "query_page_count",
    "matter_id",
    "matter_guid",
    "matter_file",
    "matter_file_year",
    "matter_url",
    "law_number",
    "matter_type",
    "status",
    "committee",
    "prime_sponsor",
    "council_member_sponsors",
    "title",
    "borough",
    "affected_council_districts",
    "application_numbers_in_title",
    "current_recall_text_flag",
    "committee_land_use_flag",
    "current_recall_rule_flag",
    "candidate_signal_reasons",
    "broad_audit_land_use_signal_flag",
    "outside_current_recall_universe_flag",
    "current_query_matter_type_flag",
]

MATTER_INDEX_FIELDS = QUERY_ROW_FIELDS + [
    "queried_matter_types",
    "queried_matter_type_values",
    "query_row_count",
    "candidate_review_priority",
]


def normalize_space(value):
    return re.sub(r"\s+", " ", "" if value is None else str(value)).strip()


def attrs_dict(attrs):
    return {name.lower(): "" if value is None else value for name, value in attrs}


def write_csv(path, rows, fieldnames):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def read_csv_rows(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def save_text(path, text):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(text, encoding="utf-8")


def sha256(path):
    output_path = "../temp/legistar_shasum.txt"
    command = f"shasum -a 256 {shell_quote(path)} > {shell_quote(output_path)}"
    run_shell(command, "../temp/legistar_shasum_stderr.txt")
    return Path(output_path).read_text(encoding="utf-8").split()[0]


def shell_quote(value):
    return "'" + str(value).replace("'", "'\"'\"'") + "'"


def run_shell(command, stderr_path):
    status = os.system(f"{command} 2> {shell_quote(stderr_path)}")
    if status != 0:
        stderr = Path(stderr_path).read_text(encoding="utf-8") if Path(stderr_path).exists() else ""
        raise RuntimeError(stderr or f"Command failed with status {status}: {command}")


class FormInputParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.inputs = {}

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "input":
            return

        attr = attrs_dict(attrs)
        name = attr.get("name", "")
        if not name:
            return

        input_type = attr.get("type", "").lower()
        if input_type in {"submit", "button", "image"}:
            return
        if input_type in {"checkbox", "radio"} and "checked" not in attr:
            return

        self.inputs[name] = attr.get("value", "")


class MatterTypeParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.in_type_select = False
        self.in_option = False
        self.option_value = ""
        self.option_text = []
        self.rows = []

    def handle_starttag(self, tag, attrs):
        attr = attrs_dict(attrs)
        if tag.lower() == "select":
            select_id = " ".join([attr.get("id", ""), attr.get("name", "")])
            self.in_type_select = "lstTypeBasic" in select_id
        elif self.in_type_select and tag.lower() == "option":
            self.in_option = True
            self.option_value = attr.get("value", "")
            self.option_text = []

    def handle_data(self, data):
        if self.in_option:
            self.option_text.append(data)

    def handle_endtag(self, tag):
        if tag.lower() == "option" and self.in_option:
            matter_type = normalize_space("".join(self.option_text))
            matter_type_value = normalize_space(self.option_value)
            if matter_type and matter_type_value and matter_type.lower() not in {"all", "all types"}:
                self.rows.append(
                    {
                        "source_id": SOURCE_ID,
                        "pull_date": PULL_DATE,
                        "matter_type": matter_type,
                        "matter_type_value": matter_type_value,
                    }
                )
            self.in_option = False
        elif tag.lower() == "select" and self.in_type_select:
            self.in_type_select = False


class PageParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.info_texts = []
        self.record_texts = []
        self.page_links = {}
        self.in_info = False
        self.info_text = []
        self.in_record_span = False
        self.record_text = []
        self.in_pager_cell = False
        self.in_pager_link = False
        self.pager_href = ""
        self.pager_text = []

    def handle_starttag(self, tag, attrs):
        attr = attrs_dict(attrs)
        tag = tag.lower()
        class_name = attr.get("class", "")
        if tag == "div" and "rgInfoPart" in class_name:
            self.in_info = True
            self.info_text = []
        elif tag == "span" and "rmText" in class_name:
            self.in_record_span = True
            self.record_text = []
        elif tag == "td" and "rgPagerCell" in class_name:
            self.in_pager_cell = True
        elif self.in_pager_cell and tag == "a":
            self.in_pager_link = True
            self.pager_href = attr.get("href", "")
            self.pager_text = []

    def handle_data(self, data):
        if self.in_info:
            self.info_text.append(data)
        if self.in_record_span:
            self.record_text.append(data)
        if self.in_pager_link:
            self.pager_text.append(data)

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag == "div" and self.in_info:
            self.info_texts.append(normalize_space("".join(self.info_text)))
            self.in_info = False
        elif tag == "span" and self.in_record_span:
            self.record_texts.append(normalize_space("".join(self.record_text)))
            self.in_record_span = False
        elif tag == "a" and self.in_pager_link:
            page_text = normalize_space("".join(self.pager_text))
            match = re.search(r"__doPostBack\('([^']+)'", self.pager_href)
            if page_text.isdigit() and match:
                self.page_links[int(page_text)] = match.group(1)
            self.in_pager_link = False
        elif tag == "td" and self.in_pager_cell:
            self.in_pager_cell = False


class GridParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.rows = []
        self.in_row = False
        self.in_cell = False
        self.cells = []
        self.cell_text = []
        self.first_href = ""

    def handle_starttag(self, tag, attrs):
        attr = attrs_dict(attrs)
        tag = tag.lower()
        class_name = attr.get("class", "")
        if tag == "tr" and ("rgRow" in class_name or "rgAltRow" in class_name):
            self.in_row = True
            self.cells = []
            self.first_href = ""
        elif self.in_row and tag == "td":
            self.in_cell = True
            self.cell_text = []
        elif self.in_row and self.in_cell and tag == "a" and not self.first_href:
            self.first_href = attr.get("href", "")

    def handle_data(self, data):
        if self.in_row and self.in_cell:
            self.cell_text.append(data)

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag == "td" and self.in_cell:
            self.cells.append(normalize_space("".join(self.cell_text)))
            self.in_cell = False
        elif tag == "tr" and self.in_row:
            self.rows.append({"cells": self.cells, "href": self.first_href})
            self.in_row = False


def curl_request(method, url, payload=None):
    Path(COOKIE_JAR).parent.mkdir(parents=True, exist_ok=True)
    response_path = "../temp/legistar_curl_response.html"
    payload_path = "../temp/legistar_curl_payload.txt"
    stderr_path = "../temp/legistar_curl_stderr.txt"

    command_parts = [
        "curl",
        "-sS",
        "-L",
        "--connect-timeout 20",
        "--max-time 90",
        "-b",
        shell_quote(COOKIE_JAR),
        "-c",
        shell_quote(COOKIE_JAR),
        "-A",
        shell_quote("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Safari/537.36"),
        "-e",
        shell_quote(BASE_URL),
        "-o",
        shell_quote(response_path),
    ]
    if method == "POST":
        Path(payload_path).write_text(urlencode(payload or {}), encoding="utf-8")
        command_parts.extend(
            [
                "-H",
                shell_quote("Content-Type: application/x-www-form-urlencoded"),
                "--data-binary",
                "@" + shell_quote(payload_path),
            ]
        )
    command_parts.append(shell_quote(url))
    command = " ".join(command_parts)

    last_error = None
    for attempt in range(1, 4):
        status = os.system(f"{command} 2> {shell_quote(stderr_path)}")
        if status == 0:
            return Path(response_path).read_text(encoding="utf-8")
        last_error = Path(stderr_path).read_text(encoding="utf-8") if Path(stderr_path).exists() else ""
        if attempt < 3:
            time.sleep(5 * attempt)
    raise RuntimeError(last_error)


def parse_form_inputs(html_text):
    parser = FormInputParser()
    parser.feed(html_text)
    return parser.inputs


def combo_client_state(value, text):
    return json.dumps(
        {
            "logEntries": [],
            "value": value,
            "text": text,
            "enabled": True,
            "checkedIndices": [],
            "checkedItemsTextOverflows": False,
        },
        separators=(",", ":"),
    )


def legislation_payload(html_text, query_year, matter_type, matter_type_value, event_target):
    payload = parse_form_inputs(html_text)
    payload.update(
        {
            "__EVENTTARGET": event_target,
            "__EVENTARGUMENT": "",
            "ctl00$ContentPlaceHolder1$txtSearch": "",
            "ctl00$ContentPlaceHolder1$lstYears": str(query_year),
            "ctl00_ContentPlaceHolder1_lstYears_ClientState": combo_client_state(str(query_year), str(query_year)),
            "ctl00$ContentPlaceHolder1$lstTypeBasic": matter_type,
            "ctl00_ContentPlaceHolder1_lstTypeBasic_ClientState": combo_client_state(matter_type_value, matter_type),
            "ctl00$ContentPlaceHolder1$chkID": "on",
            "ctl00$ContentPlaceHolder1$chkText": "on",
        }
    )
    return payload


def parse_matter_type_lookup(html_text):
    parser = MatterTypeParser()
    parser.feed(html_text)
    seen = set()
    rows = []
    for row in parser.rows:
        key = (row["matter_type"], row["matter_type_value"])
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)
    if not rows:
        list_match = re.search(
            r'id="ctl00_ContentPlaceHolder1_lstTypeBasic_DropDown".*?<ul class="rcbList">(.*?)</ul>',
            html_text,
            flags=re.DOTALL,
        )
        data_match = re.search(
            r'"_uniqueId":"ctl00\$ContentPlaceHolder1\$lstTypeBasic".*?"itemData":\[(.*?)\]',
            html_text,
            flags=re.DOTALL,
        )
        if list_match and data_match:
            matter_types = [
                normalize_space(html.unescape(re.sub(r"<.*?>", "", match.group(1))))
                for match in re.finditer(r'<li class="rcbItem">(.*?)</li>', list_match.group(1), flags=re.DOTALL)
            ]
            matter_type_values = [
                html.unescape(match.group(1))
                for match in re.finditer(r'\{"value":"([^"]+)"', data_match.group(1))
            ]
            for matter_type, matter_type_value in zip(matter_types, matter_type_values):
                if matter_type and matter_type_value and matter_type.lower() not in {"all", "all types"}:
                    rows.append(
                        {
                            "source_id": SOURCE_ID,
                            "pull_date": PULL_DATE,
                            "matter_type": matter_type,
                            "matter_type_value": matter_type_value,
                        }
                    )
        if not rows:
            raise RuntimeError("Legistar matter-type lookup parsed to zero rows.")
    return sorted(rows, key=lambda row: (row["matter_type"], row["matter_type_value"]))


def parse_page_info(html_text):
    parser = PageParser()
    parser.feed(html_text)
    for text in parser.info_texts:
        match = re.search(
            r"Page\s+(\d+)\s+of\s+(\d+)\s*,\s*items\s+(\d+)\s+to\s+(\d+)\s+of\s+(\d+)\s*\.?",
            text,
        )
        if match:
            return {
                "current_page": int(match.group(1)),
                "page_count": int(match.group(2)),
                "page_first_item": int(match.group(3)),
                "page_last_item": int(match.group(4)),
                "record_count": int(match.group(5)),
                "page_links": parser.page_links,
            }
    for text in parser.record_texts:
        match = re.match(r"^([0-9,]+) records?$", text)
        if match:
            record_count = int(match.group(1).replace(",", ""))
            return {
                "current_page": 1,
                "page_count": 1,
                "page_first_item": 1 if record_count else 0,
                "page_last_item": record_count,
                "record_count": record_count,
                "page_links": parser.page_links,
            }
    raise RuntimeError("Could not parse Legistar page count.")


def extract_matter_id_and_guid(href):
    parsed = urlparse(html.unescape(href).replace("&amp;", "&"))
    query = parse_qs(parsed.query)
    return query.get("ID", [""])[0], query.get("GUID", [""])[0]


def extract_borough(title):
    match = re.search(r"Borough of ([A-Za-z ]+?)(?:,|\.| in |$)", title, flags=re.IGNORECASE)
    if not match:
        return ""
    return normalize_space(match.group(1)).title()


def extract_council_districts(title):
    match = re.search(
        r"Council District(?:s| Nos?\.?| no\.?)?\s*([0-9,\sand-]+)",
        title,
        flags=re.IGNORECASE,
    )
    if not match:
        return ""
    districts = re.findall(r"\d{1,2}", match.group(1))
    return "; ".join(dict.fromkeys(districts))


def extract_application_numbers(title):
    matches = [normalize_space(match.group(0)).upper() for match in APPLICATION_RE.finditer(title)]
    return "; ".join(dict.fromkeys(matches))


def parse_grid_rows(html_text, query_year, matter_type, matter_type_value, page_info):
    parser = GridParser()
    parser.feed(html_text)
    rows = []
    for raw_row in parser.rows:
        cells = raw_row["cells"]
        if len(cells) < 8 or not raw_row["href"]:
            continue
        matter_id, matter_guid = extract_matter_id_and_guid(raw_row["href"])
        matter_file = normalize_space(cells[0])
        title = normalize_space(cells[7])
        parsed_matter_type = normalize_space(cells[2])
        committee = normalize_space(cells[4])
        current_text_flag = bool(CURRENT_RECALL_TEXT_RE.search(title))
        committee_land_use_flag = "land use" in committee.lower()
        current_recall_rule_flag = (
            parsed_matter_type in CURRENT_QUERY_MATTER_TYPES
            or current_text_flag
            or committee_land_use_flag
        )
        rows.append(
            {
                "source_id": SOURCE_ID,
                "pull_date": PULL_DATE,
                "query_year": query_year,
                "query_matter_type": matter_type,
                "query_matter_type_value": matter_type_value,
                "query_page": page_info["current_page"],
                "query_record_count": page_info["record_count"],
                "query_page_count": page_info["page_count"],
                "matter_id": matter_id,
                "matter_guid": matter_guid,
                "matter_file": matter_file,
                "matter_file_year": re.search(r"-(\d{4})$", matter_file).group(1)
                if re.search(r"-(\d{4})$", matter_file)
                else "",
                "matter_url": urljoin(BASE_URL, html.unescape(raw_row["href"]).replace("&amp;", "&")),
                "law_number": normalize_space(cells[1]),
                "matter_type": parsed_matter_type,
                "status": normalize_space(cells[3]),
                "committee": committee,
                "prime_sponsor": normalize_space(cells[5]),
                "council_member_sponsors": normalize_space(cells[6]),
                "title": title,
                "borough": extract_borough(title),
                "affected_council_districts": extract_council_districts(title),
                "application_numbers_in_title": extract_application_numbers(title),
                "current_recall_text_flag": current_text_flag,
                "committee_land_use_flag": committee_land_use_flag,
                "current_recall_rule_flag": current_recall_rule_flag,
            }
        )
    return rows


def extract_signal_reasons(row):
    title = normalize_space(row.get("title"))
    committee = normalize_space(row.get("committee"))
    reasons = []
    if str(row.get("current_recall_rule_flag")).lower() == "true":
        reasons.append("current_recall_text_or_committee_rule")
    if normalize_space(row.get("application_numbers_in_title")):
        reasons.append("application_number")
    if ZONING_RE.search(title):
        reasons.append("zoning_or_special_district")
    if CITY_MAP_RE.search(title):
        reasons.append("city_map_or_street_mapping")
    if SITE_REAL_PROPERTY_RE.search(title):
        reasons.append("site_selection_acquisition_disposition")
    if LANDMARK_RE.search(title):
        reasons.append("landmark_or_historic_district")
    if SIDEWALK_REVOCABLE_RE.search(title):
        reasons.append("sidewalk_cafe_revocable_consent_or_franchise")
    if "land use" in committee.lower():
        reasons.append("land_use_committee")
    return "; ".join(dict.fromkeys(reasons))


def candidate_priority(reasons):
    reason_set = set(reasons.split("; ")) if reasons else set()
    if reason_set & {
        "current_recall_text_or_committee_rule",
        "application_number",
        "zoning_or_special_district",
    }:
        return "high"
    if reason_set & {
        "city_map_or_street_mapping",
        "site_selection_acquisition_disposition",
    }:
        return "medium"
    if reason_set:
        return "low"
    return ""


def safe_stub(value):
    stub = re.sub(r"[^a-z0-9]+", "_", normalize_space(value).lower()).strip("_")
    return stub or "missing"


def fetch_search_pages(query_year, matter_type, matter_type_value):
    landing_html = curl_request("GET", BASE_URL)
    first_page_html = curl_request(
        "POST",
        BASE_URL,
        legislation_payload(
            landing_html,
            query_year,
            matter_type,
            matter_type_value,
            "ctl00$ContentPlaceHolder1$btnSearch",
        ),
    )

    page_fetch_rows = []
    matter_rows = []
    current_html = first_page_html
    page_info = parse_page_info(current_html)
    page_links = page_info["page_links"]

    for page_number in range(1, page_info["page_count"] + 1):
        if page_number > 1:
            if page_number not in page_links:
                raise RuntimeError(f"Missing pager link for {query_year} {matter_type} page {page_number}.")
            current_html = curl_request(
                "POST",
                BASE_URL,
                legislation_payload(
                    current_html,
                    query_year,
                    matter_type,
                    matter_type_value,
                    page_links[page_number],
                ),
            )
            page_info = parse_page_info(current_html)
            page_links.update(page_info["page_links"])

        raw_path = (
            Path("../output/source_files")
            / SOURCE_ID
            / PULL_DATE
            / f"year_{query_year}"
            / safe_stub(matter_type)
            / "index_pages"
            / f"page_{page_number:03d}.html"
        )
        save_text(raw_path, current_html)
        parsed_rows = parse_grid_rows(current_html, query_year, matter_type, matter_type_value, page_info)
        matter_rows.extend(parsed_rows)
        page_fetch_rows.append(
            {
                "source_id": SOURCE_ID,
                "pull_date": PULL_DATE,
                "query_year": query_year,
                "query_matter_type": matter_type,
                "query_matter_type_value": matter_type_value,
                "query_page": page_number,
                "reported_current_page": page_info["current_page"],
                "reported_page_count": page_info["page_count"],
                "reported_record_count": page_info["record_count"],
                "parsed_rows": len(parsed_rows),
                "raw_path": str(raw_path),
                "file_size_bytes": raw_path.stat().st_size,
                "checksum_sha256": sha256(raw_path),
            }
        )

    return matter_rows, page_fetch_rows


def current_recall_ids():
    ids = set()
    for year in RECALL_YEARS:
        for row in read_csv_rows(f"../input/legistar_{year}_broad_recall_matter_index.csv"):
            matter_id = normalize_space(row.get("matter_id"))
            if matter_id:
                ids.add(matter_id)
    return ids


def collapse_unique(values):
    clean_values = [normalize_space(value) for value in values if normalize_space(value)]
    return "; ".join(dict.fromkeys(clean_values))


def write_checklist():
    Path("../output/research_understanding_checklist.md").write_text(
        "\n".join(
            [
                "# Research Understanding Checklist",
                "",
                "## Session Goal",
                "- [x] Research question or task: audit whether the council land-use recall universe misses land-use-looking Legistar matters outside the current three matter types.",
                "- [x] Why this matters: missing matter types could mechanically understate member-deference conflicts or make the time series look cleaner than the true council record.",
                "- [x] What changed in this session: added a separate audit task that scans all Legistar matter types and produces a candidate queue, not a production sample change.",
                "",
                "## Stage 1: Problem And Motivation",
                "- [x] Problem: the production recall starts from Land Use Application, Land Use Call-Up, and Resolution queries.",
                "- [x] Why a naive approach could fail: relevant land-use items could appear under other Legistar matter types.",
                "- [x] Decision type: scope and data-quality.",
                "- [ ] Mastery status: needs user review after reading the candidate outputs.",
                "",
                "## Stage 2: Data Provenance And Raw Inputs",
                "- [x] Primary source: NYC Council Legistar legislation search pages, queried by year and matter type.",
                "- [x] Validation source: Legistar's own reported record counts on each search result.",
                "- [x] Current-production comparison source: fetched broad-recall matter indexes from tasks/fetch_council_land_use_records.",
                "- [ ] Mastery status: pending.",
                "",
                "## Stage 3: Cleaning And Construction Logic",
                "- [x] Mechanical: parse Legistar result grids, matter ids, matter files, titles, committees, statuses, borough and council-district text.",
                "- [x] Data-quality: compare parsed rows to reported search-result counts by year and matter type.",
                "- [x] Substantive/scope: flag broad land-use-looking candidates by text signals, but do not automatically add them to the main deference universe.",
                "- [ ] Mastery status: pending.",
                "",
                "## Open Questions",
                "- [ ] Which outside-current candidates are true council land-use decisions relevant to member deference?",
                "- [ ] Are any candidate matter types systematically missing in early years, post-2002 years, or particular policy categories?",
                "",
            ]
        ),
        encoding="utf-8",
    )


landing_html = curl_request("GET", BASE_URL)
save_text(
    Path("../output/source_files") / SOURCE_ID / PULL_DATE / "legislation_search_landing_page.html",
    landing_html,
)
matter_type_lookup = parse_matter_type_lookup(landing_html)
write_csv(
    "../output/legistar_matter_type_lookup.csv",
    matter_type_lookup,
    ["source_id", "pull_date", "matter_type", "matter_type_value"],
)

existing_recall_ids = current_recall_ids()
all_matter_rows = []
all_page_fetch_rows = []

for query_year in RECALL_YEARS:
    for query in matter_type_lookup:
        matter_rows, page_fetch_rows = fetch_search_pages(
            query_year,
            query["matter_type"],
            query["matter_type_value"],
        )
        all_matter_rows.extend(matter_rows)
        all_page_fetch_rows.extend(page_fetch_rows)
        print(f"Fetched {query_year} {query['matter_type']}: {len(matter_rows)} rows", flush=True)
        time.sleep(0.03)

write_csv(
    "../output/legistar_all_type_page_fetches.csv",
    all_page_fetch_rows,
    [
        "source_id",
        "pull_date",
        "query_year",
        "query_matter_type",
        "query_matter_type_value",
        "query_page",
        "reported_current_page",
        "reported_page_count",
        "reported_record_count",
        "parsed_rows",
        "raw_path",
        "file_size_bytes",
        "checksum_sha256",
    ],
)

if not all_matter_rows:
    raise RuntimeError("All-type Legistar audit parsed zero matter rows.")

for row in all_matter_rows:
    if not normalize_space(row.get("matter_id")):
        raise RuntimeError("All-type Legistar audit found a parsed row without matter_id.")
    row["candidate_signal_reasons"] = extract_signal_reasons(row)
    row["broad_audit_land_use_signal_flag"] = bool(row["candidate_signal_reasons"])
    row["outside_current_recall_universe_flag"] = normalize_space(row["matter_id"]) not in existing_recall_ids
    row["current_query_matter_type_flag"] = normalize_space(row["query_matter_type"]) in CURRENT_QUERY_MATTER_TYPES

write_csv("../output/legistar_all_type_query_rows.csv", all_matter_rows, QUERY_ROW_FIELDS)

query_summary_map = {}
for row in all_page_fetch_rows:
    key = (row["query_year"], row["query_matter_type"], row["query_matter_type_value"])
    if key not in query_summary_map:
        query_summary_map[key] = {
            "query_year": row["query_year"],
            "query_matter_type": row["query_matter_type"],
            "query_matter_type_value": row["query_matter_type_value"],
            "reported_records": 0,
            "parsed_rows": 0,
            "reported_pages": 0,
            "fetched_pages": set(),
        }
    query_summary_map[key]["reported_records"] = max(
        int(query_summary_map[key]["reported_records"]),
        int(row["reported_record_count"] or 0),
    )
    query_summary_map[key]["parsed_rows"] += int(row["parsed_rows"] or 0)
    query_summary_map[key]["reported_pages"] = max(
        int(query_summary_map[key]["reported_pages"]),
        int(row["reported_page_count"] or 0),
    )
    query_summary_map[key]["fetched_pages"].add(row["query_page"])

query_summary = []
for row in query_summary_map.values():
    query_summary.append(
        {
            "query_year": row["query_year"],
            "query_matter_type": row["query_matter_type"],
            "query_matter_type_value": row["query_matter_type_value"],
            "reported_records": row["reported_records"],
            "parsed_rows": row["parsed_rows"],
            "reported_pages": row["reported_pages"],
            "fetched_pages": len(row["fetched_pages"]),
            "rows_match_reported_records": row["reported_records"] == row["parsed_rows"],
        }
    )
query_summary = sorted(query_summary, key=lambda row: (int(row["query_year"]), row["query_matter_type"]))
write_csv(
    "../output/council_land_use_matter_type_query_summary.csv",
    query_summary,
    [
        "query_year",
        "query_matter_type",
        "query_matter_type_value",
        "reported_records",
        "parsed_rows",
        "reported_pages",
        "fetched_pages",
        "rows_match_reported_records",
    ],
)

matter_index_map = {}
for row in sorted(
    all_matter_rows,
    key=lambda x: (normalize_space(x["matter_id"]), int(x["query_year"]), normalize_space(x["query_matter_type"])),
):
    matter_id = normalize_space(row["matter_id"])
    if matter_id not in matter_index_map:
        matter_index_map[matter_id] = dict(row)
        matter_index_map[matter_id]["queried_matter_types_list"] = []
        matter_index_map[matter_id]["queried_matter_type_values_list"] = []
        matter_index_map[matter_id]["query_row_count"] = 0
    matter_index_map[matter_id]["queried_matter_types_list"].append(row["query_matter_type"])
    matter_index_map[matter_id]["queried_matter_type_values_list"].append(row["query_matter_type_value"])
    matter_index_map[matter_id]["query_row_count"] += 1

matter_index = []
for matter_id, row in matter_index_map.items():
    row["queried_matter_types"] = collapse_unique(row["queried_matter_types_list"])
    row["queried_matter_type_values"] = collapse_unique(row["queried_matter_type_values_list"])
    row["outside_current_recall_universe_flag"] = matter_id not in existing_recall_ids
    row["candidate_signal_reasons"] = extract_signal_reasons(row)
    row["broad_audit_land_use_signal_flag"] = bool(row["candidate_signal_reasons"])
    row["candidate_review_priority"] = candidate_priority(row["candidate_signal_reasons"])
    matter_index.append(row)

matter_index = sorted(matter_index, key=lambda row: (int(row["query_year"]), normalize_space(row["matter_file"])))
write_csv("../output/legistar_all_type_matter_index.csv", matter_index, MATTER_INDEX_FIELDS)

priority_order = {"high": 1, "medium": 2, "low": 3, "": 4}
candidates = [
    row
    for row in matter_index
    if row["outside_current_recall_universe_flag"] and row["broad_audit_land_use_signal_flag"]
]
candidates = sorted(
    candidates,
    key=lambda row: (
        priority_order.get(normalize_space(row["candidate_review_priority"]), 9),
        int(row["query_year"]),
        normalize_space(row["matter_type"]),
        normalize_space(row["matter_file"]),
    ),
)
write_csv("../output/council_land_use_outside_current_type_candidates.csv", candidates, MATTER_INDEX_FIELDS)

candidate_summary_map = {}
for row in candidates:
    key = (
        normalize_space(row["candidate_review_priority"]),
        normalize_space(row["candidate_signal_reasons"]),
        normalize_space(row["matter_type"]),
    )
    if key not in candidate_summary_map:
        candidate_summary_map[key] = {
            "candidate_review_priority": key[0],
            "candidate_signal_reasons": key[1],
            "matter_type": key[2],
            "matter_ids": set(),
            "first_year": int(row["query_year"]),
            "last_year": int(row["query_year"]),
        }
    candidate_summary_map[key]["matter_ids"].add(row["matter_id"])
    candidate_summary_map[key]["first_year"] = min(candidate_summary_map[key]["first_year"], int(row["query_year"]))
    candidate_summary_map[key]["last_year"] = max(candidate_summary_map[key]["last_year"], int(row["query_year"]))

if candidate_summary_map:
    candidate_summary = [
        {
            "candidate_review_priority": row["candidate_review_priority"],
            "candidate_signal_reasons": row["candidate_signal_reasons"],
            "matter_type": row["matter_type"],
            "matter_count": len(row["matter_ids"]),
            "first_year": row["first_year"],
            "last_year": row["last_year"],
        }
        for row in candidate_summary_map.values()
    ]
else:
    candidate_summary = [
        {
            "candidate_review_priority": "none",
            "candidate_signal_reasons": "none",
            "matter_type": "none",
            "matter_count": 0,
            "first_year": "",
            "last_year": "",
        }
    ]

candidate_summary = sorted(
    candidate_summary,
    key=lambda row: (
        priority_order.get(normalize_space(row["candidate_review_priority"]), 9),
        -int(row["matter_count"]),
        normalize_space(row["matter_type"]),
    ),
)
write_csv(
    "../output/council_land_use_outside_current_type_candidate_summary.csv",
    candidate_summary,
    [
        "candidate_review_priority",
        "candidate_signal_reasons",
        "matter_type",
        "matter_count",
        "first_year",
        "last_year",
    ],
)

all_matter_id_set = set(matter_index_map)
query_counts_reconcile = all(row["rows_match_reported_records"] for row in query_summary)
current_recall_covered = existing_recall_ids.issubset(all_matter_id_set)
qc_rows = [
    {
        "check": "matter_type_lookup_nonempty",
        "value": len(matter_type_lookup),
        "pass": len(matter_type_lookup) > 0,
    },
    {
        "check": "all_type_query_rows_nonempty",
        "value": len(all_matter_rows),
        "pass": len(all_matter_rows) > 0,
    },
    {
        "check": "all_type_matter_ids_unique_after_collapse",
        "value": len(matter_index) - len(all_matter_id_set),
        "pass": len(matter_index) == len(all_matter_id_set),
    },
    {
        "check": "all_type_query_count_reconciles",
        "value": sum(1 for row in query_summary if row["rows_match_reported_records"]),
        "pass": query_counts_reconcile,
    },
    {
        "check": "all_current_recall_ids_found_in_all_type_index",
        "value": sum(1 for matter_id in existing_recall_ids if matter_id in all_matter_id_set),
        "pass": current_recall_covered,
    },
    {
        "check": "outside_current_candidates_reported_for_review",
        "value": len(candidates),
        "pass": True,
    },
]
write_csv("../output/council_land_use_matter_type_universe_audit_qc.csv", qc_rows, ["check", "value", "pass"])

if not query_counts_reconcile:
    raise RuntimeError("At least one all-type Legistar query did not reconcile to the reported row count.")
if not current_recall_covered:
    raise RuntimeError("At least one current recall matter id was absent from the all-type Legistar index.")

write_checklist()
