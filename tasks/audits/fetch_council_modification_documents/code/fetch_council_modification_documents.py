# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/fetch_council_modification_documents/code")
# recall_years <- "2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024 2025"
# fetch_mode <- "download"
# report_fetch_scope <- "all_matched"
# attachment_fetch_mode <- "candidate_attachments"
# full_text_export <- "omit_full_text"

from __future__ import annotations

import hashlib
import html
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import pandas as pd

try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover - optional local parser
    BeautifulSoup = None


if len(sys.argv) != 6:
    raise RuntimeError(
        "Usage: python3 fetch_council_modification_documents.py "
        "<recall_years> <fetch_mode> <report_fetch_scope> <attachment_fetch_mode> <full_text_export>"
    )

RECALL_YEARS = [year.strip() for year in sys.argv[1].split() if year.strip()]
FETCH_MODE = sys.argv[2].strip()
REPORT_FETCH_SCOPE = sys.argv[3].strip()
ATTACHMENT_FETCH_MODE = sys.argv[4].strip()
FULL_TEXT_EXPORT = sys.argv[5].strip()

if not all(re.fullmatch(r"\d{4}", year) for year in RECALL_YEARS):
    raise RuntimeError("recall_years must be a space-separated list of four-digit years.")
if FETCH_MODE not in {"index_only", "download"}:
    raise RuntimeError("fetch_mode must be index_only or download.")
if REPORT_FETCH_SCOPE not in {"all_matched", "modification_signal"}:
    raise RuntimeError("report_fetch_scope must be all_matched or modification_signal.")
if ATTACHMENT_FETCH_MODE not in {"reports_only", "candidate_attachments"}:
    raise RuntimeError("attachment_fetch_mode must be reports_only or candidate_attachments.")
if FULL_TEXT_EXPORT not in {"omit_full_text", "write_full_text"}:
    raise RuntimeError("full_text_export must be omit_full_text or write_full_text.")

DOCUMENT_LINKS_OUTPUT = Path("../output/ulurp_modification_council_document_links.csv")
DOCUMENT_TEXT_OUTPUT = Path("../output/ulurp_modification_council_document_text.csv")
DOCUMENT_SNIPPETS_OUTPUT = Path("../output/ulurp_modification_council_document_snippets.csv")
DOWNLOAD_DIR = Path("../temp/council_documents")

LINK_COLUMNS = [
    "document_id",
    "project_ids",
    "matter_id",
    "matter_file",
    "query_year",
    "source_field",
    "document_family",
    "document_role",
    "source_priority",
    "source_url",
    "source_label",
    "matter_title",
    "download_selected",
    "download_reason",
    "points_of_agreement_candidate",
    "committee_report_candidate",
    "resolution_candidate",
    "m_matter_candidate",
    "fetch_mode",
    "fetch_status",
    "local_path",
    "extraction_status",
    "text_row_count",
    "snippet_row_count",
]
TEXT_COLUMNS = [
    "document_id",
    "project_ids",
    "matter_id",
    "matter_file",
    "query_year",
    "document_family",
    "source_doc",
    "page",
    "document_text",
    "extraction_method",
    "confidence",
]
SNIPPET_COLUMNS = [
    "snippet_id",
    "document_id",
    "project_ids",
    "matter_id",
    "matter_file",
    "query_year",
    "document_family",
    "keyword_family",
    "source_doc",
    "page",
    "snippet",
    "extraction_method",
    "confidence",
]
def normalize_space(value: object) -> str:
    text = "" if pd.isna(value) else str(value)
    text = text.replace("\x00", " ")
    text = re.sub(r"[\x01-\x08\x0b\x0c\x0e-\x1f\x7f]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def split_semicolon(value: object) -> list[str]:
    text = normalize_space(value)
    if text == "":
        return []
    return [part.strip() for part in text.split(";") if part.strip()]


def stable_id(*parts: object) -> str:
    text = "||".join(normalize_space(part) for part in parts)
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:20]


def true_false(value: bool) -> str:
    return "true" if value else "false"


def write_csv(path: Path, rows: list[dict[str, object]], columns: list[str]) -> None:
    df = pd.DataFrame(rows, columns=columns)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(temp_path, index=False)
    if path.exists() and path.read_bytes() == temp_path.read_bytes():
        temp_path.unlink()
        path.touch()
    else:
        temp_path.replace(path)


def assert_unique(df: pd.DataFrame, key_cols: list[str], df_name: str) -> None:
    duplicates = df[df.duplicated(key_cols, keep=False)]
    if not duplicates.empty:
        raise RuntimeError(f"{df_name} is not unique by {', '.join(key_cols)}.")


def read_existing_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise RuntimeError(f"Required input is missing: {path}")
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def resolve_raw_path(raw_path: object) -> Path | None:
    text = normalize_space(raw_path)
    if text == "":
        return None

    candidate = Path(text)
    if candidate.exists():
        return candidate

    fetch_code_relative = Path("../../fetch_council_land_use_records/code") / text
    if fetch_code_relative.exists():
        return fetch_code_relative

    fetch_output_relative = Path("../../fetch_council_land_use_records/output") / text.removeprefix("../output/")
    if fetch_output_relative.exists():
        return fetch_output_relative

    return None


def collect_link_labels(html_path: Path | None) -> dict[str, str]:
    if html_path is None:
        return {}

    try:
        page_bytes = html_path.read_bytes()
    except OSError:
        return {}

    labels: dict[str, str] = {}
    anchor_pattern = re.compile(rb"<a\b[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", re.IGNORECASE | re.DOTALL)
    for match in anchor_pattern.finditer(page_bytes):
        href = html.unescape(match.group(1).decode("utf-8", errors="ignore"))
        if href == "":
            continue

        label_text = re.sub(rb"<[^>]+>", b" ", match.group(2))
        label = normalize_space(html.unescape(label_text.decode("utf-8", errors="ignore")))
        if label == "":
            title_match = re.search(rb"title=[\"']([^\"']+)[\"']", match.group(0), re.IGNORECASE | re.DOTALL)
            if title_match:
                label = normalize_space(html.unescape(title_match.group(1).decode("utf-8", errors="ignore")))
        if label == "":
            label = "NA_not_stated"

        labels[unquote(href)] = label
        labels[urljoin("https://legistar.council.nyc.gov/", href)] = label
    return labels


def label_for_url(url: str, labels: dict[str, str]) -> str:
    if url in labels:
        return labels[url]

    parsed_url = urlparse(url)
    parsed_path = unquote(parsed_url.path)
    parsed_path_query = parsed_path
    if parsed_url.query:
        parsed_path_query = parsed_path_query + "?" + unquote(parsed_url.query)

    for href, label in labels.items():
        href_text = unquote(href)
        if parsed_path_query and parsed_path_query in href_text:
            return label
        if parsed_path and parsed_path in href_text:
            return label

    title_values = parse_qs(parsed_url.query).get("Title", [])
    if title_values:
        return normalize_space(unquote(title_values[0]).replace("+", " "))

    return "NA_not_stated"


def classify_document(source_field: str, source_label: str, source_url: str, matter_file: str, title: str) -> dict[str, object]:
    text = " ".join([source_field, source_label, source_url, matter_file, title]).lower()
    matter_file_upper = matter_file.upper()

    points_of_agreement = bool(re.search(r"\bpoints?\s+of\s+agreement\b|\bpoa\b|restrictive declaration|letter agreement", text))
    committee_report = bool(re.search(r"committee report|land use committee|subcommittee report", text))
    resolution = bool(re.search(r"\bresolution\b|\bres\s+\d+|legislation details|legislation text|with text|viewreport", text))
    modification_text = bool(re.search(r"\bapproved?\s+with\s+modifications\b|\bwith\s+modifications\b", text))
    m_matter = bool(re.match(r"^M\s+\d+", matter_file_upper))

    if points_of_agreement:
        document_family = "points_of_agreement_candidate"
        source_priority = 1
    elif m_matter:
        document_family = "m_matter_or_report_candidate"
        source_priority = 1
    elif committee_report:
        document_family = "committee_report_candidate"
        source_priority = 2
    elif source_field == "detail_report_urls":
        document_family = "legislation_detail_report"
        source_priority = 3
    elif resolution:
        document_family = "resolution_or_legislation_details"
        source_priority = 3
    elif source_field == "detail_history_detail_urls":
        document_family = "history_detail_page"
        source_priority = 4
    elif source_field == "detail_meeting_detail_urls":
        document_family = "meeting_detail_page"
        source_priority = 4
    else:
        document_family = "attachment_or_meeting_file"
        source_priority = 4

    return {
        "document_family": document_family,
        "source_priority": source_priority,
        "points_of_agreement_candidate": points_of_agreement,
        "committee_report_candidate": committee_report,
        "resolution_candidate": resolution,
        "modification_text_candidate": modification_text,
        "m_matter_candidate": m_matter,
    }


DOWNLOAD_ATTACHMENT_PATTERN = re.compile(
    r"committee report|resolution|legislation text|with text|points?\s+of\s+agreement|"
    r"restrictive declaration|letter agreement|city planning commission approval|"
    r"\bcpc\b.*approval|approval letter|land use application|modified|modification|"
    r"zoning text|environmental assessment|negative declaration|eas|eis",
    re.IGNORECASE,
)

NON_SUBSTANTIVE_ATTACHMENT_PATTERN = re.compile(
    r"hearing testimony|hearing transcript|land use calendar|calendar|"
    r"agenda|minutes|notice",
    re.IGNORECASE,
)


def download_decision(
    source_field: str,
    source_label: str,
    classification: dict[str, object],
    council_modification_signal: str,
) -> tuple[bool, str]:
    if FETCH_MODE == "index_only":
        return False, "index_only"

    if source_field == "detail_report_urls":
        if (
            REPORT_FETCH_SCOPE == "modification_signal"
            and council_modification_signal != "true"
            and not bool(classification["modification_text_candidate"])
        ):
            return False, "report_scope_not_modification_signal"
        return True, "detail_report_url"

    if source_field != "detail_attachment_urls":
        return False, "link_type_not_downloaded"

    if ATTACHMENT_FETCH_MODE == "reports_only":
        return False, "attachment_fetch_mode_reports_only"

    if source_label == "NA_not_stated":
        return False, "attachment_label_missing"

    if NON_SUBSTANTIVE_ATTACHMENT_PATTERN.search(source_label):
        return False, "non_substantive_attachment_label"

    if bool(classification["points_of_agreement_candidate"]):
        return True, "points_of_agreement_candidate"
    if bool(classification["committee_report_candidate"]):
        return True, "committee_report_candidate"
    if bool(classification["resolution_candidate"]):
        return True, "resolution_candidate"
    if DOWNLOAD_ATTACHMENT_PATTERN.search(source_label):
        return True, "substantive_attachment_label"

    return False, "attachment_label_not_candidate"


def classify_action_text(matter_file: str, action_text: str) -> dict[str, object]:
    text = " ".join([matter_file, action_text]).lower()
    m_matter = bool(re.match(r"^M\s+\d+", matter_file.upper()))

    if m_matter or re.search(r"\bmodified\b|\bmodification\b|\bwith modifications\b", text):
        family = "council_action_detail_modification_signal"
        priority = 2
    else:
        family = "council_action_detail_text"
        priority = 4

    return {
        "document_family": family,
        "source_priority": priority,
        "points_of_agreement_candidate": bool(re.search(r"\bpoints?\s+of\s+agreement\b|\bpoa\b|restrictive declaration", text)),
        "committee_report_candidate": bool(re.search(r"committee report|land use committee|subcommittee report", text)),
        "resolution_candidate": bool(re.search(r"\bresolution\b|\bres\s+\d+", text)),
        "m_matter_candidate": m_matter,
    }


KEYWORD_PATTERNS = [
    ("modification_signal", re.compile(r"\bmodified\b|\bmodification\b|\bwith modifications\b", re.IGNORECASE)),
    ("unit_quantity", re.compile(r"\b\d[\d,]*\s+(?:dwelling\s+)?units?\b|\bresidential units?\b", re.IGNORECASE)),
    ("affordability", re.compile(r"\baffordable\b|\bAMI\b|\binclusionary\b|\bMIH\b", re.IGNORECASE)),
    ("height_or_bulk", re.compile(r"\bheight\b|\bstor(?:y|ies)\b|\bfloor area\b|\bFAR\b|\bzoning floor area\b", re.IGNORECASE)),
    ("parking", re.compile(r"\bparking\b|\bspaces?\b|\bgarage\b", re.IGNORECASE)),
    ("cost_mitigation", re.compile(r"\bmitigation\b|\bcapital improvement\b|\binfrastructure\b|\bsewer\b|\bstreet improvement\b|\btraffic mitigation\b|\btransportation improvement\b|\bsidewalk\b|\bwater main\b", re.IGNORECASE)),
    ("design", re.compile(r"\bdesign\b|\bopen space\b|\bsetback\b|\bfacade\b|\bbulk\b", re.IGNORECASE)),
    ("local_benefit_commitment", re.compile(r"\bschool\b|\bpark\b|\btransit\b|\bjobs?\b|\blocal hiring\b|\btenant\b|\bcommunity\b", re.IGNORECASE)),
    ("points_of_agreement", re.compile(r"\bpoints?\s+of\s+agreement\b|\bpoa\b|restrictive declaration|letter agreement", re.IGNORECASE)),
]


def snippet_window(text: str, start: int, end: int, width: int = 220) -> str:
    left = max(0, start - width)
    right = min(len(text), end + width)
    return normalize_space(text[left:right])


def extract_snippets(text: str) -> list[dict[str, str]]:
    snippets: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for keyword_family, pattern in KEYWORD_PATTERNS:
        for match in pattern.finditer(text):
            snippet = snippet_window(text, match.start(), match.end())
            key = (keyword_family, snippet)
            if key in seen:
                continue
            seen.add(key)
            snippets.append({"keyword_family": keyword_family, "snippet": snippet})
            if len([row for row in snippets if row["keyword_family"] == keyword_family]) >= 5:
                break

    return snippets


def strip_html(path: Path) -> str:
    text = path.read_text(encoding="utf-8", errors="ignore")
    if BeautifulSoup is None:
        return normalize_space(re.sub(r"<[^>]+>", " ", text))
    return normalize_space(BeautifulSoup(text, "html.parser").get_text(" "))


def extract_downloaded_text(path: Path) -> tuple[str, str]:
    header = path.read_bytes()[:8]
    if header.startswith(b"%PDF"):
        try:
            result = subprocess.run(
                ["pdftotext", "-layout", str(path), "-"],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=45,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(f"pdftotext timed out after {exc.timeout} seconds") from exc
        if result.returncode != 0:
            raise RuntimeError(normalize_space(result.stderr))
        return normalize_space(result.stdout), "pdftotext"

    raw = path.read_text(encoding="utf-8", errors="ignore")
    if "<html" in raw[:1000].lower() or "<body" in raw[:1000].lower():
        return strip_html(path), "html_text"
    return normalize_space(raw), "plain_text"


def download_url(url: str, document_id: str) -> tuple[str, str, str]:
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DOWNLOAD_DIR / f"{document_id}.bin"
    if out_path.exists() and out_path.stat().st_size > 0:
        text, extraction_method = extract_downloaded_text(out_path)
        return str(out_path), text, extraction_method + "_download_cached"

    result = subprocess.run(
        [
            "curl",
            "-sS",
            "-L",
            "--fail",
            "--max-time",
            "60",
            "--user-agent",
            "nyc-ulurp-modification-research/0.1",
            "-o",
            str(out_path),
            url,
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        if out_path.exists():
            out_path.unlink()
        raise RuntimeError(normalize_space(result.stderr))

    text, extraction_method = extract_downloaded_text(out_path)
    return str(out_path), text, extraction_method + "_download"


spine = read_existing_csv(Path("../input/ulurp_modification_project_spine.csv"))
crosswalk = read_existing_csv(Path("../input/ulurp_modification_project_matter_crosswalk.csv"))

assert_unique(spine, ["project_id"], "Modification spine")
if crosswalk.duplicated(["project_id", "matter_id", "application_key"]).any():
    raise RuntimeError("Project-matter crosswalk is not unique by project_id, matter_id, application_key.")

matter_projects = (
    crosswalk.groupby("matter_id", as_index=False)
    .agg(
        project_ids=("project_id", lambda x: "; ".join(sorted(set(normalize_space(v) for v in x if normalize_space(v) != "")))),
        application_keys=("application_key", lambda x: "; ".join(sorted(set(normalize_space(v) for v in x if normalize_space(v) != "")))),
        council_modification_signal=("council_modification_signal", lambda x: "true" if any(str(v).upper() == "TRUE" for v in x) else "false"),
    )
)
assert_unique(matter_projects, ["matter_id"], "Collapsed matter-project crosswalk")
matter_project_lookup = matter_projects.set_index("matter_id").to_dict("index")
matched_matter_ids = set(matter_projects["matter_id"])

detail_frames = []
action_frames = []
for year in RECALL_YEARS:
    detail_frames.append(read_existing_csv(Path(f"../input/legistar_{year}_broad_recall_detail_files.csv")))
    action_frames.append(read_existing_csv(Path(f"../input/legistar_{year}_broad_recall_action_details.csv")))

detail_files = pd.concat(detail_frames, ignore_index=True)
action_details = pd.concat(action_frames, ignore_index=True)

detail_files = detail_files[detail_files["matter_id"].isin(matched_matter_ids)].copy()
action_details = action_details[action_details["matter_id"].isin(matched_matter_ids)].copy()

link_rows: list[dict[str, object]] = []
text_rows: list[dict[str, object]] = []
snippet_rows: list[dict[str, object]] = []
failure_rows: list[dict[str, object]] = []

for _, row in detail_files.iterrows():
    matter_id = normalize_space(row.get("matter_id"))
    matter_info = matter_project_lookup[matter_id]
    matter_file = normalize_space(row.get("matter_file"))
    query_year = normalize_space(row.get("query_year"))
    matter_title = normalize_space(row.get("detail_title"))
    labels: dict[str, str] = {}
    if ATTACHMENT_FETCH_MODE == "candidate_attachments" and matter_info["council_modification_signal"] == "true":
        labels = collect_link_labels(resolve_raw_path(row.get("raw_path")))

    for source_field in [
        "matter_url",
        "detail_attachment_urls",
        "detail_report_urls",
        "detail_history_detail_urls",
        "detail_meeting_detail_urls",
    ]:
        for source_url in split_semicolon(row.get(source_field)):
            source_label = label_for_url(source_url, labels)
            classification = classify_document(source_field, source_label, source_url, matter_file, matter_title)
            document_id = stable_id(matter_id, source_field, source_url)
            local_path = "NA_not_stated"
            text_row_count = 0
            snippet_row_count = 0
            should_download, download_reason = download_decision(
                source_field,
                source_label,
                classification,
                matter_info["council_modification_signal"],
            )
            extraction_status = "not_requested" if FETCH_MODE == "index_only" else "not_selected_for_download"

            if should_download:
                try:
                    local_path, downloaded_text, extraction_method = download_url(source_url, document_id)
                    if downloaded_text != "":
                        text_rows.append(
                            {
                                "document_id": document_id,
                                "project_ids": matter_info["project_ids"],
                                "matter_id": matter_id,
                                "matter_file": matter_file,
                                "query_year": query_year,
                                "document_family": classification["document_family"],
                                "source_doc": source_url,
                                "page": "NA_not_stated",
                                "document_text": downloaded_text,
                                "extraction_method": extraction_method,
                                "confidence": "medium",
                            }
                        )
                        text_row_count = 1
                        for snippet in extract_snippets(downloaded_text):
                            snippet_id = stable_id(document_id, snippet["keyword_family"], snippet["snippet"])
                            snippet_rows.append(
                                {
                                    "snippet_id": snippet_id,
                                    "document_id": document_id,
                                    "project_ids": matter_info["project_ids"],
                                    "matter_id": matter_id,
                                    "matter_file": matter_file,
                                    "query_year": query_year,
                                    "document_family": classification["document_family"],
                                    "keyword_family": snippet["keyword_family"],
                                    "source_doc": source_url,
                                    "page": "NA_not_stated",
                                    "snippet": snippet["snippet"],
                                    "extraction_method": extraction_method,
                                    "confidence": "medium",
                                }
                            )
                        snippet_row_count = len([x for x in snippet_rows if x["document_id"] == document_id])
                        extraction_status = "extracted"
                    else:
                        extraction_status = "empty_text"
                except Exception as exc:  # pragma: no cover - network and binary extraction audit path
                    failure_rows.append(
                        {
                            "document_id": document_id,
                            "matter_id": matter_id,
                            "source_url": source_url,
                            "failure_stage": "download_or_extract",
                            "failure_reason": normalize_space(exc),
                        }
                    )
                    extraction_status = "failed"

            link_rows.append(
                {
                    "document_id": document_id,
                    "project_ids": matter_info["project_ids"],
                    "matter_id": matter_id,
                    "matter_file": matter_file,
                    "query_year": query_year,
                    "source_field": source_field,
                    "document_family": classification["document_family"],
                    "document_role": "council_legistar_link",
                    "source_priority": classification["source_priority"],
                    "source_url": source_url,
                    "source_label": source_label,
                    "matter_title": matter_title,
                    "download_selected": true_false(should_download),
                    "download_reason": download_reason,
                    "points_of_agreement_candidate": true_false(classification["points_of_agreement_candidate"]),
                    "committee_report_candidate": true_false(classification["committee_report_candidate"]),
                    "resolution_candidate": true_false(classification["resolution_candidate"]),
                    "m_matter_candidate": true_false(classification["m_matter_candidate"]),
                    "fetch_mode": FETCH_MODE,
                    "fetch_status": "indexed" if FETCH_MODE == "index_only" else ("downloaded" if extraction_status in {"extracted", "empty_text"} else ("download_skipped" if not should_download else "download_failed")),
                    "local_path": local_path,
                    "extraction_status": extraction_status,
                    "text_row_count": text_row_count,
                    "snippet_row_count": snippet_row_count,
                }
            )

for _, row in action_details.iterrows():
    matter_id = normalize_space(row.get("matter_id"))
    matter_info = matter_project_lookup[matter_id]
    matter_file = normalize_space(row.get("matter_file"))
    query_year = normalize_space(row.get("query_year"))
    history_sequence = normalize_space(row.get("history_sequence"))
    source_url = normalize_space(row.get("history_detail_url"))
    combined_text = normalize_space(
        " ".join(
            [
                normalize_space(row.get("action_detail_title")),
                normalize_space(row.get("action_detail_result")),
                normalize_space(row.get("agenda_note")),
                normalize_space(row.get("minutes_note")),
                normalize_space(row.get("action_detail_action")),
                normalize_space(row.get("action_detail_text")),
            ]
        )
    )
    if combined_text == "":
        continue

    classification = classify_action_text(matter_file, combined_text)
    document_id = stable_id(matter_id, history_sequence, source_url, "action_detail_text")
    snippets = extract_snippets(combined_text)

    link_rows.append(
        {
            "document_id": document_id,
            "project_ids": matter_info["project_ids"],
            "matter_id": matter_id,
            "matter_file": matter_file,
            "query_year": query_year,
            "source_field": "action_detail_text",
            "document_family": classification["document_family"],
            "document_role": "council_action_text",
            "source_priority": classification["source_priority"],
            "source_url": source_url,
            "source_label": normalize_space(row.get("action_detail_title")) or "NA_not_stated",
            "matter_title": normalize_space(row.get("action_detail_title")),
            "download_selected": "false",
            "download_reason": "action_detail_text_already_extracted",
            "points_of_agreement_candidate": true_false(classification["points_of_agreement_candidate"]),
            "committee_report_candidate": true_false(classification["committee_report_candidate"]),
            "resolution_candidate": true_false(classification["resolution_candidate"]),
            "m_matter_candidate": true_false(classification["m_matter_candidate"]),
            "fetch_mode": FETCH_MODE,
            "fetch_status": "indexed",
            "local_path": normalize_space(row.get("raw_path")) or "NA_not_stated",
            "extraction_status": "extracted",
            "text_row_count": 1,
            "snippet_row_count": len(snippets),
        }
    )
    text_rows.append(
        {
            "document_id": document_id,
            "project_ids": matter_info["project_ids"],
            "matter_id": matter_id,
            "matter_file": matter_file,
            "query_year": query_year,
            "document_family": classification["document_family"],
            "source_doc": source_url,
            "page": "NA_not_stated",
            "document_text": combined_text,
            "extraction_method": "legistar_action_detail_text",
            "confidence": "medium",
        }
    )

    for snippet in snippets:
        snippet_id = stable_id(document_id, snippet["keyword_family"], snippet["snippet"])
        snippet_rows.append(
            {
                "snippet_id": snippet_id,
                "document_id": document_id,
                "project_ids": matter_info["project_ids"],
                "matter_id": matter_id,
                "matter_file": matter_file,
                "query_year": query_year,
                "document_family": classification["document_family"],
                "keyword_family": snippet["keyword_family"],
                "source_doc": source_url,
                "page": "NA_not_stated",
                "snippet": snippet["snippet"],
                "extraction_method": "legistar_action_detail_text",
                "confidence": "medium",
            }
        )

link_df = pd.DataFrame(link_rows, columns=LINK_COLUMNS)
text_df = pd.DataFrame(text_rows, columns=TEXT_COLUMNS)
snippet_df = pd.DataFrame(snippet_rows, columns=SNIPPET_COLUMNS)

assert_unique(link_df, ["document_id"], "Council document links")
if not text_df.empty:
    assert_unique(text_df, ["document_id"], "Council document text")
if not snippet_df.empty:
    assert_unique(snippet_df, ["snippet_id"], "Council document snippets")

downloaded_text_rows = 0
if not text_df.empty:
    downloaded_text_rows = int(text_df["extraction_method"].str.contains("_download", regex=False).sum())

qc_rows = [
    {
        "check_name": "spine_project_rows",
        "check_value": len(spine),
        "status": "pass" if len(spine) > 0 else "fail",
    },
    {
        "check_name": "crosswalk_matter_rows",
        "check_value": len(matter_projects),
        "status": "pass" if len(matter_projects) > 0 else "fail",
    },
    {
        "check_name": "matched_detail_matter_rows",
        "check_value": detail_files["matter_id"].nunique(),
        "status": "pass" if detail_files["matter_id"].nunique() > 0 else "fail",
    },
    {
        "check_name": "matched_action_matter_rows",
        "check_value": action_details["matter_id"].nunique(),
        "status": "pass" if action_details["matter_id"].nunique() > 0 else "fail",
    },
    {
        "check_name": "document_link_rows",
        "check_value": len(link_df),
        "status": "pass" if len(link_df) > 0 else "fail",
    },
    {
        "check_name": "document_text_rows",
        "check_value": len(text_df),
        "status": "pass" if len(text_df) > 0 else "fail",
    },
    {
        "check_name": "downloaded_document_text_rows",
        "check_value": downloaded_text_rows,
        "status": "pass" if FETCH_MODE == "index_only" or downloaded_text_rows > 0 else "fail",
    },
    {
        "check_name": "snippet_rows",
        "check_value": len(snippet_df),
        "status": "pass" if len(snippet_df) > 0 else "fail",
    },
    {
        "check_name": "fetch_failure_rows",
        "check_value": len(failure_rows),
        "status": "pass",
    },
]

write_csv(DOCUMENT_LINKS_OUTPUT, link_rows, LINK_COLUMNS)
if FULL_TEXT_EXPORT == "write_full_text":
    write_csv(DOCUMENT_TEXT_OUTPUT, text_rows, TEXT_COLUMNS)
write_csv(DOCUMENT_SNIPPETS_OUTPUT, snippet_rows, SNIPPET_COLUMNS)

if any(row["status"] == "fail" for row in qc_rows):
    raise RuntimeError("Council modification document fetch QC failed.")
