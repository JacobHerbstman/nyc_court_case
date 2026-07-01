#!/usr/bin/env python3

import csv
import io
import json
import os
import re
import subprocess
import tempfile
from html import unescape
from html.parser import HTMLParser
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen




BATCH_SIZE = 1


class TextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.skip_depth = 0
        self.parts = []

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        if tag in {"script", "style", "noscript"}:
            self.skip_depth += 1
        if self.skip_depth == 0 and tag in {"p", "div", "br", "tr", "td", "th", "li", "h1", "h2", "h3", "h4"}:
            self.parts.append("\n")

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag in {"script", "style", "noscript"} and self.skip_depth > 0:
            self.skip_depth -= 1
        if self.skip_depth == 0 and tag in {"p", "div", "tr", "li", "h1", "h2", "h3", "h4"}:
            self.parts.append("\n")

    def handle_data(self, data):
        if self.skip_depth == 0:
            value = data.strip()
            if value:
                self.parts.append(value + " ")

    def text(self):
        value = unescape("".join(self.parts))
        value = re.sub(r"[ \t\r\f\v]+", " ", value)
        value = re.sub(r"\n\s*\n+", "\n\n", value)
        return value.strip()


def write_text_if_changed(text, path):
    try:
        with open(path, "r", encoding="utf-8") as input_file:
            old_text = input_file.read()
    except FileNotFoundError:
        old_text = None
    if old_text != text:
        with open(path, "w", encoding="utf-8") as output_file:
            output_file.write(text)


def write_csv_if_changed(rows, fieldnames, path):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    write_text_if_changed(output.getvalue(), path)


def read_csv_rows(path):
    with open(path, "r", encoding="utf-8", newline="") as input_file:
        return list(csv.DictReader(input_file))


def clean_value(value):
    if value is None:
        return ""
    return " ".join(str(value).split())


def split_values(value):
    parts = []
    for part in re.split(r"[;|]", clean_value(value)):
        part = part.strip()
        if part and part not in parts:
            parts.append(part)
    return parts


def split_urls(value):
    parts = []
    for part in clean_value(value).split(";"):
        part = part.strip()
        if part and part not in parts:
            parts.append(part)
    return parts


def clean_extracted_text(text):
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def text_from_pdf_bytes(data):
    with tempfile.NamedTemporaryFile(suffix=".pdf") as pdf_file:
        pdf_file.write(data)
        pdf_file.flush()
        text = subprocess.check_output(["pdftotext", pdf_file.name, "-"], text=True, errors="replace")
    return clean_extracted_text(text)


def fetch_url_text(url):
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(request, timeout=45) as response:
        data = response.read()
        content_type = response.headers.get_content_type()
    if content_type == "application/pdf" or data[:4] == b"%PDF":
        return text_from_pdf_bytes(data), content_type
    parser = TextExtractor()
    parser.feed(data.decode("utf-8", errors="replace"))
    return parser.text(), content_type


def legistar_report_url(url):
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    legistar_id = query.get("ID", [""])[0]
    guid = query.get("GUID", [""])[0]
    if legistar_id == "" or guid == "":
        return ""
    return (
        "https://legistar.council.nyc.gov/ViewReport.ashx?M=R&N=Master&GID=61"
        f"&ID={legistar_id}&GUID={guid}&Extra=WithText&Title=Legislation+Details+(With+Text)"
    )


def source_text_for_url(url):
    detail_text, detail_content_type = fetch_url_text(url)
    report_url = legistar_report_url(url)
    if report_url == "":
        return f"DETAIL URL: {url}\nCONTENT TYPE: {detail_content_type}\n\n{detail_text}"
    try:
        report_text, report_content_type = fetch_url_text(report_url)
    except Exception as exc:
        report_text = f"REPORT FETCH ERROR: {exc}"
        report_content_type = ""
    return (
        f"DETAIL URL: {url}\n"
        f"DETAIL CONTENT TYPE: {detail_content_type}\n\n"
        f"DETAIL TEXT:\n{detail_text}\n\n"
        f"REPORT WITH TEXT URL: {report_url}\n"
        f"REPORT CONTENT TYPE: {report_content_type}\n\n"
        f"REPORT TEXT:\n{report_text}"
    )


def source_index(text, terms):
    lines = [line.strip() for line in text.splitlines()]
    selected_lines = set()
    lower_terms = [term.lower() for term in terms if term]

    for index, line in enumerate(lines):
        lowered = line.lower()
        term_hit = any(term in lowered for term in lower_terms)
        geography_hit = re.search(
            r"\bcouncil\s+district\b|\bcommunity\s+(?:board|district)\b|\bblock\b|\blot\b|\bapplication\b|\bulurp\b|\baddress\b",
            line,
            flags=re.IGNORECASE,
        )
        if term_hit or geography_hit:
            for nearby in range(max(0, index - 3), min(len(lines), index + 4)):
                selected_lines.add(nearby)

    indexed_lines = []
    previous_line = -2
    for line_number in sorted(selected_lines)[:180]:
        if line_number > previous_line + 1:
            indexed_lines.append("...")
        indexed_lines.append(f"L{line_number + 1}: {lines[line_number]}")
        previous_line = line_number
    if len(selected_lines) > 180:
        indexed_lines.append(f"... [{len(selected_lines) - 180} additional indexed lines omitted]")
    return "\n".join(indexed_lines) if indexed_lines else "No source-index hits found."


def key_terms(row):
    terms = [
        row["signature_review_id"],
        row["adjudication_project_name"],
        row["adjudication_project_area"],
        row["application_keys"],
        row["zap_project_ids"],
        row["zap_project_names"] if "zap_project_names" in row else "",
        row["matter_files"],
        row["matched_bbls"],
        row["matched_addresses"],
    ]
    for value in [
        row["adjudication_project_name"],
        row["adjudication_project_area"],
        row["application_keys"],
        row["matter_files"],
        row["matched_bbls"],
    ]:
        for part in re.split(r"[;,/|]", clean_value(value)):
            part = part.strip()
            if len(part) >= 4:
                terms.append(part)
    return list(dict.fromkeys(clean_value(term) for term in terms if clean_value(term)))


def search_queries(row):
    queries = []
    project_name = clean_value(row["adjudication_project_name"])
    project_area = clean_value(row["adjudication_project_area"])
    applications = split_values(row["application_keys"])
    matter_files = split_values(row["matter_files"])
    bbls = split_values(row["matched_bbls"])

    if project_name:
        queries.append(f'"{project_name}" "Council District"')
    for application in applications[:3]:
        queries.append(f'"{application}" "Council District"')
        queries.append(f'"{application}" NYC Council')
    for matter_file in matter_files[:2]:
        queries.append(f'"{matter_file}" "Council District"')
    for bbl in bbls[:2]:
        queries.append(f'"{bbl}" "Council District"')
    if project_area:
        words = project_area.split()
        if len(words) >= 4:
            queries.append(f'"{" ".join(words[:8])}" "Council District"')

    return list(dict.fromkeys(queries))[:6]


def prompt_header(row, queries):
    query_text = "\n".join(f"- {query}" for query in queries)
    return f"""# NYC Council Land-Use Manual-Queue Web Review

You are helping audit a PhD research dataset of New York City Council land-use decisions.

Task: decide whether the affected Council district(s) can be promoted for this signature.

Use this evidence hierarchy:
1. Official Council, City Record, ZAP, DCP/CPC, ULURP, HPD, LPC, EDC, or agency records define the project geography.
2. If official records give an address, BBL, block/lots, named site, or bounded project area but no Council District line, that can still be enough. Say that the district is externally coded from project geography.
3. Use web search to find official records first. Use news/project pages only to corroborate location, neighborhood, local member, or boundaries.
4. Do not use current Council district evidence when the vote-year boundary may differ. Flag current/historical ambiguity explicitly.
5. Do not promote if the official geography is too vague, citywide/text-only, not land use, or only one partial site from a multi-site/area-wide action.

Return exactly one JSON object, no markdown, with these fields:
signature_review_id, recommended_council_districts, confidence (high|medium|low), promotion_decision (promote|promote_with_caveat|defer|reject), evidence_type (explicit_official_district|official_geography_external_boundary|official_geography_current_boundary_only|citywide_or_text_only|not_land_use|unresolved), official_geography_basis, source_index_lines_used, web_sources_checked, web_or_news_corroboration, historical_boundary_issue, remaining_uncertainty, human_review_needed (yes|no).

signature_review_id: {row["signature_review_id"]}
vote_year: {row["query_year"]}
vote_date: {row["vote_date"]}
project_name: {row["adjudication_project_name"]}
prior_ai_claimed_districts: {row["claimed_council_districts"]}
deterministic_districts: {row["deterministic_council_districts"]}
deterministic_status: {row["deterministic_verification_status"]}
deterministic_basis: {row["deterministic_verification_basis"]}
historical_boundary_release_used_by_code: {row["boundary_release"]}
historical_boundary_year_used_by_code: {row["boundary_year"]}
boundary_relation_to_vote_year: {row["boundary_relation_to_vote_year"]}
matched_bbls: {row["matched_bbls"]}
matched_addresses: {row["matched_addresses"]}
all_source_district_summaries: {row["all_source_district_summaries"]}
project_area_from_prior_ai: {row["adjudication_project_area"]}
prior_source_check_summary: {row["adjudication_source_check_summary"]}
matter_files: {row["matter_files"]}
application_keys: {row["application_keys"]}
matter_urls: {row["matter_urls"]}

Suggested web searches:
{query_text}
"""


os.makedirs("../output/manual_queue_web_review_batches", exist_ok=True)

rows = read_csv_rows("../output/council_land_use_ai_geography_deterministic_manual_queue.csv")
status_order = {
    "conflict": 1,
    "partial_conflict": 2,
    "conflict_pre_boundary_archive": 3,
    "tentative_match_pre_boundary_archive": 4,
    "tentative_partial_match_pre_boundary_archive": 5,
    "not_verified_no_deterministic_match": 6,
}
rows = sorted(
    rows,
    key=lambda row: (
        status_order.get(row["deterministic_verification_status"], 99),
        int(row["signature_review_id"].split("_")[-1]),
    ),
)

case_manifest_rows = []
query_rows = []
batch_rows = []
case_prompts = []

for row in rows:
    queries = search_queries(row)
    for query in queries:
        query_rows.append({"signature_review_id": row["signature_review_id"], "search_query": query})

    prompt_parts = [prompt_header(row, queries)]
    terms = key_terms(row)
    fetched_count = 0
    for source_number, url in enumerate(split_urls(row["matter_urls"]), start=1):
        print(f"Fetching {row['signature_review_id']} source {source_number}: {url}", flush=True)
        try:
            text = source_text_for_url(url)
            index_text = source_index(text, terms)
        except Exception as exc:
            index_text = f"FETCH ERROR: {exc}"
        fetched_count += 1
        prompt_parts.extend(
            [
                "\n" + "=" * 80,
                f"SOURCE {source_number}",
                f"URL: {url}",
                "SOURCE INDEX:",
                index_text,
            ]
        )

    prompt = "\n".join(prompt_parts).strip() + "\n"
    case_path = f"../output/manual_queue_web_review_batches/{row['signature_review_id']}_manual_queue_web_prompt.md"
    write_text_if_changed(prompt, case_path)
    case_prompts.append({"signature_review_id": row["signature_review_id"], "prompt": prompt})
    case_manifest_rows.append(
        {
            "signature_review_id": row["signature_review_id"],
            "case_prompt_path": case_path,
            "deterministic_verification_status": row["deterministic_verification_status"],
            "adjudication_candidate_category": row["adjudication_candidate_category"],
            "source_count": fetched_count,
            "search_query_count": len(queries),
            "prompt_characters": len(prompt),
        }
    )

for start in range(0, len(case_prompts), BATCH_SIZE):
    batch_id = f"{len(batch_rows) + 1:03d}"
    batch_cases = case_prompts[start : start + BATCH_SIZE]
    batch_text = "\n\n".join(case["prompt"] for case in batch_cases)
    batch_path = f"../output/manual_queue_web_review_batches/manual_queue_web_review_batch_{batch_id}.md"
    write_text_if_changed(batch_text, batch_path)
    batch_rows.append(
        {
            "batch_id": batch_id,
            "batch_path": batch_path,
            "signature_count": len(batch_cases),
            "signature_review_ids": "|".join(case["signature_review_id"] for case in batch_cases),
            "char_count": len(batch_text),
        }
    )

write_csv_if_changed(
    case_manifest_rows,
    [
        "signature_review_id",
        "case_prompt_path",
        "deterministic_verification_status",
        "adjudication_candidate_category",
        "source_count",
        "search_query_count",
        "prompt_characters",
    ],
    "../output/manual_queue_web_review_batches/manual_queue_web_review_case_manifest.csv",
)
write_csv_if_changed(
    batch_rows,
    ["batch_id", "batch_path", "signature_count", "signature_review_ids", "char_count"],
    "../output/manual_queue_web_review_batches/manual_queue_web_review_batch_manifest.csv",
)
write_text_if_changed(
    "\n".join(json.dumps(row, ensure_ascii=True) for row in batch_rows) + "\n",
    "../output/manual_queue_web_review_batches/manual_queue_web_review_batches.jsonl",
)
write_csv_if_changed(
    query_rows,
    ["signature_review_id", "search_query"],
    "../output/manual_queue_web_review_batches/manual_queue_web_review_search_queries.csv",
)
if batch_rows:
    with open(batch_rows[0]["batch_path"], "r", encoding="utf-8") as input_file:
        write_text_if_changed(input_file.read(), "../output/manual_queue_web_review_batches/manual_queue_web_review_next_batch.md")
