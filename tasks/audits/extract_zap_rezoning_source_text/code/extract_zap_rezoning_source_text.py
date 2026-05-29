#!/usr/bin/env python3

import csv
import glob
import hashlib
import io
import os
import re
import subprocess
import sys
import textwrap


MAX_CPC_REPORTS_PER_PROJECT = 3
MAX_EXTRA_DOCUMENTS_PER_PROJECT = 2
CHATGPT_BATCH_PROJECT_COUNT = 25
PAGE_SNIPPETS_PER_DOCUMENT = 3
PAGE_SNIPPET_CHAR_LIMIT = 3500
PROJECT_EVIDENCE_CHAR_LIMIT = 14000
CURL_CONNECT_TIMEOUT_SECONDS = 10
CURL_MAX_TIME_SECONDS = 90
CURL_USER_AGENT = "Mozilla/5.0"
OCR_DPI = 200
OCR_MAX_PAGES = 80

ZONING_KEYWORD_PATTERN = re.compile(
    r"(?i)"
    r"existing zoning|proposed zoning|zoning map amendment|zoning text amendment|"
    r"zoning district|zoning change|zoning comparison|rezon(?:e|ing)|"
    r"mapped|mapping|floor area ratio|\bFAR\b|residential FAR|"
    r"mandatory inclusionary|MIH|contextual|special district|"
    r"upzon|downzon|lower density|higher density|"
    r"\bR-?\d{1,2}(?:-[0-9A-Z]+|[A-Z]+)?\b|"
    r"\bC-?\d(?:-[0-9A-Z]+|[A-Z]+)?\b|"
    r"\bM-?\d(?:-[0-9A-Z]+|[A-Z]+)?\b|"
    r"\bblocks?\b|\bacres?\b|\blots?\b"
)

HIGH_VALUE_DOCUMENT_TITLE_PATTERN = re.compile(
    r"(?i)"
    r"zoning comparison|zoning change|zoning map|project description|"
    r"supplemental|land use|application|site data"
)


def clean_text(value):
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def truthy(value):
    return str(value).strip().upper() in {"TRUE", "T", "1", "YES"}


def write_csv_if_changed(rows, fieldnames, path):
    writer_buffer = io.StringIO()
    writer = csv.DictWriter(writer_buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    new_text = writer_buffer.getvalue()

    try:
        with open(path, "r", encoding="utf-8", newline="") as existing_file:
            old_text = existing_file.read()
    except FileNotFoundError:
        old_text = None

    if old_text != new_text:
        with open(path, "w", encoding="utf-8", newline="") as output_file:
            output_file.write(new_text)


def write_text_if_changed(text, path):
    try:
        with open(path, "r", encoding="utf-8") as existing_file:
            old_text = existing_file.read()
    except FileNotFoundError:
        old_text = None

    if old_text != text:
        with open(path, "w", encoding="utf-8") as output_file:
            output_file.write(text)


def safe_filename_part(value):
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value))[:80]
    return cleaned.strip("_") or "missing"


def url_hash(value):
    return hashlib.sha1(str(value).encode("utf-8")).hexdigest()[:12]


def source_type_rank(row):
    source_type = row.get("source_type", "")
    family = row.get("document_family", "")
    title = row.get("document_title", "")
    action_code = row.get("action_code", "")

    if source_type == "cpc_report" and action_code == "ZM":
        return 1
    if source_type == "cpc_report_nycgov_fallback" and action_code == "ZM":
        return 2
    if source_type == "cpc_report" and action_code == "ZR":
        return 3
    if source_type == "cpc_report_nycgov_fallback" and action_code == "ZR":
        return 4
    if family == "docket_description":
        return 5
    if family in {"zoning_document", "project_description", "land_use_application", "land_use"}:
        return 6 if HIGH_VALUE_DOCUMENT_TITLE_PATTERN.search(title or "") else 8
    if family in {"final_eis", "draft_eis", "eas"}:
        return 7 if HIGH_VALUE_DOCUMENT_TITLE_PATTERN.search(title or "") else 9
    return 10


def selected_reason(row):
    source_type = row.get("source_type", "")
    family = row.get("document_family", "")
    action_code = row.get("action_code", "")
    title = row.get("document_title", "")

    if source_type.startswith("cpc_report") and action_code == "ZM":
        return "zoning_map_cpc_report"
    if source_type.startswith("cpc_report") and action_code == "ZR":
        return "zoning_text_cpc_report"
    if family == "docket_description":
        return "zap_docket_text"
    if family in {"zoning_document", "project_description", "land_use_application", "land_use"}:
        return "preferred_public_document"
    if family in {"final_eis", "draft_eis", "eas"} and HIGH_VALUE_DOCUMENT_TITLE_PATTERN.search(title or ""):
        return "environmental_project_description"
    return "other_selected_document"


def download_pdf(url, pdf_path):
    completed = subprocess.run(
        [
            "curl",
            "--silent",
            "--show-error",
            "--location",
            "--fail",
            "--user-agent",
            CURL_USER_AGENT,
            "--connect-timeout",
            str(CURL_CONNECT_TIMEOUT_SECONDS),
            "--max-time",
            str(CURL_MAX_TIME_SECONDS),
            "--output",
            pdf_path,
            url,
        ],
        capture_output=True,
        text=True,
        timeout=CURL_MAX_TIME_SECONDS + 10,
        check=False,
    )

    if completed.returncode != 0:
        return "download_failed", clean_text(completed.stderr) or f"curl exited {completed.returncode}"
    if not os.path.exists(pdf_path) or os.path.getsize(pdf_path) == 0:
        return "download_failed", "downloaded file is missing or empty"
    return "downloaded", ""


def extract_pdf_text(pdf_path, text_path):
    completed = subprocess.run(
        ["pdftotext", "-layout", "-enc", "UTF-8", pdf_path, "-"],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )

    if completed.returncode != 0:
        return "", "text_extract_failed", clean_text(completed.stderr) or f"pdftotext exited {completed.returncode}"

    text = completed.stdout
    with open(text_path, "w", encoding="utf-8") as output_file:
        output_file.write(text)

    if clean_text(text) == "":
        return text, "empty_text", ""
    return text, "text_extracted", ""


def pdf_page_count(pdf_path):
    completed = subprocess.run(
        ["pdfinfo", pdf_path],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    if completed.returncode != 0:
        return None

    for line in completed.stdout.splitlines():
        match = re.match(r"Pages:\s+(\d+)", line)
        if match:
            return int(match.group(1))
    return None


def extract_pdf_text_with_ocr(pdf_path, text_path):
    page_count = pdf_page_count(pdf_path)
    if page_count is None:
        return "", "ocr_failed", "pdfinfo did not return a page count"
    if page_count > OCR_MAX_PAGES:
        return "", "ocr_skipped_too_long", f"{page_count} pages exceeds OCR_MAX_PAGES={OCR_MAX_PAGES}"

    image_prefix = text_path.replace(".txt", "_ocr_page")
    completed = subprocess.run(
        [
            "pdftoppm",
            "-r",
            str(OCR_DPI),
            "-png",
            "-f",
            "1",
            "-l",
            str(page_count),
            pdf_path,
            image_prefix,
        ],
        capture_output=True,
        text=True,
        timeout=max(120, page_count * 20),
        check=False,
    )

    if completed.returncode != 0:
        return "", "ocr_failed", clean_text(completed.stderr) or f"pdftoppm exited {completed.returncode}"

    image_paths = glob.glob(f"{image_prefix}-*.png")
    image_paths.sort(
        key=lambda path: int(re.search(r"-(\d+)\.png$", path).group(1))
        if re.search(r"-(\d+)\.png$", path)
        else 999999
    )

    if not image_paths:
        return "", "ocr_failed", "pdftoppm did not produce page images"

    page_texts = []
    ocr_error = ""
    for page_number, image_path in enumerate(image_paths, start=1):
        try:
            completed = subprocess.run(
                ["tesseract", image_path, "stdout", "--psm", "6"],
                capture_output=True,
                text=True,
                timeout=90,
                check=False,
            )
            if completed.returncode != 0:
                ocr_error = clean_text(completed.stderr) or f"tesseract exited {completed.returncode} on page {page_number}"
                break
            page_texts.append(completed.stdout)
        finally:
            try:
                os.remove(image_path)
            except FileNotFoundError:
                pass

    text = "\f".join(page_texts)
    with open(text_path, "w", encoding="utf-8") as output_file:
        output_file.write(text)

    if ocr_error:
        return text, "ocr_failed", ocr_error
    if clean_text(text) == "":
        return text, "empty_ocr_text", ""
    return text, "ocr_text_extracted", ""


def page_score(page_text):
    matches = ZONING_KEYWORD_PATTERN.findall(page_text)
    score = len(matches)
    for phrase in [
        "zoning map amendment",
        "existing zoning",
        "proposed zoning",
        "zoning comparison",
        "project description",
        "floor area ratio",
    ]:
        if phrase in page_text.lower():
            score += 5
    return score


def page_snippets(text):
    snippets = []
    for page_number, page_text in enumerate(text.split("\f"), start=1):
        cleaned_page = clean_text(page_text)
        if cleaned_page == "":
            continue

        score = page_score(cleaned_page)
        if score == 0:
            continue

        snippets.append(
            {
                "page_number": page_number,
                "score": score,
                "snippet_text": cleaned_page[:PAGE_SNIPPET_CHAR_LIMIT],
            }
        )

    snippets.sort(key=lambda row: (-row["score"], row["page_number"]))
    return snippets[:PAGE_SNIPPETS_PER_DOCUMENT]


def truncated_project_evidence(project_snippets):
    chunks = []
    remaining = PROJECT_EVIDENCE_CHAR_LIMIT
    for snippet in project_snippets:
        header = (
            f"[{snippet['document_rank']}] {snippet['document_title']} "
            f"({snippet['document_family']}, page {snippet['page_number']}): "
        )
        text = snippet["snippet_text"]
        chunk = header + text
        if len(chunk) > remaining:
            chunk = chunk[:remaining]
        chunks.append(chunk)
        remaining -= len(chunk) + 2
        if remaining <= 0:
            break
    return "\n\n".join(chunks)


with open("../input/zap_rezoning_direction_text_candidate_queue.csv", "r", encoding="utf-8", newline="") as input_file:
    queue_rows = [
        row for row in csv.DictReader(input_file)
        if truthy(row.get("remaining_reviewed_unknown_flag", "TRUE"))
    ]

queue_rows.sort(
    key=lambda row: (
        row.get("source_lookup_priority") != "high",
        row.get("text_candidate_direction") == "no_local_text_candidate",
        -float(row.get("affected_lot_acres") or 0),
        row.get("completed_year", ""),
        row.get("project_id", ""),
    )
)

with open("../input/zap_project_source_document_links.csv", "r", encoding="utf-8", newline="") as input_file:
    source_links = list(csv.DictReader(input_file))

with open("../input/zap_project_source_docket_text.csv", "r", encoding="utf-8", newline="") as input_file:
    docket_rows = list(csv.DictReader(input_file))

source_links_by_project = {}
for row in source_links:
    source_links_by_project.setdefault(row["project_id"], []).append(row)

docket_rows_by_project = {}
for row in docket_rows:
    docket_rows_by_project.setdefault(row["project_id"], []).append(row)

selected_documents = []
seen_project_url = set()

for queue_rank, queue_row in enumerate(queue_rows, start=1):
    project_id = queue_row["project_id"]
    project_links = source_links_by_project.get(project_id, [])

    cpc_rows = [
        row for row in project_links
        if row.get("document_family") == "cpc_report" and row.get("action_code") in {"ZM", "ZR"}
    ]
    cpc_rows.sort(
        key=lambda row: (
            source_type_rank(row),
            row.get("ulurp_number", ""),
            row.get("source_type", ""),
            row.get("document_url", ""),
        )
    )

    selected_cpc_rows = []
    seen_action_key = set()
    for row in cpc_rows:
        action_key = (row.get("action_code", ""), row.get("ulurp_number", ""))
        if action_key in seen_action_key:
            continue
        seen_action_key.add(action_key)
        selected_cpc_rows.append(row)
        if len(selected_cpc_rows) >= MAX_CPC_REPORTS_PER_PROJECT:
            break

    extra_rows = [
        row for row in project_links
        if row.get("document_family") in {
            "zoning_document",
            "project_description",
            "land_use_application",
            "land_use",
            "final_eis",
            "draft_eis",
            "eas",
        }
        and HIGH_VALUE_DOCUMENT_TITLE_PATTERN.search(row.get("document_title", ""))
    ]
    extra_rows.sort(
        key=lambda row: (
            source_type_rank(row),
            int(row.get("source_priority") or 99),
            row.get("document_title", ""),
        )
    )

    for row in selected_cpc_rows + extra_rows[:MAX_EXTRA_DOCUMENTS_PER_PROJECT]:
        dedupe_key = (project_id, row.get("document_url", ""))
        if dedupe_key in seen_project_url:
            continue
        seen_project_url.add(dedupe_key)
        selected_documents.append(
            {
                **row,
                "queue_rank": queue_rank,
                "selected_reason": selected_reason(row),
                "document_rank": len([x for x in selected_documents if x["project_id"] == project_id]) + 1,
                "download_required": True,
            }
        )

    project_docket_entries = []
    seen_docket_entry = set()
    for docket_row in docket_rows_by_project.get(project_id, []):
        docket_entry = clean_text(
            f"{docket_row.get('disposition_name', '')}: {docket_row.get('docket_description', '')}"
        )
        if docket_entry == "" or docket_entry in seen_docket_entry:
            continue
        seen_docket_entry.add(docket_entry)
        project_docket_entries.append(docket_entry)

    if project_docket_entries:
        selected_documents.append(
            {
                "project_id": project_id,
                "project_name": queue_row.get("project_name", ""),
                "completed_year": queue_row.get("completed_year", ""),
                "queue_rank": queue_rank,
                "source_type": "docket_description",
                "source_container_id": "",
                "source_container_title": "ZAP disposition docket descriptions",
                "document_title": "ZAP disposition docket description",
                "document_family": "docket_description",
                "source_priority": 2,
                "preferred_for_direction_scope_review": True,
                "document_url": f"https://zap.planning.nyc.gov/projects/{project_id}",
                "api_url": "",
                "action_code": "",
                "ulurp_number": "",
                "ceqr_number": "",
                "document_created_at": "",
                "fetch_status": "",
                "fetch_http_status": "",
                "fetch_error": "",
                "selected_reason": "zap_docket_text",
                "document_rank": len([x for x in selected_documents if x["project_id"] == project_id]) + 1,
                "download_required": False,
                "docket_description": "\n".join(project_docket_entries),
            }
        )

document_index_rows = []
snippet_rows = []

for document_number, document in enumerate(selected_documents, start=1):
    project_id = document["project_id"]
    document_url = document.get("document_url", "")
    file_stem = "_".join([
        f"{int(document['queue_rank']):04d}",
        safe_filename_part(project_id),
        safe_filename_part(document.get("ulurp_number", "")),
        url_hash(document_url),
    ])
    pdf_path = f"../temp/source_pdfs/{file_stem}.pdf"
    text_path = f"../temp/source_text/{file_stem}.txt"
    download_status = "not_required"
    download_error = ""
    text_status = "text_extracted"
    text_error = ""
    extracted_text = clean_text(document.get("docket_description", ""))

    if document.get("download_required"):
        download_status, download_error = download_pdf(document_url, pdf_path)
        if download_status == "downloaded":
            extracted_text, text_status, text_error = extract_pdf_text(pdf_path, text_path)
            if text_status == "empty_text":
                extracted_text, text_status, text_error = extract_pdf_text_with_ocr(pdf_path, text_path)
        else:
            text_status = "not_extracted"
    else:
        write_text_if_changed(extracted_text, text_path)

    if document_number == 1 or document_number % 50 == 0 or document_number == len(selected_documents):
        print(f"Processed {document_number}/{len(selected_documents)} selected source documents", flush=True)

    document_index_rows.append(
        {
            "project_id": project_id,
            "project_name": document.get("project_name", ""),
            "completed_year": document.get("completed_year", ""),
            "queue_rank": document.get("queue_rank", ""),
            "document_rank": document.get("document_rank", ""),
            "selected_reason": document.get("selected_reason", ""),
            "source_type": document.get("source_type", ""),
            "document_family": document.get("document_family", ""),
            "source_priority": document.get("source_priority", ""),
            "action_code": document.get("action_code", ""),
            "ulurp_number": document.get("ulurp_number", ""),
            "document_title": document.get("document_title", ""),
            "document_url": document_url,
            "local_pdf_path": pdf_path if document.get("download_required") else "",
            "local_text_path": text_path,
            "download_status": download_status,
            "download_error": download_error,
            "text_status": text_status,
            "text_error": text_error,
            "text_char_count": len(extracted_text),
        }
    )

    for snippet_number, snippet in enumerate(page_snippets(extracted_text), start=1):
        snippet_rows.append(
            {
                "project_id": project_id,
                "project_name": document.get("project_name", ""),
                "completed_year": document.get("completed_year", ""),
                "queue_rank": document.get("queue_rank", ""),
                "document_rank": document.get("document_rank", ""),
                "snippet_rank": snippet_number,
                "selected_reason": document.get("selected_reason", ""),
                "source_type": document.get("source_type", ""),
                "document_family": document.get("document_family", ""),
                "source_priority": document.get("source_priority", ""),
                "action_code": document.get("action_code", ""),
                "ulurp_number": document.get("ulurp_number", ""),
                "document_title": document.get("document_title", ""),
                "document_url": document_url,
                "page_number": snippet["page_number"],
                "snippet_score": snippet["score"],
                "snippet_text": snippet["snippet_text"],
            }
        )

snippet_rows.sort(
    key=lambda row: (
        int(row["queue_rank"]),
        int(row["document_rank"]),
        int(row["snippet_rank"]),
    )
)

packet_rows = []
snippets_by_project = {}
index_by_project = {}
for row in snippet_rows:
    snippets_by_project.setdefault(row["project_id"], []).append(row)
for row in document_index_rows:
    index_by_project.setdefault(row["project_id"], []).append(row)

for queue_rank, queue_row in enumerate(queue_rows, start=1):
    project_id = queue_row["project_id"]
    project_snippets = snippets_by_project.get(project_id, [])
    project_documents = index_by_project.get(project_id, [])
    source_lines = [
        f"{row['document_rank']}. {row['document_title']} [{row['selected_reason']}] {row['document_url']}"
        for row in project_documents
    ]

    packet_rows.append(
        {
            "project_id": project_id,
            "completed_year": queue_row.get("completed_year", ""),
            "event_period": queue_row.get("event_period", ""),
            "queue_rank": queue_rank,
            "project_name": queue_row.get("project_name", ""),
            "project_brief": queue_row.get("project_brief", ""),
            "borough_name_standardized": queue_row.get("borough_name_standardized", ""),
            "affected_lot_acres": queue_row.get("affected_lot_acres", ""),
            "source_lookup_priority": queue_row.get("source_lookup_priority", ""),
            "missing_direction_reason": queue_row.get("missing_direction_reason", ""),
            "text_candidate_direction": queue_row.get("text_candidate_direction", ""),
            "text_candidate_basis": queue_row.get("text_candidate_basis", ""),
            "text_zoning_codes": queue_row.get("text_zoning_codes", ""),
            "parsed_zoning_changes": queue_row.get("parsed_zoning_changes", ""),
            "selected_document_count": len(project_documents),
            "extracted_document_count": sum(row["text_status"] in {"text_extracted", "ocr_text_extracted"} for row in project_documents),
            "evidence_snippet_count": len(project_snippets),
            "source_documents": "\n".join(source_lines),
            "official_source_evidence": truncated_project_evidence(project_snippets),
            "chatgpt_review_status": "",
            "suggested_rezoning_direction": "",
            "suggested_rezoning_class": "",
            "suggested_housing_intent": "",
            "suggested_scope_type": "",
            "suggested_scope_blocks": "",
            "suggested_scope_acres": "",
            "suggested_confidence": "",
            "suggested_evidence_note": "",
        }
    )

batch_records = []
for row in packet_rows[:CHATGPT_BATCH_PROJECT_COUNT]:
    batch_records.append(
        "\n".join(
            [
                "-----",
                f"project_id: {row['project_id']}",
                f"completed_year: {row['completed_year']}",
                f"project_name: {row['project_name']}",
                f"borough: {row['borough_name_standardized']}",
                f"affected_lot_acres_current_bbl_scope: {row['affected_lot_acres']}",
                f"text_candidate_direction: {row['text_candidate_direction']}",
                f"missing_direction_reason: {row['missing_direction_reason']}",
                f"text_zoning_codes: {row['text_zoning_codes']}",
                f"parsed_zoning_changes: {row['parsed_zoning_changes']}",
                "",
                "source_documents:",
                row["source_documents"],
                "",
                "official_source_evidence:",
                row["official_source_evidence"],
            ]
        )
    )

batch_prompt = "\n".join(
    [
        "# ZAP Rezoning Source-Based First-Pass Review",
        "",
        "Classify each NYC zoning map amendment using only the official-source evidence below.",
        "This is a first pass for human review, not a final research label.",
        "",
        "Return CSV with exactly these columns:",
        "project_id,chatgpt_review_status,suggested_rezoning_direction,suggested_rezoning_class,suggested_housing_intent,suggested_scope_type,suggested_scope_blocks,suggested_scope_acres,suggested_confidence,suggested_evidence_note",
        "",
        "Allowed suggested_rezoning_direction values: upzoning, downzoning, mixed, no_material_residential_change, unknown.",
        "Allowed suggested_housing_intent values: yes, no, unclear.",
        "Allowed suggested_scope_type values: single_site, small_area, corridor, neighborhood, large_neighborhood, very_large_neighborhood, unknown.",
        "Allowed suggested_confidence values: high, medium, low.",
        "",
        "Rules:",
        "- Base direction on residential capacity, not whether the project is politically described as neighborhood preservation.",
        "- Treat contextual/form restrictions as downzoning or mixed when they reduce residential envelope even without a lower numeric FAR.",
        "- Treat commercial overlays alone as no_material_residential_change unless the source shows an underlying residential district change.",
        "- Use unknown when evidence does not identify the before/after zoning or residential capacity implication.",
        "- In suggested_evidence_note, cite the document number and page number from the evidence.",
        "",
        *batch_records,
        "",
    ]
)

qc_rows = [
    {
        "metric": "queued_unresolved_project_count",
        "value": len(queue_rows),
        "status": "pass" if len(queue_rows) > 0 else "fail",
        "note": "Parser-unknown, still-unresolved projects read from the text-candidate queue.",
    },
    {
        "metric": "selected_source_document_count",
        "value": len(selected_documents),
        "status": "pass" if len(selected_documents) > 0 else "fail",
        "note": "Official source documents selected for extraction.",
    },
    {
        "metric": "project_with_selected_document_count",
        "value": len({row["project_id"] for row in selected_documents}),
        "status": "pass" if len({row["project_id"] for row in selected_documents}) == len(queue_rows) else "fail",
        "note": "Projects with at least one selected source document.",
    },
    {
        "metric": "downloaded_document_count",
        "value": sum(row["download_status"] == "downloaded" for row in document_index_rows),
        "status": "pass",
        "note": "PDF documents successfully downloaded. Docket text rows are not downloads.",
    },
    {
        "metric": "download_failed_document_count",
        "value": sum(row["download_status"] == "download_failed" for row in document_index_rows),
        "status": "pass",
        "note": "PDF document downloads that failed and are retained for follow-up.",
    },
    {
        "metric": "text_extracted_document_count",
        "value": sum(row["text_status"] in {"text_extracted", "ocr_text_extracted"} for row in document_index_rows),
        "status": "pass" if any(row["text_status"] in {"text_extracted", "ocr_text_extracted"} for row in document_index_rows) else "fail",
        "note": "Selected documents with nonempty extracted text.",
    },
    {
        "metric": "project_with_evidence_snippet_count",
        "value": len({row["project_id"] for row in snippet_rows}),
        "status": "pass" if len({row["project_id"] for row in snippet_rows}) > 0 else "fail",
        "note": "Projects with at least one zoning/scope keyword evidence snippet.",
    },
    {
        "metric": "chatgpt_review_packet_rows",
        "value": len(packet_rows),
        "status": "pass" if len(packet_rows) == len(queue_rows) else "fail",
        "note": "One ChatGPT review packet row per unresolved project.",
    },
]

write_csv_if_changed(
    document_index_rows,
    [
        "project_id",
        "project_name",
        "completed_year",
        "queue_rank",
        "document_rank",
        "selected_reason",
        "source_type",
        "document_family",
        "source_priority",
        "action_code",
        "ulurp_number",
        "document_title",
        "document_url",
        "local_pdf_path",
        "local_text_path",
        "download_status",
        "download_error",
        "text_status",
        "text_error",
        "text_char_count",
    ],
    "../output/zap_rezoning_source_document_index.csv",
)

write_csv_if_changed(
    snippet_rows,
    [
        "project_id",
        "project_name",
        "completed_year",
        "queue_rank",
        "document_rank",
        "snippet_rank",
        "selected_reason",
        "source_type",
        "document_family",
        "source_priority",
        "action_code",
        "ulurp_number",
        "document_title",
        "document_url",
        "page_number",
        "snippet_score",
        "snippet_text",
    ],
    "../output/zap_rezoning_source_text_snippets.csv",
)

write_csv_if_changed(
    packet_rows,
    [
        "project_id",
        "completed_year",
        "event_period",
        "queue_rank",
        "project_name",
        "project_brief",
        "borough_name_standardized",
        "affected_lot_acres",
        "source_lookup_priority",
        "missing_direction_reason",
        "text_candidate_direction",
        "text_candidate_basis",
        "text_zoning_codes",
        "parsed_zoning_changes",
        "selected_document_count",
        "extracted_document_count",
        "evidence_snippet_count",
        "source_documents",
        "official_source_evidence",
        "chatgpt_review_status",
        "suggested_rezoning_direction",
        "suggested_rezoning_class",
        "suggested_housing_intent",
        "suggested_scope_type",
        "suggested_scope_blocks",
        "suggested_scope_acres",
        "suggested_confidence",
        "suggested_evidence_note",
    ],
    "../output/zap_rezoning_chatgpt_review_packet.csv",
)

write_text_if_changed(textwrap.dedent(batch_prompt), "../output/zap_rezoning_chatgpt_review_batch_001.md")
write_csv_if_changed(qc_rows, ["metric", "value", "status", "note"], "../output/zap_rezoning_source_text_qc.csv")

if any(row["status"] == "fail" for row in qc_rows):
    print("ZAP rezoning source text extraction QC failed.", file=sys.stderr)
    sys.exit(1)

print("Wrote ZAP rezoning source text extraction outputs to ../output")
