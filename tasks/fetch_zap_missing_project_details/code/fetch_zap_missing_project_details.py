#!/usr/bin/env python3
# Run from tasks/fetch_zap_missing_project_details/code.

import csv
import hashlib
import json
import subprocess
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import quote

if len(sys.argv) != 2 or not 1 <= int(sys.argv[1]) <= 4:
    raise ValueError("Usage: python3 fetch_zap_missing_project_details.py <workers:1-4>")
workers = int(sys.argv[1])
with open("../input/zap_project_universe.csv", newline="", encoding="utf-8") as stream:
    projects = list(csv.DictReader(stream))
projects = [p for p in projects if p["withdrawn_or_terminated"] == "True"
            or p["ulurp_scope"] == "unclassified" or p["project_status"] == "Record Closed"]
assert len({p["project_id"] for p in projects}) == len(projects)


def fetch_project(project):
    """Retrieve one public response, preserving validated raw bytes for restart."""
    project_id = project["project_id"]
    url = ("https://zap-api-production.herokuapp.com/projects/" + quote(project_id, safe="")
           + "?include=actions,milestones,dispositions,packages,artifacts")
    raw = Path("../../../data_raw/dcp_zap_project_details/20260914", project_id + ".json")
    record = {"project_id": project_id, "bulk_project_status": project["project_status"],
              "bulk_ulurp_non": project["ulurp_non"], "source_url": url,
              "source_snapshot_date": "2026-09-14", "fetch_status": "", "fetch_error": "",
              "source_sha256": "", "raw_path": str(raw), "response": None}
    if raw.exists():
        body = raw.read_bytes()
        payload = json.loads(body)
        if payload.get("data", {}).get("id") != project_id:
            raise ValueError(f"Saved raw project ID mismatch: {project_id}")
    else:
        for attempt in range(2):
            result = subprocess.run(
                ["curl", "-LsS", "--connect-timeout", "10", "--max-time", "45",
                 "--user-agent", "nyc-ulurp-research/1.0", "--write-out", "\n%{http_code}", url],
                capture_output=True, timeout=50)
            body, _, http_status = result.stdout.rpartition(b"\n")
            if result.returncode == 0 and http_status == b"200":
                break
            if http_status not in (b"429", b"500", b"502", b"503", b"504", b"000") and result.returncode == 0:
                break
            if attempt == 0:
                time.sleep(2)
        if result.returncode != 0 or http_status != b"200":
            record["fetch_status"] = "http_" + http_status.decode("ascii", errors="replace")
            record["fetch_error"] = (result.stderr or body).decode("utf-8", errors="replace")[:400]
            return record
        try:
            payload = json.loads(body)
        except (ValueError, UnicodeError) as error:
            record.update(fetch_status="invalid_json", fetch_error=str(error))
            return record
        if payload.get("data", {}).get("id") != project_id:
            record.update(fetch_status="project_id_mismatch", fetch_error="Public response returned a different project ID.")
            return record
        Path("../temp", project_id + ".json").write_bytes(body)
        Path("../temp", project_id + ".json").replace(raw)
    record.update(fetch_status="success", source_sha256=hashlib.sha256(body).hexdigest(), response=payload)
    return record


counts = Counter()
with ThreadPoolExecutor(max_workers=workers) as executor, open("../temp/zap_missing_project_details.jsonl", "w", encoding="utf-8") as stream:
    for number, record in enumerate(executor.map(fetch_project, projects), 1):
        stream.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        counts[record["fetch_status"]] += 1
        if number % 100 == 0 or number == len(projects):
            print(number, "of", len(projects), dict(counts), flush=True)
Path("../temp/zap_missing_project_details.jsonl").replace("../output/zap_missing_project_details.jsonl")
report = {
    "dataset": "zap_missing_project_details.jsonl", "rows": len(projects), "key": ["project_id"],
    "unique_nonmissing_key": True, "fetch_status_counts": dict(sorted(counts.items())),
    "selection": "Every withdrawn/terminated/record-closed project, plus every project with missing ulurp_non, in the full export.",
    "snapshot_note": "Live API retrieved separately from the bulk export; disagreement is retained, not overwritten.",
    "source_snapshot_date": "2026-09-14",
    "sha256_jsonl_bytes": hashlib.sha256(Path("../output/zap_missing_project_details.jsonl").read_bytes()).hexdigest(),
    "missing_response": len(projects) - counts["success"],
    "nested_schema": "response.data contains project attributes/relationships; response.included contains linked typed entities with source IDs.",
}
Path("../report/zap_missing_project_details.json").write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps(report), flush=True)
