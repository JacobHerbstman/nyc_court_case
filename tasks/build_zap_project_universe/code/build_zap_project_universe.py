#!/usr/bin/env python3
# Run from tasks/build_zap_project_universe/code.

import csv
import hashlib
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, "../../_lib")
from data_reports import save_csv

for name, expected_sha256 in json.loads(Path("source_snapshot_sha256.json").read_text()).items():
    assert hashlib.sha256(Path("../input", name).read_bytes()).hexdigest() == expected_sha256, name

with open("../input/zap_project_data.csv", encoding="utf-8-sig", newline="") as stream:
    reader = csv.DictReader(stream)
    projects = list(reader)
metadata = json.loads(Path("../input/zap_project_metadata.json").read_text())
official_count = int(json.loads(Path("../input/zap_project_count.json").read_text())[0]["count"])
assert len(projects) == official_count, "Full export disagrees with independent Socrata count."
assert len({p["project_id"] for p in projects}) == len(projects), "Duplicate project IDs."
assert all(p["project_id"].strip() for p in projects), "Blank project ID."
assert all(p["dcp_visibility"] == "General Public" for p in projects)

with open("../input/zap_bbl.csv", encoding="utf-8-sig", newline="") as stream:
    parcels = list(csv.DictReader(stream))
assert len(parcels) == int(json.loads(Path("../input/zap_bbl_count.json").read_text())[0]["count"])
project_ids = {row["project_id"] for row in projects}
assert all(p["project_id"] in project_ids for p in parcels)
parcel_rows = Counter(p["project_id"] for p in parcels)
parcel_ids = defaultdict(set)
for row in parcels:
    if row["bbl"]:
        parcel_ids[row["project_id"]].add(row["bbl"])

version = re.search(r"Current version:\s*(\d+)", metadata["description"]).group(1)
for project in projects:
    status = project["project_status"]
    project.update(
        source_dataset_id="hgx4-8ukb",
        source_snapshot_date="2026-09-14",
        source_release=version,
        project_page_url="https://zap.planning.nyc.gov/projects/" + project["project_id"],
        withdrawn_or_terminated=status.startswith(("Withdrawn", "Terminated")),
        ulurp_scope=("explicit_ulurp" if project["ulurp_non"] == "ULURP" else
                     "unclassified" if not project["ulurp_non"] else "other_reported_process"),
        ulurp_number_present=bool(project["ulurp_numbers"].strip()),
        actions_present=bool(project["actions"].strip()),
        project_brief_present=bool(project["project_brief"].strip()),
        certified_referred_date_present=bool(project["certified_referred"]),
        historical_date_cluster_flag=(project["certified_referred"] == "01/31/2011" and
                                      not project["ulurp_non"]),
        bbl_source_row_count=parcel_rows[project["project_id"]],
        distinct_nonblank_bbl_count=len(parcel_ids[project["project_id"]]),
    )
projects.sort(key=lambda row: row["project_id"])
save_csv(projects, list(projects[0]), "../output/zap_project_universe.csv", ["project_id"])

# The received files stay byte-for-byte unchanged; these checks describe them.
raw_report = {"snapshot_date": "2026-09-14", "project_release": version,
              "project_rows": len(projects), "independent_project_count": official_count,
              "bbl_rows": len(parcels), "bbl_projects": len(parcel_rows),
              "bbl_duplicate_project_bbl_pairs": len(parcels) - len({(p["project_id"], p["bbl"]) for p in parcels}),
              "project_status_counts": dict(sorted(Counter(p["project_status"] for p in projects).items())),
              "source_sha256": {}}
for name in ["zap_project_data.csv", "zap_project_metadata.json", "zap_project_count.json",
             "zap_bbl.csv", "zap_bbl_metadata.json", "zap_bbl_count.json", "zapprojects_datadictionary.xlsx"]:
    raw_report["source_sha256"][name] = hashlib.sha256(Path("../input", name).read_bytes()).hexdigest()
raw_report["bbl_columns"] = {
    field: {"source_type": "string", "missing": sum(not p[field] for p in parcels),
            "distinct_nonmissing": len({p[field] for p in parcels if p[field]})}
    for field in parcels[0]
}
Path("../report/raw_export.json").write_text(json.dumps(raw_report, indent=2) + "\n")
print(json.dumps({"project_rows": len(projects), "bbl_rows": len(parcels), "release": version}))
