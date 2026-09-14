#!/usr/bin/env python3
# Run from tasks/audits/audit_zap_universe_coverage/code.

import csv
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

sys.path.insert(0, "../../../_lib")
from data_reports import save_csv

if len(sys.argv) != 2 or int(sys.argv[1]) < 2:
    raise ValueError("Usage: python3 audit_zap_universe_coverage.py <cluster_min_projects>")
cluster_min_projects = int(sys.argv[1])

with open("../input/zap_project_universe.csv", newline="", encoding="utf-8") as stream:
    projects = list(csv.DictReader(stream))
with open("../input/previous_zap_project_data.csv", newline="", encoding="utf-8-sig") as stream:
    previous = list(csv.DictReader(stream))
with open("../input/ulurp_corpus_application_spine.csv", newline="", encoding="utf-8") as stream:
    spine = list(csv.DictReader(stream))
with open("../input/ulurp_cpc_report_manifest.csv", newline="", encoding="utf-8") as stream:
    reports = list(csv.DictReader(stream))
with open("../input/zap_missing_project_details.jsonl", encoding="utf-8") as stream:
    details = [json.loads(line) for line in stream]

assert len({p["project_id"] for p in previous}) == len(previous)
assert len({p["project_id"] for p in projects}) == len(projects)
assert len({p["project_id"] for p in details}) == len(details)
assert len({p["application_key"] for p in reports}) == len(reports)
old_by_id = {p["project_id"]: p for p in previous}
current_by_id = {p["project_id"]: p for p in projects}
detail_by_id = {p["project_id"]: p for p in details}
assert set(detail_by_id) <= set(current_by_id)
spine_ids = {p["project_id"] for p in spine}
reports_by_key = {p["application_key"]: p for p in reports}
reports_by_project = defaultdict(set)
for report in reports:
    for project_id in re.split(r"\s*[;|]\s*", report["zap_project_ids"]):
        if project_id:
            reports_by_project[project_id].add(report["application_key"])

coverage = []
actions = []
api_entity_counts = Counter()
for project in projects:
    project_id = project["project_id"]
    detail = detail_by_id.get(project_id)
    entities = detail["response"].get("included", []) if detail and detail["fetch_status"] == "success" else []
    detail_actions = [e for e in entities if e["type"] == "actions"]
    entity_keys = [(e["type"], e["id"]) for e in entities]
    assert len(entity_keys) == len(set(entity_keys)), f"Repeated entity within project {project_id}"
    api_entity_counts.update(e["type"] for e in entities)
    if detail and detail["fetch_status"] == "success":
        for relationship in ["actions", "milestones", "dispositions", "packages", "artifacts"]:
            references = detail["response"]["data"].get("relationships", {}).get(relationship, {}).get("data") or []
            assert all((ref["type"], ref["id"]) in entity_keys for ref in references), (project_id, relationship)
    recovered_keys = set()
    linked_reports = set()
    for entity in detail_actions:
        attributes = entity.get("attributes", {})
        number = attributes.get("dcp-ulurpnumber") or ""
        key = re.sub(r"^[CNMI](?=\d)", "", re.sub(r"[^A-Z0-9]", "", number.upper()))
        if key:
            recovered_keys.add(key)
        if key in reports_by_key:
            linked_reports.add(key)
        actions.append({
            "project_id": project_id, "action_id": entity["id"],
            "bulk_project_status": project["project_status"], "bulk_ulurp_non": project["ulurp_non"],
            "action_name": attributes.get("dcp-name") or "",
            "action_code": attributes.get("dcp-action-value") or "",
            "raw_application_number": number, "application_key": key,
            "action_status": attributes.get("statuscode") or "",
            "action_state": attributes.get("statecode") or "",
            "cpc_report_url_raw": attributes.get("dcp-spabsoluteurl") or "",
            "matches_existing_cpc_report": key in reports_by_key,
            "source_url": detail["source_url"], "source_sha256": detail["source_sha256"],
        })
    api_scope = ""
    if detail and detail["fetch_status"] == "success":
        api_scope = detail["response"]["data"].get("attributes", {}).get("dcp-ulurp-nonulurp") or ""
    terminal = project["withdrawn_or_terminated"] == "True"
    coverage.append({
        "project_id": project_id, "project_name": project["project_name"],
        "project_status": project["project_status"], "ulurp_non": project["ulurp_non"],
        "ulurp_scope": project["ulurp_scope"], "api_ulurp_non": api_scope,
        "withdrawn_or_terminated": terminal,
        "in_previous_raw_snapshot": project_id in old_by_id,
        "in_existing_application_spine": project_id in spine_ids,
        "existing_cpc_project_link_count": len(reports_by_project[project_id]),
        "bulk_ulurp_number_present": bool(project["ulurp_numbers"]),
        "bulk_actions_present": bool(project["actions"]),
        "project_brief_present": bool(project["project_brief"]),
        "app_filed_date_present": bool(project["app_filed_date"]),
        "certified_referred_present": bool(project["certified_referred"]),
        "completed_date_present": bool(project["completed_date"]),
        "approval_date_present": bool(project["approval_date"]),
        "historical_date_cluster_flag": project["historical_date_cluster_flag"] == "True",
        "has_bbl": int(project["distinct_nonblank_bbl_count"]) > 0,
        "api_fetch_status": detail["fetch_status"] if detail else "not_targeted",
        "api_action_count": len(detail_actions) if detail and detail["fetch_status"] == "success" else "",
        "api_application_number_count": len(recovered_keys) if detail and detail["fetch_status"] == "success" else "",
        "api_linked_cpc_report_count": len(linked_reports) if detail and detail["fetch_status"] == "success" else "",
        "api_milestone_count": sum(e["type"] == "milestones" for e in entities) if detail and detail["fetch_status"] == "success" else "",
        "api_package_count": sum(e["type"] == "packages" for e in entities) if detail and detail["fetch_status"] == "success" else "",
        "api_artifact_count": sum(e["type"] == "artifacts" for e in entities) if detail and detail["fetch_status"] == "success" else "",
        "project_page_url": project["project_page_url"],
    })

status_rows = []
groups = defaultdict(list)
for row in coverage:
    groups[(row["ulurp_scope"], row["project_status"])].append(row)
for (scope, status), group in sorted(groups.items()):
    status_rows.append({
        "ulurp_scope": scope, "project_status": status, "projects": len(group),
        "in_existing_application_spine": sum(r["in_existing_application_spine"] for r in group),
        "has_existing_cpc_project_link": sum(r["existing_cpc_project_link_count"] > 0 for r in group),
        "has_bulk_ulurp_number": sum(r["bulk_ulurp_number_present"] for r in group),
        "has_bulk_action": sum(r["bulk_actions_present"] for r in group),
        "has_filed_date": sum(r["app_filed_date_present"] for r in group),
        "has_certified_referred_date": sum(r["certified_referred_present"] for r in group),
        "has_completed_date": sum(r["completed_date_present"] for r in group),
        "has_bbl": sum(r["has_bbl"] for r in group),
        "api_success": sum(r["api_fetch_status"] == "success" for r in group),
        "api_recovers_number_missing_in_bulk": sum(not r["bulk_ulurp_number_present"] and (r["api_application_number_count"] or 0) > 0 for r in group),
        "api_links_existing_cpc_report": sum((r["api_linked_cpc_report_count"] or 0) > 0 for r in group),
    })

# Keep date concepts separate. No completion/certification/ID-year fallback.
annual_counts = Counter()
date_counts = Counter()
for project in projects:
    for field in ["app_filed_date", "noticed_date", "certified_referred", "completed_date", "approval_date"]:
        value = project[field]
        year = datetime.strptime(value, "%m/%d/%Y").year if value else "missing"
        annual_counts[(project["ulurp_scope"], project["project_status"], field, str(year))] += 1
        if value:
            date_counts[(project["ulurp_scope"], project["project_status"], field, value)] += 1
annual_rows = [{"ulurp_scope": k[0], "project_status": k[1], "date_field": k[2], "year": k[3], "projects": v}
               for k, v in sorted(annual_counts.items())]
cluster_rows = [{"ulurp_scope": k[0], "project_status": k[1], "date_field": k[2], "date": k[3], "projects": v}
                for k, v in sorted(date_counts.items(), key=lambda item: (-item[1], item[0])) if v >= cluster_min_projects]

changes = []
for project_id in sorted(set(old_by_id) | set(current_by_id)):
    old = old_by_id.get(project_id)
    new = current_by_id.get(project_id)
    changed_fields = [field for field in old if old[field] != new[field]] if old and new else []
    if not old or not new or changed_fields:
        changes.append({"project_id": project_id,
                        "change_type": "added" if old is None else "removed" if new is None else "changed",
                        "changed_fields": ";".join(changed_fields),
                        "previous_project_status": old["project_status"] if old else "",
                        "current_project_status": new["project_status"] if new else ""})

save_csv(coverage, list(coverage[0]), "../output/zap_project_coverage.csv", ["project_id"])
save_csv(status_rows, list(status_rows[0]), "../output/zap_status_coverage.csv", ["ulurp_scope", "project_status"])
save_csv(annual_rows, list(annual_rows[0]), "../output/zap_dates_by_year.csv", ["ulurp_scope", "project_status", "date_field", "year"])
save_csv(cluster_rows, list(cluster_rows[0]), "../output/zap_date_clusters.csv", ["ulurp_scope", "project_status", "date_field", "date"])
save_csv(changes, ["project_id", "change_type", "changed_fields", "previous_project_status", "current_project_status"], "../output/zap_snapshot_changes.csv", ["project_id"])
save_csv(actions, ["project_id", "action_id", "bulk_project_status", "bulk_ulurp_non", "action_name", "action_code", "raw_application_number", "application_key", "action_status", "action_state", "cpc_report_url_raw", "matches_existing_cpc_report", "source_url", "source_sha256"], "../output/zap_recovered_actions.csv", ["project_id", "action_id"])

ulurp = [r for r in coverage if r["ulurp_scope"] == "explicit_ulurp"]
terminal = [r for r in ulurp if r["withdrawn_or_terminated"]]
summary = {
    "all_projects": len(coverage), "explicit_ulurp": len(ulurp),
    "ulurp_withdrawn_terminated": len(terminal),
    "ulurp_withdrawn_terminated_share": len(terminal) / len(ulurp),
    "unclassified_projects": sum(r["ulurp_scope"] == "unclassified" for r in coverage),
    "all_withdrawn_terminated": sum(r["withdrawn_or_terminated"] for r in coverage),
    "old_spine_rows": len(spine), "old_spine_projects": len(spine_ids),
    "ulurp_terminal_in_old_spine": sum(r["in_existing_application_spine"] for r in terminal),
    "ulurp_terminal_api_recovered_number": sum((r["api_application_number_count"] or 0) > 0 for r in terminal),
    "ulurp_terminal_api_linked_existing_report": sum((r["api_linked_cpc_report_count"] or 0) > 0 for r in terminal),
    "api_status_counts": dict(Counter(d["fetch_status"] for d in details)),
    "api_entity_counts": dict(sorted(api_entity_counts.items())),
    "all_requested_relationship_references_returned": True,
    "snapshot_changes": dict(Counter(r["change_type"] for r in changes)),
    "unclassified_date_cluster": sum(r["historical_date_cluster_flag"] for r in coverage),
    "unclassified_api_still_missing_scope": sum(r["ulurp_scope"] == "unclassified" and not r["api_ulurp_non"] for r in coverage),
    "corpus_report_rows": len(reports),
    "corpus_reports_without_bulk_project_link": sum(not p["zap_project_ids"] for p in reports),
}
Path("../report/summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary, indent=2))

findings = [
    "## Generated source audit: September 14, 2026\n",
    f"The complete public export has {len(coverage):,} unique projects, including {len(ulurp):,} explicitly marked ULURP. "
    f"Of the explicit ULURP projects, {len(terminal):,} are withdrawn or terminated ({len(terminal) / len(ulurp):.1%}). "
    "This is a snapshot share, not a cohort failure probability.\n",
    f"There are {summary['unclassified_projects']:,} projects with no ULURP/non-ULURP classification. "
    f"Across all reported processes and unknown scope, {summary['all_withdrawn_terminated']:,} projects are withdrawn or terminated.\n",
    f"The existing application spine has {summary['old_spine_rows']:,} rows representing {summary['old_spine_projects']:,} projects. "
    f"It includes {summary['ulurp_terminal_in_old_spine']:,} of the explicitly marked ULURP withdrawals/terminations. "
    "Membership here applies the current status to the existing, older sample.\n",
    f"The detail requests returned {sum(d['fetch_status'] == 'success' for d in details):,} successful records out of {len(details):,} selected projects. "
    f"For explicit ULURP withdrawals/terminations, the API supplies at least one application number for {summary['ulurp_terminal_api_recovered_number']:,} projects, "
    f"and recovered numbers link {summary['ulurp_terminal_api_linked_existing_report']:,} projects to at least one report already in the CPC corpus. "
    "Unmatched numbers remain unresolved; links are not proof of what decision a report contains.\n",
    "| Explicit ULURP status | Projects | In existing spine | Bulk has number | API recovers missing number |\n"
    "|---|---:|---:|---:|---:|",
]
for row in status_rows:
    if row["ulurp_scope"] == "explicit_ulurp":
        findings.append(f"| {row['project_status']} | {row['projects']:,} | {row['in_existing_application_spine']:,} | {row['has_bulk_ulurp_number']:,} | {row['api_recovers_number_missing_in_bulk']:,} |")
findings.extend([
    "\n### Date and historical coverage concerns\n",
    f"Among unclassified projects, {summary['unclassified_date_cluster']:,} share January 31, 2011 in the certified/referred field. "
    "These values are preserved and flagged for investigation, not replaced with years parsed from IDs.\n",
    "| Reported certified/referred year | Explicit ULURP projects | Withdrawn/terminated now | Share |\n"
    "|---|---:|---:|---:|",
])
for first, last in [(1990, 2001), (2002, 2013), (2014, 2025)]:
    group = [p for p in projects if p["ulurp_non"] == "ULURP" and p["certified_referred"]
             and first <= datetime.strptime(p["certified_referred"], "%m/%d/%Y").year <= last]
    count = sum(p["withdrawn_or_terminated"] == "True" for p in group)
    findings.append(f"| {first}-{last} | {len(group):,} | {count:,} | {count / len(group):.1%} |")
findings.extend([
    "\nThese are descriptive groups using the source's reported date. Missing dates and unknown process classifications are outside this table; "
    "ongoing projects remain in its denominator. Differences may reflect source coverage, case composition, follow-up time, or behavior. "
    "They do not identify a 2001 effect.\n",
    "### Snapshot reconciliation\n",
    ", ".join(f"{count:,} {kind}" for kind, count in sorted(summary["snapshot_changes"].items()))
    + " relative to the May 1, 2026 raw project snapshot. Changed fields are listed by project in the audit CSV.\n",
    f"The existing corpus has {len(reports):,} report rows; {summary['corpus_reports_without_bulk_project_link']:,} have no stored ZAP project link. "
    "This is another reconciliation queue, not evidence that those projects are absent from the full export.\n",
    "### Reproduction\n",
    "Run `make` from `tasks/audits/audit_zap_universe_coverage/code`. "
    "Its Makefile traces the full project export, targeted API acquisition, previous raw snapshot, application spine, and CPC manifest. "
    "CSV data reports provide checksums, key checks, missingness, and distinct counts.\n",
])
Path("../output/zap_universe_findings.md").write_text("\n".join(findings), encoding="utf-8")
