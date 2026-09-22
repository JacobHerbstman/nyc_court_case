#!/usr/bin/env python3
"""Measure rule coverage, compare existing labels, and prepare fresh review."""

import csv
import hashlib
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, "../../../_lib")
from data_reports import save_csv

sample_size, sample_seed = map(int, sys.argv[1:])
assert sample_size > 0
with Path("../input/ulurp_cpc_text_labels.csv").open(newline="") as source:
    rows = list(csv.DictReader(source))
with Path("../input/ulurp_cpc_report_manifest.csv").open(newline="") as source:
    manifests = list(csv.DictReader(source))
assert len(rows) == len({row["document_id"] for row in rows})
assert len(manifests) == len({row["document_id"] for row in manifests})
manifest = {row["document_id"]: row for row in manifests}
assert all(row["document_id"] in manifest for row in rows)
with Path("../input/ulurp_cpc_training_labels_jacob.csv").open(newline="") as source:
    human_rows = [row for row in csv.DictReader(source) if row["coding_complete"] == "1"]
assert len(human_rows) == len({row["document_id"] for row in human_rows})
human = {row["document_id"]: row for row in human_rows}
with open("../input/ulurp_cpc_human_coding.csv", newline="") as source:
    existing_coding = list(csv.DictReader(source))
assert len(existing_coding) == len({(row["source_document_id"], row["field"]) for row in existing_coding})
coding_by_report = defaultdict(dict)
for row in existing_coding:
    coding_by_report[row["source_document_id"]][row["field"]] = row
with Path("../input/ulurp_cpc_regex_validation_labels_codex.csv").open(newline="") as source:
    development_rows = list(csv.DictReader(source))
assert len(development_rows) == len({row["document_id"] for row in development_rows})
development = {row["document_id"] for row in development_rows}
with Path("../input/ulurp_cpc_regex_holdout_labels_codex.csv").open(newline="") as source:
    previous_holdout_rows = list(csv.DictReader(source))
assert len(previous_holdout_rows) == len({row["document_id"] for row in previous_holdout_rows})
previous_holdout = {row["document_id"] for row in previous_holdout_rows}
benchmarks = {"codex_development": {row["document_id"]: row for row in development_rows},
              "codex_benchmark": {row["document_id"]: row for row in previous_holdout_rows}}

with Path("rule_development_reports.csv").open(newline="") as source:
    inspected_rows = list(csv.DictReader(source))
assert len(inspected_rows) == len({row["document_id"] for row in inspected_rows})
by_id = {row["document_id"]: row for row in rows}
assert all(row["source_text_sha256"] == by_id[row["document_id"]]["source_text_sha256"] for row in inspected_rows)
inspected = {row["document_id"] for row in inspected_rows}
known = set(coding_by_report) | development | previous_holdout | inspected
known_applications = {manifest[document_id]["application_number"] for document_id in known}
for row in rows:
    if row["document_id"] in known:
        known_applications.update(row["companion_application_numbers"].split("; "))
known_applications.discard("")
excluded = {row["document_id"] for row in rows if
    ({row["application_number"]} | set(row["companion_application_numbers"].split("; "))) & known_applications}

fields = [
    ("Board votes for", "cb_support_votes", "cb_status"),
    ("Board votes against", "cb_opposition_votes", "cb_status"),
    ("CPC speakers for", "cpc_support_speakers", "cpc_speakers_status"),
    ("CPC speakers against", "cpc_opposition_speakers", "cpc_speakers_status"),
]
coverage = []
for period in ["all", *sorted({row["decade"] for row in rows})]:
    period_rows = [row for row in rows if period == "all" or row["decade"] == period]
    for label, field, status in fields:
        eligible = [row for row in period_rows if row[status] == "resolved"]
        reference = [row for row in period_rows if row["document_id"] in human
                     and human[row["document_id"]].get(field, "") not in {"", "unclear"}]
        checked = [row for row in reference if row[status] == "resolved"]
        exact = sum(row[field] == human[row["document_id"]][field] for row in checked)
        coverage.append(dict(period=period, field=field, label=label, narratives=len(period_rows),
            nonmissing=sum(row[field] != "" for row in period_rows), resolved=len(eligible),
            resolved_share=len(eligible) / len(period_rows), reference_nonmissing=len(reference),
            resolved_with_reference=len(checked), exact=exact,
            exact_share=exact / len(checked) if checked else None))
        for benchmark, labels in benchmarks.items():
            checked = [row for row in eligible if row["document_id"] in labels
                       and labels[row["document_id"]][field] not in {"", "unclear"}]
            coverage[-1][benchmark + "_resolved"] = len(checked)
            coverage[-1][benchmark + "_exact"] = sum(
                row[field] == labels[row["document_id"]][field] for row in checked)
save_csv(coverage, list(coverage[0]), "../output/ulurp_cpc_regex_coverage.csv", ["period", "field"])

# A fresh, balanced review sample excludes every report already used for coding.
# The review sheet omits regex predictions; coders should read the whole report.
groups = defaultdict(list)
for row in rows:
    if row["document_id"] in excluded:
        continue
    action = "zoning" if row["action_code"] in {"ZM", "ZR", "ZS"} else "other"
    route = "inverted" if row["cb_vote_rule"] == "inverted_disapproval" else (
        "both_resolved" if row["cb_status"] == row["cpc_speakers_status"] == "resolved" else "needs_review")
    groups[(row["decade"], action, route)].append(row)
population_sizes = {key: len(group) for key, group in groups.items()}
stratum_by_id = {row["document_id"]: key for key, group in groups.items() for row in group}
stratum_ids = {key: f"S{i + 1:02d}" for i, key in enumerate(sorted(groups))}
for group in groups.values():
    group.sort(key=lambda row: hashlib.sha256(f"{sample_seed}|{row['document_id']}".encode()).hexdigest())
selected = []
while len(selected) < sample_size and any(groups.values()):
    for key in sorted(groups):
        if groups[key] and len(selected) < sample_size:
            selected.append(groups[key].pop(0))
sample_sizes = defaultdict(int)
for row in selected:
    sample_sizes[stratum_by_id[row["document_id"]]] += 1
sample = []
for row in selected:
    stratum = stratum_by_id[row["document_id"]]
    sample.append(dict(document_id=row["document_id"], application_number=row["application_number"],
        year=row["year"], action_code=row["action_code"], project_name=row["project_name"],
        stratum_id=stratum_ids[stratum], stratum_population=population_sizes[stratum],
        stratum_sample=sample_sizes[stratum], sampling_weight=population_sizes[stratum] / sample_sizes[stratum],
        source_text_sha256=row["source_text_sha256"], official_pdf_url=manifest[row["document_id"]]["official_pdf_url"],
        cb_position="", cb_reported_for="", cb_reported_against="",
        cb_support_votes="", cb_opposition_votes="", cb_abstentions="",
        cb_abstention_rule="", cpc_support_speakers="", cpc_opposition_speakers="",
        evidence_pages="", coder_notes="", coding_complete=""))
assert len(sample) == sample_size
save_csv(sample, list(sample[0]), "../output/ulurp_cpc_regex_human_review_sample.csv", ["document_id"])

queue = []
for row in rows:
    if row["cb_status"] == row["cpc_speakers_status"] == "resolved":
        continue
    queue.append({field: row[field] for field in (
        "document_id", "application_number", "project_name", "year", "action_code",
        "source_text_sha256", "companion_application_numbers", "cb_status", "cb_position",
        "cb_evidence", "cb_source_application", "cpc_speakers_status",
        "cpc_speakers_evidence", "cpc_speakers_source_application",
    )} | {"official_pdf_url": manifest[row["document_id"]]["official_pdf_url"]})
    codes = coding_by_report[row["document_id"]]
    queue[-1]["existing_human_fields"] = "; ".join(sorted(codes))
    queue[-1]["unresolved_count_fields_without_completed_human_value"] = "; ".join(
        field for _, field, status in fields if row[status] != "resolved" and
        codes.get(field, {}).get("human_status") not in {"human_agreement", "single_human_coder"})
save_csv(queue, list(queue[0]), "../output/ulurp_cpc_regex_review_queue.csv", ["document_id"])

human_agreement = []
for field in sorted({row["field"] for row in existing_coding}):
    paired = [row for row in existing_coding if row["field"] == field and
              row["jacob_value"] != "" and row["tyler_value"] != "" and
              row["human_status"] != "unclear_or_nonstandard"]
    complete = [row for row in paired if row["jacob_coding_complete"] == row["tyler_coding_complete"] == "1"]
    exact = sum(row["jacob_value"] == row["tyler_value"] for row in paired)
    complete_exact = sum(row["jacob_value"] == row["tyler_value"] for row in complete)
    human_agreement.append(dict(field=field, paired_reports=len(paired), exact=exact,
        agreement_share=exact / len(paired) if paired else None,
        both_marked_complete=len(complete), completed_exact=complete_exact,
        completed_agreement_share=complete_exact / len(complete) if complete else None))
save_csv(human_agreement, list(human_agreement[0]), "../output/ulurp_cpc_human_coding_agreement.csv", ["field"])

reported_board_tallies = sum(row["cb_reported_for"] != "" and row["cb_reported_against"] != "" for row in rows)
both = sum(row["cb_status"] == row["cpc_speakers_status"] == "resolved" for row in rows)
lines = [
    "# CPC regex coverage and validation", "",
    f"The output retains {len(rows):,} analysis narratives. Explicit rules resolve both "
    f"board tallies and hearing counts for {both:,} ({both / len(rows):.1%}). "
    f"The remaining {len(queue):,} have at least one field group requiring review.", "",
    f"A literal board tally is retained for {reported_board_tallies:,} narratives "
    f"({reported_board_tallies / len(rows):.1%}), including some whose proposal orientation requires review.", "",
    "Resolved is an extraction status, not a certified accuracy level. The count table below "
    "reuses Jacob's 200-report development sample. A separate agreement CSV compares the regex "
    "with Tyler's existing coding. The earlier "
    "Codex holdout has now been inspected and is a regression benchmark, not a fresh test.", "",
    "| Field | Resolved / all narratives | Matched human counts | Exact matches |", "|---|---:|---:|---:|",
]
for row in coverage:
    if row["period"] == "all":
        rate = f"{row['exact_share']:.1%}" if row["exact_share"] is not None else "not available"
        lines.append(f"| {row['label']} | {row['resolved']:,} ({row['resolved_share']:.1%}) | "
                     f"{row['resolved_with_reference']} | {row['exact']} ({rate}) |")
lines += ["", "The two Codex-coded samples are additional development checks, not independent human labels. "
          "Disagreements remain available for adjudication in the existing comparison outputs.", "",
          "| Field | First Codex sample: exact / resolved | Second Codex sample: exact / resolved |",
          "|---|---:|---:|"]
for row in coverage:
    if row["period"] == "all":
        lines.append(f"| {row['label']} | {row['codex_development_exact']} / {row['codex_development_resolved']} | "
                     f"{row['codex_benchmark_exact']} / {row['codex_benchmark_resolved']} |")
lines += ["", "## Extraction status", "", "| Field group | Reason | Narratives |", "|---|---|---:|"]
for label, field in [("Board", "cb_status"), ("Hearing", "cpc_speakers_status")]:
    counts = defaultdict(int)
    for row in rows:
        counts[row[field]] += 1
    for status, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
        lines.append(f"| {label} | {status.replace('_', ' ')} | {count:,} |")
lines += ["", f"A fresh {sample_size}-report sheet is ready for human coding. It balances decade, "
    f"zoning versus other actions, and extraction route, and excludes the {len(excluded)} narratives belonging to previously coded or inspected report bundles. "
    "The sheet includes stratum sizes and sampling weights for the previously unreviewed population. "
    "Do not interpret an unweighted sample average as corpus accuracy.", "",
    "Abstentions remain separate from affirmative and negative votes. Effective opposition includes "
    "abstentions only where the report explicitly establishes that rule. Formal board recommendation "
    "is recorded independently of the tally. Multiple tallies and conflicting companion reports "
    "are retained for review rather than combined into a fabricated vote."]
lines += ["", "## Reusing existing human coding", "",
    f"The linked human-coding table preserves {len(existing_coding):,} field records across "
    f"{len({r['source_document_id'] for r in existing_coding}):,} reports. "
    "Each coder's original values, notes, evidence, and completion flags remain separate. "
    "Matching values and single-coder judgments have usable human values; conflicts and nonstandard values remain unresolved. "
    "Provisional coding is retained and identified. No new human or model reading was used.", "",
    f"Existing completed human values cover every unresolved count field for "
    f"{sum(not row['unresolved_count_fields_without_completed_human_value'] for row in queue):,} "
    "narratives in the regex review queue. These counts can use the existing reading without changing regex extraction status.", "",
    "The earlier broad issue labels remain the comparison targets. New issue and actor detail fields are regex detections, "
    "not retrospective human judgments. Tyler's legacy development-direction field remains separate from the newer definitions.", "",
    "| Field | Paired reports | Agreement |", "|---|---:|---:|"]
for row in human_agreement:
    if row["paired_reports"]:
        lines.append(f"| {row['field'].replace('_', ' ')} | {row['paired_reports']} | "
                     f"{row['exact']} ({row['agreement_share']:.1%}) |")
lines += ["", "These are comparisons of existing development labels, including provisional rows in the all-recorded comparison. "
    "Separate completed-row results are in the CSV. Differences can reflect coding thresholds. "
    "All previously coded reports, including Tyler's, and their related bundles are excluded from the unused review sheet. "
    "The extraction queue identifies available human fields so future reading can target unresolved information."]
Path("../output/ulurp_cpc_regex_coverage.md").write_text("\n".join(lines) + "\n")
print(f"Summarized {len(rows):,} narratives; prepared {len(sample)} fresh human reviews.")
