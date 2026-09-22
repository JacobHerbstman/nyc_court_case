#!/usr/bin/env python3
"""Check frozen Codex readings and compare them with unchanged human judgments."""

import csv
import hashlib
import json
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, "../../../_lib")
from data_reports import save_csv

# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/pilot_ulurp_cpc_llm_labels/code")
with open("../output/cpc_subagent_sample_v3.csv", newline="") as source:
    sample_rows = list(csv.DictReader(source))
with open("../input/ulurp_cpc_human_coding.csv", newline="") as source:
    human_rows = list(csv.DictReader(source))
with open("../input/ulurp_cpc_text_labels.csv", newline="") as source:
    regex_rows = list(csv.DictReader(source))
assert len(sample_rows) == len({r["document_id"] for r in sample_rows})
assert len(regex_rows) == len({r["document_id"] for r in regex_rows})
assert len(human_rows) == len({(r["source_document_id"], r["field"]) for r in human_rows})
sample = {r["document_id"]: r for r in sample_rows}
regex = {r["document_id"]: r for r in regex_rows}
human = {(r["source_document_id"], r["field"]): r for r in human_rows}
schema = json.loads(Path("cpc_subagent_schema_v3.json").read_text())
fields = schema["properties"]["labels"]["properties"]
count_fields = {field for field, spec in fields.items() if "anyOf" in spec}
binary_fields = {field for field, spec in fields.items() if spec.get("enum") == ["0", "1", "unclear"]}
label_rows, evidence_rows, comparisons = [], [], []
all_responses = {}

for reader in "abc":
    packets = [json.loads(line) for line in Path(f"../output/cpc_subagent_reader_{reader}_v3.jsonl").read_text().splitlines()]
    responses = [json.loads(line) for line in Path(f"../input/cpc_subagent_reader_{reader}_v3.jsonl").read_text().splitlines()]
    assert len(responses) == len({r["response"]["document_id"] for r in responses})
    assert {r["response"]["document_id"] for r in responses} == {p["request"]["custom_id"] for p in packets}
    packets = {p["request"]["custom_id"]: p for p in packets}
    for raw in responses:
        response = raw["response"]
        doc = response["document_id"]
        assert raw["reader"] == reader
        packet = packets[doc]
        assert hashlib.sha256(json.dumps(packet["request"], sort_keys=True, ensure_ascii=False).encode()).hexdigest() == packet["request_sha256"]
        assert raw["request_sha256"] == packet["request_sha256"] == sample[doc]["request_sha256"]
        assert set(response) == set(schema["required"])
        assert isinstance(response["summary"], str) and isinstance(response["evidence"], list)
        assert set(response["labels"]) == set(response["confidence"]) == set(fields)
        sources = json.loads(packet["request"]["body"]["input"][1]["content"])["sources"]
        assert len(response["read_source_document_ids"]) == len(sources)
        assert set(response["read_source_document_ids"]) == {s["source_document_id"] for s in sources}
        pages = {(s["source_document_id"], p["pdf_page"]): p["text"] for s in sources for p in s["pages"]}
        supported_fields = set()
        for index, evidence in enumerate(response["evidence"], 1):
            assert set(evidence) == set(schema["properties"]["evidence"]["items"]["required"])
            assert isinstance(evidence["fields"], list)
            assert type(evidence["pdf_page"]) is int and evidence["pdf_page"] >= 1
            assert all(isinstance(evidence[key], str) for key in
                       ("quote", "source_document_id", "actor", "stance", "explanation"))
            assert evidence["fields"] and set(evidence["fields"]) <= set(fields)
            assert evidence["stance"] in schema["properties"]["evidence"]["items"]["properties"]["stance"]["enum"]
            page = pages.get((evidence["source_document_id"], evidence["pdf_page"]), "")
            quote = " ".join(evidence["quote"].split())
            found = bool(quote and quote in page)
            other_locations = [f"{source_id}:{page_number}" for (source_id, page_number), text in pages.items()
                               if quote and quote in text] if not found else []
            quote_status = ("cited_page_exact" if found else "cited_page_case_only"
                            if quote and quote.lower() in page.lower() else
                            "different_supplied_page_exact" if other_locations else "unmatched")
            if found:
                supported_fields.update(evidence["fields"])
            evidence_rows.append(dict(reader=reader, document_id=doc, evidence_number=index,
                fields="; ".join(evidence["fields"]), source_document_id=evidence["source_document_id"],
                pdf_page=evidence["pdf_page"], quote=evidence["quote"], actor=evidence["actor"],
                stance=evidence["stance"], explanation=evidence["explanation"], quote_found_on_cited_page=found,
                quote_match_status=quote_status, other_exact_locations="; ".join(other_locations)))
        for field, spec in fields.items():
            value = response["labels"][field]
            if field in count_fields:
                assert value is None or type(value) is int and value >= 0
            else:
                assert value in spec["enum"]
            assert response["confidence"][field] in {"high", "medium", "low"}
            evidence_required = (field in count_fields and value is not None or
                field in binary_fields and value == "1" or
                field not in count_fields | binary_fields and value not in {"none", "none_or_procedural", "unclear"})
            label_rows.append(dict(reader=reader, document_id=doc, application_number=sample[doc]["application_number"],
                primary_reading=reader == sample[doc]["primary_reader"], field=field, model_value=value,
                confidence=response["confidence"][field], evidence_required=evidence_required,
                has_verified_quote=field in supported_fields, request_sha256=raw["request_sha256"],
                summary=response["summary"]))
        all_responses[(reader, doc)] = response

for row in sample_rows:
    doc = row["document_id"]
    response = all_responses[(row["primary_reader"], doc)]
    for field in fields:
        source = human.get((doc, field), {})
        model_value = response["labels"][field]
        model_value = "" if model_value is None else str(model_value)
        regex_value = regex[doc].get(field, regex[doc].get(field + "_detected", ""))
        regex_implemented = field in regex[doc] or field + "_detected" in regex[doc]
        for coder in ("jacob", "tyler", "completed_nonconflicting"):
            if coder == "completed_nonconflicting":
                human_value = source.get("human_value", "")
                complete = source.get("human_status") in {"single_human_coder", "human_agreement"}
            else:
                human_value = source.get(coder + "_value", "")
                complete = source.get(coder + "_coding_complete") == "1"
            valid = (human_value.isdigit() if field in count_fields else
                     human_value in fields[field].get("enum", []) and human_value != "unclear")
            if not human_value:
                continue
            comparisons.append(dict(document_id=doc, application_number=row["application_number"],
                field=field, reference=coder, human_value=human_value, completed_reference=complete,
                valid_reference=valid, human_status=source.get("human_status", ""),
                model_value=model_value, regex_value=regex_value,
                model_agreement=model_value == human_value,
                regex_agreement=regex_value == human_value if regex_implemented else None,
                regex_field_implemented=regex_implemented,
                regex_count_resolved=(regex[doc]["cb_status" if field.startswith("cb_") else "cpc_speakers_status"] == "resolved") if field in count_fields else "",
                confidence=response["confidence"][field]))

agreement = []
for reference in ("jacob", "tyler", "completed_nonconflicting"):
    for field in fields:
        matched = [r for r in comparisons if r["reference"] == reference and r["field"] == field
                   and r["completed_reference"] and r["valid_reference"]]
        if not matched:
            continue
        record = dict(reference=reference, field=field, compared_reports=len(matched))
        for method in ("model", "regex"):
            if method == "regex" and not matched[0]["regex_field_implemented"]:
                for metric in ("exact", "exact_share", "available", "precision", "recall"):
                    record[method + "_" + metric] = None
                continue
            exact = sum(r[method + "_agreement"] for r in matched)
            record[method + "_exact"] = exact
            record[method + "_exact_share"] = exact / len(matched)
            record[method + "_available"] = sum(r[method + "_value"] not in {"", "unclear"} for r in matched)
            tp = sum(r["human_value"] == r[method + "_value"] == "1" for r in matched)
            positive = sum(r[method + "_value"] == "1" for r in matched)
            human_positive = sum(r["human_value"] == "1" for r in matched)
            record[method + "_precision"] = tp / positive if field in binary_fields and positive else None
            record[method + "_recall"] = tp / human_positive if field in binary_fields and human_positive else None
        agreement.append(record)

repeat_rows = []
for (reader, doc), response in all_responses.items():
    primary_reader = sample[doc]["primary_reader"]
    if reader == primary_reader:
        continue
    primary = all_responses[(primary_reader, doc)]
    for field in fields:
        repeat_rows.append(dict(document_id=doc, field=field, primary_reader=primary_reader,
            repeat_reader=reader, primary_value=primary["labels"][field], repeat_value=response["labels"][field],
            agreement=primary["labels"][field] == response["labels"][field]))

save_csv(label_rows, list(label_rows[0]), "../output/cpc_subagent_labels_v3.csv", ["reader", "document_id", "field"])
save_csv(evidence_rows, list(evidence_rows[0]), "../output/cpc_subagent_evidence_v3.csv", ["reader", "document_id", "evidence_number"])
save_csv(comparisons, list(comparisons[0]), "../output/cpc_subagent_comparison_v3.csv", ["document_id", "field", "reference"])
save_csv(agreement, list(agreement[0]), "../output/cpc_subagent_agreement_v3.csv", ["reference", "field"])
save_csv(repeat_rows, list(repeat_rows[0]), "../output/cpc_subagent_repeat_v3.csv", ["document_id", "field"])

invalid_quotes = sum(not r["quote_found_on_cited_page"] for r in evidence_rows)
missing_support = sum(r["evidence_required"] and not r["has_verified_quote"] for r in label_rows)
lines = ["# CPC Sol subagent pilot", "",
    f"Three Sol-medium readers completed {len(all_responses)} readings of {len(sample)} additional human-coded reports. "
    "Thirty primary reports balance Jacob-only, Tyler-only, and jointly completed coding across available decades. "
    "The older 20-report API pilot and overlapping source bundles were excluded. Three reports received an independent repeat reading. "
    "This is a development comparison, not a population accuracy estimate or an exact API replication.", "",
    "Readers received the fixed v3 prompt, schema, and full text of the focal and included companion reports, without human labels or regex predictions. "
    "The saved source-page quotations, submitted responses, exact dispatch messages, and request fingerprints preserve the observed run. "
    "No API calls were made. Subagent token usage was not exposed by the collaboration tool and is not estimated as an API charge.", "",
    "All three readers corrected saved citations during their own final checks; a and b also corrected request hashes. No human comparison feedback was supplied. "
    "They report unchanged labels and confidence, but original drafts were not retained. The saved readings are therefore self-checked submissions, "
    "not untouched first responses. Source notes record these limits; quotation checks below refer to submitted evidence.", "",
    f"Quote checks locate {len(evidence_rows) - invalid_quotes} of {len(evidence_rows)} quotations on their cited source pages. "
    f"There are {missing_support} labels requiring evidence without a verified quotation. "
    "A matching quotation establishes source traceability, not whether the label follows from it. "
    "The read-source lists cover every supplied source, but are self-reported reading declarations.", "",
    "The table uses completed, nonconflicting human values, one reference per report/field. "
    "Original Jacob and Tyler comparisons remain separate in the CSV; neither reader is silently selected to settle a disagreement. "
    "The regex comparison uses the current narrative-based rules; the subagents received full reports, so this comparison also changes available context. "
    "For counts, an unavailable prediction is a non-match; agreement here combines coverage and correctness. Availability is saved separately in the CSV. "
    "The 13 added detailed labels have no direct human reference and are not included in these accuracy comparisons.", "",
    "| Field | Reports | Sol exact | Regex exact |", "|---|---:|---:|---:|"]
for row in agreement:
    if row["reference"] == "completed_nonconflicting":
        regex_exact = row["regex_exact"] if row["regex_exact"] is not None else "not implemented"
        lines.append(f"| {row['field'].replace('_', ' ')} | {row['compared_reports']} | {row['model_exact']} | {regex_exact} |")
lines += ["", f"The independent repeats agree on {sum(r['agreement'] for r in repeat_rows)} of {len(repeat_rows)} field labels "
    f"across {len({r['document_id'] for r in repeat_rows})} reports. "
    "This small repeat check is descriptive; common zero labels and shared model errors can produce high agreement. "
    "Raw judgments are retained even when evidence fails or readers disagree. Human coding and the production dataset are unchanged."]
lines += ["", "## Character and revision disagreements", "",
    "The first table separates additional Sol positives from missed human positives. "
    "It uses the same completed, nonconflicting references as the main pilot table; a disagreement is not an adjudicated error.", "",
    "| Field | Reports | Both yes | Both no | Human no, Sol yes | Human yes, Sol no |",
    "|---|---:|---:|---:|---:|---:|"]
for field in ("scale_character_preservation", "revision_or_concession"):
    matched = [r for r in comparisons if r["reference"] == "completed_nonconflicting"
               and r["field"] == field and r["completed_reference"] and r["valid_reference"]]
    counts = [sum(r["human_value"] == h and r["model_value"] == m for r in matched)
              for h, m in (("1", "1"), ("0", "0"), ("0", "1"), ("1", "0"))]
    unresolved = sum(r["model_value"] not in {"0", "1"} for r in matched)
    assert sum(counts) + unresolved == len(matched)
    lines.append(f"| {field.replace('_', ' ')} | {len(matched)} | " + " | ".join(map(str, counts)) + " |")
    if unresolved:
        lines.append(f"\n{field}: {unresolved} model readings are unresolved and outside the four binary cells.\n")
lines += ["", "The second table uses all reports with valid, completed coding by both researchers in the existing human table, "
    "not just the 30-report pilot. It measures disagreement between the original human labels, without choosing a winner.", "",
    "| Field | Both completed | Agree | Jacob yes, Tyler no | Jacob no, Tyler yes |",
    "|---|---:|---:|---:|---:|"]
for field in ("scale_character_preservation", "revision_or_concession"):
    paired = [r for r in human_rows if r["field"] == field
              and r["jacob_coding_complete"] == r["tyler_coding_complete"] == "1"
              and r["jacob_value"] in {"0", "1"} and r["tyler_value"] in {"0", "1"}]
    same = sum(r["jacob_value"] == r["tyler_value"] for r in paired)
    jacob_only = sum(r["jacob_value"] == "1" and r["tyler_value"] == "0" for r in paired)
    tyler_only = sum(r["jacob_value"] == "0" and r["tyler_value"] == "1" for r in paired)
    lines.append(f"| {field.replace('_', ' ')} | {len(paired)} | {same} | {jacob_only} | {tyler_only} |")
Path("../output/cpc_subagent_findings_v3.md").write_text("\n".join(lines) + "\n")
print(f"Compared {len(sample)} reports; {invalid_quotes} invalid quotations; {missing_support} unsupported required labels.")
