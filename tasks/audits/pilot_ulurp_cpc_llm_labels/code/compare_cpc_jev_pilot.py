#!/usr/bin/env python3
"""Compare saved Jev classifications on complete bundles; preserve split results."""

import csv
import hashlib
import json
import math
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, "../../../_lib")
from data_reports import save_csv

# setwd("/Users/jacobherbstman/Desktop/nyc_court_case/tasks/audits/pilot_ulurp_cpc_llm_labels/code")
with open("../output/cpc_jev_sample_v1.csv", newline="") as source:
    sample_rows = list(csv.DictReader(source))
with open("../input/ulurp_cpc_reconciled_coding.csv", newline="") as source:
    human_rows = list(csv.DictReader(source))
with open("../output/cpc_subagent_labels_v3.csv", newline="") as source:
    sol_rows = [r for r in csv.DictReader(source) if r["primary_reading"] == "True"]
with open("../input/ulurp_cpc_text_labels.csv", newline="") as source:
    regex_rows = list(csv.DictReader(source))
assert len(sample_rows) == len({r["document_id"] for r in sample_rows}) == 30
assert len(human_rows) == len({(r["source_document_id"], r["field"]) for r in human_rows})
assert len(sol_rows) == len({(r["document_id"], r["field"]) for r in sol_rows})
assert len(regex_rows) == len({r["document_id"] for r in regex_rows})
sample = {r["document_id"]: r for r in sample_rows}
human = {(r["source_document_id"], r["field"]): r for r in human_rows}
sol = {(r["document_id"], r["field"]): r for r in sol_rows}
regex = {r["document_id"]: r for r in regex_rows}
codebook = json.loads(Path("cpc_jev_codebook_v1.json").read_text())
fields = list(codebook["binary_fields"]) + list(codebook["choice_fields"])
packets = [json.loads(line) for line in Path("../input/cpc_jev_requests_v1.jsonl").read_text().splitlines()]
assert len(packets) == len({p["request_id"] for p in packets}) == sum(int(r["request_parts"]) for r in sample_rows)
assert Path("../input/cpc_jev_requests_v1.jsonl").read_bytes() == Path("../output/cpc_jev_requests_v1.jsonl").read_bytes()
requests = {p["request_id"]: p for p in packets}
attempts, successful = {}, {}
for line in Path("../input/cpc_jev_responses_v1.jsonl").read_text().splitlines():
    record = json.loads(line)
    assert record["event"] in {"started", "received"}
    key = (record["request_id"], record.get("attempt_number", 1))
    assert record["request_sha256"] == requests[key[0]]["request_sha256"]
    if record["event"] == "started":
        assert key not in attempts
        attempts[key] = dict(record, status="unfinished")
    else:
        assert key in attempts and attempts[key]["status"] == "unfinished"
        attempts[key].update(record, status="successful" if record["valid_response"] else "failed")
        if record["valid_response"]:
            assert record["request_id"] not in successful
            successful[record["request_id"]] = record

labels, usage = [], []
for key, record in attempts.items():
    payload = json.loads(record["raw_response"]) if "raw_response" in record else {}
    tokens = payload.get("usage", {})
    usage.append(dict(request_id=key[0], attempt_number=key[1], status=record["status"],
        http_status=record.get("http_status"), started_at=record["started_at"],
        received_at=record.get("received_at", ""), requested_model=record["requested_model"],
        returned_model=payload.get("model", ""), input_tokens=tokens.get("input_tokens"),
        output_tokens=tokens.get("output_tokens"), request_sha256=record["request_sha256"]))
for request_id, record in successful.items():
    packet = requests[request_id]
    body = packet["body"]
    assert hashlib.sha256(json.dumps(body, sort_keys=True, ensure_ascii=False).encode()).hexdigest() == packet["request_sha256"]
    payload = json.loads(record["raw_response"])
    assert record["http_status"] == 200 and set(payload["answers"]) == set(fields)
    doc = packet["document_id"]
    for field, answer in payload["answers"].items():
        criteria = body["questions"][field]["criteria"]
        assert answer["type"] == "choice" and answer["choice"] in criteria
        assert math.isfinite(answer["confidence"]) and 0 <= answer["confidence"] <= 1
        probabilities = answer["probabilities"]
        assert set(probabilities) == set(criteria)
        assert all(math.isfinite(v) and 0 <= v <= 1 for v in probabilities.values())
        assert abs(sum(probabilities.values()) - 1) < 0.025
        labels.append(dict(request_id=request_id, document_id=doc,
            application_number=sample[doc]["application_number"], part_number=body["state"]["part_number"],
            total_parts=body["state"]["total_parts"], field=field, jev_value=answer["choice"],
            confidence=answer["confidence"], chosen_probability=probabilities[answer["choice"]],
            probabilities_json=json.dumps(probabilities, sort_keys=True), request_sha256=record["request_sha256"]))
whole = {(r["document_id"], r["field"]): r for r in labels if r["total_parts"] == 1}
comparisons = []
for doc, report in sample.items():
    received_parts = sum(p["document_id"] == doc and p["request_id"] in successful for p in packets)
    for field in fields:
        key = (doc, field)
        original, prediction = human.get(key, {}), whole.get(key, {})
        values = {"jacob": original.get("jacob_value", ""), "tyler": original.get("tyler_value", ""),
            "completed_nonconflicting": original.get("human_value", ""),
            "reconciled_working": original.get("reconciled_value", "")}
        complete = {"jacob": original.get("jacob_coding_complete") == "1",
            "tyler": original.get("tyler_coding_complete") == "1",
            "completed_nonconflicting": original.get("human_status") in {"single_human_coder", "human_agreement"},
            "reconciled_working": bool(original.get("reconciled_value"))}
        allowed = {"0", "1"} if field in codebook["binary_fields"] else set(codebook["choice_fields"][field]["criteria"]) - {"unclear"}
        row = dict(document_id=doc, application_number=report["application_number"], field=field,
            expected_parts=int(report["request_parts"]), received_parts=received_parts,
            report_status="split_bundle_requires_joint_review" if int(report["request_parts"]) > 1 else
                "whole_bundle" if prediction else "missing_response",
            jev_value=prediction.get("jev_value", ""), jev_confidence=prediction.get("confidence"),
            sol_value=sol[key]["model_value"], sol_confidence=sol[key]["confidence"],
            regex_value=regex[doc].get(field, regex[doc].get(field + "_detected", "")),
            regex_implemented=field in regex[doc] or field + "_detected" in regex[doc],
            human_status=original.get("human_status", ""),
            reconciliation_provenance=original.get("reconciliation_provenance", ""),
            reconciliation_scope=original.get("reconciliation_issue_scope", ""))
        for reference in values:
            row[reference + "_value"] = values[reference]
            row[reference + "_eligible"] = complete[reference] and values[reference] in allowed
        comparisons.append(row)

agreement = []
for reference in ("jacob", "tyler", "completed_nonconflicting", "reconciled_working"):
    for field in fields:
        matched = [r for r in comparisons if r["field"] == field and r[reference + "_eligible"] and r["report_status"] == "whole_bundle"]
        if not matched:
            continue
        for method in ("jev", "sol", "regex"):
            if method == "regex" and not matched[0]["regex_implemented"]:
                continue
            binary = field in codebook["binary_fields"]
            truth = reference + "_value"
            pred = method + "_value"
            tp = sum(r[truth] == r[pred] == "1" for r in matched)
            positive = sum(r[pred] == "1" for r in matched)
            human_positive = sum(r[truth] == "1" for r in matched)
            exact = sum(r[truth] == r[pred] for r in matched)
            agreement.append(dict(reference=reference, field=field, method=method, compared_reports=len(matched),
                exact=exact, exact_share=exact/len(matched),
                available=sum(r[pred] not in {"", "unclear"} for r in matched),
                reference_positive=human_positive if binary else None, predicted_positive=positive if binary else None,
                true_positive=tp if binary else None,
                false_positive=sum(r[truth] == "0" and r[pred] == "1" for r in matched) if binary else None,
                missed_positive=human_positive-tp if binary else None,
                precision=tp/positive if binary and positive else None,
                recall=tp/human_positive if binary and human_positive else None))

save_csv(labels, list(labels[0]), "../output/cpc_jev_labels_v1.csv", ["request_id", "field"])
save_csv(usage, list(usage[0]), "../output/cpc_jev_usage_v1.csv", ["request_id", "attempt_number"])
save_csv(comparisons, list(comparisons[0]), "../output/cpc_jev_comparison_v1.csv", ["document_id", "field"])
save_csv(agreement, list(agreement[0]), "../output/cpc_jev_agreement_v1.csv", ["reference", "field", "method"])
credits = [json.loads(line) for line in Path("../input/cpc_jev_credits_v1.jsonl").read_text().splitlines()]
start, end = credits[0], credits[-1]
assert end["phase"] == "after"
charge = Decimal(str(end["total_used"])) - Decimal(str(start["total_used"]))
inputs = sum(r["input_tokens"] or 0 for r in usage)
outputs = sum(r["output_tokens"] or 0 for r in usage)
whole_reports = len({r["document_id"] for r in comparisons if r["report_status"] == "whole_bundle"})
lines = ["# Jev trial results", "",
    f"Jev returned {len(successful)} of {len(packets)} requests for the 30-report development sample. "
    f"{whole_reports} reports have complete bundles in a single request; four split bundles remain visible as part-level results. "
    "All comparisons below use the same complete-bundle reports for Jev, Sol, and regex. "
    "Split reports are not dropped from the source sample or coded as negatives.", "",
    f"Saved usage: {inputs:,} input tokens and {outputs:,} output tokens. "
    f"{sum(r['status'] != 'successful' for r in usage)} unsuccessful attempts are preserved. "
    f"The account started at ${start['balance']} available / ${start['total_used']} used and ended at "
    f"${end['balance']} available / ${end['total_used']} used: observed usage increase ${charge}. "
    "This is account-level billing observed at the recorded timestamps, not a per-request invoice. "
    f"At the published non-promotional input rate, successful reported input tokens would cost ${inputs * 0.042 / 1_000_000:.6f}. "
    "The gateway currently advertises free Jev promotional pricing through September 25, 2026.", "",
    "Exact requests, responses, per-option probabilities, and confidence are saved. "
    "The returned model is a gateway alias, not an immutable release identifier. "
    "No original human coding or production labels are changed. This is development agreement, not established accuracy; "
    "the sample is small, human coding is imperfect, and Jev's revised questions differ from the frozen Sol prompt. "
    "The 13 detailed issue/actor fields have no direct original human reference. "
    "The reconciled working reference includes AI-assisted source review and is reported separately. "
    "The trial does not yet obtain verified evidence quotations. "
    "[Vercel promotional pricing](https://vercel.com/ai-gateway/models/jev) was checked on September 19, 2026."]
missing_reports = [r for r in comparisons if r["field"] == fields[0] and r["received_parts"] < r["expected_parts"]]
if missing_reports:
    lines += ["", "Reports with missing responses: " + "; ".join(
        f"{r['application_number']} ({r['received_parts']}/{r['expected_parts']} parts received)" for r in missing_reports) + ". "
        "Availability may select which reports enter the agreement tables; this is a further limit on generalization."]
for reference, title in (("completed_nonconflicting", "Original completed, nonconflicting human coding"),
                         ("reconciled_working", "Reconciled working values, including AI-assisted review")):
    if reference == "reconciled_working":
        lines += ["", "\\newpage"]
    lines += ["", "## " + title, "", "| Field | Reports | Jev matches | Sol matches | Regex matches |", "|---|---:|---:|---:|---:|"]
    for field in fields:
        rows = {r["method"]: r for r in agreement if r["reference"] == reference and r["field"] == field}
        if not rows:
            continue
        lines.append(f"| {field.replace('_', ' ')} | {rows['jev']['compared_reports']} | {rows['jev']['exact']} | {rows['sol']['exact']} | {rows['regex']['exact'] if 'regex' in rows else 'not implemented'} |")
    binary = [r for r in agreement if r["reference"] == reference and r["method"] == "jev" and r["field"] in codebook["binary_fields"]]
    lines += ["", f"Across binary fields, Jev has {sum(r['false_positive'] for r in binary)} additional positives and "
        f"{sum(r['missed_positive'] for r in binary)} missed positives relative to this reference. These are disagreements, not adjudicated errors."]
matched = [r for r in comparisons if r["report_status"] == "whole_bundle" and r["completed_nonconflicting_eligible"]]
high = [r for r in matched if r["jev_confidence"] >= 0.9]
lines += ["", f"At self-reported confidence >= 0.90, {len(high)} of {len(matched)} comparable labels qualify, "
    f"and {sum(r['jev_value'] == r['completed_nonconflicting_value'] for r in high)} agree with the original reference. "
    "This is a descriptive threshold check on the development sample, not a calibrated acceptance rule."]
lines += ["", "\\newpage", "", "## Reference prevalence and model disagreements", "",
    "The table uses reconciled working values. Additional positives and missed positives describe disagreement, not adjudicated errors.", "",
    "| Field | Reports | Reference yes | Extra yes | Missed yes |",
    "|---|---:|---:|---:|---:|"]
for r in agreement:
    if r["reference"] == "reconciled_working" and r["method"] == "jev" and r["field"] in codebook["binary_fields"]:
        lines.append(f"| {r['field'].replace('_', ' ')} | {r['compared_reports']} | {r['reference_positive']} | {r['false_positive']} | {r['missed_positive']} |")
for field in ("councilmember_position", "civic_group_position"):
    records = [r for r in comparisons if r["field"] == field and r["report_status"] == "whole_bundle" and r["reconciled_working_eligible"]]
    counts = {value: sum(r["reconciled_working_value"] == value for r in records) for value in ("none_or_procedural", "support_or_request", "opposition")}
    lines.append(f"\n{field.replace('_', ' ')} reference prevalence: {counts['none_or_procedural']} none/procedural, "
        f"{counts['support_or_request']} support/request, {counts['opposition']} opposition. "
        "High overall agreement can be driven by the absent-position category.")
lines += ["", "\\newpage", "", "## High-confidence labels by field", "",
    "Against reconciled working values, using the same complete bundles. High confidence means at least 0.90; this is not an accuracy guarantee.", "",
    "| Field | Eligible | High confidence | Matches |",
    "|---|---:|---:|---:|"]
for field in fields:
    records = [r for r in comparisons if r["field"] == field and r["report_status"] == "whole_bundle" and r["reconciled_working_eligible"]]
    if records:
        selected = [r for r in records if r["jev_confidence"] >= 0.9]
        lines.append(f"| {field.replace('_', ' ')} | {len(records)} | {len(selected)} | "
            f"{sum(r['jev_value'] == r['reconciled_working_value'] for r in selected)} |")
Path("../output/cpc_jev_findings_v1.md").write_text("\n".join(lines) + "\n")
print(f"Compared {whole_reports} whole bundles; preserved {len(labels)} part/field answers and {len(usage)} attempts; observed spend ${charge}.")
