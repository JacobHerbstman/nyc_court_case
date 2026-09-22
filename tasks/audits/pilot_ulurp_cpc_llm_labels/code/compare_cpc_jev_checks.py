#!/usr/bin/env python3
"""Separate supported evidence from rejected candidates and missing checks."""

import csv
import hashlib
import json
import math
import sys
from collections import Counter
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, "../../../_lib")
from data_reports import save_csv

# Run from tasks/audits/pilot_ulurp_cpc_llm_labels/code.
mapping = json.loads(Path("cpc_jev_codebook_v4.json").read_text())["legacy_topics"]
with open("../output/cpc_jev_claims_v5.csv", newline="") as source:
    claims = list(csv.DictReader(source))
with open("../output/cpc_jev_comparison_v4.csv", newline="") as source:
    comparison = list(csv.DictReader(source))
assert len(claims) == len({(r["document_id"], r["topic"], r["stage"]) for r in claims})
assert len(comparison) == len({(r["document_id"], r["field"]) for r in comparison})
prepared = Path("../output/cpc_jev_requests_v5.jsonl").read_bytes()
assert prepared == Path("../input/cpc_jev_requests_v5.jsonl").read_bytes()
packets = [json.loads(s) for s in prepared.decode().splitlines()]
assert len(packets) == len({p["request_id"] for p in packets})
requests = {p["request_id"]: p for p in packets}
for p in packets:
    assert hashlib.sha256(json.dumps(p["body"], sort_keys=True, ensure_ascii=False).encode()).hexdigest() == p["request_sha256"]
attempts = {}
for line in Path("../input/cpc_jev_responses_v5.jsonl").read_text().splitlines():
    r = json.loads(line)
    assert r["request_sha256"] == requests[r["request_id"]]["request_sha256"] and r["attempt_number"] == 1
    assert r["document_id"] == requests[r["request_id"]]["document_id"]
    if r["event"] == "started":
        assert r["request_id"] not in attempts
        attempts[r["request_id"]] = dict(r, status="unfinished")
    else:
        assert r["event"] == "received" and attempts[r["request_id"]]["status"] == "unfinished"
        attempts[r["request_id"]].update(r, status="successful" if r["valid_response"] else "failed")
assert all(r["status"] != "unfinished" for r in attempts.values())
answers, usage = {}, []
for request_id, packet in requests.items():
    r = attempts.get(request_id, {})
    try:
        payload = json.loads(r.get("raw_response", "{}"))
    except json.JSONDecodeError:
        assert not r["valid_response"]
        payload = {}
    tokens = payload.get("usage", {})
    usage.append(dict(request_id=request_id, status=r.get("status", "not_attempted"), http_status=r.get("http_status", ""),
        input_tokens=tokens.get("input_tokens"), output_tokens=tokens.get("output_tokens"), returned_model=payload.get("model", ""),
        started_at=r.get("started_at", ""), received_at=r.get("received_at", ""), request_sha256=packet["request_sha256"]))
    if not r.get("valid_response"):
        continue
    assert r["http_status"] == 200 and set(payload["answers"]) == set(packet["body"]["questions"])
    for field, a in payload["answers"].items():
        options = packet["body"]["questions"][field]["criteria"]
        assert a["type"] == "choice" and a["choice"] in options and set(a["probabilities"]) == set(options)
        assert all(math.isfinite(v) and 0 <= v <= 1 for v in a["probabilities"].values())
        assert abs(sum(a["probabilities"].values()) - 1) < .03 and math.isfinite(a["confidence"]) and 0 <= a["confidence"] <= 1
        answers[request_id, field] = a
credits = [json.loads(s) for s in Path("../input/cpc_jev_credits_v5.jsonl").read_text().splitlines()]
assert credits[-1]["phase"] == "after"
charge = Decimal(str(credits[-1]["total_used"])) - Decimal(str(credits[0]["total_used"]))

checks = []
for r in claims:
    request_id = r["request_id"]
    event = answers.get((request_id, r["event_question_id"]), {})
    scope = answers.get((request_id, r["scope_question_id"]), {})
    scope_choice = scope.get("choice", "unavailable") if r["scope_question_id"] else "focal_source"
    kind = event.get("choice", "unavailable")
    supported = kind in ({"concern_only", "both"} if r["stage"] == "concern_request" else {"adoption_only", "both"})
    # An unavailable or ambiguous check remains missing, never a negative label.
    if "unavailable" in (kind, scope_choice):
        status = "unavailable"
    elif "unclear" in (kind, scope_choice):
        status = "unclear"
    elif scope_choice == "different_project":
        status = "rejected_source"
    elif not supported:
        status = "rejected_event"
    else:
        status = "confirmed"
    checks.append(dict(r, event_kind=kind, event_confidence=event.get("confidence", ""), scope_choice=scope_choice,
        scope_confidence=scope.get("confidence", ""), check_status=status,
        check_request_sha256=requests[request_id]["request_sha256"]))

for r in comparison:
    candidates = [c for c in checks if c["document_id"] == r["document_id"] and c["topic"] in mapping[r["field"]]]
    assert bool(candidates) == (r["review_issue"] == "1")
    counts = Counter(c["check_status"] for c in candidates)
    status = "v4_unavailable" if r["review_issue"] == "" else "no_v4_candidate" if not candidates else "confirmed" if counts["confirmed"] else "unavailable" if counts["unavailable"] else "unclear" if counts["unclear"] else "candidate_rejected"
    r.update(v5_status=status, confirmed_claims=counts["confirmed"], rejected_claims=counts["rejected_source"] + counts["rejected_event"],
        unclear_claims=counts["unclear"], unavailable_claims=counts["unavailable"],
        confirmed_concern_request=int(any(c["check_status"] == "confirmed" and c["stage"] == "concern_request" for c in candidates)),
        confirmed_adopted=int(any(c["check_status"] == "confirmed" and c["stage"] == "adopted" for c in candidates)),
        checked_review_issue="1" if status == "confirmed" else "0" if status == "no_v4_candidate" else "")

agreement = []
for field in mapping:
    for reference in ("original_human", "working"):
        rows = [r for r in comparison if r["field"] == field and r[reference + "_eligible"] == "True"]
        for value in ("1", "0"):
            eligible = [r for r in rows if r[reference + "_value"] == value]
            answered = [r for r in eligible if r["review_issue"] in {"0", "1"}]
            counts = Counter(r["v5_status"] for r in answered)
            agreement.append(dict(field=field, reference=reference, reference_value=value, eligible=len(eligible),
                v4_answered=len(answered), v4_missing=len(eligible) - len(answered),
                v4_positive=sum(r["review_issue"] == "1" for r in answered), confirmed=counts["confirmed"],
                candidate_rejected=counts["candidate_rejected"], unclear=counts["unclear"], unavailable=counts["unavailable"],
                no_v4_candidate=counts["no_v4_candidate"]))
save_csv(checks, list(checks[0]), "../output/cpc_jev_checks_v5.csv", ["document_id", "topic", "stage"])
save_csv(usage, list(usage[0]), "../output/cpc_jev_usage_v5.csv", ["request_id"])
save_csv(comparison, list(comparison[0]), "../output/cpc_jev_comparison_v5.csv", ["document_id", "field"])
save_csv(agreement, list(agreement[0]), "../output/cpc_jev_agreement_v5.csv", ["field", "reference", "reference_value"])

counts = Counter(c["check_status"] for c in checks)
lines = ["# Topic and project checks", "",
    f"This development test checks all {len(claims)} positive v4 concern/adoption claims in {len(packets)} reports. Selected passages include their immediate neighbors. Separate questions classify the topic/event and compare companion introductions. Different application or ZAP identifiers alone do not reject a valid relationship. This is a selected-candidate check, not independent validation or a new whole-report reading.", "",
    f"The run made {len(attempts)} distinct attempts: {sum(r['status'] == 'successful' for r in usage)} successful, {sum(r['status'] == 'failed' for r in usage)} failed, and {sum(r['status'] == 'not_attempted' for r in usage)} unattempted. No identical calls or retries. Observed account usage increased by ${charge}; final balance ${credits[-1]['balance']}. Returned usage totals {sum(r['input_tokens'] or 0 for r in usage):,} input and {sum(r['output_tokens'] or 0 for r in usage):,} output tokens. The runner waits ten seconds between requests and stops on the first rate-limit response or observed charge.", "",
    "## Candidate evidence", "", "| Check result | Claims |", "|---|---:|"]
for status in ("confirmed", "rejected_event", "rejected_source", "unclear", "unavailable"):
    lines.append(f"| {status.replace('_', ' ')} | {counts[status]} |")
lines += ["", "Rejected, unclear and unavailable candidates remain unresolved: another passage may establish the topic. Prior negatives remain unverified no-candidate results. Confirmed-stage indicators measure passing evidence, not complete negative labels. All v4 rows, raw stages and original/working references remain preserved; no production labels or corpus restrictions change.", "",
    "## Retention against original completed codes", "",
    "Denominators use the same v4-answered reports with completed, nonconflicting original codes. Extra means a positive against an original zero, not established error. Lost means rejected evidence for an original positive, not a settled negative. The separate AI-assisted working comparison is in the CSV.", "",
    "| Topic | Original + | v4 found | Checked | Extra v4/check | Lost + | Unclear/missing + |", "|---|---:|---:|---:|---:|---:|---:|"]
names = {"affordability_displacement": "Affordability", "traffic_parking": "Traffic", "infrastructure_services": "Infrastructure", "environment_open_space": "Environment", "scale_character_preservation": "Character / scale"}
for field in mapping:
    a = {r["reference_value"]: r for r in agreement if r["field"] == field and r["reference"] == "original_human"}
    yes, no = a["1"], a["0"]
    lines.append(f"| {names[field]} | {yes['v4_answered']} | {yes['v4_positive']} | {yes['confirmed']} | {no['v4_positive']} / {no['confirmed']} | {yes['candidate_rejected']} | {yes['unclear']} / {yes['unavailable']} |")
lines += ["", "## Companion source judgments", "", "| Focal application | Source application | Model scope judgment |", "|---|---|---|"]
seen = set()
for r in checks:
    key = (r["document_id"], r["source_document_id"])
    if r["scope_question_id"] and key not in seen:
        seen.add(key)
        lines.append(f"| {r['application_number']} | {r['source_application_number']} | {r['scope_choice'].replace('_', ' ')} |")
lines += ["", "Prompts were frozen before calls and contain no human answers or private notes. This is unblinded development, not a holdout; model scores are not calibrated accuracy. Saved requests/responses rebuild through Make without calls. This check cannot recover missed passages or validate all source links and Council/civic fields."]
Path("../output/cpc_jev_findings_v5.md").write_text("\n".join(lines) + "\n")
print(f"Checked {len(claims)} claims: {dict(counts)}; observed charge ${charge}.")
