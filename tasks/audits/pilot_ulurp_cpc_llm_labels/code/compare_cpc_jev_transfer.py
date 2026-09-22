#!/usr/bin/env python3
"""Compare reusable questions with unchanged original and working references."""

import csv
import hashlib
import json
import math
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, "../../../_lib")
from data_reports import save_csv

# Run from tasks/audits/pilot_ulurp_cpc_llm_labels/code.
# threshold = 0.5
threshold = float(sys.argv[1])
experiment = sys.argv[2] if len(sys.argv) == 3 else "v8"
assert experiment in {"v8", "v11"}
assert 0 < threshold < 1
book = json.loads(Path(f"cpc_jev_codebook_{experiment}.json").read_text())
with open(f"../output/cpc_jev_sample_{experiment}.csv", newline="") as f:
    roster = list(csv.DictReader(f))
with open(f"../output/cpc_jev_pages_{experiment}.csv", newline="") as f:
    pages = list(csv.DictReader(f))
with open("../input/ulurp_cpc_reconciled_coding.csv", newline="") as f:
    humans = list(csv.DictReader(f))
assert len(roster) == len({r["document_id"] for r in roster})
assert len(pages) == len({(r["document_id"], r["page_id"]) for r in pages})
assert len(humans) == len({(r["source_document_id"], r["field"]) for r in humans})
page = {(r["document_id"], r["page_id"]): r for r in pages}
human = {(r["source_document_id"], r["field"]): r for r in humans}
prepared = Path(f"../output/cpc_jev_requests_{experiment}.jsonl").read_bytes()
assert prepared == Path(f"../input/cpc_jev_requests_{experiment}.jsonl").read_bytes()
packets = [json.loads(s) for s in prepared.decode().splitlines()]
requests = {p["request_id"]: p for p in packets}
assert len(requests) == len(packets) == sum(r["selected"] == "1" for r in roster)
assert {p["document_id"] for p in packets} == {r["document_id"] for r in roster if r["selected"] == "1"}
for p in packets:
    assert hashlib.sha256(json.dumps(p["body"], sort_keys=True, ensure_ascii=False).encode()).hexdigest() == p["request_sha256"]
    assert all(r["text"] in p["body"]["state"]["report"]["text"] for r in pages if r["document_id"] == p["document_id"])
attempts = {}
for line in Path(f"../input/cpc_jev_responses_{experiment}.jsonl").read_text().splitlines():
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
answers, usage = [], []
for p in packets:
    r = attempts.get(p["request_id"], {})
    try:
        payload = json.loads(r.get("raw_response", "{}"))
    except json.JSONDecodeError:
        assert not r["valid_response"]
        payload = {}
    tokens = payload.get("usage", {})
    usage.append(dict(request_id=p["request_id"], document_id=p["document_id"], status=r.get("status", "not_attempted"),
        http_status=r.get("http_status", ""), input_tokens=tokens.get("input_tokens"), output_tokens=tokens.get("output_tokens"),
        returned_model=payload.get("model", ""), request_sha256=p["request_sha256"]))
    returned = payload.get("answers", {}) if r.get("valid_response") else {}
    if returned:
        assert r["http_status"] == 200 and set(returned) == set(p["body"]["questions"])
    for field, q in p["body"]["questions"].items():
        a = returned.get(field, {})
        if field.startswith("page__"):
            if a:
                assert a["type"] == "choice" and a["choice"] in q["criteria"]
                assert set(a["probabilities"]) == set(q["criteria"])
                assert all(type(v) in (int, float) and math.isfinite(v) and 0 <= v <= 1 for v in a["probabilities"].values())
                assert abs(sum(a["probabilities"].values()) - 1) < .03
            continue
        probability = a.get("noul")
        if a:
            assert a["type"] == "noul" and type(probability) in (int, float) and math.isfinite(probability) and 0 <= probability <= 1
        value = "" if probability is None or probability == threshold else str(int(probability > threshold))
        selected = returned.get("page__" + field, {}).get("choice", "")
        evidence = page.get((p["document_id"], selected), {})
        answers.append(dict(document_id=p["document_id"], application_number=p["body"]["state"]["report"]["application_number"],
            field=field, question=q["instructions"], probability_yes=probability, value=value,
            answer_status="unavailable" if probability is None else "threshold_tie" if value == "" else "answered",
            selected_page_id=selected, source_document_id=evidence.get("source_document_id", ""),
            source_application_number=evidence.get("source_application_number", ""), pdf_page=evidence.get("pdf_page", ""),
            public_pdf_url=evidence.get("public_pdf_url", ""), evidence_text=evidence.get("text", ""),
            source_text_sha256=evidence.get("source_text_sha256", ""),
            evidence_disagrees=int(selected != "" and value in {"0", "1"} and (selected != "none") != (value == "1")),
            request_sha256=p["request_sha256"], threshold=threshold))
credits = [json.loads(s) for s in Path(f"../input/cpc_jev_credits_{experiment}.jsonl").read_text().splitlines()]
assert credits[-1]["phase"] == "after"
charge = Decimal(str(credits[-1]["total_used"])) - Decimal(str(credits[0]["total_used"]))


def union(values):
    return "1" if "1" in values else "0" if all(v == "0" for v in values) else ""


comparison = []
for p in packets:
    doc = p["document_id"]
    values = {r["field"]: r["value"] for r in answers if r["document_id"] == doc}
    candidates = {f: union([values[t + "__concern_request"] for t in topics]) for f, topics in book.get("legacy_topics", {}).items()}
    if experiment == "v8":
        for actor, field in (("council", "councilmember_position"), ("civic", "civic_group_position")):
            opposition = values[actor + "__opposition"]
            positive = union([values[actor + "__support"], values[actor + "__request"]])
            candidates[field] = "opposition" if opposition == "1" else "support_or_request" if opposition == "0" and positive == "1" else "none_or_procedural" if opposition == positive == "0" else ""
    else:
        candidates["council_involvement"] = union(list(values.values()))
        opposition = values["opposition"]
        positive = union([values[f] for f in ("support", "requested_project", "requested_provision", "motivating_concern")])
        candidates["councilmember_position"] = "opposition" if opposition == "1" else "support_or_request" if opposition == "0" and positive == "1" else "none_or_procedural" if opposition == positive == "0" else ""
    for field, value in candidates.items():
        h = human[doc, "councilmember_position" if field == "council_involvement" else field]
        comparison.append(dict(document_id=doc, application_number=h["application_number"], field=field, value=value,
            original_human=h["human_value"] if h["human_status"] in {"human_agreement", "single_human_coder"} else "",
            jacob_value=h["jacob_value"], tyler_value=h["tyler_value"], human_status=h["human_status"],
            working_reference=h["reconciled_value"], reconciliation_provenance=h["reconciliation_provenance"],
            measurement="concern_request_only" if field in book.get("legacy_topics", {}) else "separate_actor_positions" if experiment == "v8" else "broad_involvement_includes_motivating_concern"))
        if field == "council_involvement":
            for reference in ("original_human", "jacob_value", "tyler_value", "working_reference"):
                original = comparison[-1][reference]
                comparison[-1][reference] = "0" if original == "none_or_procedural" else "1" if original in {"support_or_request", "opposition"} else ""
agreement = []
for field in candidates:
    for reference in ("original_human", "jacob_value", "tyler_value", "working_reference"):
        rows = [r for r in comparison if r["field"] == field and r[reference] not in {"", "unclear"}]
        available = [r for r in rows if r["value"] != ""]
        negative = "0" if field in book.get("legacy_topics", {}) or field == "council_involvement" else "none_or_procedural"
        agreement.append(dict(field=field, reference=reference, eligible=len(rows), answered=len(available),
            matches=sum(r["value"] == r[reference] for r in available),
            reference_positive=sum(r[reference] != negative for r in available),
            positive_found=sum(r[reference] != negative and r["value"] != negative for r in available),
            extra_positive=sum(r[reference] == negative and r["value"] != negative for r in available),
            reference_negative=sum(r[reference] == negative for r in available), missing=len(rows)-len(available)))
save_csv(answers, list(answers[0]), f"../output/cpc_jev_answers_{experiment}.csv", ["document_id", "field"])
save_csv(usage, list(usage[0]), f"../output/cpc_jev_usage_{experiment}.csv", ["request_id"])
save_csv(comparison, list(comparison[0]), f"../output/cpc_jev_comparison_{experiment}.csv", ["document_id", "field"])
save_csv(agreement, list(agreement[0]), f"../output/cpc_jev_agreement_{experiment}.csv", ["field", "reference"])
names = dict(affordability_displacement="Affordability / displacement", scale_character_preservation="Character / design", traffic_parking="Traffic / parking", infrastructure_services="Infrastructure", environment_open_space="Environment", councilmember_position="Council", civic_group_position="Civic groups")
names["council_involvement"] = "Any Council involvement"
positions = {"support_or_request": "support/request", "none_or_procedural": "none/procedural", "opposition": "opposition", "0": "0", "1": "1"}
lines = ["# Reusable topic and actor questions on other coded reports" if experiment == "v8" else "# Council questions on 50 other coded reports", "",
    f"{len(packets)} selected bundles; {sum(r['status']=='successful' for r in usage)} successful requests, {sum(r['status']=='failed' for r in usage)} failed and {sum(r['status']=='not_attempted' for r in usage)} unattempted. One attempt per distinct request. Observed account charge ${charge}; balance ${credits[-1]['balance']}. Tokens: {sum(r['input_tokens'] or 0 for r in usage):,} input, {sum(r['output_tokens'] or 0 for r in usage):,} output.", "",
    "Acceptance and timing remain frozen in v7. This test asks 22 binary questions and 14 independent supporting-page questions per bundle. Eight topics distinguish discussion from concern/request; individual Council members and independent civic organizations each have separate support, opposition and request questions. Raw answers and candidate pages are retained separately." if experiment == "v8" else
    "Five binary questions retain the v9/v10 Council wording: endorsement, opposition, project requests, provision requests and motivating concerns. Five independent page selectors supply candidate evidence. Jacob confirmed that concerns prompting rezoning count as a yes for involvement. The broad legacy support/request bin includes that route, while the endorsement signal stays separate. Frozen v9/v10 component expectations are unchanged.", "",
    "Sources overlapping prior pilots or source reviews are excluded from this experiment only; the full coded roster remains in the sample CSV. Selection targets coverage of original labels, not population representativeness. Full bundles within the fixed state limit are supplied without truncation. Original references are imperfect and not transmitted to Jev. Concern-only topic questions are narrower than the older review-issue codes that can include commitments; discrepancies require source inspection, not automatic correction of either label." if experiment == "v8" else
    "The sample prioritizes all eligible original Council-positive cases and fills the remaining slots with seeded original negatives. Prior pilot/source-review overlap, incomplete references and overlength bundles remain recorded in the full coded roster; none are removed from the research universe. Sources are complete within the 90,000-character limit. This tests transfer to other reports, not population prevalence or accuracy. Original human labels are imperfect and never transmitted.", "",
    "| Field | Positive found | Extra positive / reference negatives | Exact match | Missing |", "|---|---:|---:|---:|---:|"]
for r in agreement:
    if r["reference"] == "original_human":
        lines.append(f"| {names[r['field']]} | {r['positive_found']}/{r['reference_positive']} | {r['extra_positive']}/{r['reference_negative']} | {r['matches']}/{r['answered']} | {r['missing']} |")
lines += ["", "For actor fields, positive recovery counts any substantive position; exact match additionally distinguishes opposition from support/request. Separate CSV summaries preserve Jacob, Tyler and working-reference comparisons.", "",
    f"There are {sum(r['answer_status']=='threshold_tie' for r in answers)} exact 0.50 ties and {sum(r['answer_status']=='unavailable' for r in answers)} unavailable binary answers. Neither becomes a negative. {sum(r['evidence_disagrees'] for r in answers)} binary/page selections disagree; page selection does not silently override the classification. A selected page is a candidate, not verified support.", "",
    "## Answered original-reference discrepancies", "", "| Application | Field | Original | Jev |", "|----------------|--------------------------------|---------------------|------------------------|"]
for r in comparison:
    if r["value"] != "" and r["original_human"] != r["value"]:
        lines.append(f"| {r['application_number']} | {names[r['field']]} | {positions[r['original_human']]} | {positions[r['value']]} |")
unavailable = [requests[r["request_id"]]["body"]["state"]["report"]["application_number"] for r in usage if r["status"] != "successful"]
lines += ["", "Unavailable reports: " + (", ".join(unavailable) or "none") + (". All seven mapped fields for each unavailable report remain missing in the comparison CSV." if experiment == "v8" else ". Both mapped fields for each unavailable report remain missing in the comparison CSV."), "", "All prior observations, source relationships, human codes and production labels remain unchanged. Normal Make rebuilds only from archived responses. This is a small development transfer test, not an accuracy estimate for the CPC universe."]
Path(f"../output/cpc_jev_findings_{experiment}.md").write_text("\n".join(lines) + "\n")
print(f"Compared {len(answers)} binary answers and {len(comparison)} original-code fields; observed charge ${charge}.")
