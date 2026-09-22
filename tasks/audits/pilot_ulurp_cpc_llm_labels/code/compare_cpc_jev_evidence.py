#!/usr/bin/env python3
"""Compare passage-supported codes with preserved human and earlier model codes."""

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
topic_details = {"affordability_displacement": ["affordability", "displacement"],
    "scale_character_preservation": ["neighborhood_character", "scale_density_design", "historic_preservation"]}
codebook = json.loads(Path("cpc_jev_codebook_v3.json").read_text())
with open("../output/cpc_jev_sample_v3.csv", newline="") as source:
    sample = list(csv.DictReader(source))
with open("../output/cpc_jev_passages_v3.csv", newline="") as source:
    passages = list(csv.DictReader(source))
with open("../input/ulurp_cpc_reconciled_coding.csv", newline="") as source:
    humans = list(csv.DictReader(source))
with open("../input/ulurp_cpc_text_labels.csv", newline="") as source:
    regex = list(csv.DictReader(source))
with open("../output/cpc_jev_comparison_v1.csv", newline="") as source:
    old = list(csv.DictReader(source))
with open("../output/cpc_jev_labels_v2.csv", newline="") as source:
    repeats = list(csv.DictReader(source))
assert len(sample) == len({r["document_id"] for r in sample})
assert len(passages) == len({(r["document_id"], r["passage_id"]) for r in passages})
assert len(humans) == len({(r["source_document_id"], r["field"]) for r in humans})
assert len(regex) == len({r["document_id"] for r in regex})
assert len(old) == len({(r["document_id"], r["field"]) for r in old})
passage = {(r["document_id"], r["passage_id"]): r for r in passages}
human = {(r["source_document_id"], r["field"]): r for r in humans}
regex = {r["document_id"]: r for r in regex}
previous = {(r["document_id"], r["field"]): (r["jev_value"], "v1") for r in old if r["report_status"] == "whole_bundle"}
for row in repeats:
    if row["repeat"] == "1" and row["question"].startswith("original_"):
        previous[row["document_id"], row["question"].removeprefix("original_")] = (row["value"], "v2_first_original")

answers, usage, charges, request_counts = {}, [], {}, {}
for stage in ("retrieve", "verify"):
    prepared = Path(f"../output/cpc_jev_requests_v3_{stage}.jsonl").read_bytes()
    assert prepared == Path(f"../input/cpc_jev_requests_v3_{stage}.jsonl").read_bytes()
    packets = [json.loads(s) for s in prepared.decode().splitlines()]
    assert len(packets) == len({p["request_id"] for p in packets})
    requests = {p["request_id"]: p for p in packets}
    request_counts[stage] = len(packets)
    for p in packets:
        assert hashlib.sha256(json.dumps(p["body"], sort_keys=True, ensure_ascii=False).encode()).hexdigest() == p["request_sha256"]
    attempts = {}
    for line in Path(f"../input/cpc_jev_responses_v3_{stage}.jsonl").read_text().splitlines():
        r = json.loads(line)
        assert r["request_sha256"] == requests[r["request_id"]]["request_sha256"] and r["attempt_number"] == 1
        if r["event"] == "started":
            assert r["request_id"] not in attempts, "Repeated calls are not part of this experiment."
            attempts[r["request_id"]] = dict(r, status="unfinished")
        else:
            assert r["event"] == "received" and attempts[r["request_id"]]["status"] == "unfinished"
            attempts[r["request_id"]].update(r, status="successful" if r["valid_response"] else "failed")
    assert all(r["status"] != "unfinished" for r in attempts.values())
    for request_id, r in attempts.items():
        try:
            payload = json.loads(r["raw_response"])
        except json.JSONDecodeError:
            assert not r["valid_response"]
            payload = {}
        tokens = payload.get("usage", {})
        usage.append(dict(stage=stage, request_id=request_id, status=r["status"], http_status=r["http_status"],
            input_tokens=tokens.get("input_tokens"), output_tokens=tokens.get("output_tokens"),
            started_at=r["started_at"], received_at=r["received_at"], returned_model=payload.get("model", ""),
            request_sha256=r["request_sha256"]))
        if not r["valid_response"]:
            continue
        assert r["http_status"] == 200 and set(payload["answers"]) == set(requests[request_id]["body"]["questions"])
        for field, a in payload["answers"].items():
            options = requests[request_id]["body"]["questions"][field]["criteria"]
            assert a["type"] == "choice" and a["choice"] in options and set(a["probabilities"]) == set(options)
            assert all(math.isfinite(v) and 0 <= v <= 1 for v in a["probabilities"].values())
            assert abs(sum(a["probabilities"].values()) - 1) < .03 and 0 <= a["confidence"] <= 1
            answers[stage, r["document_id"], field] = dict(a, request_sha256=r["request_sha256"])
    credits = [json.loads(s) for s in Path(f"../input/cpc_jev_credits_v3_{stage}.jsonl").read_text().splitlines()]
    assert credits[-1]["phase"] == "after"
    charges[stage] = Decimal(str(credits[-1]["total_used"])) - Decimal(str(credits[0]["total_used"]))

evidence, comparisons = [], []
for row in sample:
    doc = row["document_id"]
    values, candidates = {}, {}
    for field in codebook["questions"]:
        a = answers.get(("retrieve", doc, field), {})
        v = answers.get(("verify", doc, field), {})
        choice = a.get("choice", "")
        selected = passage.get((doc, choice), {})
        if choice not in {"", "none"}:
            assert selected and hashlib.sha256(selected["passage_text"].encode()).hexdigest() == selected["passage_sha256"]
        position = field in codebook["position_fields"]
        if not a:
            value, status = "", "retrieval_unavailable"
        elif choice == "none":
            assert not v
            value, status = ("none_or_procedural" if position else "0"), "no_passage_selected"
        elif not v:
            value, status = "", "verification_unavailable"
        else:
            value = v["choice"] if position else {"supported": "1", "not_supported": "0", "unclear": "unclear"}[v["choice"]]
            status = "unclear" if value == "unclear" else "supported" if value in {"1", "opposition", "support_or_request"} else "candidate_not_supported"
        values[field] = value
        candidates[field] = "" if position or not a else "0" if choice == "none" else "1"
        evidence.append(dict(document_id=doc, application_number=row["application_number"], field=field,
            value=value, status=status, selected_passage_id=choice, verification_choice=v.get("choice", ""),
            source_document_id=selected.get("source_document_id", ""), source_application_number=selected.get("source_application_number", ""),
            source_role=selected.get("source_role", ""), pdf_page=selected.get("pdf_page", ""),
            page_start=selected.get("page_start", ""), page_end=selected.get("page_end", ""),
            passage_text=selected.get("passage_text", ""), passage_sha256=selected.get("passage_sha256", ""),
            source_text_sha256=selected.get("source_text_sha256", ""), retrieval_confidence=a.get("confidence", ""),
            verification_confidence=v.get("confidence", ""), retrieval_request_sha256=a.get("request_sha256", ""),
            verification_request_sha256=v.get("request_sha256", "")))
    for mapped in (values, candidates):
        components = [mapped["changed_proposal"], mapped["adopted_commitment"]]
        mapped["revision_or_concession"] = "1" if "1" in components else "0" if components == ["0", "0"] else "" if "" in components else "unclear"
    for field, value in values.items():
        h = human.get((doc, field), {})
        allowed = {"support_or_request", "opposition", "none_or_procedural"} if field in codebook["position_fields"] else {"0", "1"}
        previous_value, previous_version = previous.get((doc, field), ("", ""))
        details = topic_details.get(field, [])
        detail_positive = [f for f in details if values[f] == "1"]
        consistency = ""
        if details:
            consistency = "broad_negative_detail_positive" if value == "0" and detail_positive else "broad_positive_details_negative" if value == "1" and all(values[f] == "0" for f in details) else "consistent" if value in allowed and all(values[f] in allowed for f in details) else "unresolved"
        comparisons.append(dict(document_id=doc, application_number=row["application_number"], field=field,
            value=value, candidate_value=candidates[field], topic_detail_consistency=consistency, detail_positive_fields=";".join(detail_positive), previous_jev_value=previous_value, previous_version=previous_version,
            regex_value=regex[doc].get(field, ""), original_human_value=h.get("human_value", ""),
            original_human_eligible=h.get("human_status") in {"human_agreement", "single_human_coder"} and h.get("human_value") in allowed,
            working_value=h.get("reconciled_value", ""), working_eligible=h.get("reconciled_value") in allowed,
            jacob_value=h.get("jacob_value", ""), jacob_complete=h.get("jacob_coding_complete", ""),
            tyler_value=h.get("tyler_value", ""), tyler_complete=h.get("tyler_coding_complete", ""),
            human_status=h.get("human_status", ""), reconciliation_provenance=h.get("reconciliation_provenance", "")))

agreement = []
for field in values:
    for reference in ("original_human", "working"):
        rows = [r for r in comparisons if r["field"] == field and r[reference + "_eligible"]]
        allowed = {"support_or_request", "opposition", "none_or_procedural"} if field in codebook["position_fields"] else {"0", "1"}
        positive = {"support_or_request", "opposition"} if field in codebook["position_fields"] else {"1"}
        for method, column in (("evidence_verified", "value"), ("candidate_only", "candidate_value"), ("previous_jev", "previous_jev_value"), ("regex", "regex_value")):
            if not rows or (method == "candidate_only" and field in codebook["position_fields"]):
                continue
            truth = reference + "_value"
            common = [r for r in rows if r["value"] in allowed and r["previous_jev_value"] in allowed]
            agreement.append(dict(field=field, reference=reference, method=method, reports=len(rows),
                available=sum(r[column] in allowed for r in rows), matches=sum(r[column] == r[truth] for r in rows),
                reference_positive=sum(r[truth] in positive for r in rows),
                answered_reference_positive=sum(r[truth] in positive and r[column] in allowed for r in rows),
                found_positive=sum(r[column] in positive and r[truth] in positive for r in rows),
                extra_positive=sum(r[column] in positive and r[truth] not in positive for r in rows),
                missed_positive=sum(r[column] in allowed and r[column] not in positive and r[truth] in positive for r in rows),
                unresolved_positive=sum(r[column] not in allowed and r[truth] in positive for r in rows),
                paired_reports=len(common), paired_matches=sum(r[column] == r[truth] for r in common)))

save_csv(evidence, list(evidence[0]), "../output/cpc_jev_evidence_v3.csv", ["document_id", "field"])
save_csv(usage, list(usage[0]), "../output/cpc_jev_usage_v3.csv", ["request_id"])
save_csv(comparisons, list(comparisons[0]), "../output/cpc_jev_comparison_v3.csv", ["document_id", "field"])
save_csv(agreement, list(agreement[0]), "../output/cpc_jev_agreement_v3.csv", ["field", "reference", "method"])

display_names = {"explicit_local_response": "Explicit local response", "local_request_condition": "Local request",
    "scale_character_preservation": "Character / scale / preservation", "affordability_displacement": "Affordability / displacement",
    "traffic_parking": "Traffic / parking", "infrastructure_services": "Infrastructure / services", "environment_open_space": "Environment / open space",
    "substantial_local_opposition": "Local opposition", "cb_request_or_opposition": "CB request / opposition",
    "bp_request_or_opposition": "BP request / opposition", "procedural_response": "Procedural response",
    "councilmember_position": "Council member position", "civic_group_position": "Civic group position", "revision_or_concession": "Revision / commitment"}
lines = ["# Jev passage-based topic pilot", "",
    f"The test retains {len(sample)} previously coded reports. Twelve purposive revision/character cases and eight additional original topic/actor positives were selected from the previous 46 whole bundles. This is a development sample, not a random validation sample. Original Jacob and Tyler labels remain unchanged; the AI-assisted working reference is separate.", "",
    f"There were {len(usage)} API attempts: {sum(r['status'] == 'successful' for r in usage)} successful, {sum(r['status'] == 'failed' for r in usage)} failed. No identical request was repeated. The observed account charge increased by ${sum(charges.values())}; the latest balance was ${credits[-1]['balance']}. Returned usage totals {sum(r['input_tokens'] or 0 for r in usage):,} input and {sum(r['output_tokens'] or 0 for r in usage):,} output tokens. These are gateway-reported tokens and account balance observations, not a separate invoice for each call.", "",
    "Each report was read once to select a passage for each of 20 concrete questions. A distinct second request checked selected passages with neighboring context. All source-page text was retained in nonoverlapping blocks; quotations have document IDs, PDF pages, text offsets, and hashes. A check by the same model is correlated evidence, not independent validation. Missing calls and unclear answers remain explicit. No selected passage, or a rejected candidate, means evidence was not established by this procedure; it does not prove the concern is absent elsewhere.", "",
    "The original combined topic names are retained. Affordability and displacement, and neighborhood character, physical scale/design, and preservation, are also separate exploratory fields without separate human benchmarks. Broad revision/concession is an explicit proposal revision OR an undertaken/imposed substantive commitment, including pre-review commitments. Local requests, procedural responses, and explicit responses to local requests remain distinct. Council/civic stance is classified only after selecting actor evidence. These are passage-level candidates, not a complete inventory of every actor/request in a report.", "",
    "## Comparison with original coding", "",
    "Only completed, nonconflicting original human fields are eligible here. Matches are matching labels / categorical answers. 'Found' is the number of original positives recovered among categorical answers; missing/unclear outputs are excluded from that denominator and shown separately; 'extra' means the model is positive where the original code is negative. Disagreements are not adjudicated errors. Actor positives combine support/request and opposition, while matches require the exact stance.", "",
    "| Field | Matches | Positives found | Extra yes | Missing |", "|---|---:|---:|---:|---:|"]
for r in agreement:
    if r["reference"] == "original_human" and r["method"] == "evidence_verified":
        lines.append(f"| {display_names[r['field']]} | {r['matches']}/{r['available']} | {r['found_positive']}/{r['answered_reference_positive']} | {r['extra_positive']} | {r['reports']-r['available']} |")
lines += ["", "## Same reports: earlier Jev versus passage check", "",
    "Both columns below use only reports with a categorical answer from both versions and an eligible original human field. Earlier Jev uses the first v2 original question for character/revisions and v1 elsewhere; v1 therefore covers fewer reports. Neither column estimates population accuracy.", "",
    "| Field | Earlier Jev matches | Passage-check matches |", "|---|---:|---:|"]
for r in agreement:
    if r["reference"] == "original_human" and r["method"] == "evidence_verified":
        prior = next(a for a in agreement if a["field"] == r["field"] and a["reference"] == r["reference"] and a["method"] == "previous_jev")
        lines.append(f"| {display_names[r['field']]} | {prior['paired_matches']}/{r['paired_reports']} | {r['paired_matches']}/{r['paired_reports']} |")
conflicts = [r for r in comparisons if r["topic_detail_consistency"] in {"broad_negative_detail_positive", "broad_positive_details_negative"}]
lines += ["", "## Topic consistency diagnostic", "",
    f"There are {len(conflicts)} report/topic contradictions between a combined topic and its detailed components. This logical check was added after inspecting responses; it does not change any returned label or demonstrate improvement. A positive affordability answer should imply the combined affordability/displacement topic, but agreement after such a repair would still require source validation.", "",
    "| Application | Combined topic | Direct value | Positive details |", "|---|---|---:|---|"]
for r in conflicts:
    lines.append(f"| {r['application_number']} | {'Affordability' if r['field'] == 'affordability_displacement' else 'Character'} | {r['value']} | {r['detail_positive_fields'].replace('neighborhood_character', 'neighborhood fit').replace('scale_density_design', 'scale/design').replace('historic_preservation', 'preservation').replace(';', ', ')} |")
lines += ["", "## Evidence coverage", "", "| Outcome | Report-question rows |", "|---|---:|"]
for status, count in sorted(Counter(r["status"] for r in evidence).items()):
    lines.append(f"| {status.replace('_', ' ')} | {count} |")
lines += ["", "The evidence CSV records the selected text and separate retrieval/check scores; scores are not calibrated accuracy. The comparison CSV retains original Jacob, original Tyler, nonconflicting human, working, regex, and earlier Jev values. No human labels, coder notes, or local paths were sent to the model; only public CPC material was sent.", "",
    "Prompts, selection reasons, prepared requests, raw gateway responses, and credits are archived separately. Ordinary Make rebuilds analyze saved responses and make no API calls. The corpus, withdrawn/ZAP universe, and production labels were not restricted or replaced by this pilot."]
Path("../output/cpc_jev_findings_v3.md").write_text("\n".join(lines) + "\n")
print(f"Compared {len(sample)} reports / {len(evidence)} questions; observed charge ${sum(charges.values())}.")
