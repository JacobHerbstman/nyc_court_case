#!/usr/bin/env python3
"""Compare identical short questions across response types and source context."""

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
# experiment = "v6"
threshold, experiment = float(sys.argv[1]), sys.argv[2]
assert experiment in {"v6", "v7", "v9", "v10"}
assert 0 < threshold < 1
with open(f"../output/cpc_jev_controls_{experiment}.csv", newline="") as source:
    controls = list(csv.DictReader(source))
assert len(controls) == len({(r["case_id"], r["proposition"], r["context"], r["primitive"]) for r in controls})
prepared = Path(f"../output/cpc_jev_requests_{experiment}.jsonl").read_bytes()
assert prepared == Path(f"../input/cpc_jev_requests_{experiment}.jsonl").read_bytes()
packets = [json.loads(s) for s in prepared.decode().splitlines()]
assert len(packets) == len({p["request_id"] for p in packets})
requests = {p["request_id"]: p for p in packets}
assert {(r["request_id"], r["question_id"]) for r in controls} == {(p["request_id"], q) for p in packets for q in p["body"]["questions"]}
for p in packets:
    assert hashlib.sha256(json.dumps(p["body"], sort_keys=True, ensure_ascii=False).encode()).hexdigest() == p["request_sha256"]
for r in controls:
    p = requests[r["request_id"]]
    assert hashlib.sha256(p["body"]["state"]["report"]["text"].encode()).hexdigest() == r["context_sha256"]
    assert p["body"]["questions"][r["question_id"]]["instructions"] == r["question"]
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
returned, usage = {}, []
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
        question = packet["body"]["questions"][field]
        assert a["type"] == question["type"]
        if a["type"] == "noul":
            probability = a["noul"]
        else:
            assert a["choice"] in {"yes", "no"} and set(a["probabilities"]) == {"yes", "no"}
            assert all(type(v) in (int, float) and math.isfinite(v) and 0 <= v <= 1 for v in a["probabilities"].values())
            assert abs(sum(a["probabilities"].values()) - 1) < .03
            assert math.isfinite(a["confidence"]) and 0 <= a["confidence"] <= 1
            probability = a["probabilities"]["yes"]
        assert type(probability) in (int, float) and math.isfinite(probability) and 0 <= probability <= 1
        returned[request_id, field] = dict(probability_yes=probability, returned_choice=a.get("choice", ""), confidence=a.get("confidence", ""))
credits = [json.loads(s) for s in Path(f"../input/cpc_jev_credits_{experiment}.jsonl").read_text().splitlines()]
assert credits[-1]["phase"] == "after"
charge = Decimal(str(credits[-1]["total_used"])) - Decimal(str(credits[0]["total_used"]))

answers = []
for r in controls:
    a = returned.get((r["request_id"], r["question_id"]), {})
    probability = a.get("probability_yes")
    value = "" if probability is None or probability == threshold else str(int(probability > threshold))
    answers.append(dict(r, probability_yes=probability, value=value,
        answer_status="unavailable" if probability is None else "threshold_tie" if value == "" else "answered",
        matches_expected="" if value == "" else int(value == r["expected"]),
        returned_choice=a.get("returned_choice", ""), confidence=a.get("confidence", ""),
        threshold=threshold, request_sha256=requests[r["request_id"]]["request_sha256"]))

agreement = []
for context in ("excerpt", "full", "guided"):
    for primitive in ("choice", "noul"):
        rows = [r for r in answers if r["context"] == context and r["primitive"] == primitive]
        if not rows:
            continue
        answered = [r for r in rows if r["value"] in {"0", "1"}]
        agreement.append(dict(context=context, primitive=primitive, planned=len(rows), answered=len(answered),
            missing=sum(r["answer_status"] == "unavailable" for r in rows), ties=sum(r["answer_status"] == "threshold_tie" for r in rows),
            matches=sum(r["matches_expected"] == 1 for r in answered),
            positive_expected=sum(r["expected"] == "1" for r in answered), positive_found=sum(r["expected"] == r["value"] == "1" for r in answered),
            negative_expected=sum(r["expected"] == "0" for r in answered), negative_correct=sum(r["expected"] == r["value"] == "0" for r in answered),
            wrong_positive=sum(r["expected"] == "0" and r["value"] == "1" for r in answered),
            wrong_negative=sum(r["expected"] == "1" and r["value"] == "0" for r in answered)))
pairs = []
for case, proposition, primitive in sorted({(r["case_id"], r["proposition"], r["primitive"]) for r in answers}):
    group = {r["context"]: r for r in answers if (r["case_id"], r["proposition"], r["primitive"]) == (case, proposition, primitive)}
    if experiment in {"v9", "v10"} and "excerpt" not in group:
        continue
    assert {"excerpt", "full"} <= group.keys()
    assert len({r["question"] for r in group.values()}) == 1
    short, full = group["excerpt"], group["full"]
    pairs.append(dict(case_id=case, proposition=proposition, primitive=primitive, question=short["question"],
        expected_excerpt=short["expected"], expected_full=full["expected"], excerpt_value=short["value"], full_value=full["value"],
        excerpt_probability=short["probability_yes"], full_probability=full["probability_yes"],
        guided_value=group.get("guided", {}).get("value", ""),
        expected_changes_with_context=int(short["expected"] != full["expected"]),
        same_expected_lost_in_full=int(short["expected"] == full["expected"] == short["value"] and full["value"] not in {"", full["expected"]}),
        same_expected_recovered_in_full=int(short["expected"] == full["expected"] == full["value"] and short["value"] not in {"", short["expected"]})))
save_csv(answers, list(answers[0]), f"../output/cpc_jev_answers_{experiment}.csv", ["case_id", "proposition", "context", "primitive"])
save_csv(usage, list(usage[0]), f"../output/cpc_jev_usage_{experiment}.csv", ["request_id"])
save_csv(agreement, list(agreement[0]), f"../output/cpc_jev_agreement_{experiment}.csv", ["context", "primitive"])
save_csv(pairs, list(pairs[0]), f"../output/cpc_jev_pairs_{experiment}.csv", ["case_id", "proposition", "primitive"])

title = "# Short factual questions: controlled diagnosis" if experiment == "v6" else "# Applicant acceptance and timing: narrow refinement"
lines = [title, "",
    f"{len(packets)} distinct requests test 25 propositions across nine selected cases from six source PDFs. The exact question is asked as Choice yes/no and native Noul on identical text. The earlier v5 window is compared with the entire selected source report; a guided-page control supplies the previously missed affordability passage. Expectations were recorded before calls by Codex, not taken from the original human codebook. A no means the supplied text does not establish the proposition. This is selected method development, not held-out or population accuracy." if experiment == "v6" else
    f"{len(packets)} distinct requests test seven acceptance/timing propositions in four selected source reports. The exact v6 excerpt and full-report text is retained; only the acceptance wording and separation of timing change. Expectations were frozen by Codex before calls and are not human adjudications or held-out accuracy. A no means the supplied text does not establish the proposition.", "",
    f"{sum(r['status'] == 'successful' for r in usage)} requests succeeded; {sum(r['status'] == 'failed' for r in usage)} failed and {sum(r['status'] == 'not_attempted' for r in usage)} were unattempted. No identical request was repeated. Observed account usage increased by ${charge}; final balance ${credits[-1]['balance']}. Returned token usage: {sum(r['input_tokens'] or 0 for r in usage):,} input and {sum(r['output_tokens'] or 0 for r in usage):,} output. {"Both response types use" if experiment == "v6" else "Native Noul uses"} the fixed probability threshold {threshold}; exact ties stay unresolved. Scores are not calibrated accuracy for CPC reports.", "",
    "## Agreement with frozen diagnostic expectations", "", "| Context | Response type | Match | Positive found | Negative correct | Missing / ties |", "|---|---|---:|---:|---:|---:|"]
for r in agreement:
    lines.append(f"| {r['context']} | {r['primitive']} | {r['matches']}/{r['answered']} | {r['positive_found']}/{r['positive_expected']} | {r['negative_correct']}/{r['negative_expected']} | {r['missing']} / {r['ties']} |")
lines += ["", "The two response types share wording and state, but differ in API representation and criterion keys. Full reports and excerpts share wording and criterion definitions, but differ in available context. Comparisons with historical v5 also change the question and state layout, so they do not isolate wording alone. Correct expectations can change when the excerpt omits evidence present later in the report." if experiment == "v6" else
    "A board request, applicant acceptance, and timing remain distinct. The bedroom agreement is deliberately a positive inside the board-conditions section; the section heading alone cannot determine acceptance. Full reports and excerpts use identical questions but can correctly yield different answers. Earlier successful questions remain in v6 and were not rerun.", "",
    "## Disagreements requiring inspection", "", "| Case / proposition | Context / type | Expected | Returned yes probability |", "|---|---|---:|---:|"]
for r in answers:
    if r["matches_expected"] == 0:
        lines.append(f"| {r['case_id'].replace('_', ' ')} / {r['proposition'].replace('_', ' ')} | {r['context']} / {r['primitive']} | {r['expected']} | {r['probability_yes']:.3f} |")
if not any(r["matches_expected"] == 0 for r in answers):
    lines.append("| No answered disagreement | | | |")
if experiment == "v7":
    lines += ["", "## All acceptance and timing checks", "",
        "| Proposition | Expected excerpt / full | Yes probability excerpt / full |",
        "|---|---:|---:|"]
    for r in pairs:
        probabilities = ["missing" if r[k] is None else f"{r[k]:.2f}" for k in ("excerpt_probability", "full_probability")]
        lines.append(f"| {r['case_id'].replace('_', ' ')} / {r['proposition'].replace('_', ' ')} | {r['expected_excerpt']} / {r['expected_full']} | {' / '.join(probabilities)} |")
    lines += ["", "An exact 0.50 is unresolved, not a correct negative. Excerpt negatives mean insufficient evidence in that excerpt, not absence of an agreement elsewhere. Acceptance does not by itself establish when or why a change occurred."]
lines += ["", f"Among pairs with the same expectation in both contexts, {sum(r['same_expected_lost_in_full'] for r in pairs)} correct excerpt answers become incorrect with the full report, and {sum(r['same_expected_recovered_in_full'] for r in pairs)} incorrect excerpt answers become correct. {"These counts include both response types, not independent cases." if experiment == "v6" else "These selected checks do not establish corpus-wide accuracy."}", "",
    "All controls, source/page references, raw probabilities and unavailable answers are retained. Source text and every prior pilot/human output remain unchanged; these controls are not new production labels. Normal Make rebuilds the comparison from saved requests and responses without inference calls.", "",
    "The design follows TypeSafe's advice to [ask one focused question at a time](https://docs.typesafe.ai/primitives), while batching independent questions against the same text. [Noul](https://docs.typesafe.ai/primitives/noul) supplies a yes probability; [Choice](https://docs.typesafe.ai/primitives/choice) supplies named options and their probabilities. The test observes behavior; it cannot reveal Jev's internal reasoning or establish a universal model limitation." if experiment == "v6" else
    "Question-design references are documented in the preceding v6 research record. These observations do not reveal Jev's internal reasoning."]
if experiment in {"v9", "v10"}:
    full_answers = {(r["case_id"], r["proposition"]): dict(r, signal_source=experiment) for r in answers if r["context"] == "full"}
    if experiment == "v10":
        with open("../output/cpc_jev_answers_v9.csv", newline="") as source:
            previous = [r for r in csv.DictReader(source) if r["context"] == "full"]
        assert len(previous) == len({(r["case_id"], r["proposition"]) for r in previous})
        for r in previous:
            r["matches_expected"] = int(r["matches_expected"]) if r["matches_expected"] != "" else ""
            full_answers.setdefault((r["case_id"], r["proposition"]), dict(r, signal_source="v9_carried_not_rerun"))
    council = []
    for case in dict.fromkeys(k[0] for k in full_answers):
        rows = [r for (c, _), r in full_answers.items() if c == case]
        values = {r["proposition"]: r["value"] for r in rows}
        expected = {r["proposition"]: r["expected"] for r in rows}
        involved = "1" if "1" in values.values() else "0" if all(v == "0" for v in values.values()) else ""
        requested = [values[k] for k in ("support", "requested_project", "requested_provision")]
        position = "opposition" if values["opposition"] == "1" else "support_or_request" if values["opposition"] == "0" and "1" in requested else "concern_only" if values["opposition"] == "0" and all(v == "0" for v in requested) and values["motivating_concern"] == "1" else "none_or_procedural" if involved == "0" else ""
        council.append(dict(case_id=case, document_id=rows[0]["document_id"], application_number=rows[0]["focal_application_number"],
            cohort=rows[0]["cohort"], original_council_code=rows[0]["original_council_code"],
            jacob_council_code=rows[0]["jacob_council_code"], tyler_council_code=rows[0]["tyler_council_code"],
            **values, any_involvement=involved, position_candidate=position,
            expected_involvement=str(int("1" in expected.values())),
            component_matches=sum(r["matches_expected"] == 1 for r in rows),
            component_missing=sum(r["value"] == "" for r in rows)))
        if experiment == "v10":
            council[-1]["signal_sources_json"] = json.dumps({r["proposition"]: r["signal_source"] for r in rows}, sort_keys=True)
            council[-1]["diagnostic_conflict_questions"] = ";".join(r["proposition"] for r in rows if r["matches_expected"] != 1)
    save_csv(council, list(council[0]), f"../output/cpc_jev_council_{experiment}.csv", ["case_id"])
    lines = ["# Council positions, requests and motivating concerns", "",
        f"{len(packets)} distinct requests: {sum('_full_' in p['request_id'] for p in packets)} full source bundles and {sum('_excerpt_' in p['request_id'] for p in packets)} excerpt controls. {sum(r['status']=='successful' for r in usage)} succeeded, {sum(r['status']=='failed' for r in usage)} failed, {sum(r['status']=='not_attempted' for r in usage)} unattempted. Observed account charge ${charge}; balance ${credits[-1]['balance']}. Tokens: {sum(r['input_tokens'] or 0 for r in usage):,} input and {sum(r['output_tokens'] or 0 for r in usage):,} output.", "",
        "Five independent questions distinguish endorsement, opposition (including withdrawal), requesting the project, requesting a specific provision, and concerns that prompted it. Representatives explicitly speaking for members count; institutional actions and routine filing do not. The wording is reusable and does not name the expected member. Full text for the six v8 cases is unchanged. The three additional reports were chosen using prior human codes and source inspection, so this is selected development, not held-out accuracy." if experiment == "v9" else
        "This follow-up revises only endorsement, requesting the project and requesting a particular provision. Opposition and motivating-concern readings are carried from v9. Six full bundles and three excerpts are supplied; all source text and expected judgments remain unchanged. These are selected diagnostic cases, not held-out accuracy.", "",
        "Expectations below were recorded by Codex before calls and are not original human labels. The original Council code is preserved alongside the new signals; concern-only involvement does not become endorsement. Exact probability ties remain unresolved at the unchanged 0.5 threshold.", "",
        "| Context | Expected positives found | Expected negatives correct | Matches | Missing / ties |",
        "|---|---:|---:|---:|---:|"]
    for r in agreement:
        lines.append(f"| {r['context']} | {r['positive_found']}/{r['positive_expected']} | {r['negative_correct']}/{r['negative_expected']} | {r['matches']}/{r['answered']} | {r['missing']} / {r['ties']} |")
    lines += ["", "## Full-bundle signals", "", "| Application | Original code | New candidate | Component matches |", "|----------------|-----------------------|-----------------------|------------------|"]
    for r in council:
        lines.append(f"| {r['application_number']} | {r['original_council_code'].replace('_', ' ')} | {r['position_candidate'].replace('_', ' ') or 'unresolved'} | {r['component_matches']}/5 |")
    if experiment == "v10":
        lines += ["", "Only endorsement and the two request questions were revised and retested in this follow-up. The full-bundle table combines 18 newly tested component answers with 27 unchanged v9 answers, including three entire reports. Each signal's source is recorded in the Council CSV. Missing new answers are retained, not replaced by earlier values. The excerpt/full agreement table above uses only this follow-up's 27 questions. These counts are not 45 new tests."]
    lines += ["", "Component matches use all five planned checks as the denominator, including unresolved answers. Substantive involvement is broader than endorsement. Opposition has precedence only when explicitly established; unresolved opposition cannot silently become support.", "",
        "## Checks needing inspection", "", "| Case / question | Context | Expected | Yes probability |", "|------------------------------|----------|---------:|------------:|"]
    unresolved = [r for r in answers if r["matches_expected"] != 1]
    for r in unresolved:
        lines.append(f"| {r['case_id'].replace('_',' ')} / {r['proposition'].replace('_',' ')} | {r['context']} | {r['expected']} | {r['probability_yes'] if r['probability_yes'] is not None else 'missing'} |")
    if not unresolved:
        lines.append("| None | | | |")
    lines += ["", "Questions, full text, diagnostic references, probabilities and missing answers are retained. Prior codebooks, raw observations, original human coding and production labels remain unchanged. Normal Make rebuilds from archived responses without inference calls. No identical request was repeated."]

Path(f"../output/cpc_jev_findings_{experiment}.md").write_text("\n".join(lines) + "\n")
print(f"Compared {len(answers)} diagnostic answers; observed charge ${charge}.")
