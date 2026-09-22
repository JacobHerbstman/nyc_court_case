#!/usr/bin/env python3
"""Explicit, bounded acquisition; preserve every attempt and exact request."""

import hashlib
import json
import math
import os
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import requests

sys.exit('Jev acquisition paused at Jacob\'s request on September 22. Saved-response analysis remains available.')

# Run from tasks/audits/pilot_ulurp_cpc_llm_labels/code via make acquire-jev.
request_limit, budget_dollars, max_attempts = int(sys.argv[1]), float(sys.argv[2]), int(sys.argv[3])
experiment = sys.argv[4] if len(sys.argv) == 5 else "v1"
assert experiment in {"v1", "v2", "v3_retrieve", "v3_verify", "v4", "v5", "v6", "v7", "v8", "v9", "v10", "v11", "v12", "v13", "v13_verify", "v14_verify", "v15", "v15_verify", "v16", "v16_verify", "v17"}
if (experiment.startswith("v3_") or experiment in {"v4", "v5", "v6", "v7", "v8", "v9", "v10", "v11"}):
    assert max_attempts == 1, "Evidence pilot permits one attempt per distinct request."
if experiment in {"v12", "v13", "v13_verify", "v14_verify", "v15", "v15_verify", "v16", "v16_verify", "v17"}:
    assert max_attempts in {1, 2}, "Repair test allows only one recovery of a received server failure."
assert request_limit >= 1 and 1 <= max_attempts <= 5
assert (experiment == "v1" and 0 < budget_dollars <= 0.25) or (experiment != "v1" and budget_dollars == 0)
archive = Path("../../../../data_raw/cpc_jev_pilot") / {
    "v1": "20260919_vercel_v1_verified", "v2": "20260919_vercel_v2_repeats",
    "v3_retrieve": "20260919_vercel_v3_retrieve", "v3_verify": "20260919_vercel_v3_verify",
    "v4": "20260919_vercel_v4_topics", "v5": "20260919_vercel_v5_checks", "v6": "20260919_vercel_v6_simple",
    "v7": "20260919_vercel_v7_acceptance", "v8": "20260919_vercel_v8_transfer", "v9": "20260919_vercel_v9_council", "v10": "20260919_vercel_v10_council_requests", "v11": "20260919_vercel_v11_council_scale", "v12": "20260921_vercel_v12_repair", "v13": "20260922_vercel_v13_screen", "v13_verify": "20260922_vercel_v13_verify", "v14_verify": "20260922_vercel_v14_verify", "v15": "20260922_vercel_v15_screen", "v15_verify": "20260922_vercel_v15_verify", "v16": "20260922_vercel_v16_screen", "v16_verify": "20260922_vercel_v16_verify", "v17": "20260922_vercel_v17_statements"}[experiment]
transient_statuses = {429, 502, 503, 504, 529} | ({0} if experiment != "v1" else set())
key = os.environ.get("AI_GATEWAY_API_KEY", "")
if not key and Path("../../../../.env").is_file():
    for line in Path("../../../../.env").read_text().splitlines():
        name, separator, value = line.strip().partition("=")
        if separator and name.strip() == "AI_GATEWAY_API_KEY":
            key = value.strip().strip("\"'")
if not key:
    sys.exit("Missing AI_GATEWAY_API_KEY in the environment or project .env; no API call made.")


def record_credits(phase):
    response = requests.get("https://ai-gateway.vercel.sh/v1/credits",
        headers={"Authorization": f"Bearer {key}"}, timeout=30, allow_redirects=False)
    response.raise_for_status()
    credits = response.json()
    record = dict(phase=phase, checked_at=datetime.now(timezone.utc).isoformat(),
        request_limit=request_limit, balance=credits["balance"], total_used=credits["total_used"])
    with (archive / "credits.jsonl").open("a") as output:
        output.write(json.dumps(record) + "\n")
    print(f"Credits {phase}: balance={record['balance']}, total used={record['total_used']}", flush=True)
    return record


def record_attempt(record):
    with (archive / "responses.jsonl").open("a") as output:
        output.write(json.dumps(record) + "\n")
        output.flush()
        os.fsync(output.fileno())


packet_bytes = Path(f"../output/cpc_jev_requests_{experiment}.jsonl").read_bytes()
packets = [json.loads(line) for line in packet_bytes.decode().splitlines()]
if (experiment.startswith("v3_") or experiment in {"v4", "v5", "v6", "v7", "v8", "v9", "v10", "v11", "v12", "v13", "v13_verify", "v14_verify", "v15", "v15_verify", "v16", "v16_verify", "v17"}):
    request_limit = min(request_limit, len(packets))
assert 1 <= request_limit <= len(packets) or (experiment in {'v15_verify', 'v16_verify'} and not packets)
if experiment in {'v15', 'v16'}:
    assert max_attempts == 1, 'This recovery permits one additional attempt per prior server failure.'
if experiment == 'v17':
    assert max_attempts == 1 and len(packets) == request_limit == 12, 'Small statement pilot: twelve requests, one attempt each.'
assert len({p["request_id"] for p in packets}) == len(packets)
for packet in packets:
    assert packet["body"]["model"] == "typesafe-ai/jev"
    assert hashlib.sha256(json.dumps(packet["body"], sort_keys=True, ensure_ascii=False).encode()).hexdigest() == packet["request_sha256"]

# External observations are immutable; normal analysis builds do not regenerate them.
if (archive / "requests.jsonl").exists():
    assert (archive / "requests.jsonl").read_bytes() == packet_bytes, "Requests changed; preserve the previous acquisition and use a new explicit vintage."
else:
    with (archive / "requests.jsonl").open("xb") as output:
        output.write(packet_bytes)

if not packets and experiment in {'v15_verify', 'v16_verify'}:
    # A complete exact cache hit needs no inference; record that actual empty batch.
    with (archive / 'responses.jsonl').open('a'):
        pass

attempts, received, successful = {}, {}, {}
if (archive / "responses.jsonl").exists():
    for line in (archive / "responses.jsonl").read_text().splitlines():
        record = json.loads(line)
        assert record["event"] in {"started", "received"}
        attempt_key = (record["request_id"], record.get("attempt_number", 1))
        target = attempts if record["event"] == "started" else received
        assert attempt_key not in target
        target[attempt_key] = record
        if record["event"] == "received" and record["valid_response"]:
            assert record["request_id"] not in successful
            successful[record["request_id"]] = record
assert set(received) == set(attempts), "Unfinished call: inspect its outcome before any explicit retry."
for attempt_key, record in received.items():
    assert record["request_sha256"] == attempts[attempt_key]["request_sha256"]
    assert record["valid_response"] or record["http_status"] in transient_statuses, "Non-transient failure: inspect before retry."
assert {k[0] for k in attempts} <= {p["request_id"] for p in packets}

# Reserve 64k input tokens at the published non-promotional rate for every attempt.
# This caps planned model charges; it is not a gateway-enforced billing limit.
reserved_per_call = 64000 * 0.042 / 1_000_000
credits_before = record_credits("before")
if experiment != "v1":
    first_credits = json.loads((archive / "credits.jsonl").read_text().splitlines()[0])
    assert Decimal(str(credits_before["total_used"])) <= Decimal(str(first_credits["total_used"])), "Observed a charge since this free-only experiment began; stopped."
assert Decimal(str(credits_before["balance"])) >= Decimal(str(budget_dollars)), "Insufficient available trial credit."
completed, total_attempts = len(successful), len(attempts)
try:
    for packet in packets:
        request_id = packet["request_id"]
        previous = [r for k, r in received.items() if k[0] == request_id]
        assert all(r["request_sha256"] == packet["request_sha256"] for r in previous)
        if request_id in successful:
            continue
        if experiment in {'v12', 'v13', 'v13_verify', 'v14_verify', 'v15', 'v15_verify', 'v16', 'v16_verify', 'v17'} and previous and previous[-1]['http_status'] not in {502, 503, 504, 529}:
            print(f"{request_id}: no recovery of a rate limit, invalid answer, or unknown transport outcome.", flush=True)
            continue
        if completed >= request_limit:
            break
        previous_attempts = max((k[1] for k in attempts if k[0] == request_id), default=0)
        if previous_attempts >= max_attempts:
            print(f"{request_id}: still unavailable after {previous_attempts} attempts; retained as missing.", flush=True)
            continue
        for attempt_number in range(previous_attempts + 1, max_attempts + 1):
            assert experiment != "v1" or (total_attempts + 1) * reserved_per_call <= budget_dollars, "Model-charge planning budget reached."
            if attempt_number > 1:
                time.sleep(5 * (attempt_number - 1))
            record = dict(event="started", request_id=request_id, attempt_number=attempt_number,
                document_id=packet["document_id"], request_sha256=packet["request_sha256"],
                endpoint="https://ai-gateway.vercel.sh/typesafe/v1/systemone",
                requested_model=packet["body"]["model"], started_at=datetime.now(timezone.utc).isoformat())
            record_attempt(record)
            total_attempts += 1
            try:
                response = requests.post(record["endpoint"], headers={"Authorization": f"Bearer {key}"},
                    json=packet["body"], timeout=(15, 60 if experiment != "v1" else 120), allow_redirects=False)
            except (requests.Timeout, requests.ConnectionError) as error:
                record.update(event="received", received_at=datetime.now(timezone.utc).isoformat(),
                    http_status=0, valid_response=False, raw_response=json.dumps({"error": type(error).__name__, "outcome_unknown": True}))
                record_attempt(record)
                print(f"{request_id}: transport failure {type(error).__name__}; outcome unknown, attempt saved.", flush=True)
                if experiment == "v1":
                    raise
                checkpoint = record_credits("checkpoint")
                assert Decimal(str(checkpoint["total_used"])) <= Decimal(str(credits_before["total_used"])), "Observed a charge; stopped."
                if experiment in {"v13", "v13_verify", "v14_verify", "v15", "v15_verify", "v16", "v16_verify", "v17"}:
                    break  # Unknown transport outcomes cannot be safely retried.
                continue
            try:
                payload = response.json()
            except ValueError:
                payload = None
            valid = response.status_code == 200 and isinstance(payload, dict)
            valid = valid and isinstance(payload.get("answers"), dict) and set(payload["answers"]) == set(packet["body"]["questions"])
            if valid:
                for field, answer in payload["answers"].items():
                    question = packet["body"]["questions"][field]
                    if question["type"] == "noul":
                        value = answer.get("noul") if isinstance(answer, dict) else None
                        valid = valid and isinstance(answer, dict) and answer.get("type") == "noul" and type(value) in (int, float) and math.isfinite(value) and 0 <= value <= 1
                    else:
                        valid = valid and isinstance(answer, dict) and answer.get("type") == "choice" and answer.get("choice") in question["criteria"]
            record.update(event="received", received_at=datetime.now(timezone.utc).isoformat(),
                http_status=response.status_code, valid_response=bool(valid), raw_response=response.text,
                gateway_request_id=response.headers.get("x-vercel-id"))
            record_attempt(record)
            if valid:
                completed += 1
                if experiment != "v1" and ((experiment.startswith("v3_") or experiment in {"v4", "v5", "v6", "v7", "v8", "v9", "v10", "v11", "v12", "v13", "v13_verify", "v14_verify", "v15", "v15_verify", "v16", "v16_verify", "v17"}) or completed % 10 == 0):
                    checkpoint = record_credits("checkpoint")
                    assert Decimal(str(checkpoint["total_used"])) <= Decimal(str(credits_before["total_used"])), "Observed a charge during the free-only experiment; stopped."
                print(f"Saved {request_id} ({completed}/{request_limit}); returned model: {payload.get('model', 'not supplied')}", flush=True)
                break
            print(f"{request_id}: HTTP {response.status_code}; attempt {attempt_number} saved.", flush=True)
            if experiment in {"v4", "v5", "v6", "v7", "v8", "v9", "v10", "v11", "v12", "v13", "v13_verify", "v14_verify", "v15", "v15_verify", "v16", "v16_verify", "v17"} and response.status_code == 429:
                sys.exit("Provider rate limit: stopped this acquisition; preserve pending reports for later.")
            if response.status_code not in transient_statuses:
                sys.exit("Request failed; stopped without changing or dropping source text.")
            if attempt_number == max_attempts:
                print(f"{request_id}: retry limit reached; retained as missing while other requests proceed.", flush=True)
        time.sleep(10 if experiment in {"v5", "v6", "v7", "v8", "v9", "v10", "v11", "v12", "v13", "v13_verify", "v14_verify", "v15", "v15_verify", "v16", "v16_verify", "v17"} else 1)
finally:
    credits_after = record_credits("after")
    print(f"Observed account usage increase: ${Decimal(str(credits_after['total_used'])) - Decimal(str(credits_before['total_used']))}")
print(f"Acquisition complete: {completed} saved responses. No production labels changed.")
