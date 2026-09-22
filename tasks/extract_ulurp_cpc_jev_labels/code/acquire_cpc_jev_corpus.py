#!/usr/bin/env python3
"""Explicit corpus acquisition; stream requests and preserve each observation."""
import csv
import fcntl
import hashlib
import json
import math
import os
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime, timezone
from decimal import Decimal
from email.utils import parsedate_to_datetime
from pathlib import Path
import requests

# Run from tasks/extract_ulurp_cpc_jev_labels/code via make acquire.
# Jacob paused acquisition on September 21 pending application-attribution repair.
sys.exit('Jev acquisition is paused: repair and audit source attribution before an explicitly authorized restart. Saved responses are unchanged.')
workers, minimum_interval, request_limit, attempt = int(sys.argv[1]), float(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
assert 1 <= workers <= 4 and minimum_interval >= 1 and request_limit >= 0 and 1 <= attempt <= 6
archive = Path('../../../data_raw/cpc_jev_corpus/20260919_v2')
lock = (archive / '.acquire.lock').open('a')
if attempt > 1:
    print(f'Recovery attempt {attempt}: waiting for the active acquisition to finish.', flush=True)
fcntl.flock(lock, fcntl.LOCK_EX | (fcntl.LOCK_NB if attempt == 1 else 0))
key = os.environ.get('AI_GATEWAY_API_KEY', '')
if not key:
    for line in Path('../../../.env').read_text().splitlines():
        name, separator, value = line.strip().partition('=')
        if separator and name.strip() == 'AI_GATEWAY_API_KEY':
            key = value.strip().strip('\"\'')
assert key, 'Missing saved API key; no request sent.'
with open('../output/cpc_jev_request_index_v2.csv') as f:
    index = {r['request_id']: r for r in csv.DictReader(f)}
# Stream fingerprints so the complete corpus is never loaded into memory.
for prepared, saved in [(Path('../output/cpc_jev_requests_v2.jsonl'), archive/'requests.jsonl'),
                        (Path('cpc_jev_codebook_v1.json'), archive/'codebook.json')]:
    if saved.exists():
        with prepared.open('rb') as a, saved.open('rb') as b:
            assert hashlib.file_digest(a,'sha256').hexdigest() == hashlib.file_digest(b,'sha256').hexdigest(), 'Frozen requests changed; use a new explicit vintage.'
    else:
        temporary = saved.with_suffix(saved.suffix+'.tmp')
        shutil.copyfile(prepared, temporary)
        os.replace(temporary, saved)
with open('../../../data_raw/cpc_jev_corpus/20260919_v1/completed_requests.jsonl') as f:
    carried_documents = {json.loads(line)['document_id'] for line in f}
seen, received = {}, {}
if (archive/'responses.jsonl').exists():
    for line in (archive/'responses.jsonl').open():
        r = json.loads(line)
        assert r['request_sha256'] == index[r['request_id']]['request_sha256']
        target = seen if r['event']=='started' else received
        key_id = (r['request_id'], r.get('attempt', 1))
        assert r['event'] in {'started','received'} and key_id not in target
        target[key_id] = r
assert received.keys() <= seen.keys()
previous = {}
for (request_id, number), r in sorted(received.items()):
    previous[request_id] = r
started_requests = {request_id for request_id, number in seen}
successful_requests = {request_id for (request_id, number), r in received.items() if r.get('valid_response')}
eligible = set()
for request_id in index:
    if index[request_id].get('document_id') in carried_documents:
        continue
    if attempt == 1 and request_id not in started_requests:
        eligible.add(request_id)
    elif attempt > 1 and request_id not in successful_requests:
        prior = previous.get(request_id, {})
        prior_attempt = prior.get('attempt', 1)
        if prior.get('http_status') in {429, 502, 503, 504, 529} and prior_attempt == attempt - 1 and (request_id, attempt) not in seen:
            eligible.add(request_id)
# Unknown in-flight outcomes are retained and skipped on resume, never repeated.
for request_id, number in seen.keys() - received.keys():
    eligible.discard(request_id)
    print(f'{request_id} attempt {number}: earlier outcome unknown; retained for audit, not repeated.', flush=True)


def credits(phase):
    # Retry this read-only check before sending further model requests.
    for check_attempt in range(6):
        try:
            response = requests.get('https://ai-gateway.vercel.sh/v1/credits', headers={'Authorization':f'Bearer {key}'}, timeout=30, allow_redirects=False)
            response.raise_for_status()
            break
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as error:
            failed_response = getattr(error, 'response', None)
            status = getattr(failed_response, 'status_code', None)
            if check_attempt == 5 or isinstance(error, requests.HTTPError) and status not in {429, 502, 503, 504, 529}:
                raise
            delay = max([5, 15, 60, 120, 300][check_attempt], retry_after_seconds(getattr(failed_response, 'headers', {}).get('retry-after')))
            print(f'Credit check temporarily unavailable ({type(error).__name__}); pausing submissions and retrying in {delay:.0f} seconds.', flush=True)
            time.sleep(delay)
    c = response.json()
    r = dict(phase=phase, checked_at=datetime.now(timezone.utc).isoformat(), balance=c['balance'], total_used=c['total_used'])
    with (archive/'credits.jsonl').open('a') as f:
        f.write(json.dumps(r)+'\n')
    return r


def record(r):
    with (archive/'responses.jsonl').open('a') as f:
        f.write(json.dumps(r)+'\n')
        f.flush()
        os.fsync(f.fileno())


def retry_after_seconds(value):
    """Retry-After can be a number of seconds or an HTTP date."""
    try:
        seconds = float(value)
        return max(0, seconds) if math.isfinite(seconds) else 0
    except (TypeError, ValueError):
        try:
            return max(0, (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return 0


def evaluate(packet):
    r = dict(event='received', request_id=packet['request_id'], document_id=packet['document_id'], request_sha256=packet['request_sha256'], attempt=attempt)
    try:
        response = requests.post('https://ai-gateway.vercel.sh/typesafe/v1/systemone', headers={'Authorization':f'Bearer {key}'},
            json=packet['body'], timeout=(15,60), allow_redirects=False)
        r.update(http_status=response.status_code, raw_response=response.text, gateway_request_id=response.headers.get('x-vercel-id'), retry_after=response.headers.get('retry-after'))
        try:
            payload = response.json()
        except ValueError:
            payload = None
        valid = response.status_code==200 and isinstance(payload,dict) and isinstance(payload.get('answers'),dict) and set(payload['answers'])==set(packet['body']['questions'])
        if valid:
            for field, answer in payload['answers'].items():
                q = packet['body']['questions'][field]
                if not isinstance(answer,dict) or answer.get('type') != q['type']:
                    valid = False
                    break
                if q['type']=='noul':
                    value = answer.get('noul')
                    valid = valid and type(value) in (int,float) and math.isfinite(value) and 0 <= value <= 1
                else:
                    probabilities = answer.get('probabilities',{})
                    valid = valid and answer.get('choice') in q['criteria'] and set(probabilities)==set(q['criteria'])
                    valid = valid and all(type(v) in (int,float) and math.isfinite(v) and 0 <= v <= 1 for v in probabilities.values())
                    valid = valid and abs(sum(probabilities.values())-1)<.03
        r['valid_response'] = bool(valid)
    except (requests.Timeout,requests.ConnectionError) as error:
        r.update(http_status=0, valid_response=False, raw_response=json.dumps(dict(error=type(error).__name__, outcome_unknown=True)))
    r['received_at'] = datetime.now(timezone.utc).isoformat()
    return r


before = credits('before')
first = json.loads((archive/'credits.jsonl').open().readline())
assert Decimal(str(before['total_used'])) <= Decimal(str(first['total_used'])), 'Observed charge since this free-only run began; stopped.'
print(f"Before: balance=${before['balance']}, used=${before['total_used']}; {len(index)} planned requests, {len(seen)} saved attempts; {len(eligible)} eligible for attempt {attempt}.", flush=True)
pending, submitted, finished = {}, 0, 0
print(f'{len(carried_documents)} complete reports reused from the first packet vintage; they will not be requested again.', flush=True)
next_start, consecutive_failures, rate_limit_streak, stop = 0, 0, 0, ''
if attempt > 1 and eligible:
    next_start = time.monotonic() + 900
    print('Waiting 15 minutes before this recovery pass; successful requests are excluded.', flush=True)
# A restart also respects any provider cooldown saved by the previous process.
ordered_responses = sorted(received.values(), key=lambda r: r.get('received_at', ''))
for r in reversed(ordered_responses):
    if r.get('valid_response'):
        break
    consecutive_failures += 1
if consecutive_failures >= 10 and ordered_responses[-1]['http_status'] in {0, 502, 503, 504, 529}:
    last = ordered_responses[-1]
    elapsed = (datetime.now(timezone.utc) - datetime.fromisoformat(last['received_at'])).total_seconds()
    cooldown = max(retry_after_seconds(last.get('retry_after')), 900 * 2 ** min(consecutive_failures - 10, 2))
    remaining = max(0, cooldown - elapsed)
    next_start = max(next_start, time.monotonic() + remaining)
    print(f'Respecting saved provider outage: waiting at least {remaining:.0f} seconds before submissions.', flush=True)
for r in received.values():
    if r['http_status'] == 429:
        elapsed = (datetime.now(timezone.utc) - datetime.fromisoformat(r['received_at'])).total_seconds()
        try:
            remaining = float(r.get('retry_after') or 0) - elapsed
        except ValueError:
            remaining = retry_after_seconds(r.get('retry_after'))
        next_start = max(next_start, time.monotonic() + max(0, 60 - elapsed, remaining))
if not eligible:
    next_start = 0
stream = (archive/'requests.jsonl').open()
exhausted = False
try:
    with ThreadPoolExecutor(max_workers=workers) as pool:
        while not exhausted or pending:
            if not stop and not exhausted and len(pending)<workers and time.monotonic()>=next_start:
                for line in stream:
                    packet = json.loads(line)
                    if packet['request_id'] in eligible and packet['document_id'] not in carried_documents:
                        break
                else:
                    exhausted = True
                    continue
                assert hashlib.sha256(json.dumps(packet['body'],sort_keys=True,ensure_ascii=False).encode()).hexdigest()==packet['request_sha256']
                r = dict(event='started',request_id=packet['request_id'],document_id=packet['document_id'],request_sha256=packet['request_sha256'],
                    started_at=datetime.now(timezone.utc).isoformat(), endpoint='https://ai-gateway.vercel.sh/typesafe/v1/systemone', requested_model='typesafe-ai/jev', attempt=attempt)
                record(r)
                seen[(r['request_id'], attempt)] = r
                pending[pool.submit(evaluate,packet)] = r
                submitted += 1
                next_start = time.monotonic()+minimum_interval
                if submitted == len(eligible) or request_limit and submitted>=request_limit:
                    exhausted = True
            if pending:
                done,_ = wait(pending,timeout=.25,return_when=FIRST_COMPLETED)
                for future in done:
                    r = future.result()
                    record(r)
                    del pending[future]
                    finished += 1
                    consecutive_failures = 0 if r['valid_response'] else consecutive_failures+1
                    if r['valid_response']:
                        rate_limit_streak = 0
                    checkpoint = credits('checkpoint')
                    if Decimal(str(checkpoint['total_used']))>Decimal(str(before['total_used'])):
                        stop = 'Observed account charge; stopped new submissions.'
                    if r['http_status']==429:
                        rate_limit_streak += 1
                        cooldown = max(retry_after_seconds(r.get('retry_after')), min(900, 60 * 2 ** min(rate_limit_streak - 1, 4)))
                        next_start = max(next_start, time.monotonic() + cooldown)
                        print(f'Provider rate limit: pausing new submissions for at least {cooldown:.0f} seconds. This failed request remains saved; no answer is repeated.', flush=True)
                        if rate_limit_streak >= 6:
                            stop = 'Six rate limits without a successful answer; stopped for inspection.'
                    elif r['http_status']==400 and 'max_tokens_exceeded' in r.get('raw_response', ''):
                        print(f"{r['request_id']}: token-limit failure saved for smaller-packet repair; continuing unrelated requests.", flush=True)
                    elif not r['valid_response'] and r['http_status'] not in {0,502,503,504,529}:
                        stop = 'Unexpected/invalid response; stopped new submissions for inspection.'
                    elif consecutive_failures>=10:
                        cooldown = max(retry_after_seconds(r.get('retry_after')), 900 * 2 ** min(consecutive_failures - 10, 2))
                        next_start = max(next_start, time.monotonic() + cooldown)
                        print(f'Provider outage: pausing new submissions for at least {cooldown:.0f} seconds. Continuing only with unattempted requests afterward.', flush=True)
                    print(f"Attempt {attempt}, {finished}/{len(eligible)} {r['request_id']}: {'saved' if r['valid_response'] else 'unavailable'} HTTP {r['http_status']}; account used=${checkpoint['total_used']}",flush=True)
            else:
                time.sleep(min(1,max(0,next_start-time.monotonic())))
            if stop:
                if not exhausted:
                    print(stop,flush=True)
                exhausted = True
                # Drain in-flight calls without losing observations.
except BaseException:
    # The executor waits for submitted work; persist completed outcomes on interruption.
    for future in pending:
        if future.done() and not future.cancelled() and future.exception() is None:
            record(future.result())
    raise
finally:
    stream.close()
    after = credits('after')
    print(f"Observed account usage increase: ${Decimal(str(after['total_used']))-Decimal(str(before['total_used']))}; balance=${after['balance']}",flush=True)
if stop:
    sys.exit(stop)
print(f'Finished attempt {attempt}: {finished}/{len(eligible)} eligible outcomes recorded. No successful request repeated.',flush=True)
