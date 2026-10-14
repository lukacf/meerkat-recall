"""Independent D28 corpus/response/cohort expectations; no dispatch capability."""
from __future__ import annotations

import base64
import importlib.util
import json
import math
import types
from pathlib import Path

import oracle

ROOT = Path(__file__).resolve().parents[2]
CORPUS = Path(__file__).with_name("workload-corpus.json")
CONTRACT = Path(__file__).with_name("workload-contract.json")
SPEC = importlib.util.spec_from_file_location("recall_workload_capture_parser", ROOT / "scripts/qualification/recall_preview.py")
capture_parser = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(capture_parser)


def canonical_hash(value):
    return oracle.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode())


def semantic140_expectations():
    """Execute exactly the independently pinned source snapshot, only for140."""
    path = ROOT / "tests/recall_preview_semantics/semantic140_expectations.py"
    raw = path.read_bytes()
    oracle.require(oracle.sha256(raw) == "dbb7b64d9e30033574942f8c43fd3301f223088cc6bd8213cc006fc15d37721f",
                   "semantic140 expectations source changed")
    module = types.ModuleType("independent_semantic140_expectations")
    exec(compile(raw, str(path), "exec", dont_inherit=True), module.__dict__)
    return module


def corpus(profile_id="d28-v1"):
    oracle.require(profile_id in ("d28-v1", "semantic140-v1"), "unknown closed workload profile")
    path = CORPUS if profile_id == "d28-v1" else CORPUS.with_name("semantic140-corpus.json")
    raw = path.read_bytes()
    if profile_id == "semantic140-v1":
        oracle.require(oracle.sha256(raw) == semantic140_expectations().FROZEN_INPUTS["corpus"],
                       "semantic140 corpus identity changed")
    return capture_parser.strict_json_body(raw)


def contract(profile_id="d28-v1"):
    oracle.require(profile_id in ("d28-v1", "semantic140-v1"), "unknown closed workload profile")
    path = CONTRACT if profile_id == "d28-v1" else CONTRACT.with_name("semantic140-contract.json")
    raw = path.read_bytes()
    if profile_id == "semantic140-v1":
        oracle.require(oracle.sha256(raw) == semantic140_expectations().FROZEN_INPUTS["workload_contract"],
                       "semantic140 contract identity changed")
    return capture_parser.strict_json_body(raw)


def request(query):
    return {"query": query["query"], "limit": 50, "ranking_mode": "semantic",
            "source_revision_constraints": {row["key"]: row["revision"] for row in query["required"]}}


def workload_response(query, response, corpus=None):
    """Check all wire results; score the first three distinct evidence sources.

    Hashes omitted by the public DTO are explicitly computed from independently
    frozen source bytes. No direct fetch, source injection or query repair occurs.
    """
    frozen = globals()["corpus"]() if corpus is None else corpus
    expected = next((row for row in frozen["queries"] if row["id"] == query.get("id")), None)
    oracle.require(expected == query, "query differs from frozen workload input")
    oracle.require(response.get("framing_complete") is True and response.get("raw_body_complete") is True
                   and not response.get("capture_error"), "incomplete workload transport")
    oracle.require(response["status"] == 200, "failed workload HTTP status")
    raw = base64.b64decode(response["raw_body_base64"], validate=True)
    oracle.integer(response["bytes"], "raw response size")
    oracle.require(len(raw) == response["bytes"] <= 262144, "workload response byte bound")
    oracle.require(oracle.sha256(raw) == response["body_sha256"], "workload raw body digest differs")
    body = capture_parser.strict_json_body(raw)
    oracle.require(body == response["body"], "parsed workload response differs from retained bytes")
    oracle.require(body.get("partial") is False, "partial workload result")
    coverage = body.get("coverage")
    oracle.require(type(coverage) is dict, "workload coverage missing")
    for required in query["required"]:
        row = coverage.get(required["key"])
        oracle.require(type(row) is dict and row.get("requested_revision") == required["revision"]
                       and row.get("state") == "complete", "required workload coverage incomplete")
    results = body.get("results")
    oracle.require(type(results) is list and len(results) <= 50, "workload wire result limit")
    known = {(row["space"], row["key"], row["revision"]): row for row in frozen["sources"]}
    forbidden = {(row["space"], row["key"], row["revision"]) for row in query["forbidden_evidence"]}
    ordered, quotes = [], {}
    for hit in results:
        oracle.require(hit.get("kind") in {"claim", "entity", "chunk"}, "unknown workload result kind")
        oracle.nonempty(hit.get("text"), "workload result text")
        evidence = hit.get("evidence")
        oracle.require(type(evidence) is list and bool(evidence), "workload result lacks evidence")
        for item in evidence:
            space = item.get("space_id", query["space"])
            identity = (space, item["source_id"], item["revision"])
            oracle.require(space == query["space"] and identity not in forbidden, "forbidden workload evidence")
            source = known.get(identity)
            oracle.require(source is not None, "unknown workload source or revision")
            row = coverage.get(item["source_id"])
            oracle.require(type(row) is dict and row.get("state") == "complete"
                           and row.get("published_revision") == item["revision"]
                           and row.get("desired_revision") == item["revision"], "stale workload evidence")
            quote = oracle.nonempty(item.get("quote"), "workload exact quote")
            oracle.exact_evidence({"space": space, "key": item["source_id"], "revision": item["revision"],
                "start": item["span"]["start"], "end": item["span"]["end"], "quote": quote,
                "quote_hash": item.get("quote_hash", oracle.sha256(quote.encode())),
                "revision_hash": item.get("revision_hash", oracle.sha256(source["text"].encode()))}, source, space)
            if identity not in quotes:
                ordered.append(identity)
                quotes[identity] = []
            quotes[identity].append(quote)
    top = ordered[:3]
    required_ids = {(query["space"], row["key"], row["revision"]) for row in query["required"]}
    oracle.require(required_ids <= set(top), "required workload evidence absent from metric top three")
    for required in query["required"]:
        identity = (query["space"], required["key"], required["revision"])
        oracle.require(any(required["quote_fragment"] in quote for quote in quotes[identity]),
                       "returned source quote omits independently expected answer")
    rank = min(ordered.index(identity) + 1 for identity in required_ids)
    precision = len(required_ids & set(top)) / 3
    oracle.require(precision >= query["min_precision"], "workload precision below fixed cutoff")
    return {"verdict": "passed", "query_id": query["id"], "precision_at_k": precision,
            "reciprocal_rank": 1 / rank, "distinct_sources": len(ordered), "wire_results": len(results),
            "evidence_hash_origin": "wire-if-present-otherwise-independent-corpus",
            "physical_provider_attempts": None}


def finite_time(value, label):
    oracle.require(type(value) in (int, float) and math.isfinite(value) and value >= 0, label)
    return value


def evaluate_cohort(observation):
    """Original D28 or the explicitly closed successor; never dispatch requests."""
    profile_id = observation.get("profile_id", "d28-v1")
    oracle.require(profile_id in ("d28-v1", "semantic140-v1"), "unknown closed workload profile")
    if profile_id == "semantic140-v1":
        return evaluate_semantic140_cohort(observation)
    chosen, frozen = contract(), corpus()
    oracle.require(observation.get("measured") is True, "authored workload is not measured")
    oracle.require(observation["backend"] in ("elephant-db", "surreal-db"), "unknown workload backend")
    oracle.require(observation["workload_contract_sha256"] == canonical_hash(chosen), "workload contract changed")
    oracle.require(observation["corpus_sha256"] == chosen["corpus_sha256"] == oracle.sha256(CORPUS.read_bytes()), "workload corpus changed")
    oracle.require(observation["sources_per_space"] == chosen["sources_per_space"], "expected 50 total sources across named spaces")
    oracle.require(canonical_hash(frozen["schedule"]) == chosen["schedule_sha256"], "workload schedule changed")
    expected = {row["id"]: row for row in frozen["schedule"]}
    rows = observation["requests"]
    oracle.require(type(rows) is list and len(rows) == 200, "workload attempt count differs")
    oracle.require(len({row["id"] for row in rows}) == 200 and {row["id"] for row in rows} == set(expected), "workload attempt identities differ")
    release = finite_time(observation["barrier_released_ms"], "invalid caller barrier")
    callers = observation["callers"]
    caller_ids = {row["caller_id"] for row in frozen["schedule"]}
    oracle.require(type(callers) is list and len(callers) == 50 and {row["id"] for row in callers} == caller_ids, "50 caller identities required")
    times = {}
    for caller in callers:
        ready = finite_time(caller["ready_ms"], "invalid caller ready time")
        finish = finite_time(caller["finished_ms"], "invalid caller finished time")
        oracle.require(ready <= release < finish <= 120000, "caller did not participate in common barrier")
        times[caller["id"]] = finish
    queries = {row["id"]: row for row in frozen["queries"]}
    groups = {key: [] for key in caller_ids}
    events, durations, ranks = [], [], []
    for row in rows:
        scheduled = expected[row["id"]]
        oracle.require(all(row[key] == scheduled[key] for key in ("caller_id", "ordinal", "space", "query_id")), "request schedule assignment changed")
        oracle.require(type(row["ordinal"]) is int, "invalid request ordinal")
        query = queries[row["query_id"]]
        oracle.require(row["request_body"] == request(query), "original workload request changed or fell back")
        start = finite_time(row["started_ms"], "invalid request start")
        end = finite_time(row["ended_ms"], "invalid request end")
        oracle.require(release <= start < end <= times[row["caller_id"]], "request outside caller lifetime")
        duration = end - start
        oracle.require(duration <= 8000, "workload request exceeded timeout")
        scores = workload_response(query, row["response"], frozen)
        ranks.append(scores["reciprocal_rank"])
        durations.append(duration)
        groups[row["caller_id"]].append((row["ordinal"], start, end))
        events.extend(((start, 1), (end, -1)))
    for group in groups.values():
        group.sort()
        oracle.require([row[0] for row in group] == [0, 1, 2, 3], "four sequential calls per caller required")
        oracle.require(all(left[2] <= right[1] for left, right in zip(group, group[1:])), "one caller issued overlapping calls")
    active = peak = 0
    for _, delta in sorted(events):
        active += delta
        peak = max(active, peak)
    oracle.require(type(observation["peak_in_flight"]) is int and observation["peak_in_flight"] == peak, "physical overlap differs from intervals")
    values = sorted(durations)
    p95, p99 = values[math.ceil(.95 * len(values))-1], values[math.ceil(.99 * len(values))-1]
    oracle.require(p95 < 2000 and p99 < 5000, "strict workload latency target failed")
    mean_rank = sum(ranks) / 200
    oracle.require(mean_rank >= .90, "workload MRR below .90")
    return {"verdict": "passed", "backend": observation["backend"], "calls": 200,
            "concurrent_callers": 50, "measured_peak_in_flight": peak, "p95_ms": p95, "p99_ms": p99,
            "mean_reciprocal_rank": mean_rank, "provider_attempt_correlation": "separate gateway evidence required",
            "ob3_consumer_fallback_qualified": False}


def interval_peak(intervals):
    """Count observed half-open client intervals; equal ends precede starts."""
    events = []
    for start, end in intervals:
        oracle.require(start < end, "nonpositive client interval")
        events.extend(((start, 1), (end, -1)))
    active = peak = 0
    for _, delta in sorted(events):
        active += delta
        oracle.require(active >= 0, "invalid client interval accounting")
        peak = max(peak, active)
    oracle.require(active == 0, "unsettled client interval")
    return peak


def bounded_json_bytes(value, maximum):
    """Measure retained JSON without materializing a second full observation."""
    total = 0
    encoder = json.JSONEncoder(ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    for part in encoder.iterencode(value):
        total += len(part.encode("utf-8"))
        oracle.require(total <= maximum, "semantic140 original capture bound exceeded")
    return total


def evaluate_semantic140_cohort(observation):
    """Independently qualify the closed 560-attempt client workload only."""
    pins = semantic140_expectations()
    chosen, frozen = contract(pins.PROFILE_ID), corpus(pins.PROFILE_ID)
    original = corpus()
    oracle.require(oracle.sha256(CORPUS.read_bytes()) == pins.ORIGINAL_CORPUS_SHA256,
                   "original workload corpus changed")
    oracle.require(all(frozen[key] == original[key] for key in ("sources", "queries", "spaces", "metric")),
                   "semantic140 changed original sources, queries, or expectations")
    oracle.require(canonical_hash(frozen["schedule"]) == pins.SCHEDULE_SHA256 == chosen["schedule_sha256"],
                   "semantic140 schedule changed")
    oracle.require(canonical_hash(chosen) == pins.CONTRACT_CANONICAL_SHA256,
                   "semantic140 normative contract changed")
    oracle.require(observation.get("profile_id") == pins.PROFILE_ID and observation.get("measured") is True,
                   "semantic140 observation is not measured under its closed profile")
    oracle.require(observation["backend"] in ("elephant-db", "surreal-db"), "unknown workload backend")
    oracle.require(observation["workload_contract_sha256"] == pins.CONTRACT_CANONICAL_SHA256
                   and observation["contract_sha256"] == pins.FROZEN_INPUTS["workload_contract"]
                   and observation["corpus_sha256"] == pins.FROZEN_INPUTS["corpus"]
                   and observation["provider_allocation_sha256"] == pins.FROZEN_INPUTS["provider_allocation"],
                   "semantic140 mixed profile inputs")
    for key in ("preparation_contract", "typed_delta", "rendered_map"):
        oracle.require(observation[key + "_sha256"] == pins.FROZEN_INPUTS[key], "semantic140 mixed preparation input")
    oracle.require(observation["sources_per_space"] == {"qualification-household": 25, "qualification-business": 25},
                   "semantic140 source inventory changed")
    oracle.require(observation.get("product_acceptance") is False
                   and observation.get("fallback_executed") is False
                   and type(observation.get("retries")) is int and observation["retries"] == 0,
                   "semantic140 fallback, retry, or whole-product claim")
    bounded_json_bytes(observation, pins.OBSERVATION_BYTES)
    rows, callers = observation["requests"], observation["callers"]
    row_bytes = 0
    for row in rows:
        row_bytes += bounded_json_bytes(row, pins.OBSERVATION_BYTES - row_bytes)
    expected = {row["id"]: row for row in frozen["schedule"]}
    oracle.require(type(rows) is list and len(rows) == len(expected) == 560
                   and len({row["id"] for row in rows}) == 560
                   and {row["id"] for row in rows} == set(expected), "semantic140 exact 560 attempts required")
    caller_ids = {row["caller_id"] for row in frozen["schedule"]}
    oracle.require(type(callers) is list and len(callers) == len(caller_ids) == 140
                   and {row["id"] for row in callers} == caller_ids, "semantic140 exact 140 callers required")
    release = finite_time(observation["barrier_released_ms"], "invalid caller barrier")
    finishes = {}
    for caller in callers:
        ready = finite_time(caller["ready_ms"], "invalid caller ready time")
        finish = finite_time(caller["finished_ms"], "invalid caller finished time")
        oracle.require(ready <= release < finish <= 120000, "caller missed original common barrier/deadline")
        finishes[caller["id"]] = finish
    queries = {query["id"]: query for query in frozen["queries"]}
    groups = {caller: [] for caller in caller_ids}
    per_query = {query: [] for query in queries}
    lifetimes, sent_waiting, durations, ranks = [], [], [], []
    for row in rows:
        scheduled = expected[row["id"]]
        oracle.require(all(row[key] == scheduled[key] for key in ("caller_id", "ordinal", "space", "query_id"))
                       and type(row["ordinal"]) is int, "semantic140 request assignment changed")
        query = queries[row["query_id"]]
        oracle.require(row["request_body"] == request(query), "semantic140 original query changed")
        oracle.require(row.get("capture_error") is None and row.get("oracle_error_type") is None
                       and row.get("connection_cleanup_failed", False) is False
                       and row.get("run_deadline_cancelled", False) is False
                       and row.get("oracle_verdict") == "passed"
                       and row.get("evaluation_fallbacks") == [], "semantic140 failed original request/cleanup")
        start, connected, sent, first, end = [finite_time(row[key], "invalid semantic140 client timestamp") for key in
            ("started_ms", "connected_ms", "client_write_complete_ms", "first_response_byte_ms", "ended_ms")]
        oracle.require(release <= start <= connected <= sent <= first <= end <= finishes[row["caller_id"]]
                       and sent < end, "semantic140 original client interval order")
        duration = end - start
        oracle.require(duration <= 8000, "semantic140 original request timeout exceeded")
        scores = workload_response(query, row["response"], frozen)
        oracle.require(row.get("independent_response_verdict") == scores, "semantic140 per-request verdict mismatch")
        ranks.append(scores["reciprocal_rank"])
        per_query[row["query_id"]].append(scores)
        durations.append(duration)
        groups[row["caller_id"]].append((row["ordinal"], start, end))
        lifetimes.append((start, end)); sent_waiting.append((sent, end))
    for group in groups.values():
        group.sort()
        oracle.require([row[0] for row in group] == [0, 1, 2, 3]
                       and all(left[2] <= right[1] for left, right in zip(group, group[1:])),
                       "semantic140 caller sequence differs")
    peak, sent_peak = interval_peak(lifetimes), interval_peak(sent_waiting)
    oracle.require(type(observation["peak_in_flight"]) is int and observation["peak_in_flight"] == peak == 140,
                   "semantic140 request lifetime peak must be exactly140")
    oracle.require(type(observation["peak_sent_waiting_response"]) is int
                   and observation["peak_sent_waiting_response"] == sent_peak == 140,
                   "semantic140 sent-waiting peak must be exactly140")
    values = sorted(durations)
    p95, p99 = values[531], values[554]
    oracle.require(p95 < 2000 and p99 < 5000, "strict workload latency target failed")
    mean_rank = sum(ranks) / 560
    oracle.require(mean_rank >= .90, "workload MRR below .90")
    weights = sorted(len(rows) for rows in per_query.values())
    oracle.require(len(per_query) == 100 and weights == [4] * 20 + [6] * 80,
                   "semantic140 original query weighting differs")
    return {"verdict": "passed", "profile_id": pins.PROFILE_ID, "backend": observation["backend"],
            "calls": 560, "concurrent_callers": 140, "measured_peak_in_flight": peak,
            "measured_peak_sent_waiting_response": sent_peak, "server_execution_concurrency": None,
            "overlap_definition": "client request lifetimes and write-complete/response-complete intervals; not server execution concurrency",
            "p95_ms": p95, "p99_ms": p99, "percentile_ranks_one_based": {"p95": 532, "p99": 555},
            "mean_reciprocal_rank": mean_rank,
            "per_query": {key: {"attempts": len(rows), "mean_reciprocal_rank": sum(row["reciprocal_rank"] for row in rows) / len(rows),
                                "minimum_precision_at_k": min(row["precision_at_k"] for row in rows)} for key, rows in sorted(per_query.items())},
            "provider_attempt_correlation": "separate gateway evidence required",
            "ob3_consumer_fallback_qualified": False, "product_acceptance": False, "retained_row_bytes": row_bytes}
