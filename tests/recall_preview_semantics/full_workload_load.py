"""FW12: exactly reviewed lexical requests from 140 ready callers.

Measures client wire intervals only. No semantic-quality or server-overlap claim.
"""
import asyncio
import inspect
import time
from urllib.parse import quote

from full_workload_transport import decode, encode, require, sha


def validate_plan(plan, expected_sha256):
    require(sha(encode(plan)) == expected_sha256, "original independent load plan hash differs")
    require(type(plan) is dict and set(plan) == {"format", "requests"}
            and plan["format"] == "recall-full-workload-lexical-load/v1", "load plan shape differs")
    requests = plan["requests"]
    require(type(requests) is list and len(requests) == 280, "exactly 280 reviewed requests required")
    identities, coordinates = set(), set()
    for row in requests:
        require(type(row) is dict and set(row) == {"id", "caller", "ordinal", "space", "body", "expected_coverage"},
                "explicit original load request required")
        require(type(row["id"]) is str and row["id"] and row["id"] not in identities, "duplicate load request identity")
        require(type(row["caller"]) is int and 0 <= row["caller"] < 140
                and type(row["ordinal"]) is int and row["ordinal"] in (1, 2), "original caller schedule differs")
        identities.add(row["id"])
        coordinate = (row["caller"], row["ordinal"])
        require(coordinate not in coordinates, "duplicate original caller ordinal")
        coordinates.add(coordinate)
        require(row["space"] in ("qualification-household", "qualification-business"), "foreign load space")
        body = row["body"]
        require(type(body) is dict and set(body) == {"query", "ranking_mode", "limit", "source_revision_constraints"}
                and type(body["query"]) is str and 0 < len(body["query"].encode()) <= 8192
                and body["ranking_mode"] == "lexical" and type(body["limit"]) is int and 1 <= body["limit"] <= 50,
                "original explicit lexical request required; semantic mode is inactive")
        constraints, coverage = body["source_revision_constraints"], row["expected_coverage"]
        require(type(constraints) is dict and 0 < len(constraints) <= 56 and type(coverage) is dict
                and set(coverage) == set(constraints), "independent exact revision/coverage expectations required")
        for key, revision in constraints.items():
            require(type(key) is str and key and type(revision) is str and revision,
                    "original external source revision constraint absent")
            require(coverage[key] == {"requested_revision": revision, "published_revision": revision,
                                      "desired_revision": revision, "state": "complete"},
                    "FW12 original complete coverage expectation differs")
    return decode(encode(plan))


def successful_response(result, expected):
    """Protocol predicate only; callers must separately validate every hit."""
    if (result.get("status") != 200 or result.get("framing_complete") is not True
            or result.get("error_type") is not None):
        return False
    body = result.get("body")
    return (type(body) is dict and type(body.get("results")) is list and body.get("partial") is False
            and type(body.get("coverage")) is dict
            and set(body["coverage"]) == set(expected)
            and all(body["coverage"].get(key) == value for key, value in expected.items()))


def validate_requirements(plan, requirements, expected_sha256):
    require(sha(encode(requirements)) == expected_sha256, "original independent load evidence hash differs")
    require(type(requirements) is dict and set(requirements) == {row["id"] for row in plan["requests"]},
            "all 280 original load intents need literal evidence expectations")
    fields = {"source_key", "revision", "quote", "quote_hash"}
    for row in plan["requests"]:
        expected = requirements[row["id"]]
        require(type(expected) is dict and set(expected) == {"required_evidence", "required_claim_evidence"},
                "explicit load evidence and claim expectations required")
        ordinary, claims = expected["required_evidence"], expected["required_claim_evidence"]
        require(type(ordinary) is list and 1 <= len(ordinary) <= 100
                and type(claims) is list and len(claims) <= 100,
                "known-positive load requests require bounded nonempty evidence")
        for literal in ordinary + claims:
            require(type(literal) is dict and set(literal) == fields
                    and all(type(value) is str and value for value in literal.values())
                    and sha(literal["quote"].encode()) == literal["quote_hash"]
                    and row["body"]["source_revision_constraints"].get(literal["source_key"]) == literal["revision"],
                    "literal load evidence differs from the original requested source revision")
        require(all(literal in ordinary for literal in claims),
                "canonical claim expectations must retain the same original evidence")
    return decode(encode(requirements))


def wire_peak(rows):
    events = []
    for row in rows:
        start, end = row.get("client_write_complete_ns"), row.get("ended_ns")
        if type(start) is int and type(end) is int and start < end:
            events.extend(((start, 1), (end, -1)))
    active = peak = 0
    # A request ending at a time boundary does not overlap one starting there.
    for _, delta in sorted(events):
        active += delta
        peak = max(peak, active)
    return peak


async def run_load(client, spy, reviewed_plan, plan_sha256, *, evidence_requirements,
                   evidence_sha256, validate_evidence):
    plan = validate_plan(reviewed_plan, plan_sha256)
    requirements = validate_requirements(plan, evidence_requirements, evidence_sha256)
    require(callable(validate_evidence) and not inspect.iscoroutinefunction(validate_evidence),
            "original synchronous source-bound evidence verifier required")
    require(client.count + 280 <= client.limits["public_http_calls_per_backend"], "original HTTP allocation cannot fit FW12")
    original_spy = spy.snapshot_calls()
    require(original_spy["active"] == 0 and not original_spy["held_ids"] and original_spy["error_type"] is None,
            "original controlled provider work is not quiescent before FW12")
    remaining = client.deadline - client.clock()
    require(remaining > 0, "original workload deadline expired")
    started = time.monotonic_ns()
    gate, failed, ready = asyncio.Event(), asyncio.Event(), asyncio.Queue()
    ready_ids, rows, unexecuted, errors = [], [], [], []
    original_count = client.count
    tasks = []

    async def caller(identifier):
        requests = sorted((row for row in plan["requests"] if row["caller"] == identifier), key=lambda row: row["ordinal"])
        await ready.put(identifier)
        await gate.wait()
        for scheduled in requests:
            if failed.is_set():
                unexecuted.append(scheduled["id"])
                continue
            try:
                result = await client.request("POST", "/v1/spaces/" + quote(scheduled["space"], safe="") + "/search",
                    scheduled["space"], scheduled["body"], role="data", response_limit=262144)
                verdict = successful_response(result, scheduled["expected_coverage"])
                row = {"request_id": scheduled["id"], "caller": identifier, "ordinal": scheduled["ordinal"],
                       "observed": result, "protocol_complete": verdict, "evidence_verified": False}
                rows.append(row)
                if not verdict:
                    failed.set()
                else:
                    expected = requirements[scheduled["id"]]
                    checked = validate_evidence(scheduled["space"], scheduled["body"], result["body"],
                                                expected["required_evidence"], expected["required_claim_evidence"])
                    if inspect.iscoroutine(checked):
                        checked.close()
                    require(checked is None, "source-bound evidence verifier did not complete synchronously")
                    row["evidence_verified"] = True
            except (Exception, asyncio.CancelledError) as error:
                failed.set()
                errors.append({"request_id": scheduled["id"], "error_type": type(error).__name__})
                if isinstance(error, asyncio.CancelledError):
                    raise
    try:
        async with asyncio.timeout(min(180.0, remaining)):
            tasks = [asyncio.create_task(caller(identifier)) for identifier in range(140)]
            for _ in range(140):
                ready_ids.append(await ready.get())
            require(set(ready_ids) == set(range(140)) and client.count == original_count,
                    "all 140 original callers must be ready before any dispatch")
            gate.set()
            await asyncio.gather(*tasks)
    except (Exception, asyncio.CancelledError) as error:
        failed.set()
        errors.append({"error_type": type(error).__name__, "phase": "load"})
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    seen = {row["request_id"] for row in rows} | {row.get("request_id") for row in errors}
    unexecuted = sorted(set(unexecuted) | {row["id"] for row in plan["requests"] if row["id"] not in seen})
    provider = spy.snapshot_calls()
    unchanged_provider = (provider["physical_connections"] == original_spy["physical_connections"]
                          and provider["error_type"] is None and provider["active"] == 0)
    complete = (not failed.is_set() and len(rows) == 280 and client.count - original_count == 280
                and all(row["evidence_verified"] for row in rows)
                and not unexecuted and not errors and unchanged_provider)
    return {"format": "recall-full-workload-lexical-observation/v1", "case": "FW12", "plan_sha256": plan_sha256,
            "evidence_sha256": evidence_sha256,
            "protocol_verdict": "passed" if complete else "unmet", "qualified": False,
            "semantic_load": "unmet: separate allocation and independent thresholds are inactive",
            "started_ns": started, "ended_ns": time.monotonic_ns(), "ready_callers": sorted(ready_ids),
            "physical_http_calls": client.count - original_count, "provider_count_unchanged": unchanged_provider,
            "peak_client_wire_intervals": wire_peak([row["observed"] for row in rows]),
            "overlap_definition": "client write-complete to request-finally timestamps; not server execution overlap",
            "requests": sorted(rows, key=lambda row: (row["caller"], row["ordinal"])),
            "errors": errors, "unexecuted": unexecuted}
