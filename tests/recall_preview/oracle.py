"""Independent, fail-closed checks for captured Recall qualification observations.

This module does not implement Recall and cannot certify a capture's authenticity.
Raw transport/backend receipts must accompany observations. Fixture/self-test passes
are intentionally insufficient for installed-artifact or real-provider acceptance.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any


class Rejected(ValueError):
    """An expectation was violated or a required observation was absent."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise Rejected(message)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def nonempty(value: Any, field: str) -> str:
    require(isinstance(value, str) and bool(value.strip()), f"missing {field}")
    return value


def integer(value: Any, field: str, minimum: int = 0) -> int:
    require(type(value) is int and value >= minimum, f"invalid {field}")
    return value


def identity(item: dict[str, Any]) -> tuple[str, str]:
    return nonempty(item.get("key"), "source key"), nonempty(item.get("revision"), "revision")


def exact_evidence(evidence: dict, source: dict, space: str) -> None:
    """Offsets and hashes are independently recomputed over immutable UTF-8 bytes."""
    require(evidence.get("space") == space == source["space"], "evidence changed space")
    require(identity(evidence) == identity(source), "evidence changed source/revision")
    raw = source["text"].encode("utf-8")
    start = integer(evidence.get("start"), "span start")
    end = integer(evidence.get("end"), "span end", 1)
    require(start < end <= len(raw), "empty or out-of-range evidence span")
    try:
        raw[:start].decode("utf-8")
        quote = raw[start:end].decode("utf-8")
    except UnicodeDecodeError as error:
        raise Rejected("evidence span splits UTF-8") from error
    require(evidence.get("quote") == quote, "quote differs from immutable bytes")
    require(evidence.get("quote_hash") == sha256(raw[start:end]), "quote hash mismatch")
    require(evidence.get("revision_hash") == sha256(raw), "revision hash mismatch")


def retrieval(query: dict, capture: dict, corpus: dict) -> dict:
    """No query rewriting, direct document fetch, judge override or sparse fallback."""
    require(capture.get("query") == query["query"], "original query changed")
    require(capture.get("space") == query["space"], "query changed space")
    require(capture.get("k") == query["k"], "query cutoff changed")
    require(capture.get("outcome") == "complete", "unavailable/incomplete is not zero matches")
    require(capture.get("evaluation_fallbacks") == [], "fallback context is not retrieval")
    results = capture.get("results")
    require(isinstance(results, list), "missing actual result list")
    require(len(results) <= query["k"], "result cutoff exceeded")
    sources = {(row["space"], *identity(row)): row for row in corpus["sources"]}
    observed = []
    revisions: dict[str, str] = {}
    for result in results:
        pair = identity(result)
        require(result.get("space") == query["space"], "cross-space result")
        require(pair not in observed, "duplicate result inflates retrieval score")
        require(pair[0] not in revisions or revisions[pair[0]] == pair[1], "mixed source revisions")
        revisions[pair[0]] = pair[1]
        source = sources.get((query["space"], *pair))
        require(source is not None, "result has no independently known source revision")
        require(source["kind"] != "degraded-withheld", "consumer-withheld rendering was indexed")
        require(result.get("published_revision") == pair[1], "result misstates published revision")
        require(result.get("desired_revision") == pair[1], "stale result presented as current")
        require(result.get("processing_state") == "complete", "pending source presented as current")
        evidence = result.get("evidence")
        require(isinstance(evidence, list) and bool(evidence), "result lacks exact evidence")
        for span in evidence:
            exact_evidence(span, source, query["space"])
        observed.append(pair)
    required = {identity(row) for row in query["required"]}
    forbidden = {identity(row) for row in query["forbidden"]}
    require(not (set(observed) & forbidden), "forbidden obsolete/distractor result")
    require(required <= set(observed), "known-answer evidence was not retrieved")
    require(not query.get("expect_empty") or not results, "negative control returned evidence")
    # Fixed cutoff denominator: one lucky result cannot turn sparse retrieval into 100%.
    precision = len(set(observed) & required) / query["k"]
    require(precision >= query["min_precision"], "precision below frozen threshold")
    return {"precision_at_k": precision, "required_found": len(required & set(observed)), "returned": len(results)}


def publication(observation: dict) -> None:
    """Inspect complete effects at every fault boundary, including replayed receipt."""
    expected = set(observation["expected_effect_ids"])
    require(bool(expected), "missing independently expected canonical effects")
    expected_hashes = observation["expected_effect_hashes"]
    initial_hashes = observation["initial_effect_hashes"]
    require(set(expected_hashes) == expected == set(initial_hashes), "effect content inventory incomplete")
    require(expected_hashes != initial_hashes, "fixture must observe a real semantic change")
    require(all(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) for value in expected_hashes.values()), "missing expected canonical content hashes")
    snapshots = observation["snapshots"]
    require(len(snapshots) >= 3, "need pre-commit, interrupted and replay observations")
    for snapshot in snapshots:
        actual = snapshot["effect_ids"]
        require(len(actual) == len(set(actual)), "duplicate canonical effect")
        require(set(actual) in (set(), expected), "partial canonical effects became observable")
        complete = set(actual) == expected
        require(snapshot["effect_hashes"] == (expected_hashes if complete else initial_hashes), "effect IDs concealed missing or changed canonical content")
        require(snapshot["receipt_complete"] is complete, "receipt disagrees with canonical effects")
        require(snapshot["outbox_complete"] is complete, "outbox disagrees with canonical effects")
    require(set(snapshots[0]["effect_ids"]) == set(), "missing clean pre-commit baseline")
    require(set(snapshots[-1]["effect_ids"]) == expected, "retry never completed")
    require(observation["replay_receipt"] == observation["first_complete_receipt"], "retry changed receipt")
    require(observation["stale_attempt_effect_ids"] == [], "stale attempt published")
    require(observation["stale_head_effect_ids"] == [], "stale desired head published")


def source_cas(observation: dict) -> None:
    accepted = observation["accepted"]
    retry = observation["identical_retry"]
    replacement = observation["replacement"]
    require(accepted == retry, "identical retry did not return original receipt")
    nonempty(accepted.get("operation_id"), "original operation identity")
    nonempty(accepted.get("desired_revision"), "accepted desired revision")
    generation = nonempty(accepted.get("generation"), "accepted generation")
    require(replacement["generation"] != generation, "replacement did not rotate head")
    require(replacement["published_revision"] == accepted["desired_revision"], "replacement concealed old publication")
    require(replacement["desired_revision"] != replacement["published_revision"], "pending replacement lost desired/published distinction")
    require(observation["conflicting_idempotency_outcome"] == "conflict", "idempotency collision accepted")
    require(observation["stale_acceptance_outcome"] == "conflict", "old expected head accepted")
    require(observation["stale_publication_outcome"] == "conflict", "old accepted head published")
    require(observation["competing_publication_winners"] == 1, "head published by multiple attempts")


def checkpoint(observation: dict) -> None:
    deliveries = observation["deliveries"]
    require(len(deliveries) >= 3, "missing failed delivery, restart and duplicate replay")
    require(deliveries[0]["success"] is False, "missing injected consumer failure")
    cursor = observation["initial_cursor"]
    seen: set[str] = set()
    for delivery in deliveries:
        require(delivery["cursor_before"] == cursor, "checkpoint lost across delivery/restart")
        after = delivery["cursor_after"]
        if delivery["success"]:
            require(after == delivery["event_cursor"], "successful event not acknowledged")
            if delivery["event_id"] in seen:
                require(delivery["new_effect_count"] == 0, "duplicate event duplicated effect")
            seen.add(delivery["event_id"])
        else:
            require(after == cursor, "failed consumer advanced durable checkpoint")
        cursor = after
    require(deliveries[1]["after_restart"] is True, "restart recovery not observed")
    require(deliveries[1]["success"] is True, "failed event was not retried successfully")
    require(deliveries[1]["event_id"] == deliveries[0]["event_id"], "restart skipped failed event")
    require(deliveries[2]["event_id"] == deliveries[1]["event_id"], "no duplicate delivery exercised")


def lifecycle(observation: dict, *, erase: bool, restore: bool = False) -> None:
    exclusive = set(observation["exclusive_ids"])
    independent = set(observation["independently_supported_ids"])
    require(bool(exclusive) and bool(independent), "need exclusive and independent support controls")
    for snapshot in observation["post_acceptance_visibility"]:
        visible = set(snapshot["visible_ids"])
        require(not exclusive & visible, "retracted/erased exclusive support remains visible")
        require(independent <= visible, "independent current support destroyed")
    require(len(observation["post_acceptance_visibility"]) >= 2, "need immediate and post-retry visibility")
    require(observation["delayed_attempt_outcome"] == "conflict", "delayed work revived source")
    require(observation["other_space_unchanged"] is True, "same external key changed another space")
    if not erase:
        return
    required_tables = {"source", "source_revision", "doc", "doc_revision", "doc_node", "node_embedding", "work_artifact", "work_item", "work_item_archive", "work_run", "work_run_archive", "assertion", "exclusive_graph", "knowledge_view_revision"}
    counts = observation["physical_payload_rows"]
    require(required_tables <= counts.keys(), "physical erasure inventory incomplete")
    require(all(type(count) is int and count == 0 for count in counts.values()), "payload remains physically stored")
    integer(observation.get("backup_retention_seconds"), "finite backup retention", 1)
    require(observation["retention_enforced"] is True, "backup retention only documented")
    require(observation["cleanup_state"] == "erased", "visibility suppression is not physical completion")
    require(observation["journal_payload_free"] is True, "erasure journal retained payload")
    nonempty(observation.get("processor_retention_boundary"), "processor retention boundary")
    if restore:
        require(observation["journal_survived_old_snapshot"] is True, "journal rolled back with snapshot")
        require(observation["ready_before_replay"] is False, "restored service ready before replay")
        require(observation["reads_before_replay"] == "unavailable", "restore exposed erased evidence")
        require(observation["replay_complete"] is True, "replay never completed")
        require(observation["restored_erased_payload_rows"] == 0, "old snapshot resurrected erased payload")


def provider_dispatch(observation: dict) -> None:
    required = {"query_embedding", "document_embedding", "extraction", "summary", "promotion_disambiguation", "truth_maintenance", "wiki_planner", "wiki_organizer", "wiki_writer", "wiki_verifier", "media_render", "identity_disambiguation", "support_verification"}
    bindings = observation["bindings"]
    require(len(bindings) >= 2, "need two named space bindings")
    identities = {(binding["provider"], binding["model"], binding["dimensions"]) for binding in bindings.values()}
    require(len(identities) == len(bindings), "space profiles are not independently configured")
    calls = observation["calls"]
    require(required <= {call["operation"] for call in calls}, "not every outbound model operation covered")
    for call in calls:
        binding = bindings.get(call["space"])
        require(binding is not None, "dispatch fell back to default space")
        if call["approval"] != "approved":
            require(call["outbound_count"] == 0, "missing/revoked binding dispatched")
            require(call["outcome"] == "forbidden_provider", "provider refusal not explicit")
        else:
            require(call["binding_id"] == binding["binding_id"], "wrong approved account/endpoint binding")
            require(call["profile_id"] == binding["profile_id"], "wrong model/profile/preprocessing")
        require(call["credential_in_receipt"] is False, "receipt includes credential")
    require(any(call["approval"] == "revoked" and call["retry"] is True for call in calls), "pinned retry revocation not exercised")


def semantic_provider(observation: dict) -> None:
    require(observation["provider_mode"] == "real", "stub/fake vectors cannot qualify semantic acceptance")
    require(observation["provider_request_receipts"], "missing actual provider dispatch provenance")
    require(observation["original_query_scores_passed"] is True, "shape/variance is not relevance")
    require(observation["negative_controls_passed"] is True, "missing negative semantic controls")
    require(observation["fake_vector_control_rejected"] is True, "fake-vector failure mode not detected")
    for vector in observation["vectors"]:
        require(len(vector) == observation["dimensions"], "provider dimension mismatch")
        require(all(type(value) in (int, float) and math.isfinite(value) for value in vector), "nonfinite vector")
        require(any(value != 0 for value in vector), "zero vector")
    require(len(observation["vectors"]) >= 2, "insufficient independent provider observations")


def public_errors(observation: dict) -> None:
    outcomes = {"unauthorized", "forbidden_space", "disabled_capability", "stale_revision", "processing", "unavailable", "zero_matches", "oversized", "invalid_request", "not_found", "provider_unapproved"}
    by_transport = observation["outcomes"]
    for transport in ("http", "mcp"):
        actual = by_transport[transport]
        require(outcomes <= actual.keys(), f"{transport}: error matrix incomplete")
        codes = [nonempty(actual[name]["code"], f"{transport}/{name} code") for name in outcomes]
        require(len(set(codes)) == len(codes), f"{transport}: distinct outcomes collapsed")
        for name, response in actual.items():
            require(response["success"] is (name == "zero_matches"), f"{transport}/{name}: misleading success")
            require(response["bytes"] <= observation["output_budget_bytes"], f"{transport}: output bound exceeded")
            require(response["duration_ms"] <= observation["timeout_budget_ms"], f"{transport}: timeout bound exceeded")
    integer(observation.get("output_budget_bytes"), "agreed output budget", 1)
    integer(observation.get("timeout_budget_ms"), "agreed timeout budget", 1)


def artifact(observation: dict, path: Path | None = None) -> None:
    require(re.fullmatch(r"[0-9a-f]{40}", observation["source_sha"]) is not None, "missing exact source SHA")
    require(re.fullmatch(r"[0-9a-f]{64}", observation["artifact_sha256"]) is not None, "missing artifact digest")
    require(observation["installed_artifact_sha256"] == observation["artifact_sha256"], "installed artifact drift")
    if path is not None:
        require(sha256(path.read_bytes()) == observation["artifact_sha256"], "local artifact bytes differ")
    required = {"clean_install", "version_provenance", "auth_provision", "http", "mcp", "named_spaces", "ready", "quiesce_export", "restart_persistence", "restore", "exclusive_writer", "sigterm_drain", "optional_standalone", "pdf_notices"}
    require(required <= observation["checks"].keys(), "installed artifact checks incomplete")
    require(all(observation["checks"][name] == "passed" for name in required), "artifact check skipped/failed")
    if observation["shape"] == "native-darwin-arm64":
        require(observation["host_os"] == "Darwin" and observation["host_arch"] == "arm64", "native artifact not run on Darwin ARM64")
    elif observation["shape"] == "container-pv":
        require(observation["persistent_volume_reused"] is True, "container restart used fresh store")
        require(observation["image_digest"] == f"sha256:{observation['artifact_sha256']}", "container image digest differs")
    else:
        raise Rejected("unsupported artifact shape")


def workload(observation: dict, contract: dict | None = None) -> None:
    """Evaluate a separately supplied, pre-run workload contract and raw timings.

    OB3's reported worker count is not a simultaneous-request requirement. The
    caller supplies the agreed schedule/budgets independently of observations.
    This evaluator checks captured data; it does not authenticate its producer.
    """
    require(observation["measured"] is True, "workload fixture is not a measurement")
    require(isinstance(contract, dict), "separate agreed workload contract required")
    require(contract["format"] == "recall-preview-workload/v1", "unknown workload contract")
    nonempty(contract["consumer_workload_reference"], "consumer workload reference")
    fields = ("request_count", "minimum_peak_in_flight", "p95_ms", "p99_ms",
              "request_timeout_ms", "response_budget_bytes", "duration_budget_ms")
    for field in fields:
        integer(contract.get(field), f"agreed {field}", 1)
    require(contract["minimum_peak_in_flight"] <= contract["request_count"], "impossible concurrency contract")
    spaces = contract["spaces"]
    require(isinstance(spaces, list) and len(spaces) >= 2
            and all(isinstance(space, str) and space for space in spaces)
            and len(set(spaces)) == len(spaces), "explicit distinct workload spaces required")
    digest = sha256(json.dumps(contract, sort_keys=True, separators=(",", ":"), allow_nan=False).encode())
    require(observation["workload_contract_sha256"] == digest, "captured workload contract differs")
    require(set(observation["sources_per_space"]) == set(spaces), "workload source spaces differ")
    for count in observation["sources_per_space"].values():
        integer(count, "sources per space", 50)
    rows = observation["requests"]
    require(isinstance(rows, list) and len(rows) == contract["request_count"], "request sample count differs")
    ids, seen_spaces, events, durations = set(), set(), [], []
    for row in rows:
        request_id = nonempty(row["id"], "request identity")
        require(request_id not in ids, "duplicate request sample")
        ids.add(request_id)
        require(row["space"] in spaces, "unexpected request space")
        seen_spaces.add(row["space"])
        start, end = row["started_ms"], row["ended_ms"]
        require(all(type(value) in (int, float) and math.isfinite(value) for value in (start, end))
                and 0 <= start < end <= contract["duration_budget_ms"], "invalid request timing")
        duration = end - start
        require(duration <= contract["request_timeout_ms"], "request timeout budget exceeded")
        integer(row["response_bytes"], "response bytes", 0)
        require(row["response_bytes"] <= contract["response_budget_bytes"], "response byte budget exceeded")
        require(row["oracle_verdict"] == "passed", "load response failed correctness")
        events.extend(((start, 1), (end, -1)))
        durations.append(duration)
    require(seen_spaces == set(spaces), "workload omitted named-space requests")
    current = peak = 0
    for _, delta in sorted(events):  # End-before-start: adjacent calls do not overlap.
        current += delta
        peak = max(peak, current)
    integer(observation["peak_in_flight"], "reported peak concurrency", 1)
    require(observation["peak_in_flight"] == peak, "reported concurrency differs from timings")
    require(peak >= contract["minimum_peak_in_flight"], "agreed overlap was not measured")
    values = sorted(durations)
    require(values[math.ceil(0.95 * len(values)) - 1] <= contract["p95_ms"], "p95 exceeded")
    require(values[math.ceil(0.99 * len(values)) - 1] <= contract["p99_ms"], "p99 exceeded")


CHECKS = {
    "publication": publication,
    "source_cas": source_cas,
    "checkpoint": checkpoint,
    "retract": lambda value: lifecycle(value, erase=False),
    "erase": lambda value: lifecycle(value, erase=True),
    "restore_replay": lambda value: lifecycle(value, erase=True, restore=True),
    "provider_dispatch": provider_dispatch,
    "semantic_provider": semantic_provider,
    "public_errors": public_errors,
    "artifact": artifact,
    "workload": workload,
}


def evaluate(kind: str, observation: dict, *, workload_contract: dict | None = None) -> dict:
    """Absent/malformed input is a failure, never a silently skipped passing case."""
    try:
        require(kind in CHECKS, "unknown oracle")
        if kind == "workload":
            workload(observation, workload_contract)
        else:
            CHECKS[kind](observation)
    except (Rejected, KeyError, TypeError, ValueError, AttributeError) as error:
        return {"oracle": kind, "verdict": "failed", "reason": str(error)}
    return {"oracle": kind, "verdict": "passed", "scope": "captured-observations-only"}
