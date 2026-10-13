"""Pure independent consistency oracle for original OB3 outage receipt bytes.

Caller supplies the independently frozen recipe/baseline bytes. This never runs
OB3, contacts Recall, or treats the producer's verdict as evidence of success.
"""

from __future__ import annotations

from datetime import datetime
import hashlib
import json
import re

MAX_BYTES = 4 * 1024 * 1024
MAX_U64 = 2**64 - 1
REASONS = frozenset(("unavailable", "timeout", "refused", "transport",
                     "stale_revision", "partial", "forbidden", "error",
                     "no_expected_sources"))
POR = frozenset(("failed", "degraded", "fetched", "inserted"))
PUSH = frozenset(("pushed", "current", "conflicts", "errors", "tombstoned",
                  "payload_too_large", "retracted", "retract_conflicts",
                  "retract_errors", "retract_skipped_incomplete", "retract_cap_exceeded"))
PRODUCER_CHECKS = frozenset(("reads_positive", "participating_equals_expected",
                           "accounting_total", "accounting_per_worker",
                           "all_requested_completed", "por_counts_equal_baseline",
                           "cross_total", "participants_match_reads", "no_duplicate_participants"))


class Incomplete(ValueError):
    """Missing, malformed, unknown, or unbound original evidence."""


def require(condition: bool, code: str) -> None:
    if not condition:
        raise Incomplete(code)


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def decode(raw: bytes) -> dict:
    require(type(raw) is bytes and 0 < len(raw) <= MAX_BYTES, "raw_size_or_type")

    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "duplicate_json_key")
            result[key] = value
        return result

    def nonfinite(_):
        raise Incomplete("nonfinite_json")

    try:
        value = json.loads(raw, object_pairs_hook=pairs, parse_constant=nonfinite)
    except (UnicodeError, json.JSONDecodeError, RecursionError, ValueError) as error:
        if isinstance(error, Incomplete):
            raise
        raise Incomplete("invalid_json") from None
    require(type(value) is dict, "object_required")
    return value


def fields(value, expected: set | frozenset) -> dict:
    require(type(value) is dict and set(value) == expected, "object_fields")
    return value


def count(value) -> int:
    require(type(value) is int and 0 <= value <= MAX_U64, "counter_type_or_range")
    return value


def text(value) -> str:
    require(type(value) is str, "identity_type_or_size")
    try:
        length = len(value.encode())
    except UnicodeError:
        raise Incomplete("identity_utf8") from None
    require(0 < length <= 512, "identity_type_or_size")
    return value


def identities(value) -> list[str]:
    require(type(value) is list and len(value) <= 4096, "worker_identity_list")
    values = [text(item) for item in value]
    require(len(values) == len(set(values)), "duplicate_worker_identity")
    return values


def counters(value, names, *, sparse=False) -> dict[str, int]:
    require(type(value) is dict, "counter_object")
    require(set(value) <= names if sparse else set(value) == names, "unknown_or_missing_counter")
    return {key: count(value.get(key, 0)) for key in names}


def timestamp(value) -> datetime:
    text(value)
    require(re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})", value)
            is not None, "timestamp_shape")
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise Incomplete("timestamp_value") from None


def evaluate_receipt_bytes(raw_receipt: bytes, raw_expectations: bytes) -> dict:
    """Return only a scoped verdict; neither input can authorize execution.

    The expectation bytes must originate in the independently reviewed recipe,
    original controller interval and baseline, never the candidate receipt.
    """
    result = {"format": "recall-ob3-outage-oracle/v1", "scope": "external_receipt_consistency",
              "product_acceptance": False, "producer_verdict_is_authority": False,
              "verdict": "unmet", "checks": {}, "errors": [],
              "receipt_sha256": digest(raw_receipt) if type(raw_receipt) is bytes and len(raw_receipt) <= MAX_BYTES else None,
              "expectations_sha256": digest(raw_expectations) if type(raw_expectations) is bytes and len(raw_expectations) <= MAX_BYTES else None,
              "proof_limits": ["Caller must bind original independently reviewed recipe, baseline, source and capture custody.",
                               "Receipt counters alone do not prove physical sends, process cleanup, artifact identity or production acceptance."]}
    try:
        receipt, expected = decode(raw_receipt), decode(raw_expectations)
        fields(expected, {"format", "run_id", "ob3_git_sha", "recall_contract_version",
                          "recall_endpoint_state", "batch_kind", "requested_worker_ids",
                          "expected_participating_workers", "por_baseline", "maximum_logical_reads",
                          "earliest_started_at", "latest_finished_at"})
        require(expected["format"] == "recall-ob3-outage-expectations/v1", "expectation_version")
        fields(receipt, {"receipt_version", "run_id", "ob3_git_sha", "recall_contract_version",
                         "recall_endpoint_state", "batch", "expected_participating_workers",
                         "participating_workers", "read_total", "workers", "por_push", "verdict"})
        # The original v1 is retained as historical bytes, never reinterpreted
        # as a v2 receipt merely because independent counters happen to balance.
        require(receipt["receipt_version"] == "ob3-recall-outage-2", "receipt_version")
        requested = set(identities(expected["requested_worker_ids"]))
        participants = set(identities(expected["expected_participating_workers"]))
        require(bool(participants) and participants <= requested, "independent_participant_set")
        require(expected["recall_contract_version"] == "recall-preview-1", "contract_version")
        require(expected["recall_endpoint_state"] in ("down", "refused", "timeout", "unavailable"), "outage_state")
        require(expected["batch_kind"] in ("review", "analysis"), "batch_kind")
        require(re.fullmatch(r"[0-9a-f]{40}", text(expected["ob3_git_sha"])) is not None, "original_source_identity")
        maximum = count(expected["maximum_logical_reads"])
        require(maximum > 0, "original_read_bound")
        earliest, latest = timestamp(expected["earliest_started_at"]), timestamp(expected["latest_finished_at"])
        require(earliest <= latest, "original_interval")
        declared_expected = set(identities(receipt["expected_participating_workers"]))
        declared_active = set(identities(receipt["participating_workers"]))
        batch = fields(receipt["batch"], {"kind", "requested_workers", "completed_workers", "failed_workers",
                                          "started_at", "finished_at"})
        batch_counts = {key: count(batch[key]) for key in ("requested_workers", "completed_workers", "failed_workers")}
        started, finished = timestamp(batch["started_at"]), timestamp(batch["finished_at"])
        total = fields(receipt["read_total"], {"reads", "evidence", "fallbacks", "size_retries"})
        reads, evidence, retries = count(total["reads"]), count(total["evidence"]), count(total["size_retries"])
        total_fallbacks = counters(total["fallbacks"], REASONS)
        workers = receipt["workers"]
        require(type(workers) is dict and len(workers) <= 4096, "worker_inventory")
        sums = {"reads": 0, "evidence": 0, **dict.fromkeys(REASONS, 0)}
        active = set()
        per_worker = True
        for identity, worker in workers.items():
            text(identity)
            fields(worker, {"reads", "evidence", "fallbacks"})
            wr, we = count(worker["reads"]), count(worker["evidence"])
            reasons = counters(worker["fallbacks"], REASONS, sparse=True)
            per_worker &= we + sum(reasons.values()) == wr
            if wr:
                active.add(identity)
            sums["reads"] += wr
            sums["evidence"] += we
            for reason, amount in reasons.items():
                sums[reason] += amount
        require(all(value <= MAX_U64 for value in sums.values()), "aggregate_overflow")
        push = fields(receipt["por_push"], {"baseline", "with_recall", "recall"})
        original_baseline = counters(expected["por_baseline"], POR)
        baseline, actual_por = counters(push["baseline"], POR), counters(push["with_recall"], POR)
        counters(push["recall"], PUSH)
        producer = fields(receipt["verdict"], {"status", "checks"})
        require(producer["status"] in ("PASS", "FAIL", "INCONCLUSIVE"), "producer_status")
        flags = fields(producer["checks"], PRODUCER_CHECKS)
        require(all(type(value) is bool for value in flags.values()), "producer_check_type")
        checks = {
            "original_identity": all(text(receipt[key]) == text(expected[key]) for key in
                                     ("run_id", "ob3_git_sha", "recall_contract_version", "recall_endpoint_state")),
            "original_batch_kind": batch["kind"] == expected["batch_kind"],
            "original_interval": earliest <= started <= finished <= latest,
            "all_requested_completed": batch_counts["requested_workers"] == len(requested)
                and batch_counts["completed_workers"] == len(requested) and batch_counts["failed_workers"] == 0,
            "independent_participants": declared_expected == participants == declared_active == active,
            "only_requested_workers": set(workers) <= requested,
            "positive_bounded_reads": 0 < reads <= maximum,
            "accounting_per_worker": per_worker,
            "accounting_total": evidence + sum(total_fallbacks.values()) == reads,
            "summed_worker_totals": sums["reads"] == reads and sums["evidence"] == evidence
                and all(sums[key] == total_fallbacks[key] for key in REASONS),
            "outage_used_no_evidence": evidence == 0,
            "configured_expected_sources": total_fallbacks["no_expected_sources"] == 0,
            "size_retry_bound": retries <= reads,
            "original_por_baseline": baseline == original_baseline == actual_por,
        }
        result.update(verdict="pass" if all(checks.values()) else "fail", checks=checks,
                      errors=[key for key, passed in checks.items() if not passed],
                      producer_reported_status=producer["status"])
    except Incomplete as error:
        result["errors"] = [str(error)]
    return result
