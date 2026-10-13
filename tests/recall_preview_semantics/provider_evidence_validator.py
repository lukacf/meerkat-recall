"""Pure original preparation-provider accounting; no I/O, clock or model calls.

Input bytes belong to the caller's bounded immutable Mapping. Every referenced
record is independently hashed when read; the mapping is never materialized.
D owns the normative gates. This module does not qualify model quality, public
request-to-dispatch attribution, raw vector inventory, or a whole product.
"""
from __future__ import annotations

from collections.abc import Mapping
import hashlib
import json
import math
from pathlib import Path
import re
import types

MAX_PROVIDER_BYTES = 20 * 1024**3
MAX_RECORD_BYTES = 8 * 1024**2
MAX_REFS = 16384
SPACES = {"qualification-household", "qualification-business"}


def is_semantic140(allocation_sha256):
    return type(allocation_sha256) is str and allocation_sha256 == "e42df2638349fea37f94eb7f9ea79d2afad170a2d36a655869a40f25f83dcd01"


def semantic140():
    """Load only the successor's exact source snapshot; ignore cached imports."""
    path = Path(__file__).with_name("semantic140_expectations.py")
    raw = path.read_bytes()
    require(hashlib.sha256(raw).hexdigest() == "dbb7b64d9e30033574942f8c43fd3301f223088cc6bd8213cc006fc15d37721f",
            "dynamic_semantic140_expectations_source")
    module = types.ModuleType("independent_semantic140_expectations")
    exec(compile(raw, str(path), "exec", dont_inherit=True), module.__dict__)
    return module


def require(condition, code):
    if not condition:
        raise ValueError(code)


def fields(value, expected, code="dynamic_provider_fields"):
    require(type(value) is dict and set(value) == set(expected), code)
    return value


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def strict_json(raw):
    require(type(raw) is bytes, "dynamic_provider_original_bytes")
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "dynamic_provider_duplicate_json_key")
            result[key] = value
        return result
    def constant(_value):
        raise ValueError("dynamic_provider_nonfinite_json")
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs, parse_constant=constant)
        encode(value)
        return value
    except (UnicodeError, RecursionError, TypeError, OverflowError) as error:
        raise ValueError("dynamic_provider_invalid_json") from error


def exact(left, right):
    return type(left) is type(right) and encode(left) == encode(right)


def integer(value, minimum=0, maximum=2**63-1):
    return type(value) is int and minimum <= value <= maximum


def number(value):
    return type(value) in (int, float) and math.isfinite(value)


def checksum(value):
    require(type(value) is str and re.fullmatch(r"[a-f0-9]{64}", value) is not None,
            "dynamic_provider_exact_hash")
    return value


class Evidence:
    def __init__(self, original):
        require(isinstance(original, Mapping) and 0 < len(original) <= MAX_REFS,
                "dynamic_provider_original_mapping")
        self.raw, self.refs, self.total = original, {}, 0

    def read_hash(self, sha, kind, maximum=MAX_RECORD_BYTES, size=None):
        checksum(sha)
        require(type(kind) is str and kind and 0 < maximum <= MAX_RECORD_BYTES,
                "dynamic_provider_reference_kind")
        # This is the only lookup. No iteration, copied mapping, retained body,
        # cache warming or fallback to a path is performed by this validator.
        try:
            raw = self.raw[sha]
        except (KeyError, OSError, RuntimeError) as error:
            raise ValueError("dynamic_provider_capture_missing") from error
        require(type(raw) is bytes and len(raw) <= maximum and (size is None or len(raw) == size)
                and digest(raw) == sha, "dynamic_provider_original_hash_size")
        value = (kind, len(raw))
        require(sha not in self.refs or self.refs[sha] == value, "dynamic_provider_reference_alias")
        if sha not in self.refs:
            require(len(self.refs) < MAX_REFS and self.total + len(raw) <= MAX_PROVIDER_BYTES,
                    "dynamic_provider_capture_inventory_bound")
            self.refs[sha] = value
            self.total += len(raw)
        return raw

    def read(self, reference, kind, maximum=MAX_RECORD_BYTES):
        fields(reference, {"sha256", "bytes", "kind"}, "dynamic_provider_reference_fields")
        require(reference["kind"] == kind and integer(reference["bytes"], 0, maximum),
                "dynamic_provider_reference_kind_size")
        return self.read_hash(reference["sha256"], kind, maximum, reference["bytes"])

    def json(self, reference, kind, maximum=MAX_RECORD_BYTES):
        return strict_json(self.read(reference, kind, maximum))


DISPATCH_FIELDS = {"id", "backend", "space", "operation", "model", "input_bound", "output_cap", "reserved",
    "request_hash", "forwarded_hash", "transforms", "started", "finished", "outcome", "status", "response_hash",
    "response_bytes", "usage_input", "usage_output", "requested_output_cap"}
LEDGER_FIELDS = {"format", "manifest_sha256", "halted", "reserved_nano_usd", "dispatches", "raw_capture", "provider_quality_acceptance"}


def provider_ledger(value, context):
    successor = is_semantic140(context["anchor"]["allocation_sha256"])
    attempt_cap = semantic140().PHYSICAL_ATTEMPTS if successor else 2100
    money_cap = semantic140().MAXIMUM_NANO_USD if successor else 3_000_000_000
    fields(value, LEDGER_FIELDS)
    require(value["format"] == "recall-gateway-receipt/v1" and value["manifest_sha256"] == context["anchor"]["manifest_sha256"]
            and value["halted"] is None and value["raw_capture"] == "private-synthetic-bodies-v1"
            and value["provider_quality_acceptance"] is False and type(value["dispatches"]) is list
            and len(value["dispatches"]) <= attempt_cap, "dynamic_provider_original_ledger")
    total = 0
    for index, row in enumerate(value["dispatches"], 1):
        fields(row, DISPATCH_FIELDS)
        require(type(row["id"]) is int and row["id"] == index and row["backend"] in ("elephant-db", "surreal-db")
                and row["space"] in SPACES and row["operation"] == "embedding" and type(row["reserved"]) is int and row["reserved"] == 1064960
                and row["outcome"] in ("complete", "upstream_error") and integer(row["status"], 100, 599)
                and integer(row["response_bytes"], 0, 8 * 1024**2)
                and integer(row["input_bound"], 1, 8192) and row["output_cap"] == row["requested_output_cap"] == 0
                and type(row["output_cap"]) is type(row["requested_output_cap"]) is int
                and row["transforms"] == "", "dynamic_provider_original_dispatch")
        require(all(row[key] is None or integer(row[key], 0, 8192) for key in ("usage_input", "usage_output"))
                and row["usage_output"] in (None, 0), "dynamic_provider_ledger_usage_bounds")
        for key in ("request_hash", "forwarded_hash", "response_hash"):
            checksum(row[key])
        for key in ("started", "finished"):
            require(type(row[key]) in (int, float) and math.isfinite(row[key]), "dynamic_provider_dispatch_clock")
        require(context["anchor"]["clock"]["wall_start"] <= row["started"] <= row["finished"]
                <= context["anchor"]["clock"]["deadline_wall"], "dynamic_provider_whole_dispatch_clock")
        total += row["reserved"]
    require(type(value["reserved_nano_usd"]) is int and value["reserved_nano_usd"] == total <= money_cap,
            "dynamic_provider_no_refund_reservations")
    return value["dispatches"]


def provider_transport(capture, response_raw, phase):
    transport = fields(capture.get("transport"), {"format", "started_mono", "request_started_mono",
        "deadline_mono", "read_completed_mono", "response_headers", "eof_observed", "remaining_length",
        "chunked", "response_closed"}, "dynamic_provider_transport_fields")
    require(transport["format"] == "recall-provider-transport/v2", "dynamic_provider_transport_format")
    for key in ("started_mono", "request_started_mono", "deadline_mono", "read_completed_mono"):
        require(number(transport[key]), "dynamic_provider_transport_original_clock")
    start, request, end, deadline = (transport[key] for key in
        ("started_mono", "request_started_mono", "read_completed_mono", "deadline_mono"))
    require(phase["phase_start"]["mono"] <= start <= request <= end < deadline
            and deadline <= start + 120 and deadline <= phase["deadline_mono"],
            "dynamic_provider_transport_original_deadline")
    headers = fields(transport["response_headers"],
                     {"content_length", "transfer_encoding", "content_encoding", "content_type"})
    for values in headers.values():
        require(type(values) is list and len(values) <= 1
                and all(type(value) is str and len(value.encode("utf-8")) <= 4096 for value in values),
                "dynamic_provider_transport_duplicate_or_invalid_header")
    length, transfer, encoding, content_type = (headers[key] for key in
        ("content_length", "transfer_encoding", "content_encoding", "content_type"))
    require(not (length and transfer) and (not encoding or encoding == ["identity"])
            and capture.get("content_type") == (content_type[0] if content_type else "application/octet-stream"),
            "dynamic_provider_transport_header_conflict")
    # This cohort admits only non-streaming embedding JSON. A transport EOF
    # cannot turn an unexpected event stream into a completed embedding reply.
    require(capture["content_type"].split(";", 1)[0].strip().lower() == "application/json",
            "dynamic_provider_embedding_json_content_type")
    require(transport["eof_observed"] is True and transport["response_closed"] is True
            and type(transport["chunked"]) is bool,
            "dynamic_provider_transport_original_eof")
    remaining = transport["remaining_length"]
    if length:
        require(re.fullmatch(r"[0-9]+", length[0]) is not None
                and int(length[0]) == len(response_raw) <= MAX_RECORD_BYTES
                and transport["chunked"] is False and type(remaining) is int and remaining == 0,
                "dynamic_provider_transport_content_length")
    elif transfer:
        require(transfer[0].strip().lower() == "chunked" and transport["chunked"] is True
                and (remaining is None or type(remaining) is int and remaining == 0),
                "dynamic_provider_transport_chunked")
    else:
        require(transport["chunked"] is False and remaining is None,
                "dynamic_provider_transport_close_delimited")
    return start, end


def provider_usage(row, response_raw):
    # Usage is advisory and cannot release the original durable reservation.
    # Recompute the gateway's embedding-only observation from the actual body.
    try:
        response = strict_json(response_raw)
    except ValueError:
        response = None
    require(type(response) is dict, "dynamic_provider_original_embedding_json_response")
    require(not (type(response) is dict and response.get("status") in ("incomplete", "failed", "cancelled")),
            "dynamic_provider_explicit_incomplete")
    require(row["outcome"] == ("upstream_error" if row["status"] >= 300 else "complete"),
            "dynamic_provider_original_outcome")
    usage = response.get("usage") if type(response) is dict else None
    original = None
    if type(usage) is dict:
        inputs, outputs = usage.get("input_tokens", usage.get("prompt_tokens")), usage.get("output_tokens", 0)
        if integer(inputs) and integer(outputs):
            original = (inputs, outputs)
    if original is None:
        require(row["usage_input"] is None and row["usage_output"] is None,
                "dynamic_provider_unknown_usage_not_fabricated")
    else:
        require(type(row["usage_input"]) is int and type(row["usage_output"]) is int
                and (row["usage_input"], row["usage_output"]) == original
                and original[0] <= row["input_bound"] and original[1] == 0,
                "dynamic_provider_actual_usage_bound")


def provider_dispatch_original(record, raw_files, evidence, identity, configurations_by_name, phase, events, capture_bounds):
    fields(record, {"ledger", "binding", "send_intent"})
    row = record["ledger"]
    require(type(record["send_intent"]) is int and record["send_intent"] == 1, "dynamic_provider_original_send_intent")
    binding = fields(record["binding"], {"identity", "scope"})
    scope = fields(binding["scope"], {"backend", "space", "operation", "model", "dimensions", "binding_id",
                                      "connection_id", "profile_id", "credential_generation"})
    require(scope["backend"] == row["backend"] == identity["backend"] and scope["space"] == row["space"]
            and scope["operation"] == row["operation"] == "embedding" and scope["model"] == row["model"],
            "dynamic_provider_original_scope")
    actual = configurations_by_name[row["space"]]
    processing = actual["processing"]
    approved = processing["bindings"]["document_embedding"]
    profile = processing["profiles"][approved["profile_id"]]
    connection = processing["connections"][profile["connection_id"]]
    require(actual["space_id"] == row["space"]
            and approved["profile_id"] == processing["active_embedding_profile_id"] == profile["id"]
            and profile["connection_id"] == connection["id"]
            and scope["connection_id"] == connection["id"] and scope["profile_id"] == profile["id"]
            and scope["credential_generation"] == connection["credential_generation"]
            and scope["model"] == profile["model"] and type(scope["dimensions"]) is int
            and scope["dimensions"] == profile["dimensions"]
            and approved["revoked"] is False and connection["revoked"] is False
            and connection["provider"] == "openai"
            and type(scope["binding_id"]) is str and re.fullmatch(r"[A-Za-z0-9_]{1,128}", scope["binding_id"])
            and connection["credential_ref"] == "RECALL_GATEWAY_" + scope["binding_id"],
            "dynamic_provider_current_binding")
    expected_profile = ("text-embedding-3-small", 1536) if row["space"] == "qualification-household" else ("text-embedding-3-large", 3072)
    require((scope["model"], scope["dimensions"]) == expected_profile, "dynamic_provider_named_space_profile")
    authority = fields(binding["identity"], {"account_id", "credential_generation", "configuration_sha256", "bindings_sha256",
        "allocation_sha256", "contract_sha256", "corpus_sha256", "preparation_contract_sha256", "typed_delta_sha256"})
    for key, original in (("configuration_sha256", identity["outer_configuration_sha256"]),
                          ("allocation_sha256", identity["provider_allocation_sha256"]),
                          ("contract_sha256", identity["workload_contract_sha256"]),
                          ("corpus_sha256", identity["corpus_sha256"]),
                          ("preparation_contract_sha256", identity["preparation_contract_sha256"]),
                          ("typed_delta_sha256", identity["typed_delta_sha256"])):
        require(authority[key] == original, "dynamic_provider_original_allocation_identity")
    require(authority["account_id"] == connection["account_id"]
            and authority["credential_generation"] == connection["credential_generation"], "dynamic_provider_original_account")
    fields(raw_files, {"request.original.json", "request.forwarded.json", "intent.json", "response.body", "capture.json"})
    # References resolve by original digest; filenames preserve original and
    # forwarded roles even when both legitimately contain identical bytes.
    def original(name, kind, maximum):
        return evidence.read_hash(checksum(raw_files[name]), kind, maximum)
    request_raw = original("request.original.json", "provider-request", 131072)
    forward_raw = original("request.forwarded.json", "provider-request", 131072)
    intent = strict_json(original("intent.json", "provider-dispatch-intent", 16384))
    capture = strict_json(original("capture.json", "provider-capture", 16384))
    response_raw = original("response.body", "provider-response", 8 * 1024**2)
    fields(intent, {"format", "dispatch_id", "original_sha256", "forwarded_sha256", "transforms",
                    "transport_response_byte_cap", "credential_headers_retained", "binding"})
    require(intent["format"] == "recall-private-synthetic-capture/v1" and type(intent["dispatch_id"]) is int
            and intent["dispatch_id"] == row["id"] and intent["original_sha256"] == row["request_hash"] == digest(request_raw)
            and intent["forwarded_sha256"] == row["forwarded_hash"] == digest(forward_raw)
            and intent["transforms"] == [] and intent["transport_response_byte_cap"] == 8 * 1024**2
            and intent["credential_headers_retained"] is False and exact(intent["binding"], binding),
            "dynamic_provider_original_intent")
    required = {"complete", "status", "response_sha256", "response_bytes", "provider_quality_acceptance"}
    require(type(capture) is dict and required <= set(capture) <= required | {"content_type", "transport"}
            and capture["complete"] is True and capture["provider_quality_acceptance"] is False
            and capture["status"] == row["status"] and capture["response_sha256"] == row["response_hash"] == digest(response_raw)
            and type(capture["response_bytes"]) is int and capture["response_bytes"] == row["response_bytes"] == len(response_raw),
            "dynamic_provider_original_complete_capture")
    request = strict_json(request_raw)
    require(type(request) is dict and {"model", "input"} <= set(request) <= {"model", "input", "dimensions", "encoding_format"}
            and request["model"] == profile["model"] and request.get("dimensions", profile["dimensions"]) == profile["dimensions"]
            and request.get("encoding_format", "float") == "float" and forward_raw == encode(request),
            "dynamic_provider_original_embedding_request")
    values = [request["input"]] if type(request["input"]) is str else request["input"]
    require(type(values) is list and 1 <= len(values) <= 64 and all(type(item) is str and item for item in values)
            and len(encode(request)) + 64 * len(values) == row["input_bound"] <= 8192,
            "dynamic_provider_original_embedding_bounds")
    require(phase["phase_start"]["wall"] <= row["started"] <= row["finished"]
            <= phase["phase_start"]["wall"] + 3600, "dynamic_provider_original_phase_dispatch_clock")
    start, end = provider_transport(capture, response_raw, phase)
    require(capture_bounds[0] <= start * 10**9 <= end * 10**9 <= capture_bounds[1],
            "dynamic_provider_original_capture_interval")
    provider_usage(row, response_raw)
    # Equal wall samples cannot prove physical ordering. Keep the full reserved
    # interval for the count; monotonic transport facts independently bound reads.
    events["reservation"].extend(((row["started"], 1, row["id"]), (row["finished"], -1, row["id"])))
    events["transport"].extend(((start, 1, row["id"]), (end, -1, row["id"])))
    return authority


def _provider_accounting(reference, evidence, identity, context, wrapper, observed, configurations_by_name, capture_bounds):
    bundle = evidence.json(reference, "provider-accounting", 8 * 1024**2)
    fields(bundle, {"format", "begin", "close", "baseline", "phase_clock", "sealed", "capture_inventory", "dispatch_records"})
    require(bundle["format"] == "recall-dynamic-provider-accounting/v2", "dynamic_provider_bundle_format")
    begin = evidence.json(bundle["begin"], "provider-cohort-begin", 65536)
    close = evidence.json(bundle["close"], "provider-cohort-close", 65536)
    fields(begin, {"format", "cohort_id", "backend", "phase", "baseline_sha256", "allocation_sha256", "authority"})
    require(begin["format"] == "recall-exclusive-cohort-begin/v1" and integer(begin["cohort_id"], 1, 4)
            and begin["backend"] == identity["backend"] and begin["phase"] == "preparation"
            and begin["cohort_id"] == {"elephant-db": 1, "surreal-db": 3}[identity["backend"]]
            and begin["allocation_sha256"] == identity["provider_allocation_sha256"]
            and begin["authority"] == "exclusive_gateway_cohort" and exact(begin, observed["provider_cohort_begin"]),
            "dynamic_provider_original_begin")
    fields(close, {"format", "verdict", "cohort_id", "before_sha256", "after_sha256", "capture_inventory_sha256",
                   "sealed_receipt_sha256", "physical_dispatch_reservations", "reserved_nano_usd", "unsettled_dispatches",
                   "per_request_association", "admission_closed"})
    require(close["format"] == "recall-exclusive-cohort-accounting/v1" and close["verdict"] == "passed"
            and type(close["cohort_id"]) is int and close["cohort_id"] == begin["cohort_id"]
            and close["before_sha256"] == begin["baseline_sha256"] and close["admission_closed"] is True
            and type(close["unsettled_dispatches"]) is int and close["unsettled_dispatches"] == 0
            and close["per_request_association"] == "unmet" and exact(close, observed["provider_accounting"]),
            "dynamic_provider_original_closed_cohort")
    baseline_raw = evidence.read(bundle["baseline"], "provider-baseline", 8 * 1024**2)
    require(digest(baseline_raw) == begin["baseline_sha256"], "dynamic_provider_original_baseline")
    baseline = provider_ledger(strict_json(baseline_raw), context)
    if begin["cohort_id"] == 1:
        require(not baseline, "dynamic_provider_first_cohort_baseline")
    else:
        # Original fixed ordering puts both elephant-db cohorts before native
        # preparation. This bounds their retained prefix without certifying its
        # separate preparation or measurement results.
        successor = is_semantic140(identity["provider_allocation_sha256"])
        baseline_cap = semantic140().FIRST_BACKEND_ATTEMPTS if successor else 1050
        space_cap = semantic140().FIRST_BACKEND_ATTEMPTS_PER_SPACE if successor else 525
        require(2 <= len(baseline) <= baseline_cap and all(row["backend"] == "elephant-db" for row in baseline)
                and all(sum(row["space"] == space for row in baseline) <= space_cap for space in SPACES),
                "dynamic_provider_original_prior_cohort_prefix")
    sealed_raw = evidence.read(bundle["sealed"], "provider-sealed", 8 * 1024**2)
    require(digest(sealed_raw) == close["sealed_receipt_sha256"], "dynamic_provider_original_sealed_hash")
    sealed = fields(strict_json(sealed_raw), {"before_sha256", "after", "capture_inventory", "observation_sha256", "identity"})
    require(sealed["before_sha256"] == begin["baseline_sha256"]
            and digest(encode(sealed["after"])) == close["after_sha256"]
            and digest(encode(sealed["capture_inventory"])) == close["capture_inventory_sha256"]
            and sealed["observation_sha256"] == digest(encode({key: value for key, value in observed.items() if key != "provider_accounting"})),
            "dynamic_provider_sealed_original_identity")
    after = provider_ledger(sealed["after"], context)
    # A settled liability is not successful qualification. Retain and validate
    # every failed reservation above, then refuse the qualifying result.
    require(all(row["outcome"] == "complete" and 200 <= row["status"] < 300 for row in after),
            "dynamic_provider_failed_dispatch_unqualified")
    require(exact(after[:len(baseline)], baseline) and len(after) > len(baseline), "dynamic_provider_original_ledger_prefix")
    entries = after[len(baseline):]
    require(0 < len(entries) <= 450 and integer(close["physical_dispatch_reservations"], 1, 450)
            and close["physical_dispatch_reservations"] == len(entries)
            and type(close["reserved_nano_usd"]) is int and close["reserved_nano_usd"] == len(entries) * 1064960,
            "dynamic_provider_original_cohort_reservations")
    phase = evidence.json(bundle["phase_clock"], "provider-phase-clock", 131072)
    fields(phase, {"format", "cohort_id", "backend", "phase", "state", "run", "phase_start", "deadline_mono",
                   "time_cap_seconds", "closure_timestamp"})
    require(phase["format"] == "recall-original-cohort-clock/v1" and phase["cohort_id"] == begin["cohort_id"]
            and phase["backend"] == identity["backend"] and phase["phase"] == "preparation" and phase["state"] == "closed"
            and type(phase["time_cap_seconds"]) is int and phase["time_cap_seconds"] == 3600
            and phase["closure_timestamp"] is None, "dynamic_provider_original_phase_clock")
    fields(phase["run"], {"wall_start", "mono_start", "boot_id"})
    fields(phase["phase_start"], {"wall", "mono"})
    clock = context["anchor"]["clock"]
    require(type(phase["cohort_id"]) is int
            and all(number(value) and value > 0 for value in (*phase["phase_start"].values(), phase["deadline_mono"]))
            and clock["wall_start"] <= phase["phase_start"]["wall"] < clock["deadline_wall"],
            "dynamic_provider_phase_finite_clock")
    require(exact(phase["run"], {key: clock[key] for key in ("wall_start", "mono_start", "boot_id")})
            and clock["mono_start"] <= phase["phase_start"]["mono"] <= phase["deadline_mono"] <= clock["deadline_mono"]
            and phase["deadline_mono"] - phase["phase_start"]["mono"] <= 3600
            and phase["phase_start"]["mono"] * 10**9 <= capture_bounds[0] <= capture_bounds[1] <= phase["deadline_mono"] * 10**9
            and capture_bounds[1] <= wrapper["observed_monotonic_ns"] < clock["deadline_mono"] * 10**9,
            "dynamic_provider_original_phase_deadline")
    inventory_raw = evidence.read(bundle["capture_inventory"], "provider-capture-inventory", 8 * 1024**2)
    require(digest(inventory_raw) == close["capture_inventory_sha256"]
            and exact(strict_json(inventory_raw), sealed["capture_inventory"]), "dynamic_provider_original_capture_inventory")
    inventory = strict_json(inventory_raw)
    require(type(inventory) is list, "dynamic_provider_original_inventory_list")
    records = evidence.json(bundle["dispatch_records"], "provider-dispatch-records", 8 * 1024**2)
    require(type(records) is list and len(records) == len(inventory) == len(entries), "dynamic_provider_dispatch_inventory_count")
    events, counts = {"reservation": [], "transport": []}, {name: 0 for name in configurations_by_name}
    for expected, record, capture in zip(entries, records, inventory):
        fields(capture, {"dispatch_id", "files"})
        require(exact(record["ledger"], expected) and type(capture["dispatch_id"]) is int
                and capture["dispatch_id"] == expected["id"] and expected["space"] in counts,
                "dynamic_provider_original_dispatch_join")
        authority = provider_dispatch_original(record, capture["files"], evidence, identity, configurations_by_name, phase, events, capture_bounds)
        require(exact(authority, sealed["identity"]) and authority["bindings_sha256"] == context["anchor"]["bindings_sha256"],
                "dynamic_provider_original_controller_binding")
        counts[expected["space"]] += 1
    require(all(0 < count <= 225 for count in counts.values()), "dynamic_provider_per_space_original_cap")
    for original_events in events.values():
        active = 0
        # At one timestamp process starts before ends. This is conservative when
        # low-resolution samples cannot prove release-before-next-reserve.
        for _, delta, _identifier in sorted(original_events, key=lambda event: (event[0], -event[1], event[2])):
            active += delta
            require(0 <= active <= 2, "dynamic_provider_original_concurrency")
        require(active == 0, "dynamic_provider_original_unsettled")
    return {"begin": begin, "close": close, "phase_clock": phase}


def validate_provider_accounting(reference, raw_evidence, identity, controller_context,
                                 current_configurations, original_preparation_observation, capture_bounds):
    """Verify retained original cohort facts; never execute or qualify a product.

    The caller independently authenticates the controller/static review/current
    configuration. This seam cross-binds that original context and every raw
    provider artifact; it never derives authority from a reported verdict.
    """
    try:
        wrapper = fields(controller_context, {"context", "raw_sha256", "raw_bytes", "observed_wall",
                                              "observed_monotonic_ns"})
        raw = wrapper["raw_bytes"]
        require(type(raw) is bytes and len(raw) <= 131072 and digest(raw) == wrapper["raw_sha256"]
                and exact(strict_json(raw), wrapper["context"]), "dynamic_provider_original_controller")
        context = wrapper["context"]
        anchor = context["anchor"]
        require(context["anchor_sha256"] == digest(encode(anchor))
                and anchor["allocation_sha256"] == identity["provider_allocation_sha256"]
                and anchor["configuration_sha256"] == identity["outer_configuration_sha256"],
                "dynamic_provider_controller_allocation")
        clock = fields(anchor["clock"], {"wall_start", "mono_start", "boot_id", "deadline_mono",
                                          "deadline_wall", "time_cap_seconds"})
        require(all(number(clock[key]) and clock[key] > 0 for key in
                    ("wall_start", "mono_start", "deadline_mono", "deadline_wall"))
                and type(clock["time_cap_seconds"]) is int and clock["time_cap_seconds"] == 7800
                and clock["deadline_mono"] == clock["mono_start"] + 7800
                and clock["deadline_wall"] == clock["wall_start"] + 7800
                and type(clock["boot_id"]) is str and clock["boot_id"]
                and integer(wrapper["observed_monotonic_ns"], 1)
                and clock["mono_start"] * 10**9 <= wrapper["observed_monotonic_ns"] < clock["deadline_mono"] * 10**9
                and number(wrapper["observed_wall"])
                and clock["wall_start"] <= wrapper["observed_wall"] < clock["deadline_wall"],
                "dynamic_provider_original_whole_clock")
        successor = is_semantic140(identity["provider_allocation_sha256"])
        if successor:
            semantic140().require_identity(identity)
            require(anchor.get("format") == "recall-gateway-launch-anchor/v2"
                    and anchor.get("profile_id") == semantic140().PROFILE_ID
                    and anchor.get("corpus_sha256") == semantic140().FROZEN_INPUTS["corpus"],
                    "dynamic_provider_semantic140_anchor")
        else:
            require(anchor.get("profile_id") != "semantic140-v1",
                    "dynamic_provider_mixed_workload_profile")
        expected_caps = semantic140().caps() if successor else {"physical_dispatches": 2100, "maximum_nano_usd": 3_000_000_000}
        require(exact(anchor["caps"], expected_caps), "dynamic_provider_original_whole_caps")
        require(type(current_configurations) is dict and set(current_configurations) == SPACES
                and type(original_preparation_observation) is dict
                and type(capture_bounds) is tuple and len(capture_bounds) == 2
                and all(integer(value, 1) for value in capture_bounds), "dynamic_provider_input_scope")
        return _provider_accounting(reference, Evidence(raw_evidence), identity, context, wrapper,
                                    original_preparation_observation, current_configurations, capture_bounds)
    except (KeyError, IndexError, TypeError, AttributeError, OverflowError, RecursionError) as error:
        raise ValueError("dynamic_provider_malformed_original_evidence") from error
