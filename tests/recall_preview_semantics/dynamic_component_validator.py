"""Pure validation of original dynamic evidence; never process or I/O authority.

D owns the normative contract and reviews this exact source before activation.
Callers capture original bytes and recheck live custody; no parsed producer flag
can replace the original commands, protocol, namespace and publication evidence.
"""
from __future__ import annotations

import base64
from collections.abc import Mapping
import datetime as dt
import hashlib
import ipaddress
import json
import math
from pathlib import Path, PurePosixPath
import re
import tomllib
import types
import uuid
from urllib.parse import quote

CONTRACT_SHA256 = "f358239b4bbe13d6798e0d4ac28ad9d6c7d6397fe7516825db003217d44dd32d"
PREDICATE_SHA256 = "f2ccd4a31e705716c65f319531f1d29e1347fe3cbe0bd06a90787f65fceb98f0"
REFERENCE_FIELDS = {"sha256", "bytes", "kind"}
IDENTITY_FIELDS = {"source_commit", "artifact_sha256", "binary_sha256", "runtime_sha256",
                   "paired_artifact_sha256", "outer_configuration_sha256", "backend", "endpoint", "run_nonce"}
RUNTIME_FIELDS = {"format", "identity", "bootstrap", "installed_offline", "actual_runtime_configuration",
                  "actual_space_configurations", "controller", "current_processes", "capture_inventory",
                  "command_inventory", "cleanup_obligations", "outer_launch"}
PREPARATION_FIELDS = {"format", "identity", "runtime_component", "observation", "capture_inventory",
                      "configuration_before", "configuration_after", "provider_cohort_begin", "provider_accounting",
                      "provider_accounting_channel", "controller", "original_intents", "immutable_revisions"}
PREPARATION_IDENTITIES = {"corpus_sha256", "workload_contract_sha256", "preparation_contract_sha256",
                          "typed_delta_sha256", "rendered_map_sha256", "provider_allocation_sha256", "preparation_nonce"}
STATIC_FIELDS = {"format", "reviewer", "approved", "contract_sha256", "predicate_document_sha256",
                 "identities", "input_refs", "helper_sha256", "controller_policy", "bootstrap_plans",
                 "runtime_recipe", "limits", "expires_at"}
DYNAMIC_STATIC_FIELDS = {"future_receipt_sha256", "actual_space_configurations_sha256",
                        "actual_runtime_configuration_sha256", "generated_configuration_id", "process_pid",
                        "capture_inventory_sha256", "prepared_fixture_sha256"}
PROCESS_FIELDS = {"pid", "ppid", "pgrp", "session", "start_ticks"}
HASH_FIELDS = {"manifest_sha256", "proposal_sha256", "corpus_sha256", "artifact_sha256", "paired_artifact_sha256",
               "runtime_sha256", "configuration_sha256", "gateway_sha256", "bindings_sha256", "bridge_sha256",
               "context_helper_sha256", "command_sha256"}
CLOCK_FIELDS = {"wall_start", "mono_start", "boot_id", "deadline_wall", "deadline_mono", "time_cap_seconds"}
ANCHOR_FIELDS = {"format", "run_nonce", "source_commit", "allocation_sha256", "input_inventory_sha256",
                 "clock", "caps", "parent", *HASH_FIELDS}
STATIC_ANCHOR_FIELDS = {"format", "run_nonce", "source_commit", "allocation_sha256", "caps",
                        *(HASH_FIELDS - {"manifest_sha256", "configuration_sha256"})}
CONTEXT_FIELDS = {"format", "anchor", "anchor_sha256", "process", "network", "observed_wall", "observed_mono", "captures"}
BOOTSTRAP_CHECKS = {"unauthenticated_readiness", "missing_grant", "named_config_readback", "ordinary_config_readback",
                    "ordinary_empty_inventory", "wrong_space", "bootstrap_data_denial", "read_only_write_denial",
                    "grant_readback", "grant_revocation", "grant_restoration", "ready", "final_empty_inventory"}
BOOTSTRAP_IDENTITIES = {"source_commit", "artifact_sha256", "paired_artifact_sha256", "runtime_sha256",
                        "configuration_sha256", "backend", "endpoint", "run_nonce"}
BOOTSTRAP_FIELDS = {"format", "status", "qualification_scope", "qualified", "product_acceptance", *BOOTSTRAP_IDENTITIES,
                    "authentication_enabled", "clean_persistent_store", "schema_explicitly_initialized", "schema_operation",
                    "launch_context_sha256", "isolation_sha256", "plan_sha256", "allocation_started_monotonic_ns",
                    "started_monotonic_ns", "observed_monotonic_ns", "original_deadline_monotonic_ns", "whole_deadline_monotonic_ns",
                    "space_requests_sha256", "observed_configurations_sha256", "processes", "bootstrap_process",
                    "process_commands", "checks", "captures", "configurations", "token_files", "runtime_config_sha256"}
INSTALLED_CASES = ["verified_installed_binary", "absent_service", "bootstrap_unready", "fresh_cli_auth",
                   "runtime_initialized_without_workers", "two_named_spaces", "http_auth_denied", "mcp_auth_denied",
                   "wrong_space_denied", "mcp_positive", "grant_revocation", "source_acceptance_and_idempotence",
                   "pending_is_not_complete", "input_bounds", "named_export", "replacement_accepted",
                   "restore_original_heads", "graceful_restart", "read_only_capabilities"]
RECIPE = {"transient_retry_max_attempts": 3, "embedding_max_total_http_attempts": 3,
          "document_tree_create_summaries": False}
CORE_SCOPES = {"read:status", "read:space_config", "read:source", "write:source", "read:doc",
               "read:doc:metadata", "read:doc:raw", "write:doc", "read:entity", "read:assertion", "read:relationship"}
CONDITIONAL_SCOPES = {"read:reprocess_job", "write:reprocess_job", "manage:wiki"}
BOOTSTRAP_SCOPES = ["read:space_config", "write:space_config", "manage:grant", "manage:provider"]
PROOF_LIMITS = ["public profile markers and completed publication are not raw vector inventory",
                "no fabricated private publication manifest or completion certificate",
                "no per-request physical provider attribution when G reports unmet",
                "no semantic relevance or measured load acceptance from preparation",
                "current persistent service terminal cleanup remains an original owned obligation",
                "no full product acceptance from either component"]


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


def fields(value, expected, code="dynamic_fields"):
    require(type(value) is dict and set(value) == set(expected), code)
    return value


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def strict_json(raw):
    require(type(raw) is bytes, "dynamic_original_bytes")
    def pairs(items):
        value = {}
        for key, item in items:
            require(key not in value, "dynamic_duplicate_json_key")
            value[key] = item
        return value
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs)
        encode(value)
        return value
    except (ValueError, UnicodeError, RecursionError, TypeError) as error:
        raise ValueError("dynamic_invalid_json") from error


def exact(left, right):
    return type(left) is type(right) and encode(left) == encode(right)


def integer(value, minimum=0, maximum=2**63 - 1):
    return type(value) is int and minimum <= value <= maximum


def checksum(value, width=64):
    require(type(value) is str and re.fullmatch(r"[a-f0-9]{" + str(width) + r"}", value) is not None,
            "dynamic_exact_hash")
    return value


def timestamp(value):
    require(type(value) is str, "dynamic_timestamp")
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        require(parsed.tzinfo is not None, "dynamic_timezone")
        return parsed.timestamp()
    except (ValueError, OverflowError) as error:
        raise ValueError("dynamic_timestamp") from error


def private_path(value):
    require(type(value) is str and 0 < len(value) <= 1024, "dynamic_original_path")
    path = PurePosixPath(value)
    require(str(path) == value and path.is_absolute() and ".." not in path.parts
            and (value.startswith("/work/") or value.startswith("/inputs/")
                 or value in {"/launch-anchor.json", "/gateway-manifest.json", "/gateway-launch.json"}), "dynamic_original_path")
    return value


class Evidence:
    """A bounded immutable raw map, with typed reference and path accounting."""
    def __init__(self, raw_evidence, *, maximum=352 * 1024**2):
        require(isinstance(raw_evidence, Mapping) and 0 < len(raw_evidence) <= 16384, "dynamic_raw_inventory_count")
        self.raw, self.kinds, self.paths = raw_evidence, {}, {}
        self.maximum, self.total, self.seen = maximum, 0, {}
        self.keys = frozenset(raw_evidence)
        require(len(self.keys) == len(raw_evidence), "dynamic_raw_key_inventory")
        for sha in self.keys:
            checksum(sha)

    def read(self, reference, kind, maximum=8 * 1024**2):
        fields(reference, REFERENCE_FIELDS, "dynamic_reference_fields")
        checksum(reference["sha256"])
        require(reference["kind"] == kind and integer(reference["bytes"], 0, maximum), "dynamic_reference_kind_size")
        sha = reference["sha256"]
        require(sha in self.keys, "dynamic_reference_missing_or_size")
        if sha not in self.seen:
            require(self.total + reference["bytes"] <= self.maximum, "dynamic_raw_aggregate_bound")
        raw = self.raw[sha]
        require(type(raw) is bytes and len(raw) == reference["bytes"] and digest(raw) == sha,
                "dynamic_raw_hash_size")
        require(sha not in self.kinds or self.kinds[sha] == kind, "dynamic_reference_kind_alias")
        self.kinds[sha] = kind
        if sha not in self.seen:
            self.total += len(raw)
            self.seen[sha] = len(raw)
        return raw

    def json(self, reference, kind, maximum=8 * 1024**2):
        return strict_json(self.read(reference, kind, maximum))

    def inventory(self, reference):
        value = self.json(reference, "capture-inventory")
        fields(value, {"format", "files"})
        require(value["format"] == "recall-dynamic-capture-inventory/v2"
                and type(value["files"]) is list and 0 < len(value["files"]) <= 16384,
                "dynamic_capture_inventory")
        total, local_paths = 0, set()
        for item in value["files"]:
            fields(item, {"path", "ref"})
            path = private_path(item["path"])
            require(path not in local_paths, "dynamic_duplicate_capture_path")
            local_paths.add(path)
            ref = item["ref"]
            require(type(ref) is dict and type(ref.get("kind")) is str and ref["kind"], "dynamic_capture_kind")
            raw = self.read(ref, ref["kind"], 64 * 1024**2)
            require(path not in self.paths or exact(self.paths[path], ref), "dynamic_capture_path_replaced")
            self.paths[path] = ref
            total += len(raw)
        return value, total

    def original(self, path, expected_sha, kind, maximum=8 * 1024**2):
        private_path(path)
        require(path in self.paths and self.paths[path]["sha256"] == expected_sha, "dynamic_original_capture_missing")
        return self.read(self.paths[path], kind, maximum)


def original_parameter(value, object_key):
    fields(value, {object_key, "raw_sha256", "raw_bytes"})
    raw = value["raw_bytes"]
    require(type(raw) is bytes and len(raw) <= 8 * 1024**2 and digest(raw) == value["raw_sha256"]
            and exact(strict_json(raw), value[object_key]), "dynamic_parameter_raw_identity")
    return value[object_key]


def anchor_fields(value, *, static=False):
    """The original carrier is unchanged; only the closed successor has v2."""
    base = STATIC_ANCHOR_FIELDS if static else ANCHOR_FIELDS
    shape_error = "dynamic_controller_policy" if static else "dynamic_anchor_shape"
    require(type(value) is dict, shape_error)
    successor = is_semantic140(value.get("allocation_sha256"))
    fields(value, base | {"profile_id"} if successor else base,
           "dynamic_controller_policy" if static else "dynamic_fields")
    require(value["format"] == ("recall-gateway-launch-anchor/v2" if successor else "recall-gateway-launch-anchor/v1"),
            "dynamic_anchor_profile_format")
    if successor:
        require(value["profile_id"] == semantic140().PROFILE_ID
                and value["corpus_sha256"] == semantic140().FROZEN_INPUTS["corpus"],
                "dynamic_anchor_profile_identity")
    return value


def successor_review(review):
    anchor = anchor_fields(review["controller_policy"]["expected_anchor"], static=True)
    return is_semantic140(anchor["allocation_sha256"])


def reviewed_runtime_bound(reviewed_static):
    review = original_parameter(reviewed_static, "review")
    return semantic140().RUNTIME_RAW_BYTES if successor_review(review) else 352 * 1024**2


def profile_static_limits(review):
    limits = dict(STATIC_LIMITS)
    if successor_review(review):
        limits["runtime_raw_bytes"] = semantic140().RUNTIME_RAW_BYTES
    return limits


def static_review(reviewed_static, evidence, backend):
    review = original_parameter(reviewed_static, "review")
    successor = successor_review(review)
    fields(review, STATIC_FIELDS | {"profile_id"} if successor else STATIC_FIELDS)
    if successor:
        require(review["profile_id"] == semantic140().PROFILE_ID, "dynamic_review_profile_identity")
    require(review["format"] == "recall-dynamic-component-review/v2" and review["reviewer"] == "qualification"
            and review["approved"] is True and review["contract_sha256"] == CONTRACT_SHA256
            and review["predicate_document_sha256"] == PREDICATE_SHA256
            and exact(review["runtime_recipe"], RECIPE), "dynamic_static_review")
    require(type(review["identities"]) is dict and backend in review["identities"], "dynamic_review_backend")
    identities = fields(review["identities"][backend], IDENTITY_FIELDS - {"outer_configuration_sha256"})
    require(identities["backend"] == backend and backend in ("elephant-db", "surreal-db"), "dynamic_review_backend")
    for key in ("artifact_sha256", "binary_sha256", "runtime_sha256", "paired_artifact_sha256"):
        checksum(identities[key])
    checksum(identities["source_commit"], 40)
    require(identities["endpoint"] == "http://127.0.0.1:43130" and type(identities["run_nonce"]) is str
            and re.fullmatch(r"[A-Za-z0-9_-]{16,64}", identities["run_nonce"]), "dynamic_static_endpoint_nonce")
    require(type(review["helper_sha256"]) is dict and review["helper_sha256"], "dynamic_review_helpers")
    for key, sha in review["helper_sha256"].items():
        require(type(key) is str and key and not key.startswith("/") and ".." not in PurePosixPath(key).parts,
                "dynamic_review_helper_path")
        checksum(sha)
    if successor:
        require(review["helper_sha256"].get("tests/recall_preview_semantics/semantic140_expectations.py")
                == "dbb7b64d9e30033574942f8c43fd3301f223088cc6bd8213cc006fc15d37721f", "dynamic_review_semantic140_helper")
    require(type(review["input_refs"]) is dict and review["input_refs"], "dynamic_review_inputs")
    require(exact(review["limits"], profile_static_limits(review)), "dynamic_review_limits")
    reject_dynamic_static(review)
    inputs = {}
    for key, reference in review["input_refs"].items():
        require(type(key) is str and key not in DYNAMIC_STATIC_FIELDS, "dynamic_static_future_identity")
        inputs[key] = evidence.read(reference, reference["kind"])
    if successor:
        require(all(key in inputs and digest(inputs[key]) == sha for key, sha in semantic140().FROZEN_INPUTS.items()),
                "dynamic_review_semantic140_inputs")
    require(type(review["bootstrap_plans"]) is dict and backend in review["bootstrap_plans"], "dynamic_bootstrap_plan_missing")
    plan_ref = review["bootstrap_plans"][backend]
    plan = evidence.json(plan_ref, "bootstrap-plan")
    fields(plan, {"format", *(BOOTSTRAP_IDENTITIES - {"configuration_sha256"}), "expires_at", "spaces",
                  "service_scopes", "dependencies", "run_directory"}
                 | ({"scoped_wiki"} if "scoped_wiki" in plan else set()))
    require(plan["format"] == "recall-runtime-bootstrap-plan/v1" and plan["artifact_sha256"] == identities["binary_sha256"],
            "dynamic_legacy_binary_identity")
    for key in ("source_commit", "runtime_sha256", "paired_artifact_sha256", "backend", "endpoint", "run_nonce"):
        require(exact(plan[key], identities[key]), "dynamic_plan_identity")
    static_controller_policy(review, identities)
    require("scoped_wiki" not in plan or review["controller_policy"]["expected_anchor"]["allocation_sha256"] is None,
            "dynamic_static_scoped_wiki_primary_allocation")
    require(type(plan["spaces"]) is list and len(plan["spaces"]) == 2
            and len({row["name"] for row in plan["spaces"]}) == 2
            and type(plan["dependencies"]) is dict and plan["dependencies"]
            and all(review["helper_sha256"].get(key) == value for key, value in plan["dependencies"].items()), "dynamic_original_plan_spaces_helpers")
    static_plan(plan)
    private_path(plan["run_directory"])
    require(plan["run_directory"] == "/work/bootstrap-" + identities["run_nonce"] + "-" + backend,
            "dynamic_original_plan_directory")
    require(timestamp(plan["expires_at"]) <= timestamp(review["expires_at"]), "dynamic_plan_expiry_widened")
    return review, identities, plan, plan_ref, inputs


def static_controller_policy(review, identity):
    """Bind all approved immutable launch inputs without prebinding outputs."""
    policy = fields(review["controller_policy"], {"expected_anchor", "maximum_backend_seconds"})
    expected = anchor_fields(policy["expected_anchor"], static=True)
    for key in HASH_FIELDS - {"manifest_sha256", "configuration_sha256"}:
        checksum(expected[key])
    for key in ("source_commit", "runtime_sha256", "paired_artifact_sha256", "run_nonce"):
        require(exact(expected[key], identity[key]), "dynamic_static_anchor_identity")
    require(expected["artifact_sha256"] == identity["binary_sha256"], "dynamic_static_anchor_binary")
    allocation = expected["allocation_sha256"]
    if allocation is not None:
        checksum(allocation)
    caps = ({"physical_dispatches": 432, "maximum_nano_usd": 75_000_000_000} if allocation is None
            else semantic140().caps() if is_semantic140(allocation)
            else {"physical_dispatches": 2100, "maximum_nano_usd": 3_000_000_000})
    require(exact(expected["caps"], caps)
            and exact(policy["maximum_backend_seconds"], 2700 if allocation is None else 7800),
            "dynamic_static_anchor_allocation")
    return expected


def static_plan(plan):
    """Independently enforce the original classified bootstrap input recipe."""
    primary = {"semantics-household": ("synthetic-household", 3),
               "semantics-business": ("synthetic-business", 7)}
    original = {("household-policy", 3), ("business-policy", 7)}
    policies, families = set(), set()
    require(type(plan["spaces"]) is list and len(plan["spaces"]) == 2
            and len({space["name"] for space in plan["spaces"]}) == 2,
            "dynamic_static_two_distinct_spaces")
    for space in plan["spaces"]:
        fields(space, {"name", "security", "embedding_provider", "embedding_model", "embedding_dims", "purpose", "skill", "processing"})
        require(type(space["name"]) is str and re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", space["name"]),
                "dynamic_static_space_name")
        security = fields(space["security"], {"policy_id", "level", "labels", "handling"})
        policy = (security["policy_id"], security["level"])
        require(type(security["level"]) is int and security["labels"] == []
                and security["handling"] == [], "dynamic_static_policy")
        if space["name"] in primary or security["policy_id"] in {item[0] for item in primary.values()}:
            require(primary.get(space["name"]) == policy, "dynamic_static_primary_named_policy")
            families.add("primary")
        else:
            require(policy in original, "dynamic_static_policy")
            families.add("workload")
        policies.add(policy)
        require(space["embedding_provider"] == "openai" and integer(space["embedding_dims"], 1, 65536)
                and type(space["embedding_model"]) is str and space["embedding_model"]
                and type(space["purpose"]) is str and 0 < len(space["purpose"]) <= 4096
                and (space["skill"] is None or type(space["skill"]) is str), "dynamic_static_embedding_guidance")
        processing = space["processing"]
        require(type(processing) is dict and type(processing.get("generation")) is int
                and processing["generation"] == 1 and type(processing.get("index_generation")) is int
                and processing["index_generation"] == 0 and processing.get("pending_reindex") is None
                and type(processing.get("bindings")) is dict and processing["bindings"]
                and len(encode(space)) <= 131072, "dynamic_static_fresh_processing")
    require(len(families) == 1 and policies == (set(primary.values()) if families == {"primary"} else original),
            "dynamic_static_policy_separation")
    scopes = plan["service_scopes"]
    scoped_wiki = "scoped_wiki" in plan
    require(not scoped_wiki or (plan["scoped_wiki"] is True and families == {"primary"}),
            "dynamic_static_scoped_wiki_primary_marker")
    allowed = CORE_SCOPES | CONDITIONAL_SCOPES | ({"write:entity"} if scoped_wiki else set())
    require(type(scopes) is list and all(type(item) is str for item in scopes)
            and len(scopes) == len(set(scopes)) and CORE_SCOPES <= set(scopes)
            and set(scopes) <= allowed, "dynamic_static_service_scopes")
    require(not scoped_wiki or {"write:entity", "manage:wiki"} <= set(scopes),
            "dynamic_static_scoped_wiki_explicit_scopes")
    require(len(plan["dependencies"]) <= 128, "dynamic_static_helper_count")
    for path, value in plan["dependencies"].items():
        canonical = PurePosixPath(path)
        require(str(canonical) == path and path.startswith(("scripts/qualification/", "scripts/release/",
                "tests/recall_preview/", "tests/recall_preview_semantics/")), "dynamic_static_helper_path")
        checksum(value)


def process_identity(raw, expected):
    process_fields(expected)
    try:
        before, after = raw.decode("ascii").rsplit(")", 1)
        pid = int(before.split("(", 1)[0].strip())
        parts = after.split()
        require(len(parts) >= 20 and parts[0] not in ("Z", "X", "x"), "dynamic_process_not_live")
        actual = {"pid": pid, "ppid": int(parts[1]), "pgrp": int(parts[2]), "session": int(parts[3]), "start_ticks": int(parts[19])}
        require(exact(actual, expected), "dynamic_process_original_identity")
    except (UnicodeError, ValueError, IndexError) as error:
        raise ValueError("dynamic_process_stat") from error
    return actual


def process_fields(value):
    fields(value, PROCESS_FIELDS)
    require(all(integer(number, 0 if key == "ppid" else 1) for key, number in value.items()), "dynamic_process_numbers")


def network_namespace(value):
    require(type(value) is str and re.fullmatch(r"net:\[[0-9]+\]", value) is not None, "dynamic_network_namespace")


def context_evidence(wrapper, reference, evidence, review, identity, plan):
    fields(wrapper, {"context", "raw_sha256", "raw_bytes", "observed_wall", "observed_monotonic_ns"})
    raw = evidence.read(reference, "gateway-context", 131072)
    require(type(wrapper["raw_bytes"]) is bytes and raw == wrapper["raw_bytes"]
            and digest(raw) == wrapper["raw_sha256"] and exact(strict_json(raw), wrapper["context"]),
            "dynamic_controller_raw_identity")
    context = fields(wrapper["context"], CONTEXT_FIELDS)
    require(context["format"] == "recall-gateway-launch-context/v1", "dynamic_controller_format")
    anchor = anchor_fields(context["anchor"])
    require(context["anchor_sha256"] == digest(encode(anchor)),
            "dynamic_anchor_identity")
    for key in HASH_FIELDS:
        checksum(anchor[key])
    checksum(anchor["source_commit"], 40)
    for key in ("allocation_sha256", "input_inventory_sha256"):
        if anchor[key] is not None:
            checksum(anchor[key])
    parent = fields(anchor["parent"], {"process", "network_namespace"})
    process_fields(parent["process"])
    process_fields(context["process"])
    network_namespace(parent["network_namespace"])
    policy = review["controller_policy"]
    expected = static_controller_policy(review, identity)
    for key, value in expected.items():
        require(exact(anchor[key], value), "dynamic_controller_reviewed_identity")
    for key in ("source_commit", "runtime_sha256", "run_nonce"):
        require(anchor[key] == identity[key], "dynamic_controller_candidate_identity")
    require(anchor["artifact_sha256"] == identity["binary_sha256"]
            and anchor["paired_artifact_sha256"] == plan["paired_artifact_sha256"]
            and anchor["configuration_sha256"] == identity["outer_configuration_sha256"]
            and anchor["input_inventory_sha256"] is not None, "dynamic_controller_legacy_binary_or_input_identity")
    clock = fields(anchor["clock"], CLOCK_FIELDS)
    for key in CLOCK_FIELDS - {"boot_id", "time_cap_seconds"}:
        require(type(clock[key]) in (int, float) and math.isfinite(clock[key]) and clock[key] > 0, "dynamic_controller_clock")
    require(integer(clock["time_cap_seconds"]) and clock["time_cap_seconds"] in (5400, 7800)
            and clock["deadline_mono"] == clock["mono_start"] + clock["time_cap_seconds"]
            and clock["deadline_wall"] == clock["wall_start"] + clock["time_cap_seconds"], "dynamic_original_whole_deadline")
    require(type(clock["boot_id"]) is str and re.fullmatch(r"[a-f0-9-]{36}", clock["boot_id"]), "dynamic_boot_identity")
    now_ns, now_wall = wrapper["observed_monotonic_ns"], wrapper["observed_wall"]
    require(integer(now_ns, 1) and type(now_wall) in (int, float) and math.isfinite(now_wall)
            and clock["mono_start"] <= context["observed_mono"] <= now_ns / 1e9 < clock["deadline_mono"]
            and clock["wall_start"] <= context["observed_wall"] <= now_wall < min(clock["deadline_wall"], timestamp(review["expires_at"])),
            "dynamic_current_original_clock")
    expected_caps = ({"physical_dispatches": 432, "maximum_nano_usd": 75_000_000_000}
                     if anchor["allocation_sha256"] is None else semantic140().caps()
                     if is_semantic140(anchor["allocation_sha256"])
                     else {"physical_dispatches": 2100, "maximum_nano_usd": 3_000_000_000})
    require(exact(anchor["caps"], expected_caps) and clock["time_cap_seconds"] == (5400 if anchor["allocation_sha256"] is None else 7800),
            "dynamic_original_controller_caps")
    require(policy["maximum_backend_seconds"] == (2700 if anchor["allocation_sha256"] is None else 7800), "dynamic_original_backend_clock")
    network = fields(context["network"], {"namespace", "parent_namespace", "interfaces", "ipv4_routes", "ipv6_loopback_routes"})
    network_namespace(network["namespace"])
    network_namespace(network["parent_namespace"])
    require(network["namespace"] != network["parent_namespace"] == anchor["parent"]["network_namespace"]
            and network["interfaces"] == ["lo"] and type(network["ipv4_routes"]) is int and network["ipv4_routes"] == 0
            and integer(network["ipv6_loopback_routes"], 0, 64), "dynamic_original_network")
    captures = fields(context["captures"], {"dev", "ipv4", "ipv6"})
    original = {}
    for key, ref in captures.items():
        fields(ref, {"path", "sha256"})
        require(ref["path"] == f'/work/gateway-context-{context["anchor_sha256"]}.{key}.txt', "dynamic_original_network_path")
        original[key] = evidence.original(ref["path"], ref["sha256"], "gateway-network-" + key, 131072).decode("ascii")
    interfaces = sorted({line.split(":", 1)[0].strip() for line in original["dev"].splitlines() if ":" in line})
    routes = original["ipv4"].splitlines()
    ipv6 = [line.split() for line in original["ipv6"].splitlines() if line.strip()]
    require(interfaces == ["lo"] and len(routes) == 1 and routes[0].split()[:2] == ["Iface", "Destination"]
            and all(len(row) == 10 and row[-1] == "lo" for row in ipv6)
            and len(ipv6) == network["ipv6_loopback_routes"], "dynamic_original_network_capture")
    return context


def startup_captures(receipt, evidence, *, contiguous=True):
    observations, total, indices = {}, 0, set()
    require(type(receipt["captures"]) is list and 0 < len(receipt["captures"]) <= 160,
            "dynamic_startup_capture_count")
    for reference in receipt["captures"]:
        fields(reference, {"request_index", "request_path", "response_path", "request_sha256", "response_sha256"})
        index = reference["request_index"]
        require(integer(index, 1, 160) and index not in indices, "dynamic_startup_capture_index")
        indices.add(index)
        raw_request = evidence.original(reference["request_path"], reference["request_sha256"], "startup-request", 1048576)
        raw_response = evidence.original(reference["response_path"], reference["response_sha256"], "startup-response", 1048576)
        total += len(raw_request) + len(raw_response)
        require(total <= 16777216, "dynamic_startup_capture_bound")
        request, response = strict_json(raw_request), strict_json(raw_response)
        fields(request, {"request_index", "method", "path", "port", "credential_role", "space", "body",
                         "request_body_base64", "body_sha256", "harness_attempt", "timeout_seconds", "started_monotonic_ns"})
        # These are the exact optional timing/header fields of the existing
        # bounded transport, never arbitrary summary or proof fields.
        response_required = {"framing_complete", "status", "body", "error_type", "request_index", "raw_body_base64",
                             "body_sha256", "finished_monotonic_ns"}
        response_optional = {"response_headers", "first_response_byte_ns", "wire_bytes"}
        require(type(response) is dict and response_required <= set(response)
                and set(response) <= response_required | response_optional, "dynamic_startup_response_fields")
        require(request["request_index"] == response["request_index"] == index and request["harness_attempt"] == 1
                and type(request["harness_attempt"]) is int, "dynamic_startup_original_attempt")
        require(type(request["port"]) is int and request["port"] == 43130, "dynamic_startup_original_port")
        try:
            raw_input = base64.b64decode(request["request_body_base64"], validate=True)
            raw_body = base64.b64decode(response["raw_body_base64"], validate=True)
        except (ValueError, TypeError) as error:
            raise ValueError("dynamic_startup_base64") from error
        require(len(raw_input) <= 131072 and len(raw_body) <= 262144
                and digest(raw_input) == request["body_sha256"] and digest(raw_body) == response["body_sha256"]
                and raw_input == (b"" if request["body"] is None else encode(request["body"])), "dynamic_startup_wire_identity")
        require(integer(request["started_monotonic_ns"], 1) and integer(response["finished_monotonic_ns"], 1)
                and receipt["started_monotonic_ns"] <= request["started_monotonic_ns"]
                <= response["finished_monotonic_ns"] <= receipt["observed_monotonic_ns"]
                and type(request["timeout_seconds"]) in (int, float) and 0 < request["timeout_seconds"] <= 15,
                "dynamic_startup_original_request_clock")
        observations[index] = (request, response, raw_body)
    require(not contiguous or indices == set(range(1, len(indices) + 1)), "dynamic_startup_capture_gap")
    return observations


def typed_denial(response, status, code):
    require(response["framing_complete"] is True and response["error_type"] is None
            and response["status"] == status, "dynamic_original_denial_transport")
    body = fields(response["body"], {"error_code", "message", "retryable"})
    require(body["error_code"] == code and body["retryable"] is False
            and type(body["message"]) is str and len(body["message"].encode()) <= 512,
            "dynamic_original_typed_denial")


def startup_proofs(receipt, plan, captures):
    checks = fields(receipt["checks"], BOOTSTRAP_CHECKS)
    spaces = {row["name"] for row in plan["spaces"]}
    denial = {"missing_grant", "wrong_space", "bootstrap_data_denial", "read_only_write_denial", "grant_revocation"}
    for check, indices in checks.items():
        require(type(indices) is list and indices and all(integer(i, 1) and i in captures for i in indices)
                and len(indices) == len(set(indices)), "dynamic_startup_proof_indices")
        observed_spaces = set()
        for index in indices:
            request, response, raw = captures[index]
            require(response["framing_complete"] is True and response["error_type"] is None
                    and request["port"] == 43130 and exact(strict_json(raw), response["body"]),
                    "dynamic_startup_proof_wire")
            space, role = request["space"], request["credential_role"]
            observed_spaces.add(space)
            if check == "unauthenticated_readiness":
                require(request["method"] == "GET" and request["path"] == "/v1/readiness" and role is None,
                        "dynamic_readiness_original_unauthenticated")
                typed_denial(response, 401, "unauthorized")
                continue
            if check == "ready":
                require(request["method"] == "GET" and request["path"] == "/v1/readiness" and role == "operator"
                        and response["status"] == 200 and response["body"].get("worker_ready") is True
                        and response["body"].get("semantic_ingest_ready") is True
                        and response["body"].get("ready") is True and response["body"].get("draining") is False,
                        "dynamic_actual_worker_readiness")
                continue
            require(space in spaces, "dynamic_original_named_space")
            if check in denial:
                typed_denial(response, 403, "forbidden_space")
                if check == "wrong_space":
                    require(role in {"service-" + other for other in spaces - {space}}
                            and request["method"] == "GET" and request["path"] == "/v1/spaces/" + space,
                            "dynamic_actual_cross_space_authority")
                else:
                    prefix = "bootstrap-" if check == "bootstrap_data_denial" else "read-only-" if check == "read_only_write_denial" else "service-"
                    require(role == prefix + space and request["path"].startswith("/v1/spaces/" + space + "/sources")
                            and request["method"] == ("PUT" if check == "read_only_write_denial" else "GET"),
                            "dynamic_actual_denied_authority")
            else:
                require(response["status"] == 200 and request["method"] == "GET", "dynamic_actual_readback")
                if check in ("named_config_readback", "ordinary_config_readback", "grant_restoration"):
                    require(request["path"] == "/v1/spaces/" + space
                            and role == ("bootstrap-" if check == "named_config_readback" else "service-") + space
                            and exact(response["body"], receipt["configurations"][space]), "dynamic_original_config_readback")
                elif check in ("ordinary_empty_inventory", "final_empty_inventory"):
                    require(request["path"] == f"/v1/spaces/{space}/sources?limit=100" and role == "service-" + space
                            and response["body"].get("sources") == [] and response["body"].get("next_cursor") is None,
                            "dynamic_actual_clean_source_inventory")
                elif check == "grant_readback":
                    grant = response["body"]
                    require(type(grant) is dict and grant.get("space_id") == space
                            and grant.get("subject") in {"primary-service-" + space, "primary-read-only-" + space}
                            and request["path"] == f"/v1/spaces/{space}/grants/" + grant["subject"]
                            and role == "bootstrap-" + space and type(grant.get("generation")) is str and grant["generation"],
                            "dynamic_actual_current_grant")
        if check not in ("unauthenticated_readiness", "ready"):
            require(observed_spaces == spaces, "dynamic_missing_named_space_proof")


def startup_authority(receipt, plan, captures):
    """Replay original creations and grant CAS transitions, not check labels."""
    checks = receipt["checks"]
    def checked(name, space):
        selected = [index for index in checks[name] if captures[index][0]["space"] == space]
        require(len(selected) == 1, "dynamic_authority_check_count")
        return selected[0]
    def success(index, status=200):
        request, response, raw = captures[index]
        require(response["status"] == status and response["framing_complete"] is True
                and response["error_type"] is None and exact(strict_json(raw), response["body"]),
                "dynamic_authority_original_response")
        return request, response["body"]
    creation_indices = []
    for space in plan["spaces"]:
        name = space["name"]
        creations = [index for index, (request, _, _) in captures.items()
                     if request["method"] == "POST" and request["path"] == "/v1/spaces" and request["space"] == name]
        require(len(creations) == 1, "dynamic_authority_sole_space_creation")
        creation = creations[0]
        creation_indices.append(creation)
        request, created = success(creation, 201)
        require(request["credential_role"] == "bootstrap-" + name and exact(request["body"], space)
                and exact(created, receipt["configurations"][name]), "dynamic_authority_created_configuration")
        named = checked("named_config_readback", name)
        missing = checked("missing_grant", name)
        ordinary = checked("ordinary_config_readback", name)
        ordinary_empty = checked("ordinary_empty_inventory", name)
        bootstrap_denied = checked("bootstrap_data_denial", name)
        readonly_denied = checked("read_only_write_denial", name)
        revoked_denied = checked("grant_revocation", name)
        restored = checked("grant_restoration", name)
        final_empty = checked("final_empty_inventory", name)
        transitions = []
        for role in ("service", "read-only"):
            subject = "primary-" + role + "-" + name
            path = f"/v1/spaces/{name}/grants/{subject}"
            writes = sorted(index for index, (request, _, _) in captures.items()
                            if request["method"] == "PUT" and request["path"] == path)
            states = [True, False, True] if role == "service" else [True]
            scopes = plan["service_scopes"] if role == "service" else [s for s in plan["service_scopes"] if s.startswith("read:")]
            require(len(writes) == len(states), "dynamic_authority_grant_transition_count")
            generations, previous = set(), None
            for index, active in zip(writes, states):
                request, grant = success(index)
                require(request["credential_role"] == "bootstrap-" + name and request["space"] == name
                        and exact(request["body"], {"active": active, "scopes": scopes, "expected_generation": previous}),
                        "dynamic_authority_grant_original_cas")
                require(type(grant) is dict and grant.get("space_id") == name and grant.get("subject") == subject
                        and grant.get("active") is active and type(grant.get("scopes")) is list
                        and all(type(item) is str for item in grant["scopes"])
                        and len(grant["scopes"]) == len(set(grant["scopes"])) and set(grant["scopes"]) == set(scopes)
                        and type(grant.get("generation")) is str and grant["generation"]
                        and grant["generation"] not in generations, "dynamic_authority_grant_actual_fields")
                generations.add(grant["generation"])
                previous = grant["generation"]
                reads = [other for other in checks["grant_readback"] if other > index and
                         captures[other][0]["path"] == path and exact(captures[other][1]["body"], grant)]
                require(len(reads) == 1 and reads[0] == index + 1, "dynamic_authority_grant_original_readback")
                read, body = success(reads[0])
                require(read["method"] == "GET" and read["credential_role"] == "bootstrap-" + name
                        and read["space"] == name and exact(body, grant), "dynamic_authority_grant_read_authority")
            transitions.append(writes)
        service, readonly = transitions
        require(creation < named < missing < service[0] < readonly[0] < ordinary < ordinary_empty
                < bootstrap_denied < readonly_denied < service[1] < revoked_denied < service[2] < restored
                < final_empty, "dynamic_authority_original_sequence")
        require(captures[readonly_denied][0]["path"] == f"/v1/spaces/{name}/sources/bootstrap-denied"
                and exact(captures[readonly_denied][0]["body"], {"revision": "denied-v1", "text": "denial control",
                    "media_type": "text/plain", "source_url": None, "observed_at": None, "labels": [], "expected_generation": None}),
                "dynamic_authority_readonly_original_mutation")
    require(len([1 for request, _, _ in captures.values() if request["method"] == "POST"
                 and request["path"] == "/v1/spaces"]) == len(creation_indices), "dynamic_authority_no_extra_space_creation")
    require(len(checks["grant_readback"]) == 8, "dynamic_authority_complete_grant_inventory")


def original_isolation(receipt, context, evidence):
    raw = evidence.original("/work/isolation.json", receipt["isolation_sha256"], "startup-isolation", 131072)
    proof = fields(strict_json(raw), {"format", "network_namespace", "parent_network_namespace", "interfaces",
                                    "ipv4_routes", "provider_credentials_present"})
    require(proof["format"] == "recall-gateway-isolation/v1"
            and proof["network_namespace"] == context["network"]["namespace"]
            and proof["parent_network_namespace"] == context["network"]["parent_namespace"]
            and proof["interfaces"] == ["lo"] and type(proof["ipv4_routes"]) is int and proof["ipv4_routes"] == 0
            and proof["provider_credentials_present"] is False, "dynamic_original_isolation")
    return proof


def configurations(plan, actual):
    require(type(actual) is dict and set(actual) == {row["name"] for row in plan["spaces"]}, "dynamic_config_inventory")
    static_fields = {"security", "embedding_provider", "embedding_model", "embedding_dims", "purpose", "skill", "processing"}
    required = {"id", "space_id", "created_at", "updated_at", "created_by", "security", "subjects", "subjects_state",
                "provenance", "embedding_provider", "embedding_model", "embedding_dims", "purpose", "processing"}
    for request in plan["spaces"]:
        fields(request, static_fields | {"name"})
        value = actual[request["name"]]
        require(type(value) is dict and required <= set(value) <= required | {"deleted_at", "skill"}, "dynamic_config_fields")
        require(type(value["id"]) is str and re.fullmatch(r"space_config:[a-f0-9-]{36}", value["id"])
                and value["space_id"] == request["name"] and value["created_by"] == "primary-bootstrap-" + request["name"]
                and value.get("deleted_at") is None and value["subjects"] == [] and value["subjects_state"] == "complete",
                "dynamic_config_fresh_identity")
        require(value["created_at"] == value["updated_at"] and timestamp(value["created_at"]) > 0,
                "dynamic_config_fresh_timestamp")
        for key in static_fields:
            require(exact(value.get(key), request[key]), "dynamic_config_requested_field")
        fields(value["provenance"], {"sources", "chain"})
        require(value["provenance"] == {"sources": [], "chain": []}, "dynamic_config_fresh_provenance")
    return actual


def startup_journal(receipt, plan, reference, evidence):
    raw = evidence.read(reference, "startup-command-journal", 262144)
    original_ref = fields(receipt["process_commands"], {"path", "sha256"})
    require(digest(raw) == original_ref["sha256"]
            and raw == evidence.original(original_ref["path"], original_ref["sha256"], "startup-command-journal", 262144),
            "dynamic_command_original_bytes")
    journal = fields(strict_json(raw), {"format", "processes"})
    require(journal["format"] == "recall-runtime-process-commands/v1" and type(journal["processes"]) is list
            and 0 < len(journal["processes"]) <= 16, "dynamic_original_command_inventory")
    commands = {}
    for number, row in enumerate(journal["processes"], 1):
        fields(row, {"index", "purpose", "command", "cwd", "environment_names", "logs", "started_monotonic_ns", "pid", "exit_code", "log_sha256"})
        require(type(row["index"]) is int and row["index"] == number and integer(row["pid"], 1)
                and integer(row["started_monotonic_ns"], 1)
                and receipt["started_monotonic_ns"] <= row["started_monotonic_ns"] <= receipt["observed_monotonic_ns"]
                and type(row["command"]) is list and row["command"] and all(type(arg) is str for arg in row["command"])
                and type(row["environment_names"]) is list
                and row["environment_names"] == sorted(set(row["environment_names"])), "dynamic_original_command_identity")
        require(PurePosixPath(private_path(row["cwd"])).is_relative_to(plan["run_directory"])
                and row["logs"] == [plan["run_directory"] + f"/logs/{number:02}.{suffix}" for suffix in ("stdout", "stderr")],
                "dynamic_original_command_paths")
        if row["exit_code"] is None:
            require(row["log_sha256"] is None, "dynamic_unfinished_service_logs")
        else:
            require(type(row["exit_code"]) is int and row["exit_code"] == 0
                    and type(row["log_sha256"]) is dict and set(row["log_sha256"]) == set(row["logs"]),
                    "dynamic_completed_command_failure")
            for path, sha in row["log_sha256"].items():
                evidence.original(path, sha, "process-log", 4194304)
        commands[number] = row
    for name, process in receipt["processes"].items():
        rows = [row for row in commands.values() if row["pid"] == process["pid"]]
        require(len(rows) == 1, "dynamic_original_service_command_count")
        row = rows[0]
        expected = (["/opt/recall/bin/elephant", "serve", "--server-only", "--shutdown-timeout-seconds", "60"] if name == "recall"
                    else ["/opt/recall/bin/recall-surreal", "start", "--bind", "127.0.0.1:43131", "file:" + plan["run_directory"] + "/paired/database.db"])
        require(row["exit_code"] is None and row["command"] == expected
                and row["purpose"] == ("recall-authenticated-service" if name == "recall" else "paired-persistent-server"),
                "dynamic_original_service_command")
    require(len({row["pid"] for row in commands.values()}) == len(commands), "dynamic_duplicate_owned_command_pid")
    credential_commands(commands, plan, receipt)
    return commands


def credential_commands(commands, plan, receipt):
    """Require the seven original scoped CLI issuers and only owned services."""
    roles = {"operator": ("primary-runtime-operator", None, False, False)}
    for space in plan["spaces"]:
        name = space["name"]
        for role in ("bootstrap", "service", "read-only"):
            roles[role + "-" + name] = ("primary-" + role + "-" + name, space, role == "bootstrap", role == "read-only")
    allowed = {"issue-" + role for role in roles} | {"recall-authenticated-service"}
    if plan["backend"] == "surreal-db":
        allowed |= {"paired-persistent-server", "explicit-fresh-schema"}
    require({row["purpose"] for row in commands.values()} == allowed
            and len(commands) == len(allowed), "dynamic_original_command_recipe_inventory")
    first_service = min(row["started_monotonic_ns"] for row in commands.values() if row["exit_code"] is None)
    for role, (subject, space, bootstrap, readonly) in roles.items():
        row = next(row for row in commands.values() if row["purpose"] == "issue-" + role)
        argv = row["command"]
        require(len(argv) >= 17, "dynamic_original_credential_command")
        wanted = ["/opt/recall/bin/elephant", "service-token", "issue", "--secret-file", plan["run_directory"] + "/private/signing.key",
                  "--subject", subject, "--expires-in", argv[8], "--issuer", "recall-primary-bootstrap",
                  "--audience", "recall-primary", "--output", plan["run_directory"] + "/private/" + role + ".token"]
        # The original argv stores the lifetime immediately after --expires-in.
        require(argv[8].isascii() and argv[8].isdecimal()
                and 1 <= int(argv[8]) <= (3600 if space is None or bootstrap else 7800),
                "dynamic_original_credential_lifetime")
        if space is None:
            wanted += ["--global-admin"]
            scopes = ["read:status", "read:system", "write:system"]
        else:
            wanted += ["--bootstrap-space" if bootstrap else "--space", space["name"], "--clearances-file",
                       plan["run_directory"] + "/private/" + role + ".clearances.json"]
            scopes = BOOTSTRAP_SCOPES if bootstrap else plan["service_scopes"]
            if readonly:
                scopes = [scope for scope in scopes if scope.startswith("read:")]
                wanted += ["--read-only"]
        for scope in scopes:
            wanted += ["--scope", scope]
        require(argv == wanted and row["exit_code"] == 0 and type(row["exit_code"]) is int
                and row["cwd"] == plan["run_directory"] + "/service"
                and row["started_monotonic_ns"] < first_service
                and "JWT_SECRET_FILE" in row["environment_names"] and "SKIP_AUTH" not in row["environment_names"]
                and "JWT_SECRET" not in row["environment_names"], "dynamic_original_scoped_cli_authority")



def gateway_manifest(value, anchor, identity):
    """The original G schemas, independently parsed without host authority."""
    require(type(value) is dict, "dynamic_outer_manifest_fields")
    cohort = anchor["allocation_sha256"] is not None
    primary = {"format", "run_id", "proposal_sha256", "catalog_sha256", "corpus_sha256",
               "artifact_sha256", "configuration_sha256", "account_id", "upstream_ips", "bindings",
               "endpoint", "gateway_endpoint", "bound_version", "live_ready", "runtime_sha256",
               "transport_response_byte_cap", "synthetic_corpus_only", "gateway_sha256", "run_directory"}
    settlement = value.get("format") == "recall-gateway-manifest/v2"
    if cohort:
        required = {"format", "live_ready", "run_id", "run_directory", "accounting_policy", "account_id",
                    "credential_generation", "artifact_sha256", "configuration_sha256", "runtime_sha256",
                    "gateway_sha256", "contract_sha256", "corpus_sha256", "allocation_path", "allocation_sha256",
                    "bindings", "endpoint", "gateway_endpoint", "upstream_ips", "synthetic_corpus_only",
                    "transport_response_byte_cap", "bound_version", "preparation_contract_path",
                    "preparation_contract_sha256", "typed_delta_path", "typed_delta_sha256"}
        successor = is_semantic140(anchor["allocation_sha256"])
        fields(value, required | {"profile_id"} if successor else required, "dynamic_outer_manifest_fields")
        if successor:
            require(value["profile_id"] == anchor.get("profile_id") == semantic140().PROFILE_ID
                    and anchor["format"] == "recall-gateway-launch-anchor/v2", "dynamic_outer_manifest_profile")
        require(value["format"] == ("recall-semantic140-embedding-manifest/v1" if successor else "recall-exclusive-embedding-manifest/v1")
                and value["accounting_policy"] == ("separate-exclusive-semantic140-embedding-reservation/v1" if successor
                                                  else "separate-exclusive-workload-embedding-reservation/v1")
                and value["bound_version"] == "utf8-wire-plus-protocol-v1", "dynamic_outer_manifest_policy")
        pins = {"allocation_sha256": "0a54fcd4c6359e39611567a06857f2d61975b3f19a80052f6ea49691f6d1633f",
                "preparation_contract_sha256": "b1fd4c43e1c8c506ca13e261a29d65a7d06246bf11ef53c02e97945cd3ef56d1",
                "typed_delta_sha256": "51b34aec951d783a93b903a1468fba9a8833ba2f31f705bd784c66c9b26688f8",
                "contract_sha256": "696e0fa0f8736306d6e66f5ce26658a21fb013255e5727b5287406008c350a5a",
                "corpus_sha256": "2192437954109fc17ebbea5b3960eb6b398a2ee91a725ce70666b119e86179dc"}
        if successor:
            pins = {"allocation_sha256": semantic140().FROZEN_INPUTS["provider_allocation"],
                    "contract_sha256": semantic140().FROZEN_INPUTS["workload_contract"],
                    **{key + "_sha256": semantic140().FROZEN_INPUTS[key]
                       for key in ("preparation_contract", "typed_delta", "corpus")}}
        require(all(value[key] == wanted for key, wanted in pins.items()), "dynamic_outer_original_allocation")
        for key in ("allocation_path", "preparation_contract_path", "typed_delta_path"):
            require(type(value[key]) is str and value[key] and "\x00" not in value[key], "dynamic_outer_manifest_path")
    else:
        if settlement:
            primary |= {"accounting_policy", "rate_version", "credential_generation", "reasoning_sdk_sha256",
                        "reasoning_projection_version"}
        require(primary <= set(value) <= primary | {"output_cap_mode"}, "dynamic_outer_manifest_fields")
        require(value["format"] in ("recall-gateway-manifest/v1", "recall-gateway-manifest/v2"),
                "dynamic_outer_manifest_policy")
        if settlement:
            require(value["accounting_policy"] == "full-context-terminal-v2"
                    and value["rate_version"] == "sol-standard-long-max-2026-10-06"
                    and value["bound_version"] == "provider-full-context-v1"
                    and value["reasoning_sdk_sha256"] == "105229842f1a221277513653d5396c8e36a116384dde406182f2e173cdd0f8c3"
                    and value["reasoning_projection_version"] == "sdk-0.8.51-reasoning-projection-v1",
                    "dynamic_outer_manifest_policy")
        else:
            require(value["bound_version"] == "utf8-wire-plus-protocol-v1", "dynamic_outer_manifest_policy")
        modes = ("preserve", "cap_at_16384") if settlement else ("reject", "cap_at_16384")
        require(value.get("output_cap_mode", modes[0]) in modes, "dynamic_outer_output_mode")
        require(value["catalog_sha256"] == "9b40bf9b99a93b3f6d1ba7cf0b402fd46b01b4aac42205c9681c8e894a03e2ce"
                and value["corpus_sha256"] == "fa6a84149483d3511fd559ac9a13e89d6c1c041ae97d6f15084b52b7d7f21c1b",
                "dynamic_outer_original_proposal")
    require(value["live_ready"] is True and value["synthetic_corpus_only"] is True
            and type(value["transport_response_byte_cap"]) is int and value["transport_response_byte_cap"] == 8388608,
            "dynamic_outer_original_capture_scope")
    require(value["endpoint"] == "https://api.openai.com/v1"
            and value["gateway_endpoint"] == "http://127.0.0.1:43129/v1", "dynamic_outer_original_endpoint")
    for key in value:
        if key.endswith("_sha256"):
            checksum(value[key])
            if cohort:
                require(value[key] != "0" * 64, "dynamic_outer_manifest_digest")
    for key in ("run_id", "account_id"):
        require(type(value[key]) is str and 0 < len(value[key]) <= (128 if cohort else 131072),
                "dynamic_outer_manifest_identity")
    require(type(value["run_directory"]) is str and value["run_directory"].startswith("/")
            and "\x00" not in value["run_directory"], "dynamic_outer_manifest_path")
    if cohort or settlement:
        require(integer(value["credential_generation"], 1), "dynamic_outer_credential_generation")
    require(value["run_id"] == identity["run_nonce"] == anchor["run_nonce"], "dynamic_outer_original_nonce")
    for key in ("artifact_sha256", "runtime_sha256", "gateway_sha256", "corpus_sha256", "configuration_sha256"):
        require(value[key] == anchor[key], "dynamic_outer_original_manifest_binding")
    require(value.get("proposal_sha256", value.get("allocation_sha256")) == anchor["proposal_sha256"]
            and value.get("allocation_sha256") == anchor["allocation_sha256"], "dynamic_outer_original_proposal_binding")
    require(type(value["upstream_ips"]) is list and 1 <= len(value["upstream_ips"]) <= 8,
            "dynamic_outer_upstream_inventory")
    for address in value["upstream_ips"]:
        require(type(address) is str, "dynamic_outer_upstream_address")
        try:
            parsed = ipaddress.ip_address(address)
        except ValueError:
            raise ValueError("dynamic_outer_upstream_address") from None
        require(parsed.is_global and not parsed.is_multicast, "dynamic_outer_upstream_address")
    bindings = value["bindings"]
    require(type(bindings) is list and (len(bindings) == 4 if cohort else 1 <= len(bindings) <= 64),
            "dynamic_outer_binding_inventory")
    ids, coverage, connections, profiles = set(), set(), set(), set()
    operations = {"embedding", "triage", "extraction", "support_verification", "summary",
                  "promotion_and_identity_disambiguation", "truth_maintenance", "wiki_planner_writer_verifier"}
    for binding in bindings:
        names = {"id", "backend", "space", "operation", "model", "dimensions", "connection_id", "profile_id"}
        require(type(binding) is dict and names - ({"dimensions"} if not cohort else set()) <= set(binding) <= names,
                "dynamic_outer_binding_fields")
        for key in ("id", "connection_id", "profile_id"):
            require(type(binding[key]) is str and 0 < len(binding[key]) <= (128 if cohort else 131072),
                    "dynamic_outer_binding_identity")
        name = binding["id"]
        require(re.fullmatch(r"[A-Za-z0-9_]+", name) is not None and name not in ids, "dynamic_outer_binding_identity")
        ids.add(name)
        require(binding["backend"] in ("elephant-db", "surreal-db")
                and binding["space"] in (("qualification-household", "qualification-business") if cohort
                                         else ("semantics-household", "semantics-business"))
                and binding["operation"] in ({"embedding"} if cohort else operations), "dynamic_outer_binding_scope")
        if binding["operation"] == "embedding":
            model, dimensions = (("text-embedding-3-small", 1536) if binding["space"].endswith("household")
                                 else ("text-embedding-3-large", 3072))
            require(binding["model"] == model and type(binding.get("dimensions")) is int
                    and binding["dimensions"] == dimensions, "dynamic_outer_embedding_profile")
        else:
            require(binding["model"] == "gpt-6.1-sol" and binding.get("dimensions") is None,
                    "dynamic_outer_generation_profile")
        coverage.add((binding["backend"], binding["space"]))
        connection = (binding["backend"], binding["space"], binding["connection_id"])
        profile = (binding["backend"], binding["space"], binding["profile_id"])
        if not cohort:
            require(connection not in connections and profile not in profiles, "dynamic_outer_shared_role_binding")
        connections.add(connection)
        profiles.add(profile)
    require(not cohort or len(coverage) == 4, "dynamic_outer_binding_coverage")
    require(digest(encode(bindings)) == anchor["bindings_sha256"], "dynamic_outer_original_bindings_hash")
    return value


def outer_launch(reference, evidence, inventory, processes, context, identity, plan, review):
    """Bind the two original read-only G files, not reconstructed anchor claims."""
    value = evidence.json(reference, "gateway-launch-inputs", 16384)
    fields(value, {"format", "manifest", "spec", "input_inventory"})
    require(value["format"] == "recall-gateway-launch-inputs/v2", "dynamic_outer_launch_format")
    require(exact(value["input_inventory"], processes["input_inventory"]), "dynamic_outer_same_input_inventory")
    for ref in (reference, value["manifest"], value["spec"], value["input_inventory"]):
        require(any(exact(row["ref"], ref) for row in inventory["files"]), "dynamic_outer_capture_membership")
    for name, path in (("manifest", "/gateway-manifest.json"), ("spec", "/gateway-launch.json")):
        require(any(row["path"] == path and exact(row["ref"], value[name]) for row in inventory["files"]),
                "dynamic_outer_original_fixed_path")
    manifest_raw = evidence.read(value["manifest"], "gateway-manifest", 131072)
    spec_raw = evidence.read(value["spec"], "gateway-launch-spec", 131072)
    anchor = context["anchor"]
    require(digest(manifest_raw) == anchor["manifest_sha256"], "dynamic_outer_original_manifest_hash")
    require(digest(spec_raw) == anchor["configuration_sha256"] == identity["outer_configuration_sha256"],
            "dynamic_outer_original_spec_hash")
    manifest = gateway_manifest(strict_json(manifest_raw), anchor, identity)
    spec = fields(strict_json(spec_raw), {"format", "command", "environment", "bindings", "inputs"},
                  "dynamic_outer_spec_fields")
    require(spec["format"] == "recall-gateway-launch/v1"
            and manifest["configuration_sha256"] == value["spec"]["sha256"], "dynamic_outer_spec_format")
    require(exact(spec["bindings"], manifest["bindings"]), "dynamic_outer_original_spec_bindings")
    command = spec["command"]
    require(type(command) is list and 1 <= len(command) <= 64
            and all(type(arg) is str and len(arg) <= 4096 and "\x00" not in arg for arg in command)
            and command[0].startswith("/"), "dynamic_outer_command_shape")
    require(digest(encode(command)) == anchor["command_sha256"], "dynamic_outer_original_command_hash")
    allowed = {"RUST_LOG", "SURREALDB_URL", "SURREALDB_NAMESPACE", "SURREALDB_DATABASE", "ELEPHANT_HOST",
               "ELEPHANT_PORT", "RECALL_BACKEND", "JWT_ISSUER", "JWT_AUDIENCE", "PIPELINE_SKILLS_DIR"}
    cohort = anchor["allocation_sha256"] is not None
    if cohort:
        allowed.add("PIPELINE_WORK_TRANSIENT_RETRY_MAX_ATTEMPTS")
    environment = spec["environment"]
    require(type(environment) is dict and set(environment) <= allowed
            and all(type(item) is str and len(item) <= 1024 and "\x00" not in item for item in environment.values()),
            "dynamic_outer_environment")
    require(not cohort or environment.get("PIPELINE_WORK_TRANSIENT_RETRY_MAX_ATTEMPTS") == "3",
            "dynamic_outer_original_queue_recipe")
    if "PIPELINE_SKILLS_DIR" in environment:
        path = PurePosixPath(environment["PIPELINE_SKILLS_DIR"])
        require(path.is_absolute() and str(path) == environment["PIPELINE_SKILLS_DIR"]
                and ".." not in path.parts and str(path).startswith("/opt/recall/"), "dynamic_outer_runtime_asset_path")
    inputs = fields(spec["inputs"], {"directory", "inventory_sha256"}, "dynamic_outer_inputs_fields")
    directory = inputs["directory"]
    require(type(directory) is str and PurePosixPath(directory).is_absolute()
            and str(PurePosixPath(directory)) == directory and ".." not in PurePosixPath(directory).parts
            and "\x00" not in directory, "dynamic_outer_inputs_directory")
    inventory_raw = evidence.read(value["input_inventory"], "input-inventory")
    require(inputs["inventory_sha256"] == anchor["input_inventory_sha256"] == digest(inventory_raw)
            == digest(encode(strict_json(inventory_raw))), "dynamic_outer_original_inventory_hash")
    # Bind the launched embedding capabilities to both original named-space
    # recipes. Later public configuration checks must still validate their CAS.
    for space in plan["spaces"]:
        processing = space["processing"]
        for operation in ("query_embedding", "document_embedding"):
            profile = processing["profiles"][processing["bindings"][operation]["profile_id"]]
            connection = processing["connections"][profile["connection_id"]]
            matches = [row for row in manifest["bindings"] if row["backend"] == identity["backend"]
                       and row["space"] == space["name"] and row["operation"] == "embedding"
                       and row["connection_id"] == connection["id"] and row["profile_id"] == profile["id"]]
            require(len(matches) == 1, "dynamic_outer_original_profile_binding")
            binding = matches[0]
            require(connection["account_id"] == manifest["account_id"]
                    and connection["endpoint"] == manifest["gateway_endpoint"]
                    and connection["credential_ref"] == "RECALL_GATEWAY_" + binding["id"]
                    and profile["model"] == binding["model"] and profile["dimensions"] == binding["dimensions"],
                    "dynamic_outer_original_account_profile")
            if "credential_generation" in manifest:
                require(type(connection["credential_generation"]) is int
                        and connection["credential_generation"] == manifest["credential_generation"],
                        "dynamic_outer_original_credential_generation")
    # Original D28 file hashes must also match the independent reviewed inputs;
    # host-only paths in G's manifest are never treated as child file authority.
    if cohort:
        for name, key in (("provider_allocation", "allocation_sha256"), ("workload_contract", "contract_sha256"),
                          ("corpus", "corpus_sha256"), ("preparation_contract", "preparation_contract_sha256"),
                          ("typed_delta", "typed_delta_sha256")):
            require(name in review["input_refs"] and review["input_refs"][name]["sha256"] == manifest[key],
                    "dynamic_outer_reviewed_input_binding")
    return value


def current_processes(reference, evidence, receipt, context, wrapper):
    observed = evidence.json(reference, "current-processes", 262144)
    fields(observed, {"format", "observed_monotonic_ns", "observed_wall", "processes", "current_network_namespace", "boot_id", "input_inventory", "anchor"})
    require(observed["format"] == "recall-dynamic-current-processes/v2"
            and integer(observed["observed_monotonic_ns"], 1)
            and receipt["observed_monotonic_ns"] <= observed["observed_monotonic_ns"] <= wrapper["observed_monotonic_ns"]
            and context["observed_wall"] <= observed["observed_wall"] <= wrapper["observed_wall"]
            and observed["current_network_namespace"] == context["network"]["namespace"]
            and observed["boot_id"] == context["anchor"]["clock"]["boot_id"], "dynamic_current_process_clock_namespace")
    actual_anchor = evidence.read(observed["anchor"], "gateway-anchor", 131072)
    require(actual_anchor == encode(context["anchor"]) and digest(actual_anchor) == context["anchor_sha256"],
            "dynamic_original_readonly_anchor")
    inputs_raw = evidence.read(observed["input_inventory"], "input-inventory")
    require(digest(inputs_raw) == context["anchor"]["input_inventory_sha256"], "dynamic_original_readonly_input_inventory")
    inputs = fields(strict_json(inputs_raw), {"format", "files"})
    require(inputs["format"] == "recall-gateway-inputs/v1" and type(inputs["files"]) is list
            and 0 < len(inputs["files"]) <= 256, "dynamic_original_input_inventory_shape")
    paths, total = set(), 0
    for item in inputs["files"]:
        fields(item, {"path", "bytes", "sha256"})
        path = item["path"]
        require(type(path) is str and str(PurePosixPath(path)) == path and not path.startswith("/")
                and ".." not in PurePosixPath(path).parts and path not in paths and integer(item["bytes"], 0, 8 * 1024**2),
                "dynamic_original_input_entry")
        paths.add(path)
        checksum(item["sha256"])
        total += item["bytes"]
    require(total <= 32 * 1024**2, "dynamic_original_input_bound")
    processes = observed["processes"]
    require(type(processes) is dict and set(processes) == {"controller", "bootstrap", *receipt["processes"]},
            "dynamic_current_process_inventory")
    for name, item in processes.items():
        fields(item, {"identity", "stat", "stat_after", "executable", "executable_sha256"})
        wanted = item["identity"]
        if name not in ("controller", "bootstrap"):
            require(exact(wanted, receipt["processes"][name]), "dynamic_original_service_process_identity")
            wanted = {"pid": wanted["pid"], "ppid": wanted["ppid"], "pgrp": wanted["process_group"],
                      "session": wanted["session"], "start_ticks": wanted["start_ticks"]}
        process_identity(evidence.read(item["stat"], "process-stat", 8192), wanted)
        process_identity(evidence.read(item["stat_after"], "process-stat", 8192), wanted)
        checksum(item["executable_sha256"])
        require(type(item["executable"]) is str and item["executable"].startswith("/"), "dynamic_current_executable")
        if name == "controller":
            require(exact(item["identity"], context["process"]), "dynamic_original_controller_process")
        elif name == "bootstrap":
            require(exact(item["identity"], receipt["bootstrap_process"])
                    and item["identity"]["ppid"] == context["process"]["pid"], "dynamic_original_bootstrap_parent")
        else:
            original = receipt["processes"][name]
            require(item["identity"]["ppid"] == receipt["bootstrap_process"]["pid"]
                    and item["executable"] == original["executable"]
                    and item["executable_sha256"] == original["executable_sha256"], "dynamic_original_live_service")
    return observed


def cleanup_obligations(reference, evidence, receipt):
    value = evidence.json(reference, "cleanup-obligations", 65536)
    fields(value, {"format", "run_nonce", "backend", "original_deadline_monotonic_ns", "processes"})
    require(value["format"] == "recall-dynamic-cleanup-obligations/v2"
            and value["run_nonce"] == receipt["run_nonce"] and value["backend"] == receipt["backend"]
            and type(value["original_deadline_monotonic_ns"]) is int
            and value["original_deadline_monotonic_ns"] == receipt["original_deadline_monotonic_ns"]
            and exact(value["processes"], receipt["processes"]), "dynamic_original_cleanup_obligations")
    return value


def startup_identity(receipt, plan, plan_reference, identity, context, wrapper, review):
    fields(receipt, BOOTSTRAP_FIELDS)
    require(receipt["format"] == "recall-runtime-bootstrap/v1" and receipt["status"] == "bootstrapped"
            and receipt["qualification_scope"] == "authenticated-clean-persistent-startup-only"
            and receipt["qualified"] is False and receipt["product_acceptance"] is False,
            "dynamic_bootstrap_is_not_qualification")
    for key in BOOTSTRAP_IDENTITIES - {"configuration_sha256"}:
        require(exact(receipt[key], plan[key]), "dynamic_original_bootstrap_identity")
    require(receipt["configuration_sha256"] == identity["outer_configuration_sha256"]
            and receipt["launch_context_sha256"] == wrapper["raw_sha256"]
            and receipt["plan_sha256"] == plan_reference["sha256"], "dynamic_actual_bootstrap_anchor_plan")
    for key in ("authentication_enabled", "clean_persistent_store", "schema_explicitly_initialized"):
        require(receipt[key] is True, "dynamic_bootstrap_missing_observation")
    for key in ("runtime_config_sha256", "isolation_sha256", "space_requests_sha256", "observed_configurations_sha256"):
        checksum(receipt[key])
    names = {row["name"] for row in plan["spaces"]}
    require(type(receipt["token_files"]) is dict and set(receipt["token_files"]) == names, "dynamic_original_token_roles")
    for name, path in receipt["token_files"].items():
        require(path == plan["run_directory"] + "/private/service-" + name + ".token", "dynamic_original_token_path")
    require(receipt["space_requests_sha256"] == digest(encode(plan["spaces"]))
            and receipt["observed_configurations_sha256"] == digest(encode(receipt["configurations"])), "dynamic_original_configuration_hashes")
    times = [receipt[key] for key in ("allocation_started_monotonic_ns", "started_monotonic_ns", "observed_monotonic_ns",
                                   "original_deadline_monotonic_ns", "whole_deadline_monotonic_ns")]
    clock = context["anchor"]["clock"]
    require(all(integer(value, 1) for value in times) and times[0] <= times[1] <= times[2]
            <= wrapper["observed_monotonic_ns"] < times[3] <= times[4]
            and times[4] == int(clock["deadline_mono"] * 1e9)
            and times[0] >= int(clock["mono_start"] * 1e9)
            and times[3] - times[0] <= review["controller_policy"]["maximum_backend_seconds"] * 10**9,
            "dynamic_original_bootstrap_clock")
    require(type(receipt["processes"]) is dict
            and set(receipt["processes"]) == ({"recall", "paired"} if plan["backend"] == "surreal-db" else {"recall"}),
            "dynamic_original_service_inventory")
    fields(receipt["bootstrap_process"], PROCESS_FIELDS)
    for name, process in receipt["processes"].items():
        fields(process, {"pid", "ppid", "process_group", "session", "start_ticks", "executable", "executable_sha256"})
        require(all(integer(process[key], 1) for key in ("pid", "ppid", "process_group", "session", "start_ticks"))
                and process["pid"] == process["process_group"] == process["session"]
                and process["ppid"] == receipt["bootstrap_process"]["pid"]
                and process["executable"] == "/opt/recall/bin/" + ("elephant" if name == "recall" else "recall-surreal")
                and process["executable_sha256"] == plan["artifact_sha256" if name == "recall" else "paired_artifact_sha256"],
                "dynamic_original_owned_service")
    return receipt


def original_rpc(raw, media, identity):
    """Strict bounded original JSON/SSE response, not a fabricated tool proof."""
    if media.split(";", 1)[0].strip().lower() == "application/json":
        value = strict_json(raw)
    elif media.split(";", 1)[0].strip().lower() == "text/event-stream":
        normalized = raw.replace(b"\r\n", b"\n")
        require(b"\r" not in normalized and normalized.endswith(b"\n\n"), "dynamic_rpc_truncated_sse")
        replies = []
        for block in normalized.split(b"\n\n"):
            data = []
            for line in block.split(b"\n"):
                if not line or line.startswith(b":"):
                    continue
                require(b":" in line, "dynamic_rpc_sse_field")
                key, payload = line.split(b":", 1)
                require(key in (b"data", b"event", b"id", b"retry"), "dynamic_rpc_sse_field")
                if key == b"data":
                    data.append(payload[1:] if payload.startswith(b" ") else payload)
            if data:
                replies.append(strict_json(b"\n".join(data)))
        require(len(replies) == 1, "dynamic_rpc_sse_original_count")
        value = replies[0]
    else:
        raise ValueError("dynamic_rpc_media")
    require(type(value) is dict and set(value) == {"jsonrpc", "id", "result"}
            and value["jsonrpc"] == "2.0" and exact(value["id"], identity), "dynamic_rpc_original_identity")
    result = value["result"]
    require(type(result) is dict and result.get("isError") is not True and type(result.get("content")) is list
            and len(result["content"]) == 1 and type(result["content"][0]) is dict
            and result["content"][0].get("type") == "text" and type(result["content"][0].get("text")) is str,
            "dynamic_rpc_original_tool_result")
    body = strict_json(result["content"][0]["text"].encode())
    return value, body["data"] if type(body) is dict and body.get("ok") is True else body


def schema_initialization(receipt, plan, captures, commands):
    operation = fields(receipt["schema_operation"], {"kind", "capture_index", "process_index"})
    if plan["backend"] == "surreal-db":
        require(operation["kind"] == "migrate" and operation["capture_index"] is None
                and integer(operation["process_index"], 1) and operation["process_index"] in commands,
                "dynamic_native_schema_command_missing")
        row = commands[operation["process_index"]]
        require(row["purpose"] == "explicit-fresh-schema" and row["exit_code"] == 0
                and row["command"] == ["/opt/recall/bin/elephant", "migrate"], "dynamic_native_original_schema_command")
    else:
        require(operation["kind"] == "initialize_runtime" and operation["process_index"] is None
                and integer(operation["capture_index"], 1) and operation["capture_index"] in captures,
                "dynamic_embedded_schema_capture_missing")
        request, response, raw = captures[operation["capture_index"]]
        require(operation["capture_index"] < min(index for index, (item, _, _) in captures.items()
                    if item["method"] == "POST" and item["path"] == "/v1/spaces"), "dynamic_schema_before_space_creation")
        args = {"backend": "elephant-db", "snapshot_path": plan["run_directory"] + "/service/state.snapshot",
                "ingest_enabled": False, "extractor_required": False, "multimodal_asset_root": plan["run_directory"] + "/media"}
        require(request["method"] == "POST" and request["path"] == "/mcp/full" and request["port"] == 43130
                and request["credential_role"] == "operator" and request["space"] is None
                and request["body"]["method"] == "tools/call"
                and exact(request["body"]["params"], {"name": "initialize_runtime", "arguments": args})
                and response["status"] == 200 and response["framing_complete"] is True and response["error_type"] is None,
                "dynamic_embedded_original_schema_call")
        media = response.get("response_headers", {}).get("content-type", [])
        require(type(media) is list and len(media) == 1, "dynamic_original_schema_media")
        envelope, body = original_rpc(raw, media[0], request["body"]["id"])
        require(exact(envelope, response["body"]) and type(body) is dict and body.get("configured") is True
                and body.get("storage_ready") is True and body.get("backend") == "elephant-db",
                "dynamic_embedded_original_schema_result")


def saved_runtime_configuration(reference, evidence, receipt, plan):
    raw = evidence.read(reference, "runtime-configuration-toml", 1048576)
    require(digest(raw) == receipt["runtime_config_sha256"], "dynamic_original_runtime_configuration_bytes")
    try:
        value = tomllib.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeError) as error:
        raise ValueError("dynamic_original_runtime_toml") from error
    # This saved file proves backend/path settings only. Effective worker,
    # provider and tree settings need their separate original product capture.
    require(value.get("runtime", {}).get("ingest_enabled") is True
            and value.get("storage", {}).get("backend") == plan["backend"]
            and value.get("multimodal", {}).get("asset_root") == plan["run_directory"] + "/media",
            "dynamic_saved_runtime_backend_paths")
    if plan["backend"] == "elephant-db":
        require(value["storage"]["elephant_db"]["snapshot_path"] == plan["run_directory"] + "/service/state.snapshot",
                "dynamic_saved_runtime_snapshot_path")
    else:
        storage = value["storage"]["surreal_db"]
        require(storage["url"] == "ws://127.0.0.1:43131" and storage["namespace"] == "primary"
                and storage["database"] == "fixture", "dynamic_saved_runtime_native_path")
    return value


FROZEN_INPUTS = {
    "corpus": "2192437954109fc17ebbea5b3960eb6b398a2ee91a725ce70666b119e86179dc",
    "workload_contract": "696e0fa0f8736306d6e66f5ce26658a21fb013255e5727b5287406008c350a5a",
    "preparation_contract": "b1fd4c43e1c8c506ca13e261a29d65a7d06246bf11ef53c02e97945cd3ef56d1",
    "typed_delta": "51b34aec951d783a93b903a1468fba9a8833ba2f31f705bd784c66c9b26688f8",
    "rendered_map": "9dbcade15daa3fb504838c2fe4f743b4b5fc37da20b2c0f29e2e3aea49ad57ab",
}
PREPARATION_LIMITS = {"sources": 50, "sources_per_space": 25, "http_requests": 4096,
    "minimum_http_requests_with_revision_inspection": 454, "additional_poll_and_existing_call_remainder": 3642,
    "prepare_seconds": 3600, "request_seconds": 10, "close_seconds": 130, "response_bytes": 262144,
    "capture_bytes": 268435456, "embedding_nodes_per_source": 64, "source_text_bytes": 512,
    "provider_attempts_per_backend": 450, "provider_attempts_per_space": 225, "provider_concurrency": 2,
    "reservation_nano_usd_per_attempt": 1064960}


def safe_boundary(function):
    """Convert malformed untrusted shapes into static non-payload failures."""
    def validate(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except ValueError as error:
            code = str(error)
            if re.fullmatch(r"dynamic_[a-z0-9_]+", code):
                raise
            raise ValueError("dynamic_invalid_evidence") from None
        except (KeyError, TypeError, IndexError, AttributeError, UnicodeError, OverflowError, RecursionError, OSError):
            raise ValueError("dynamic_invalid_evidence") from None
    validate.__name__ = function.__name__
    validate.__doc__ = function.__doc__
    return validate


@safe_boundary
def validate_static(reviewed_static, raw_evidence, backend):
    """Return the original reviewed bootstrap plan, without runtime authority."""
    evidence = Evidence(raw_evidence, maximum=reviewed_runtime_bound(reviewed_static))
    _, _, plan, _, _ = static_review(reviewed_static, evidence, backend)
    return strict_json(encode(plan))


def stable_configuration(value):
    require(type(value) is dict and type(value.get("processing")) is dict, "dynamic_configuration_shape")
    result = dict(value)
    require("updated_at" in result, "dynamic_configuration_timestamp_missing")
    timestamp(result.pop("updated_at"))
    processing = dict(result["processing"])
    require(integer(processing.pop("index_generation")), "dynamic_configuration_epoch")
    result["processing"] = processing
    return result


def preparation_sources(inputs, identity):
    successor = is_semantic140(identity["provider_allocation_sha256"])
    if successor:
        semantic140().require_identity(identity)
    expected_inputs = semantic140().FROZEN_INPUTS if successor else FROZEN_INPUTS
    for name, checksum_value in expected_inputs.items():
        require(name in inputs and digest(inputs[name]) == checksum_value
                and identity[name + "_sha256"] == checksum_value, "dynamic_original_preparation_input")
    require("provider_allocation" in inputs
            and digest(inputs["provider_allocation"]) == identity["provider_allocation_sha256"],
            "dynamic_original_provider_allocation")
    corpus = strict_json(inputs["corpus"])
    typed = strict_json(inputs["preparation_contract"])
    delta = strict_json(inputs["typed_delta"])
    rendered = strict_json(inputs["rendered_map"])
    require(typed["fixture_timestamp"] == "2026-10-07T00:00:00Z" and typed["created_by"] == "d28-qualification"
            and delta["base_allocation_sha256"] == identity["provider_allocation_sha256"]
            and delta["preparation_contract_sha256"] == identity["preparation_contract_sha256"]
            and delta["corpus_sha256"] == identity["corpus_sha256"], "dynamic_original_typed_delta")
    sources, spaces = corpus["sources"], corpus["spaces"]
    require(type(sources) is list and len(sources) == 50 and type(spaces) is list and len(spaces) == 2,
            "dynamic_original_source_count")
    names = {row["id"] for row in spaces}
    require(len(names) == 2 and all(sum(row["space"] == name for row in sources) == 25 for name in names),
            "dynamic_original_source_distribution")
    require(len({(row["space"], row["key"]) for row in sources}) == 50, "dynamic_original_source_unique")
    rendered_map = {(row["space"], row["source_key"], row["revision"]): row for row in rendered["sources"]}
    require(len(rendered["sources"]) == len(rendered_map) == 50, "dynamic_rendered_inventory_count")
    for source in sources:
        raw = source["text"].encode("utf-8")
        require(0 < len(raw) <= 512 and digest(raw) == source["sha256"], "dynamic_original_source_bytes")
        row = rendered_map[(source["space"], source["key"], source["revision"])]
        require(row["input_sha256"] == source["sha256"]
                and row["input_utf8_bytes"] == len(raw)
                and row["rendered_text"].encode("utf-8") == raw + b"\n"
                and row["rendered_sha256"] == digest(raw + b"\n")
                and row["rendered_utf8_bytes"] == len(raw) + 1, "dynamic_independent_rendered_map")
    return sources, spaces, typed, rendered_map


def typed_source_request(source, identity, typed):
    nonce = identity["preparation_nonce"]
    require(type(nonce) is str and re.fullmatch(r"[A-Za-z0-9_-]{16,64}", nonce), "dynamic_preparation_nonce")
    source_hash = digest(encode([identity["backend"], nonce, source["space"], source["key"], source["revision"]]))
    def base(table):
        return {"id": table + ":" + str(uuid.uuid5(uuid.NAMESPACE_URL, "d28/" + source_hash + "/" + table)),
                "space_id": source["space"], "created_at": typed["fixture_timestamp"],
                "updated_at": typed["fixture_timestamp"], "created_by": typed["created_by"],
                "deleted_at": None, "provenance": {"sources": [], "chain": []}, "subjects": [],
                "subjects_state": "unknown", "security": typed["security_by_space"][source["space"]]}
    return {"source": {**base("source"), "type": "manual", "hash": source["sha256"],
                       "external_id": source["key"], "uri": source["source_url"], "metadata": {}},
            "doc": {**base("doc"), "title": source["key"], "text": source["text"], "source": None,
                    "sidecar_text": None, "media_attachments": [],
                    "rendition_key": f"d28/text/{source['key']}/{source['revision']}", "metadata": {}},
            "ingest_mode": "retrieval", "intake": {"revision_id": source["revision"], "idempotency_key": "d28-" + source_hash}}


def preparation_captures(reference, evidence, observed, controller, wrapper):
    """Bind original durable request intents to complete bounded raw responses."""
    inventory = evidence.json(reference, "preparation-intents", 4 * 1024**2)
    fields(inventory, {"format", "requests"})
    require(inventory["format"] == "recall-dynamic-preparation-intents/v2"
            and type(inventory["requests"]) is list and 454 <= len(inventory["requests"]) <= 4096,
            "dynamic_preparation_original_count")
    require(type(observed["http_requests"]) is int and observed["http_requests"] == len(inventory["requests"]),
            "dynamic_preparation_request_count")
    captures, previous_end, total = [], None, 0
    for index, item in enumerate(inventory["requests"], 1):
        fields(item, {"request_index", "intent", "response"})
        require(type(item["request_index"]) is int and item["request_index"] == index, "dynamic_preparation_capture_order")
        raw_intent = evidence.read(item["intent"], "preparation-intent", 131072)
        raw_response = evidence.read(item["response"], "preparation-response", 2 * 1024**2)
        total += len(raw_intent) + len(raw_response)
        require(total <= 268435456, "dynamic_preparation_capture_bound")
        intent, result = strict_json(raw_intent), strict_json(raw_response)
        fields(intent, {"format", "request_index", "method", "path", "space", "credential_role", "request_body",
                        "request_body_base64", "request_sha256", "started_monotonic_ns", "harness_attempt", "state"})
        fields(result, {"method", "path", "space", "request_body", "request_sha256", "response", "request_index",
                        "request_intent_sha256", "elapsed_ms", "error_type"})
        require(intent["format"] == "recall-workload-preparation-request/v1"
                and type(intent["harness_attempt"]) is int and intent["harness_attempt"] == 1
                and intent["state"] == "intent-only; no physical-send claim"
                and intent["request_index"] == result["request_index"] == index
                and result["request_intent_sha256"] == digest(raw_intent)
                and result["error_type"] is None, "dynamic_preparation_original_intent")
        for key in ("method", "path", "space", "request_body", "request_sha256"):
            require(exact(intent[key], result[key]), "dynamic_preparation_intent_response_binding")
        require(intent["method"] in ("GET", "POST") and intent["credential_role"] == "service-" + intent["space"],
                "dynamic_preparation_original_authority")
        payload = base64.b64decode(intent["request_body_base64"], validate=True)
        require(len(payload) <= 32768 and digest(payload) == intent["request_sha256"]
                and payload == (b"" if intent["request_body"] is None else encode(intent["request_body"])),
                "dynamic_preparation_original_request_bytes")
        response = result["response"]
        required = {"framing_complete", "status", "body", "raw_body_base64", "body_sha256", "bytes", "raw_body_complete"}
        optional = {"response_headers", "first_response_byte_ns", "wire_bytes"}
        require(type(response) is dict and required <= set(response) <= required | optional,
                "dynamic_preparation_response_fields")
        raw = base64.b64decode(response["raw_body_base64"], validate=True)
        require(response["framing_complete"] is True and response["raw_body_complete"] is True
                and integer(response["bytes"], 0, 262144) and response["bytes"] == len(raw)
                and digest(raw) == response["body_sha256"] and exact(strict_json(raw), response["body"]),
                "dynamic_preparation_original_response_bytes")
        start, elapsed = intent["started_monotonic_ns"], result["elapsed_ms"]
        require(integer(start, 1) and type(elapsed) in (int, float) and math.isfinite(elapsed) and 0 <= elapsed <= 10000,
                "dynamic_preparation_request_deadline")
        end = start + elapsed * 10**6
        clock = controller["anchor"]["clock"]
        require(clock["mono_start"] * 10**9 <= start <= end <= wrapper["observed_monotonic_ns"]
                < clock["deadline_mono"] * 10**9 and (previous_end is None or start >= previous_end - 1),
                "dynamic_preparation_original_clock_order")
        previous_end = end
        captures.append({"intent": intent, "response": response, "start": start, "end": end,
                         "intent_sha256": item["intent"]["sha256"], "response_sha256": item["response"]["sha256"]})
    require(captures[-1]["end"] - captures[0]["start"] <= 3600 * 10**9, "dynamic_preparation_phase_deadline")
    return captures


def accepted_identity(source, body):
    require(type(body) is dict, "dynamic_preparation_acceptance")
    operation = body["operation"]
    require(type(operation) is dict, "dynamic_preparation_acceptance_operation")
    for key in ("operation_id", "source_id", "source_generation"):
        require(type(operation.get(key)) is str and operation[key], "dynamic_preparation_acceptance_identity")
    require(body["source_id"] == operation["source_id"] and operation["space_id"] == source["space"]
            and operation["desired_revision"] == source["revision"]
            and type(body["doc_id"]) is str and body["doc_id"].startswith("doc:")
            and type(body["work_item_id"]) is str and body["work_item_id"], "dynamic_preparation_acceptance_binding")
    return body


def completed_identity(source, accepted, value, *, complete):
    require(type(value) is dict, "dynamic_preparation_operation_shape")
    original = accepted["operation"]
    for field, key in (("operation_id", "operation_id"), ("internal_source_id", "source_id"), ("generation", "source_generation")):
        require(value.get(field) == original[key], "dynamic_preparation_operation_replaced")
    require(value.get("desired_revision") == source["revision"] and value.get("lifecycle") == "active",
            "dynamic_preparation_original_revision_lifecycle")
    require(value.get("state") in ("pending", "running", "failed_retryable", "complete"),
            "dynamic_preparation_failed_or_unknown_operation")
    if complete:
        require(value["state"] == "complete" and value.get("published_revision") == source["revision"]
                and value.get("error_code") is None, "dynamic_preparation_original_publication_incomplete")


def classified_record(value, space, security):
    require(type(value) is dict and value.get("space_id") == space and exact(value.get("security"), security)
            and value.get("deleted_at") is None and value.get("subjects") == []
            and value.get("subjects_state") == "unknown", "dynamic_preparation_classified_record")


def public_publication(source, accepted, records, rendered, security, partition):
    """Verify complete public lineage and independent rendered spans only."""
    fields(records, {"operation", "head", "source", "doc", "nodes", "pipeline", "immutable_revision"})
    for name in ("operation", "head"):
        completed_identity(source, accepted, records[name], complete=True)
    doc, record, immutable, pipeline, nodes = (records[key] for key in ("doc", "source", "immutable_revision", "pipeline", "nodes"))
    text = rendered["rendered_text"]
    for item in (doc, record, immutable):
        classified_record(item, source["space"], security)
    require(doc["id"] == accepted["doc_id"] and doc["source"] == accepted["source_id"]
            and doc["title"] == source["key"] and doc["text"] == text, "dynamic_preparation_actual_document")
    require(record["id"] == accepted["source_id"] and record["external_id"] == source["key"]
            and record["hash"] == source["sha256"] and record.get("uri") == source["source_url"]
            and record["current_ingest_mode"] == "retrieval" and record["lifecycle_status"] == "retrieval_ready",
            "dynamic_preparation_actual_source")
    revision = pipeline["revision"]
    require(type(revision) is str and revision.startswith("doc_revision:") and pipeline["doc_id"] == accepted["doc_id"]
            and immutable["id"] == revision and immutable["doc"] == doc["id"] and immutable["status"] == "active"
            and immutable.get("supersedes") is None and immutable["text"] == text
            and immutable["text_hash"] == rendered["rendered_sha256"]
            and exact(immutable["renderer"], {"name": "elephant.renderer.text", "version": "1.0.0", "config_hash": None})
            and immutable["provenance"]["sources"] == [accepted["source_id"]], "dynamic_preparation_immutable_revision")
    timestamp(immutable["rendered_at"])
    require(type(nodes) is list and 0 < len(nodes) <= 128 and len({row["id"] for row in nodes}) == len(nodes),
            "dynamic_preparation_complete_node_inventory")
    roles = {"document": [], "paragraph": [], "chunk": []}
    embeddable, raw = [], text.encode("utf-8")
    require(digest(raw) == rendered["rendered_sha256"] and len(raw) == rendered["rendered_utf8_bytes"],
            "dynamic_preparation_rendered_identity")
    for node in nodes:
        classified_record(node, source["space"], security)
        provenance = node["provenance"]
        require(node["revision"] == revision and provenance["sources"] == [accepted["source_id"]]
                and type(provenance["chain"]) is list and any(row.get("input") == revision for row in provenance["chain"])
                and node["role"] in roles, "dynamic_preparation_node_lineage")
        span = fields(node["span"], {"start", "end"})
        require(integer(span["start"]) and integer(span["end"], 1) and 0 <= span["start"] < span["end"] <= len(raw),
                "dynamic_preparation_rendered_span")
        quote_text = raw[span["start"]:span["end"]].decode("utf-8")
        roles[node["role"]].append(span)
        if node["role"] in ("paragraph", "chunk"):
            require(node.get("text") in (None, quote_text) and node.get("embedding_model") == partition,
                    "dynamic_preparation_node_profile")
            embeddable.append(node["id"])
    require(0 < len(embeddable) <= 64, "dynamic_preparation_embedding_node_cap")
    for role, spans in (("document", [rendered["root_span"]]), ("paragraph", rendered["paragraph_spans"]), ("chunk", rendered["chunk_spans"])):
        require(exact(sorted(roles[role], key=lambda span: (span["start"], span["end"])), spans),
                "dynamic_preparation_independent_span_inventory")
    require(exact(pipeline["chunks"], {"total": len(embeddable), "embedded": len(embeddable)})
            and type(pipeline["nodes_count"]) is int and pipeline["nodes_count"] == len(nodes),
            "dynamic_preparation_public_profile_coverage")
    return {"space": source["space"], "key": source["key"], "revision": source["revision"],
            "source_id": accepted["source_id"], "doc_id": doc["id"], "internal_doc_revision_id": revision,
            "operation_id": records["operation"]["operation_id"], "generation": records["operation"]["generation"],
            "source_sha256": source["sha256"], "embeddable_node_ids": embeddable,
            "rendered_sha256": rendered["rendered_sha256"], "rendered_utf8_bytes": len(raw),
            "public_profile_marker": partition,
            "original_revision_record": {"id": revision, "text_sha256": immutable["text_hash"],
                "renderer": immutable["renderer"], "response_body_canonical_sha256": digest(encode(immutable))},
            "original_private_manifest": "unmet: not exposed by these public reads",
            "physical_vector_inventory": "unmet: node markers and completed operation are not a raw index inventory"}


def preparation_blueprints(inputs, names, typed):
    require("expected_configurations" in inputs, "dynamic_static_preparation_blueprints_missing")
    blueprint = strict_json(inputs["expected_configurations"])
    require(type(blueprint) is dict and set(blueprint) == set(names), "dynamic_static_preparation_blueprints")
    for name, row in blueprint.items():
        fields(row, {"record_security", "embedding_partition_model", "input_envelope"})
        require(exact(row["record_security"], typed["security_by_space"][name])
                and type(row["embedding_partition_model"]) is str and row["embedding_partition_model"],
                "dynamic_static_preparation_profile")
        require(exact(row["input_envelope"], {"security": row["record_security"], "deleted_at": None,
            "provenance": {"sources": [], "chain": []}, "subjects": [], "subjects_state": "unknown"}),
            "dynamic_static_preparation_input_envelope")
    return blueprint


def capture_configurations(reference, evidence, captures, names):
    refs = evidence.json(reference, "preparation-configurations", 131072)
    require(type(refs) is dict and set(refs) == set(names), "dynamic_preparation_configuration_refs")
    actual, indices = {}, {}
    for name, ref in refs.items():
        raw = evidence.read(ref, "preparation-response", 2 * 1024**2)
        matches = [row for row in captures if row["response_sha256"] == digest(raw)]
        require(len(matches) == 1, "dynamic_preparation_configuration_capture_identity")
        capture = matches[0]
        request = capture["intent"]
        require(request["method"] == "GET" and request["path"] == "/v1/spaces/" + quote(name, safe="")
                and request["space"] == name and capture["response"]["status"] == 200,
                "dynamic_preparation_configuration_original_read")
        actual[name], indices[name] = capture["response"]["body"], request["request_index"]
    return actual, indices


def replay_preparation(captures, sources, typed, rendered, blueprint, identity, before, before_indices, after, after_indices,
                       observed, immutable_reference, evidence):
    names = set(before)
    require(set(before_indices.values()) == {1, 2}
            and set(after_indices.values()) == {len(captures) - 1, len(captures)}, "dynamic_preparation_config_read_order")
    for name in names:
        require(exact(stable_configuration(before[name]), stable_configuration(after[name])),
                "dynamic_preparation_authority_changed")
    by_key = {(source["space"], source["key"]): source for source in sources}
    head_paths = {(space, "/v1/spaces/" + quote(space, safe="") + "/sources/" + quote(key, safe="") + "/head"): source
                  for (space, key), source in by_key.items()}
    absent = set()
    for item in captures[2:52]:
        intent, response = item["intent"], item["response"]
        source = head_paths[(intent["space"], intent["path"])]
        require(intent["method"] == "GET" and intent["request_body"] is None and response["status"] == 404,
                "dynamic_preparation_original_absence")
        body = fields(response["body"], {"error_code", "message", "retryable"})
        require(body["error_code"] == "not_found" and body["retryable"] is False
                and type(body["message"]) is str and len(body["message"].encode()) <= 512,
                "dynamic_preparation_original_absence_denial")
        absent.add((source["space"], source["key"]))
    require(absent == set(by_key), "dynamic_preparation_absence_inventory")
    accepted, operations, route_owner, posts = {}, {}, {}, []
    for item, source in zip(captures[52:102], sources):
        intent, response = item["intent"], item["response"]
        require(intent["method"] == "POST" and intent["path"] == "/v1/sources/ingest"
                and intent["space"] == source["space"] and response["status"] == 202
                and exact(intent["request_body"], typed_source_request(source, identity, typed)),
                "dynamic_preparation_original_submission")
        actual = accepted_identity(source, response["body"])
        key = (source["space"], source["key"])
        accepted[key] = actual
        posts.append(actual)
        path = "/v1/operations/" + quote(actual["operation"]["operation_id"], safe="")
        require((source["space"], path) not in operations, "dynamic_preparation_duplicate_operation")
        operations[(source["space"], path)] = key
        doc = "/v1/docs/" + quote(actual["doc_id"], safe="")
        for name, route in (("head", next(route for (space, route), row in head_paths.items() if row is source)),
                            ("source", "/v1/sources/" + quote(actual["source_id"], safe="")),
                            ("doc", doc), ("nodes", doc + "/chunks"), ("pipeline", doc + "/pipeline-status")):
            require((source["space"], route) not in route_owner, "dynamic_preparation_duplicate_publication_route")
            route_owner[(source["space"], route)] = (key, name)
    require(exact(posts, observed["accepted"]), "dynamic_preparation_original_accepted_inventory")
    completed, records, immutable_refs, completion_order = set(), {key: {} for key in accepted}, {}, []
    for capture in captures[102:-2]:
        intent, response = capture["intent"], capture["response"]
        route = (intent["space"], intent["path"])
        require(intent["method"] == "GET" and intent["request_body"] is None and response["status"] == 200,
                "dynamic_preparation_unreviewed_request")
        if route in operations:
            key = operations[route]
            require(key not in completed, "dynamic_preparation_repeated_completed_operation")
            completed_identity(by_key[key], accepted[key], response["body"], complete=False)
            if response["body"]["state"] == "complete":
                completed_identity(by_key[key], accepted[key], response["body"], complete=True)
                completed.add(key)
                completion_order.append(key)
                records[key]["operation"] = response["body"]
            continue
        require(route in route_owner, "dynamic_preparation_unknown_publication_read")
        key, name = route_owner[route]
        require(key in completed and name not in records[key], "dynamic_preparation_publication_read_order")
        records[key][name] = response["body"]
        if name == "pipeline":
            revision = response["body"]["revision"]
            require(type(revision) is str and revision.startswith("doc_revision:"), "dynamic_preparation_revision_identity")
            immutable_route = (key[0], "/v1/docs/" + quote(accepted[key]["doc_id"], safe="") + "/revisions/" + quote(revision, safe=""))
            require(immutable_route not in route_owner, "dynamic_preparation_duplicate_immutable_route")
            route_owner[immutable_route] = (key, "immutable_revision")
        elif name == "immutable_revision":
            immutable_refs[key] = capture["response_sha256"]
    require(completed == set(accepted), "dynamic_preparation_incomplete_original_operations")
    original_revisions = evidence.json(immutable_reference, "preparation-immutable-inventory", 131072)
    require(type(original_revisions) is list and len(original_revisions) == 50, "dynamic_preparation_immutable_inventory")
    seen = set()
    for row in original_revisions:
        fields(row, {"space", "key", "revision", "response"})
        key = (row["space"], row["key"])
        require(key in by_key and key not in seen and row["revision"] == by_key[key]["revision"],
                "dynamic_preparation_immutable_inventory_identity")
        raw = evidence.read(row["response"], "preparation-response", 2 * 1024**2)
        require(digest(raw) == immutable_refs[key], "dynamic_preparation_original_immutable_response")
        seen.add(key)
    inventory = []
    for key in completion_order:
        source = by_key[key]
        row = blueprint[key[0]]
        inventory.append(public_publication(source, accepted[key], records[key],
                         rendered[(key[0], key[1], source["revision"])], row["record_security"], row["embedding_partition_model"]))
    require(exact(inventory, observed["published_inventory"]), "dynamic_preparation_published_inventory_changed")
    return inventory


def captured_tools(captures, name):
    values = []
    for index, (request, response, raw) in captures.items():
        body = request["body"]
        if (request["method"] != "POST" or request["path"] != "/mcp/full" or type(body) is not dict
                or body.get("method") != "tools/call" or type(body.get("params")) is not dict
                or body["params"].get("name") != name):
            continue
        require(request["credential_role"] == "operator" and response["status"] == 200
                and response["framing_complete"] is True and response["error_type"] is None,
                "dynamic_runtime_recipe_original_authority")
        media = response.get("response_headers", {}).get("content-type", [])
        require(type(media) is list and len(media) == 1, "dynamic_runtime_recipe_media")
        envelope, result = original_rpc(raw, media[0], body["id"])
        require(exact(envelope, response["body"]), "dynamic_runtime_recipe_original_response")
        values.append((index, request, result))
    return values


def effective_worker_recipe(captures, plan):
    originals = captured_tools(captures, "get_runtime_config")
    require(originals, "dynamic_actual_worker_recipe_missing")
    recipes = []
    for _, request, result in originals:
        require(exact(request["body"]["params"], {"name": "get_runtime_config", "arguments": {}})
                and type(result) is dict, "dynamic_worker_recipe_original_request")
        recipe = fields(result["managed_worker_recipe"], {"format", "instance_id", "backend", "work_queue", "embed_worker"})
        require(recipe["format"] == "recall-managed-worker-recipe/v1" and recipe["backend"] == plan["backend"]
                and str(uuid.UUID(recipe["instance_id"])) == recipe["instance_id"], "dynamic_actual_worker_instance")
        queue = fields(recipe["work_queue"], {"enabled", "max_concurrent", "max_extract_concurrent", "work_item_timeout_secs",
            "transient_retry_max_attempts", "transient_retry_base_delay_secs", "transient_retry_max_delay_secs",
            "allowed_kinds", "disabled_kinds", "doc_tree"})
        require(queue["enabled"] is True and type(queue["transient_retry_max_attempts"]) is int
                and queue["transient_retry_max_attempts"] == 3 and integer(queue["max_concurrent"], 1)
                and integer(queue["max_extract_concurrent"], 1) and integer(queue["work_item_timeout_secs"], 1)
                and integer(queue["transient_retry_base_delay_secs"], 1)
                and integer(queue["transient_retry_max_delay_secs"], queue["transient_retry_base_delay_secs"]),
                "dynamic_actual_queue_retry_recipe")
        for key in ("allowed_kinds", "disabled_kinds"):
            require(type(queue[key]) is list and queue[key] == [], "dynamic_actual_queue_kinds")
        tree = fields(queue["doc_tree"], {"max_chunk_size", "chunk_overlap", "create_paragraphs", "detect_sections",
                                         "semantic_chunking", "create_summaries", "summarizer_model"})
        require(exact({key: tree[key] for key in tree if key != "summarizer_model"},
                      {"max_chunk_size": 512, "chunk_overlap": 50, "create_paragraphs": True,
                       "detect_sections": False, "semantic_chunking": True, "create_summaries": False})
                and type(tree["summarizer_model"]) is str, "dynamic_actual_tree_recipe")
        template = fields(recipe["embed_worker"], {"enabled", "batch_size", "provider", "model", "dimensions"})
        require(template["enabled"] is True and integer(template["batch_size"], 1, 128)
                and integer(template["dimensions"], 1) and all(type(template[key]) is str for key in ("provider", "model")),
                "dynamic_actual_worker_embedding_template")
        recipes.append(recipe)
    require(all(exact(recipe, recipes[0]) for recipe in recipes), "dynamic_actual_worker_instance_changed")
    return recipes[0]


def setup_process_proofs(reference, evidence, receipt, plan, context):
    wrapper = evidence.json(reference, "startup-command-inventory", 4 * 1024**2)
    fields(wrapper, {"format", "commands", "process_proofs"})
    require(wrapper["format"] == "recall-dynamic-startup-command-inventory/v2", "dynamic_setup_original_custody_missing")
    commands = startup_journal(receipt, plan, wrapper["commands"], evidence)
    require(type(wrapper["process_proofs"]) is list and len(wrapper["process_proofs"]) == len(commands),
            "dynamic_setup_process_proof_inventory")
    try:
        from installed_evidence_validator import validate_process_proof
    except ImportError:
        raise ValueError("dynamic_installed_process_validator_unavailable") from None
    for index, reference in enumerate(wrapper["process_proofs"], 1):
        snapshot = evidence.json(reference, "startup-process-proof", 2 * 1024**2)
        row = commands[index]
        expected_binary = plan["paired_artifact_sha256"] if row["command"][0] == "/opt/recall/bin/recall-surreal" else plan["artifact_sha256"]
        require(snapshot["index"] == index and exact(snapshot["intent"]["argv"], row["command"])
                and snapshot["intent"]["cwd"] == row["cwd"]
                and exact(snapshot["intent"]["environment_names"], row["environment_names"])
                and snapshot["identity"]["pid"] == row["pid"], "dynamic_setup_original_command_proof_binding")
        result = validate_process_proof(snapshot, evidence.raw, expected_binary, receipt["bootstrap_process"],
                                       receipt["original_deadline_monotonic_ns"], require_terminal=row["exit_code"] is not None)
        require(row["started_monotonic_ns"] <= result["started_ns"] <= receipt["observed_monotonic_ns"]
                and (result["reaped_ns"] is None or result["reaped_ns"] <= receipt["observed_monotonic_ns"]),
                "dynamic_setup_original_process_clock_binding")
    return commands


def runtime_identity(observed_runtime, static_identity):
    fields(observed_runtime, RUNTIME_FIELDS)
    require(observed_runtime["format"] == "recall-dynamic-runtime-evidence/v2", "dynamic_runtime_evidence_format")
    identity = fields(observed_runtime["identity"], IDENTITY_FIELDS)
    require(exact({key: value for key, value in identity.items() if key != "outer_configuration_sha256"}, static_identity),
            "dynamic_runtime_static_identity")
    checksum(identity["outer_configuration_sha256"])
    return identity


@safe_boundary
def validate_runtime(reviewed_static, observed_runtime, raw_evidence, controller_context):
    """Validate the complete installed prerequisite and current startup chain."""
    evidence = Evidence(raw_evidence, maximum=reviewed_runtime_bound(reviewed_static))
    fields(observed_runtime, RUNTIME_FIELDS)
    review, original, plan, plan_ref, inputs = static_review(reviewed_static, evidence, observed_runtime["identity"]["backend"])
    identity = runtime_identity(observed_runtime, original)
    runtime_inventory, captured_size = evidence.inventory(observed_runtime["capture_inventory"])
    require(captured_size <= 148 * 1024**2, "dynamic_current_capture_bound")
    context = context_evidence(controller_context, observed_runtime["controller"], evidence, review, identity, plan)
    receipt = evidence.json(observed_runtime["bootstrap"], "runtime-bootstrap", 4 * 1024**2)
    startup_identity(receipt, plan, plan_ref, identity, context, controller_context, review)
    original_isolation(receipt, context, evidence)
    captures = startup_captures(receipt, evidence)
    startup_proofs(receipt, plan, captures)
    startup_authority(receipt, plan, captures)
    actual = evidence.json(observed_runtime["actual_space_configurations"], "space-configurations", 4 * 1024**2)
    require(exact(actual, receipt["configurations"]), "dynamic_actual_config_receipt_binding")
    configurations(plan, actual)
    saved_runtime_configuration(observed_runtime["actual_runtime_configuration"], evidence, receipt, plan)
    processes = current_processes(observed_runtime["current_processes"], evidence, receipt, context, controller_context)
    outer_launch(observed_runtime["outer_launch"], evidence, runtime_inventory, processes, context, identity, plan, review)
    commands = setup_process_proofs(observed_runtime["command_inventory"], evidence, receipt, plan, context)
    schema_initialization(receipt, plan, captures, commands)
    effective_worker_recipe(captures, plan)
    # A global embedding worker template cannot establish the actual governed
    # provider attempt/preprocessing recipe. The original inspection route and
    # exact C carrier remain required even if the installed source uses defaults.
    effective_provider_recipes(captures, plan, actual)
    offline = evidence.json(observed_runtime["installed_offline"], "installed-offline-evidence", 8 * 1024**2)
    try:
        from installed_evidence_validator import validate_installed
    except ImportError:
        raise ValueError("dynamic_installed_validator_unavailable") from None
    prerequisite = validate_installed(reviewed_static, offline, evidence.raw)
    fields(prerequisite, {"identity", "required_cases"})
    require(exact(prerequisite["required_cases"], INSTALLED_CASES), "dynamic_installed_case_inventory")
    fields(prerequisite["identity"], {"source_commit", "artifact_sha256", "binary_sha256", "backend"})
    for key in ("source_commit", "artifact_sha256", "binary_sha256", "backend"):
        require(prerequisite["identity"][key] == identity[key], "dynamic_installed_candidate_mismatch")
    obligations = cleanup_obligations(observed_runtime["cleanup_obligations"], evidence, receipt)
    return {"format": "recall-installed-runtime-component/v2", "verdict": "passed",
            "scope": "installed-offline-and-current-runtime", "product_acceptance": False, "identity": dict(identity),
            "static_review_sha256": reviewed_static["raw_sha256"], "contract_sha256": CONTRACT_SHA256,
            "evidence_sha256": digest(encode(observed_runtime)), "controller_sha256": observed_runtime["controller"]["sha256"],
            "actual_runtime_configuration_sha256": observed_runtime["actual_runtime_configuration"]["sha256"],
            "actual_space_configurations_sha256": observed_runtime["actual_space_configurations"]["sha256"],
            "required_cases": list(INSTALLED_CASES), "remaining_cleanup_obligations": obligations,
            "proof_limits": list(PROOF_LIMITS)}


def effective_provider_recipes(captures, plan, configurations_by_name):
    """Bind actual inspected factory policy to every current named-space field."""
    by_space = {row["name"]: [] for row in plan["spaces"]}
    for _, (request, response, raw) in captures.items():
        name = request["space"]
        if name not in by_space or request["method"] != "GET" or request["path"] != f"/v1/spaces/{name}/status":
            continue
        require(request["credential_role"] == "service-" + name and response["status"] in (200, 503)
                and response["framing_complete"] is True and response["error_type"] is None
                and exact(strict_json(raw), response["body"]), "dynamic_provider_recipe_original_read")
        status = response["body"]
        require(type(status) is dict and status.get("space") == name
                and status.get("storage_ready") is True and type(status.get("embedding_execution_recipes")) is dict,
                "dynamic_actual_provider_recipe_missing")
        actual = configurations_by_name[name]
        processing = actual["processing"]
        require(status["processing_generation"] == processing["generation"], "dynamic_provider_recipe_current_generation")
        recipes = status["embedding_execution_recipes"]
        require(set(recipes) <= {"query_embedding", "document_embedding", "reindex_embedding"}
                and {"query_embedding", "document_embedding"} <= set(recipes), "dynamic_provider_recipe_operation_inventory")
        for operation, recipe in recipes.items():
            fields(recipe, {"provenance", "partition", "max_total_http_attempts", "max_inputs_per_call",
                            "initial_segment_bytes", "max_split_depth"})
            binding = processing["bindings"][operation]
            profile = processing["profiles"][binding["profile_id"]]
            connection = processing["connections"][profile["connection_id"]]
            require(binding["revoked"] is False and connection["revoked"] is False
                    and status["approved_operations"][operation] is True
                    and status["available_operations"][operation] is True,
                    "dynamic_actual_provider_recipe_revoked")
            require(exact(recipe["provenance"], {"config": {"space_id": name, "config_id": actual["id"],
                    "generation": processing["generation"], "policy_id": processing["policy_id"]},
                    "operation": operation, "binding_id": binding["id"], "profile_id": profile["id"]}),
                    "dynamic_provider_recipe_original_provenance")
            expected = {"space_id": name, "provider": connection["provider"], "account_id": connection["account_id"],
                        "endpoint": connection["endpoint"], "profile_id": profile["id"], "model": profile["model"],
                        "model_version": profile["model_version"], "dimensions": profile["dimensions"],
                        "preprocessing": profile["preprocessing"]}
            require(exact(recipe["partition"], expected) and expected["provider"] == "openai"
                    and expected["preprocessing"] == "text-v1" and recipe["initial_segment_bytes"] is None
                    and recipe["max_split_depth"] is None and type(recipe["max_total_http_attempts"]) is int
                    and recipe["max_total_http_attempts"] == 3 and type(recipe["max_inputs_per_call"]) is int
                    and recipe["max_inputs_per_call"] == 100, "dynamic_actual_provider_attempt_recipe")
            active = processing["active_embedding_profile_id"] if operation != "reindex_embedding" else processing["pending_reindex"]["to_profile_id"]
            require(profile["id"] == active, "dynamic_provider_recipe_active_profile")
        by_space[name].append(recipes)
    require(all(rows and all(exact(item, rows[0]) for item in rows) for rows in by_space.values()),
            "dynamic_actual_provider_recipe_missing_or_changed")
    return {name: rows[0] for name, rows in by_space.items()}


STATIC_LIMITS = {"runtime_raw_bytes": 369098752, "runtime_capture_bytes": 155189248,
                 "installed_capture_bytes": 100663296, "preparation": PREPARATION_LIMITS,
                 "provider_capture_bytes": 21474836480, "provider_cache_bytes": 268435456}


def reject_dynamic_static(value):
    if type(value) is dict:
        require(not set(value) & DYNAMIC_STATIC_FIELDS, "dynamic_static_future_identity")
        for item in value.values():
            reject_dynamic_static(item)
    elif type(value) is list:
        for item in value:
            reject_dynamic_static(item)


def preparation_observation(value, identity):
    required = {"format", "phase", "source_commit", "artifact_sha256", "binary_sha256", "backend", "endpoint", "nonce",
                "corpus_sha256", "contract_sha256", "preparation_contract_sha256", "provider_allocation_sha256",
                "typed_delta_sha256", "rendered_map_sha256", "provider_cohort_begin", "http_requests", "acceptance_post_attempts",
                "accepted", "published_inventory", "source_count", "query_warmups", "harness_retries", "run_error_type",
                "qualified", "product_acceptance", "proof_limits", "capture_verdict", "provider_accounting"}
    successor = is_semantic140(identity["provider_allocation_sha256"])
    fields(value, required | {"profile_id"} if successor else required)
    if successor:
        require(value["profile_id"] == semantic140().PROFILE_ID, "dynamic_preparation_profile_identity")
    require(value["format"] == "recall-workload-preparation-observation/v1" and value["phase"] == "preparation"
            and value["qualified"] is False and value["product_acceptance"] is False
            and value["run_error_type"] is None and value["capture_verdict"] == "passed", "dynamic_preparation_incomplete_observation")
    for key in ("source_commit", "artifact_sha256", "binary_sha256", "backend", "endpoint", "corpus_sha256",
                "preparation_contract_sha256", "provider_allocation_sha256", "typed_delta_sha256", "rendered_map_sha256"):
        require(exact(value[key], identity[key]), "dynamic_preparation_observation_identity")
    require(value["contract_sha256"] == identity["workload_contract_sha256"] and value["nonce"] == identity["preparation_nonce"],
            "dynamic_preparation_observation_contract_nonce")
    for key, wanted in (("acceptance_post_attempts", 50), ("source_count", 50), ("query_warmups", 0), ("harness_retries", 0)):
        require(type(value[key]) is int and value[key] == wanted, "dynamic_preparation_original_counters")
    require(exact(value["proof_limits"], ["public embedding markers are not raw physical vectors",
                "original private publication manifests are not returned by these APIs"]), "dynamic_preparation_retained_limits")
    return value


def preparation_runtime_chain(reference, evidence, reviewed_static, supplied_component, context_wrapper, review, identity, plan):
    chain = evidence.json(reference, "runtime-validation-chain", 4 * 1024**2)
    fields(chain, {"format", "component", "evidence", "validated_monotonic_ns", "validated_wall", "current_custody"})
    require(chain["format"] == "recall-dynamic-runtime-chain/v2" and integer(chain["validated_monotonic_ns"], 1)
            and type(chain["validated_wall"]) in (int, float) and math.isfinite(chain["validated_wall"])
            and chain["validated_monotonic_ns"] <= context_wrapper["observed_monotonic_ns"]
            and chain["validated_wall"] <= context_wrapper["observed_wall"], "dynamic_preparation_original_runtime_clock")
    component = evidence.json(chain["component"], "runtime-component", 4 * 1024**2)
    original_runtime = evidence.json(chain["evidence"], "runtime-evidence", 8 * 1024**2)
    require(exact(component, supplied_component) and exact(component["identity"], {key: identity[key] for key in IDENTITY_FIELDS}),
            "dynamic_preparation_original_runtime_component")
    original_wrapper = {**context_wrapper, "observed_monotonic_ns": chain["validated_monotonic_ns"], "observed_wall": chain["validated_wall"]}
    recalculated = validate_runtime(reviewed_static, original_runtime, evidence.raw, original_wrapper)
    require(exact(recalculated, component), "dynamic_preparation_runtime_predicate_changed")
    # Runtime replay uses its own independent evidence reader. Register the
    # SAME original inventory in this preparation reader before resolving the
    # original controller's path-bound network captures. No paths are inferred
    # from the later current-custody record or reconstructed from summaries.
    _, original_size = evidence.inventory(original_runtime["capture_inventory"])
    require(original_size <= 148 * 1024**2, "dynamic_current_capture_bound")
    context = context_evidence(context_wrapper, original_runtime["controller"], evidence, review, identity, plan)
    receipt = evidence.json(original_runtime["bootstrap"], "runtime-bootstrap", 4 * 1024**2)
    current = evidence.json(chain["current_custody"], "runtime-current-custody", 4 * 1024**2)
    fields(current, {"format", "current_processes", "capture_inventory", "cleanup_obligations", "outer_launch",
                     "worker_recipe_capture", "provider_recipe_captures"})
    require(current["format"] == "recall-dynamic-current-runtime-custody/v2", "dynamic_preparation_current_custody_format")
    current_inventory, size = evidence.inventory(current["capture_inventory"])
    require(size <= 148 * 1024**2, "dynamic_current_capture_bound")
    processes = current_processes(current["current_processes"], evidence, receipt, context, context_wrapper)
    require(exact(current["outer_launch"], original_runtime["outer_launch"]), "dynamic_outer_current_original_changed")
    outer_launch(current["outer_launch"], evidence, current_inventory, processes, context, identity, plan, review)
    obligations = cleanup_obligations(current["cleanup_obligations"], evidence, receipt)
    require(exact(obligations, component["remaining_cleanup_obligations"]), "dynamic_preparation_cleanup_obligation_changed")
    names = {row["name"] for row in plan["spaces"]}
    require(type(current["provider_recipe_captures"]) is dict and set(current["provider_recipe_captures"]) == names,
            "dynamic_preparation_current_provider_inventory")
    capture_refs = []
    for pair in [current["worker_recipe_capture"], *current["provider_recipe_captures"].values()]:
        fields(pair, {"request_index", "request", "response"})
        value = {"request_index": pair["request_index"]}
        for key in ("request", "response"):
            ref = pair[key]
            evidence.read(ref, "startup-" + key, 1048576)
            paths = [row["path"] for row in current_inventory["files"] if exact(row["ref"], ref)]
            require(len(paths) == 1, "dynamic_preparation_current_recipe_capture_path")
            value[key + "_path"], value[key + "_sha256"] = paths[0], ref["sha256"]
        capture_refs.append(value)
    current_receipt = {"captures": capture_refs, "started_monotonic_ns": chain["validated_monotonic_ns"],
                       "observed_monotonic_ns": processes["observed_monotonic_ns"]}
    current_captures = startup_captures(current_receipt, evidence, contiguous=False)
    original_captures = startup_captures(receipt, evidence)
    require(exact(effective_worker_recipe(current_captures, plan), effective_worker_recipe(original_captures, plan)),
            "dynamic_preparation_actual_worker_changed")
    return chain, component, original_runtime, current_captures, context, processes, obligations


def accounting_readonly_mount(raw, directory):
    """Parse original Linux mountinfo, independently of the acquisition helper."""
    require(type(raw) is bytes and len(raw) <= 1048576, "dynamic_channel_mount_bound")
    matches = []
    for line in raw.decode("utf-8", errors="strict").splitlines():
        parts = line.split()
        require(len(parts) >= 10 and parts.count("-") == 1, "dynamic_channel_mount_record")
        separator = parts.index("-")
        require(separator >= 6 and len(parts) == separator + 4
                and re.fullmatch(r"[0-9]+", parts[0]) and re.fullmatch(r"[0-9]+", parts[1]),
                "dynamic_channel_mount_record")
        # procfs escapes paths with these four octal sequences. Compare the
        # decoded path, so an escaped duplicate cannot evade uniqueness.
        def unescape(value):
            require(re.search(r"\\(?!040|011|012|134)", value) is None, "dynamic_channel_mount_escape")
            return re.sub(r"\\(040|011|012|134)", lambda match: chr(int(match[1], 8)), value)
        mount_path = unescape(parts[4])
        if mount_path == "/gateway-accounting":
            matches.append(parts)
        require(not mount_path.startswith("/gateway-accounting/"), "dynamic_channel_mount_shadow")
    require(len(matches) == 1, "dynamic_channel_mount_unique")
    parts = matches[0]
    flags = parts[5].split(",")
    require("ro" in flags and "rw" not in flags and len(flags) == len(set(flags)), "dynamic_channel_mount_writable")
    require(re.fullmatch(r"[0-9]+:[0-9]+", parts[2]) is not None, "dynamic_channel_mount_device")
    major, minor = map(int, parts[2].split(":"))
    device = directory["device"]
    # Linux's dev_t packing; pure arithmetic avoids querying host devices.
    observed_major = ((device >> 8) & 0xfff) | ((device >> 32) & 0xfffff000)
    observed_minor = (device & 0xff) | ((device >> 12) & 0xffffff00)
    require((major, minor) == (observed_major, observed_minor), "dynamic_channel_mount_device")


def accounting_original_files(index, bundle, bundle_ref, evidence, *, allocation_sha256=None):
    """Bind the exact closed export membership without caching provider bodies."""
    fields(bundle, {"format", "begin", "close", "baseline", "phase_clock", "sealed", "capture_inventory", "dispatch_records"})
    require(bundle["format"] == "recall-dynamic-provider-accounting/v2", "dynamic_channel_bundle")
    root = f'cohort-{index["cohort_id"]:02d}/'
    originals = {root + "bundle.json": bundle_ref}
    for name, filename, kind in (
            ("begin", "begin.json", "provider-cohort-begin"), ("close", "close.json", "provider-cohort-close"),
            ("baseline", "baseline.json", "provider-baseline"), ("phase_clock", "phase-clock.json", "provider-phase-clock"),
            ("sealed", "sealed.json", "provider-sealed"),
            ("capture_inventory", "capture-inventory.json", "provider-capture-inventory"),
            ("dispatch_records", "dispatch-records.json", "provider-dispatch-records")):
        ref = fields(bundle[name], REFERENCE_FIELDS)
        require(ref["kind"] == kind, "dynamic_channel_bundle_kind")
        originals[root + filename] = ref
    captures = evidence.json(bundle["capture_inventory"], "provider-capture-inventory", 8388608)
    attempt_cap = semantic140().PHYSICAL_ATTEMPTS if is_semantic140(allocation_sha256) else 2100
    require(type(captures) is list and 1 <= len(captures) <= (1680 if is_semantic140(allocation_sha256) else 2100),
            "dynamic_channel_dispatch_count")
    kinds = {"request.original.json": "provider-request", "request.forwarded.json": "provider-request",
             "intent.json": "provider-dispatch-intent", "response.body": "provider-response", "capture.json": "provider-capture"}
    dispatches = set()
    for capture in captures:
        fields(capture, {"dispatch_id", "files"})
        identifier = capture["dispatch_id"]
        require(integer(identifier, 1, attempt_cap) and identifier not in dispatches, "dynamic_channel_dispatch_identity")
        dispatches.add(identifier)
        fields(capture["files"], kinds)
        for name, kind in kinds.items():
            originals[root + f"dispatch-{identifier:06d}/" + name] = {"sha256": checksum(capture["files"][name]), "kind": kind}
    files = index["files"]
    require(type(files) is list and len(files) == len(originals) <= 10532, "dynamic_channel_file_count")
    paths, total = set(), 0
    for item in files:
        fields(item, {"path", "ref"})
        path = item["path"]
        require(type(path) is str and path in originals and path not in paths, "dynamic_channel_file_membership")
        paths.add(path)
        expected, ref = originals[path], fields(item["ref"], REFERENCE_FIELDS)
        require(all(exact(ref[key], value) for key, value in expected.items()), "dynamic_channel_original_file_ref")
        total += ref["bytes"] if type(ref["bytes"]) is int else 21474836481
        require(total <= 21474836480, "dynamic_channel_file_bound")
        evidence.read(ref, expected["kind"], 8388608)
    require(paths == set(originals), "dynamic_channel_file_membership")


def accounting_channel(reference, bundle_ref, evidence, review, context, identity, begin, close, readonly_inputs):
    """Check reviewed G acquisition of original stat, mount and closed-index bytes.

    Original G performs its effective-user guard. The legacy controller has no
    UID field; this pure predicate does not invent an independent UID sample.
    """
    channel = evidence.json(reference, "provider-accounting-channel", 16384)
    fields(channel, {"format", "anchor_sha256", "sidecar", "mountinfo", "index", "directory", "caller"})
    for ref in (reference, *(channel[key] for key in ("sidecar", "mountinfo", "index", "directory", "caller"))):
        require(any(exact(ref, captured) for captured in evidence.paths.values()), "dynamic_channel_original_inventory")
    require(channel["format"] == "recall-provider-accounting-channel/v2"
            and channel["anchor_sha256"] == context["anchor_sha256"], "dynamic_channel_original_anchor")
    sidecar = evidence.json(channel["sidecar"], "provider-accounting-sidecar", 16384)
    fields(sidecar, {"format", "anchor_sha256", "path", "directory", "helper_sha256", "read_only"})
    require(sidecar["format"] == "recall-gateway-accounting-channel/v1"
            and sidecar["anchor_sha256"] == channel["anchor_sha256"]
            and sidecar["path"] == "/gateway-accounting" and sidecar["read_only"] is True,
            "dynamic_channel_original_sidecar")
    helper = "tests/recall_preview_semantics/gateway/accounting_channel.py"
    require(sidecar["helper_sha256"] == review["helper_sha256"][helper], "dynamic_channel_reader_source")
    checksum(sidecar["helper_sha256"])
    directory = evidence.json(channel["directory"], "provider-accounting-directory", 16384)
    fields(directory, {"format", "path", "device", "inode", "uid", "mode"})
    require(directory["format"] == "recall-provider-accounting-directory/v2"
            and directory["path"] == sidecar["path"], "dynamic_channel_directory_path")
    facts = {key: directory[key] for key in ("device", "inode", "uid", "mode")}
    require(all(integer(value, 0, 2**64 - 1) for value in facts.values())
            and exact(facts, sidecar["directory"]) and directory["mode"] <= 0o700
            and directory["mode"] & 0o077 == 0, "dynamic_channel_directory_identity")
    accounting_readonly_mount(evidence.read(channel["mountinfo"], "provider-mountinfo", 1048576), directory)
    index = evidence.json(channel["index"], "provider-accounting-index", 8388608)
    fields(index, {"format", "cohort_id", "backend", "phase", "caller_sha256", "begin_sha256", "close_sha256", "reference", "files"})
    require(index["format"] == "recall-gateway-accounting-index/v1" and integer(index["cohort_id"], 1, 4)
            and index["cohort_id"] == begin["cohort_id"] == close["cohort_id"]
            and index["backend"] == identity["backend"] == begin["backend"]
            and index["phase"] == "preparation" == begin["phase"]
            and index["begin_sha256"] == digest(encode(begin)) and index["close_sha256"] == digest(encode(close))
            and exact(index["reference"], bundle_ref), "dynamic_channel_original_cohort")
    caller_raw = evidence.read(channel["caller"], "workload-phase-manifest", 131072)
    caller = strict_json(caller_raw)
    require(digest(encode(caller)) == index["caller_sha256"], "dynamic_channel_original_caller_hash")
    phase_hashes = {"contract": "workload_contract", "corpus": "corpus", "provider_allocation": "provider_allocation",
                    "preparation_contract": "preparation_contract", "typed_delta": "typed_delta", "rendered_map": "rendered_map"}
    caller_fields = {"format", "execution_authorized", "provider_dispatch_authorized", "phase", "provider_phase",
        "source_commit", "artifact_sha256", "binary_sha256", "backend", "endpoint", "expires_at", "output_directory",
        "fixture_timestamp", "nonce", "space_credentials", "credential_files", "provider_bridge_path", "provider_bridge_sha256",
        *(name + suffix for name in phase_hashes for suffix in ("_path", "_sha256"))}
    successor = is_semantic140(identity["provider_allocation_sha256"])
    fields(caller, caller_fields | {"profile_id"} if successor else caller_fields)
    if successor:
        require(caller["profile_id"] == semantic140().PROFILE_ID, "dynamic_channel_original_caller_profile")
    require(caller["format"] == "recall-dynamic-workload-phase/v2" and caller["execution_authorized"] is True
            and caller["provider_dispatch_authorized"] is True and caller["phase"] == caller["provider_phase"] == "preparation"
            and caller["nonce"] == identity["preparation_nonce"], "dynamic_channel_original_caller_phase")
    for key in ("source_commit", "artifact_sha256", "binary_sha256", "backend", "endpoint"):
        require(exact(caller[key], identity[key]), "dynamic_channel_original_caller_identity")
    for name, static_name in phase_hashes.items():
        require(caller[name + "_sha256"] == identity[static_name + "_sha256"]
                == review["input_refs"][static_name]["sha256"], "dynamic_channel_original_caller_input")
        path = private_path(caller[name + "_path"])
        require(path.startswith("/inputs/") and any(exact(row, {"path": path[len("/inputs/"):],
                "sha256": caller[name + "_sha256"], "bytes": review["input_refs"][static_name]["bytes"]})
                for row in readonly_inputs["files"]), "dynamic_channel_original_caller_input_path")
    intents_ref = review["input_refs"]["workload_intents"]
    phase_intents = evidence.json(intents_ref, intents_ref["kind"])["backends"][identity["backend"]]
    require(caller["fixture_timestamp"] == phase_intents["fixture_timestamp"]
            and caller["nonce"] == phase_intents["preparation_nonce"]
            and timestamp(caller["expires_at"]) <= timestamp(review["expires_at"]), "dynamic_channel_original_caller_intent")
    private_path(caller["output_directory"])
    bridge = "tests/recall_preview_semantics/gateway/cohort_bridge.py"
    require(caller["provider_bridge_sha256"] == review["helper_sha256"][bridge], "dynamic_channel_original_caller_bridge")
    require(type(caller["provider_bridge_path"]) is str
            and PurePosixPath(caller["provider_bridge_path"]).is_absolute(), "dynamic_channel_original_caller_bridge")
    names = {"qualification-household", "qualification-business"}
    require(exact(caller["space_credentials"], {name: "service-" + name for name in names}), "dynamic_channel_original_caller_roles")
    fields(caller["credential_files"], {"service-" + name for name in names})
    for name in names:
        expected_path = f'/work/bootstrap-{identity["run_nonce"]}-{identity["backend"]}/private/service-{name}.token'
        require(caller["credential_files"]["service-" + name] == expected_path, "dynamic_channel_original_caller_credentials")
    bundle = evidence.json(bundle_ref, "provider-accounting", 8388608)
    require(exact(evidence.json(bundle["begin"], "provider-cohort-begin", 65536), begin)
            and exact(evidence.json(bundle["close"], "provider-cohort-close", 65536), close), "dynamic_channel_original_receipts")
    accounting_original_files(index, bundle, bundle_ref, evidence, allocation_sha256=identity["provider_allocation_sha256"])
    return index


@safe_boundary
def validate_prepared(reviewed_static, observed_preparation, runtime_component, raw_evidence, controller_context):
    """Validate the original 50-source fixture and closed original provider cohort."""
    evidence = Evidence(raw_evidence, maximum=20 * 1024**3 + reviewed_runtime_bound(reviewed_static))
    fields(observed_preparation, PREPARATION_FIELDS)
    require(observed_preparation["format"] == "recall-dynamic-preparation-evidence/v2", "dynamic_preparation_evidence_format")
    identity = fields(observed_preparation["identity"], IDENTITY_FIELDS | PREPARATION_IDENTITIES)
    review, reviewed_identity, plan, _, inputs = static_review(reviewed_static, evidence, identity["backend"])
    require(exact({key: identity[key] for key in IDENTITY_FIELDS - {"outer_configuration_sha256"}}, reviewed_identity),
            "dynamic_preparation_static_identity")
    for key in PREPARATION_IDENTITIES - {"preparation_nonce"}:
        checksum(identity[key])
    sources, spaces, typed, rendered = preparation_sources(inputs, identity)
    names = {space["id"] for space in spaces}
    blueprint = preparation_blueprints(inputs, names, typed)
    require("workload_intents" in inputs, "dynamic_original_workload_intents_missing")
    intents = fields(strict_json(inputs["workload_intents"]), {"format", "backends"})
    require(intents["format"] == "recall-workload-intents/v2" and type(intents["backends"]) is dict,
            "dynamic_original_workload_intents")
    phase_intents = fields(intents["backends"][identity["backend"]], {"fixture_timestamp", "preparation_nonce", "measurement_nonce"})
    require(phase_intents["fixture_timestamp"] == typed["fixture_timestamp"]
            and phase_intents["preparation_nonce"] == identity["preparation_nonce"]
            and phase_intents["measurement_nonce"] != phase_intents["preparation_nonce"], "dynamic_original_preparation_nonce")
    _, capture_size = evidence.inventory(observed_preparation["capture_inventory"])
    require(capture_size <= 268435456, "dynamic_preparation_capture_bound")
    chain, component, original_runtime, current_recipes, context, processes, obligations = preparation_runtime_chain(
        observed_preparation["runtime_component"], evidence, reviewed_static, runtime_component, controller_context,
        review, identity, plan)
    require(exact(observed_preparation["controller"], original_runtime["controller"]), "dynamic_preparation_original_controller")
    observed = preparation_observation(evidence.json(observed_preparation["observation"], "preparation-observation", 8 * 1024**2), identity)
    captures = preparation_captures(observed_preparation["original_intents"], evidence, observed, context, controller_context)
    require(chain["validated_monotonic_ns"] <= captures[0]["start"]
            and captures[-1]["end"] <= min(row[0]["started_monotonic_ns"] for row in current_recipes.values()),
            "dynamic_preparation_before_after_runtime_clock")
    before, before_indices = capture_configurations(observed_preparation["configuration_before"], evidence, captures, names)
    after, after_indices = capture_configurations(observed_preparation["configuration_after"], evidence, captures, names)
    initial_configs = evidence.json(original_runtime["actual_space_configurations"], "space-configurations", 4 * 1024**2)
    require(exact(before, initial_configs), "dynamic_preparation_initial_configuration_changed")
    effective_provider_recipes(current_recipes, plan, after)
    publication = replay_preparation(captures, sources, typed, rendered, blueprint, identity, before, before_indices,
        after, after_indices, observed, observed_preparation["immutable_revisions"], evidence)
    begin = evidence.json(observed_preparation["provider_cohort_begin"], "provider-cohort-begin", 65536)
    require(exact(begin, observed["provider_cohort_begin"]), "dynamic_preparation_original_begin_binding")
    accounting_channel(observed_preparation["provider_accounting_channel"], observed_preparation["provider_accounting"],
        evidence, review, context, identity, begin, observed["provider_accounting"],
        evidence.json(evidence.json(original_runtime["current_processes"], "current-processes", 262144)["input_inventory"],
                      "input-inventory"))
    try:
        from provider_evidence_validator import validate_provider_accounting
    except ImportError:
        raise ValueError("dynamic_provider_validator_unavailable") from None
    accounting = validate_provider_accounting(observed_preparation["provider_accounting"], evidence.raw, identity,
        controller_context, after, observed, (captures[0]["start"], captures[-1]["end"]))
    fields(accounting, {"begin", "close", "phase_clock"})
    require(exact(accounting["begin"], begin) and exact(accounting["close"], observed["provider_accounting"]),
            "dynamic_preparation_provider_original_result")
    require(chain["validated_monotonic_ns"] <= accounting["phase_clock"]["phase_start"]["mono"] * 10**9,
            "dynamic_preparation_provider_before_runtime_validation")
    return {"format": "recall-prepared-fixture-component/v2", "verdict": "passed",
            "scope": "prepared-public-retrieval-fixture", "product_acceptance": False, "identity": dict(identity),
            "static_review_sha256": reviewed_static["raw_sha256"], "contract_sha256": CONTRACT_SHA256,
            "evidence_sha256": digest(encode(observed_preparation)), "runtime_component_sha256": chain["component"]["sha256"],
            "controller_sha256": observed_preparation["controller"]["sha256"],
            "actual_space_configurations_sha256": digest(encode(after)), "source_count": 50,
            "sources_per_space": {name: 25 for name in sorted(names)}, "published_inventory": publication,
            "provider_accounting_sha256": observed_preparation["provider_accounting"]["sha256"],
            "remaining_cleanup_obligations": obligations, "proof_limits": list(PROOF_LIMITS)}
