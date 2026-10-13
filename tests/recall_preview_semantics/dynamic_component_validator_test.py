"""Closed-world original-byte and publication controls, never product acceptance."""
from __future__ import annotations

import base64
from collections.abc import Mapping
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import socket
import subprocess
import unittest
from unittest.mock import patch

import installed_evidence_validator
from installed_evidence_validator_test import Transcript as InstalledTranscript, stat as original_stat
import provider_evidence_validator
from provider_evidence_validator_test import OriginalFixture as ProviderFixture

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("dynamic_predicate_under_test", Path(__file__).with_name("dynamic_component_validator.py"))
v = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(v)
# Independent frozen input files are read once before closed-world controls.
CORPUS = json.loads((ROOT / "tests/recall_preview/workload-corpus.json").read_bytes())
RENDERED = json.loads((ROOT / "tests/recall_preview/workload-rendered-map.json").read_bytes())
TYPED = json.loads((ROOT / "tests/recall_preview/workload-preparation-contract.json").read_bytes())
PREP_ORIGINALS = {name: (ROOT / "tests/recall_preview" / filename).read_bytes() for name, filename in (
    ("corpus", "workload-corpus.json"), ("workload_contract", "workload-contract.json"),
    ("preparation_contract", "workload-preparation-contract.json"), ("typed_delta", "workload-allocation-typed-delta.json"),
    ("rendered_map", "workload-rendered-map.json"), ("provider_allocation", "workload-allocation-proposal.json"))}


def original(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def retain(raw, kind, store):
    reference = {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw), "kind": kind}
    store[reference["sha256"]] = raw
    return reference


def fixture(index=0):
    """Literal public record relationships from independent source/map inputs."""
    source = copy.deepcopy(CORPUS["sources"][index])
    rendered = next(copy.deepcopy(row) for row in RENDERED["sources"] if (row["space"], row["source_key"], row["revision"])
                    == (source["space"], source["key"], source["revision"]))
    security = copy.deepcopy(TYPED["security_by_space"][source["space"]])
    base = {"space_id": source["space"], "security": security, "subjects": [], "subjects_state": "unknown", "deleted_at": None}
    source_id, doc_id, revision = "source:original-" + str(index), "doc:original-" + str(index), "doc_revision:original-" + str(index)
    accepted = {"source_id": source_id, "doc_id": doc_id, "work_item_id": "work_item:original-" + str(index),
                "operation": {"operation_id": "operation-original-" + str(index), "source_id": source_id,
                              "source_generation": "generation-original-" + str(index), "space_id": source["space"],
                              "desired_revision": source["revision"]}}
    complete = {"operation_id": accepted["operation"]["operation_id"], "internal_source_id": source_id,
                "generation": accepted["operation"]["source_generation"], "desired_revision": source["revision"],
                "published_revision": source["revision"], "lifecycle": "active", "state": "complete", "error_code": None}
    records = {"operation": copy.deepcopy(complete), "head": copy.deepcopy(complete),
               "source": {**base, "id": source_id, "external_id": source["key"], "hash": source["sha256"],
                          "uri": source["source_url"], "current_ingest_mode": "retrieval", "lifecycle_status": "retrieval_ready"},
               "doc": {**base, "id": doc_id, "source": source_id, "title": source["key"], "text": rendered["rendered_text"]},
               "immutable_revision": {**base, "id": revision, "doc": doc_id, "status": "active", "supersedes": None,
                       "text": rendered["rendered_text"], "text_hash": rendered["rendered_sha256"],
                       "renderer": {"name": "elephant.renderer.text", "version": "1.0.0", "config_hash": None},
                       "provenance": {"sources": [source_id], "chain": []}, "rendered_at": "2026-10-07T00:01:00Z"},
               "nodes": [], "pipeline": {"doc_id": doc_id, "revision": revision, "chunks": {"total": 2, "embedded": 2}, "nodes_count": 3}}
    # The reviewed fixture has one root including LF, paragraph/chunk excluding LF.
    for role, span in (("document", rendered["root_span"]), ("paragraph", rendered["paragraph_spans"][0]), ("chunk", rendered["chunk_spans"][0])):
        records["nodes"].append({**base, "id": "node:" + role + str(index), "role": role, "revision": revision,
               "span": copy.deepcopy(span), "provenance": {"sources": [source_id], "chain": [{"input": revision}]},
               "embedding_model": "independently-pinned-partition"})
    return source, accepted, records, rendered, security, "independently-pinned-partition"


def static_fixture():
    raws = {}
    identity = {"source_commit": "1" * 40, "artifact_sha256": "2" * 64, "binary_sha256": "3" * 64,
                "runtime_sha256": "4" * 64, "paired_artifact_sha256": "5" * 64,
                "backend": "elephant-db", "endpoint": "http://127.0.0.1:43130", "run_nonce": "original-nonce-1234"}
    spaces = []
    for name, policy, level, dimensions in (("qualification-household", "household-policy", 3, 1536), ("qualification-business", "business-policy", 7, 3072)):
        spaces.append({"name": name, "security": {"policy_id": policy, "level": level, "labels": [], "handling": []},
            "embedding_provider": "openai", "embedding_model": "original-model", "embedding_dims": dimensions,
            "purpose": "original purpose", "skill": "original skill", "processing": {"generation": 1, "index_generation": 0,
                "pending_reindex": None, "bindings": {"document_embedding": {"profile_id": "original-profile"}}}})
    helpers = {"scripts/qualification/original.py": "6" * 64}
    plan = {"format": "recall-runtime-bootstrap-plan/v1", **identity, "artifact_sha256": identity["binary_sha256"],
            "expires_at": "2026-10-07T04:00:00Z", "spaces": spaces,
            "service_scopes": ["read:status", "read:space_config", "read:source", "write:source", "read:doc",
                "read:doc:metadata", "read:doc:raw", "write:doc", "read:entity", "read:assertion", "read:relationship"], "dependencies": helpers,
            "run_directory": "/work/bootstrap-original-nonce-1234-elephant-db"}
    plan.pop("binary_sha256")
    plan_ref = retain(original(plan), "bootstrap-plan", raws)
    input_ref = retain(b'{"original":"static input"}', "original-test-input", raws)
    expected_anchor = {key: hashlib.sha256(key.encode()).hexdigest()
                       for key in v.HASH_FIELDS - {"manifest_sha256", "configuration_sha256"}}
    expected_anchor.update(format="recall-gateway-launch-anchor/v1", source_commit=identity["source_commit"],
        artifact_sha256=identity["binary_sha256"], paired_artifact_sha256=identity["paired_artifact_sha256"],
        runtime_sha256=identity["runtime_sha256"], run_nonce=identity["run_nonce"], allocation_sha256="9" * 64,
        caps={"physical_dispatches": 2100, "maximum_nano_usd": 3_000_000_000})
    review = {"format": "recall-dynamic-component-review/v2", "reviewer": "qualification", "approved": True,
        "contract_sha256": v.CONTRACT_SHA256, "predicate_document_sha256": v.PREDICATE_SHA256,
        "identities": {"elephant-db": identity}, "input_refs": {"original": input_ref}, "helper_sha256": dict(helpers),
        "controller_policy": {"expected_anchor": expected_anchor, "maximum_backend_seconds": 7800},
        "bootstrap_plans": {"elephant-db": plan_ref}, "runtime_recipe": dict(v.RECIPE),
        "limits": copy.deepcopy(v.STATIC_LIMITS), "expires_at": "2026-10-07T04:00:00Z"}
    raw = original(review)
    return {"review": review, "raw_sha256": hashlib.sha256(raw).hexdigest(), "raw_bytes": raw}, raws, plan


def authority_fixture(plan=None, configurations=None):
    if plan is None:
        _, _, plan = static_fixture()
    original_configurations = configurations
    captures, checks, configurations = {}, {key: [] for key in v.BOOTSTRAP_CHECKS}, {}
    def capture(method, path, name, role, body, response, status=200, check=None):
        index = len(captures) + 1
        request = {"method": method, "path": path, "space": name, "credential_role": role, "body": copy.deepcopy(body), "port": 43130}
        reply = {"status": status, "framing_complete": True, "error_type": None, "body": copy.deepcopy(response)}
        captures[index] = (request, reply, original(response))
        if check:
            checks[check].append(index)
        return index
    denied = {"error_code": "forbidden_space", "message": "denied", "retryable": False}
    capture("GET", "/v1/readiness", None, None, None,
            {"error_code": "unauthorized", "message": "denied", "retryable": False}, 401, "unauthenticated_readiness")
    for row in plan["spaces"]:
        name = row["name"]
        bootstrap, service = "bootstrap-" + name, "service-" + name
        config_path, source_path = "/v1/spaces/" + name, f"/v1/spaces/{name}/sources?limit=100"
        config = (copy.deepcopy(original_configurations[name]) if original_configurations is not None else
                  {"id": "space_config:00000000-0000-0000-0000-000000000001", "space_id": name, "security": row["security"]})
        configurations[name] = config
        capture("POST", "/v1/spaces", name, bootstrap, row, config, 201)
        capture("GET", config_path, name, bootstrap, None, config, check="named_config_readback")
        capture("GET", source_path, name, service, None, denied, 403, "missing_grant")
        def grant(role, active, previous, generation):
            scopes = plan["service_scopes"] if role == "service" else [s for s in plan["service_scopes"] if s.startswith("read:")]
            subject = "primary-" + role + "-" + name
            path = config_path + "/grants/" + subject
            value = {"space_id": name, "subject": subject, "active": active, "scopes": scopes, "generation": generation}
            capture("PUT", path, name, bootstrap, {"active": active, "scopes": scopes, "expected_generation": previous}, value)
            capture("GET", path, name, bootstrap, None, value, check="grant_readback")
        grant("service", True, None, "service-created")
        grant("read-only", True, None, "readonly-created")
        capture("GET", config_path, name, service, None, config, check="ordinary_config_readback")
        capture("GET", source_path, name, service, None, {"sources": [], "next_cursor": None}, check="ordinary_empty_inventory")
        capture("GET", source_path, name, bootstrap, None, denied, 403, "bootstrap_data_denial")
        capture("PUT", config_path + "/sources/bootstrap-denied", name, "read-only-" + name,
                {"revision": "denied-v1", "text": "denial control", "media_type": "text/plain", "source_url": None,
                 "observed_at": None, "labels": [], "expected_generation": None}, denied, 403, "read_only_write_denial")
        grant("service", False, "service-created", "service-revoked")
        capture("GET", source_path, name, service, None, denied, 403, "grant_revocation")
        grant("service", True, "service-revoked", "service-restored")
        capture("GET", config_path, name, service, None, config, check="grant_restoration")
    names = [space["name"] for space in plan["spaces"]]
    for own, other in (names, list(reversed(names))):
        capture("GET", "/v1/spaces/" + other, other, "service-" + own, None, denied, 403, "wrong_space")
    capture("GET", "/v1/readiness", None, "operator", None,
            {"ready": True, "worker_ready": True, "semantic_ingest_ready": True, "draining": False}, check="ready")
    for name in names:
        capture("GET", f"/v1/spaces/{name}/sources?limit=100", name, "service-" + name, None,
                {"sources": [], "next_cursor": None}, check="final_empty_inventory")
    return {"checks": checks, "configurations": configurations}, plan, captures


def credential_fixture(backend="elephant-db"):
    _, _, plan = static_fixture()
    plan["backend"] = backend
    plan["run_directory"] = "/work/bootstrap-" + plan["run_nonce"] + "-" + backend
    root, commands = plan["run_directory"], {}
    literal_bootstrap = ["read:space_config", "write:space_config", "manage:grant", "manage:provider"]
    issuers = [("operator", "primary-runtime-operator", None, False, False)]
    for space in plan["spaces"]:
        for role in ("bootstrap", "service", "read-only"):
            issuers.append((role + "-" + space["name"], "primary-" + role + "-" + space["name"], space,
                            role == "bootstrap", role == "read-only"))
    for role, subject, space, bootstrap, readonly in issuers:
        argv = ["/opt/recall/bin/elephant", "service-token", "issue", "--secret-file", root + "/private/signing.key",
                "--subject", subject, "--expires-in", "300", "--issuer", "recall-primary-bootstrap", "--audience", "recall-primary",
                "--output", root + "/private/" + role + ".token"]
        if space is None:
            argv += ["--global-admin"]
            scopes = ["read:status", "read:system", "write:system"]
        else:
            argv += ["--bootstrap-space" if bootstrap else "--space", space["name"], "--clearances-file",
                     root + "/private/" + role + ".clearances.json"]
            scopes = literal_bootstrap if bootstrap else plan["service_scopes"]
            if readonly:
                argv += ["--read-only"]
                scopes = [scope for scope in scopes if scope.startswith("read:")]
        for scope in scopes:
            argv += ["--scope", scope]
        number = len(commands) + 1
        commands[number] = {"purpose": "issue-" + role, "command": argv, "cwd": root + "/service",
            "exit_code": 0, "environment_names": ["HOME", "JWT_SECRET_FILE", "PATH"], "started_monotonic_ns": number}
    if backend == "surreal-db":
        commands[8] = {"purpose": "paired-persistent-server", "command": ["/opt/recall/bin/recall-surreal", "start",
            "--bind", "127.0.0.1:43131", "file:" + root + "/paired/database.db"], "cwd": root + "/paired",
            "environment_names": ["HOME", "PATH"], "started_monotonic_ns": 8, "exit_code": None}
        commands[9] = {"purpose": "explicit-fresh-schema", "command": ["/opt/recall/bin/elephant", "migrate"],
            "cwd": root + "/service", "environment_names": ["HOME", "JWT_SECRET_FILE", "PATH"],
            "started_monotonic_ns": 9, "exit_code": 0}
    commands[len(commands) + 1] = {"purpose": "recall-authenticated-service", "started_monotonic_ns": 10, "exit_code": None}
    return commands, plan, {}


def worker_fixture(recipe=None, index=1):
    if recipe is None:
        recipe = {"format": "recall-managed-worker-recipe/v1", "instance_id": "00000000-0000-0000-0000-000000000001",
            "backend": "elephant-db", "work_queue": {"enabled": True, "max_concurrent": 4, "max_extract_concurrent": 2,
                "work_item_timeout_secs": 300, "transient_retry_max_attempts": 3, "transient_retry_base_delay_secs": 1,
                "transient_retry_max_delay_secs": 30, "allowed_kinds": [], "disabled_kinds": [],
                "doc_tree": {"max_chunk_size": 512, "chunk_overlap": 50, "create_paragraphs": True,
                    "detect_sections": False, "semantic_chunking": True, "create_summaries": False, "summarizer_model": "unused"}},
            "embed_worker": {"enabled": True, "batch_size": 128, "provider": "openai", "model": "template", "dimensions": 1536}}
    request = {"method": "POST", "path": "/mcp/full", "credential_role": "operator", "body": {
        "jsonrpc": "2.0", "id": index, "method": "tools/call", "params": {"name": "get_runtime_config", "arguments": {}}}}
    body = {"jsonrpc": "2.0", "id": index, "result": {"content": [{"type": "text", "text": original({
        "ok": True, "data": {"managed_worker_recipe": recipe}}).decode()}], "isError": False}}
    return recipe, {index: (request, {"status": 200, "framing_complete": True, "error_type": None,
        "response_headers": {"content-type": ["application/json"]}, "body": body}, original(body))}


def runtime_fixture(*, prepared=False, backend="elephant-db"):
    """Compose independent literal M offline evidence with current C wire facts.

    No producer function or qualification result is mocked. Every reference
    points at the exact bytes originally constructed by this synthetic fixture.
    """
    installed_review, installed, raws = InstalledTranscript(backend).populate().seal()
    reviewed, static_raws, plan = static_fixture()
    raws.update(static_raws)
    review = reviewed["review"]
    identity = review["identities"].pop("elephant-db")
    review["identities"][backend] = identity
    identity.update(installed["identity"])
    plan.update(source_commit=identity["source_commit"], artifact_sha256=identity["binary_sha256"], backend=backend,
                run_directory="/work/bootstrap-" + identity["run_nonce"] + "-" + backend)
    review["input_refs"].update(installed_review["review"]["input_refs"])
    review["helper_sha256"].update(installed_review["review"]["helper_sha256"])
    for name, raw in PREP_ORIGINALS.items():
        review["input_refs"][name] = retain(raw, "original-" + name, raws)
    if prepared:
        review["input_refs"]["workload_intents"] = retain(original({"format": "recall-workload-intents/v2", "backends": {
            backend: {"fixture_timestamp": TYPED["fixture_timestamp"], "preparation_nonce": "original-preparation-123",
                            "measurement_nonce": "original-measurement-123"}}}), "workload-intents", raws)
        blueprint = {name: {"record_security": security, "embedding_partition_model": "independently-pinned-partition",
            "input_envelope": {"security": security, "deleted_at": None, "provenance": {"sources": [], "chain": []},
                               "subjects": [], "subjects_state": "unknown"}} for name, security in TYPED["security_by_space"].items()}
        review["input_refs"]["expected_configurations"] = retain(original(blueprint), "preparation-blueprints", raws)
        for helper in ("accounting_channel", "cohort_bridge"):
            review["helper_sha256"]["tests/recall_preview_semantics/gateway/" + helper + ".py"] = "6" * 64
    root, files = plan["run_directory"], {}
    def captured(path, value, kind, *, raw=False):
        reference = retain(value if raw else original(value), kind, raws)
        files[path] = reference
        return reference
    configs = {}
    for number, row in enumerate(plan["spaces"], 1):
        name = row["name"]
        row["embedding_model"] = next(space["model"] for space in CORPUS["spaces"] if space["id"] == name)
        connection = {"id": "embed", "provider": "openai", "account_id": "original-account", "endpoint": "http://127.0.0.1:43129/v1",
            "credential_ref": "RECALL_GATEWAY_original_" + str(number), "credential_generation": 1, "revoked": False}
        connection.update(account_id="original-synthetic-account",
            credential_ref="RECALL_GATEWAY_" + backend.replace("-", "_") + "_" + name.rsplit("-", 1)[1] + "_embedding")
        profile = {"id": "embedding", "connection_id": "embed", "model": row["embedding_model"], "model_version": "original-v1",
            "dimensions": row["embedding_dims"], "preprocessing": "text-v1"}
        bindings = {operation: {"id": operation + "-binding", "profile_id": "embedding", "revoked": False}
                    for operation in ("query_embedding", "document_embedding")}
        row["processing"].update(policy_id="original-provider-policy", connections={"embed": connection},
            profiles={"embedding": profile}, bindings=bindings, active_embedding_profile_id="embedding")
        configs[name] = {"id": "space_config:00000000-0000-0000-0000-00000000000" + str(number),
            "space_id": name, "created_at": "2026-10-07T01:00:10Z", "updated_at": "2026-10-07T01:00:10Z",
            "created_by": "primary-bootstrap-" + name, "subjects": [], "subjects_state": "complete",
            "provenance": {"sources": [], "chain": []}, "deleted_at": None,
            **{key: copy.deepcopy(value) for key, value in row.items() if key != "name"}}
    plan_ref = captured(root + "/plan.json", plan, "bootstrap-plan")
    review["bootstrap_plans"] = {backend: plan_ref}
    identity = dict(identity)
    inputs = {"format": "recall-gateway-inputs/v1", "files": [{"path": "plan.json", "bytes": plan_ref["bytes"], "sha256": plan_ref["sha256"]}]}
    inputs["files"] += [{"path": name + ".json", "bytes": ref["bytes"], "sha256": ref["sha256"]}
                        for name, ref in review["input_refs"].items() if name in PREP_ORIGINALS]
    inputs["files"].sort(key=lambda row: row["path"])
    inputs_ref = captured(root + "/input-inventory.json", inputs, "input-inventory")
    gateway_bindings = [{"id": current_backend.replace("-", "_") + "_" + space["id"].rsplit("-", 1)[1] + "_embedding",
        "backend": current_backend, "space": space["id"], "operation": "embedding", "model": space["model"],
        "dimensions": space["dimensions"], "connection_id": "embed", "profile_id": "embedding"}
        for current_backend in ("elephant-db", "surreal-db") for space in CORPUS["spaces"]]
    launch = {"format": "recall-gateway-launch/v1", "command": ["/usr/bin/python3", "/inputs/original-driver.py"],
        "environment": {"PIPELINE_WORK_TRANSIENT_RETRY_MAX_ATTEMPTS": "3"}, "bindings": gateway_bindings,
        "inputs": {"directory": "/original-reviewed-inputs", "inventory_sha256": inputs_ref["sha256"]}}
    # G preserves original raw whitespace; hashes do not use a reconstructed spec.
    launch_ref = captured("/gateway-launch.json", original(launch) + b"\n", "gateway-launch-spec", raw=True)
    identity["outer_configuration_sha256"] = launch_ref["sha256"]
    manifest = {"format": "recall-exclusive-embedding-manifest/v1", "live_ready": True,
        "run_id": identity["run_nonce"], "run_directory": "/original-owned-gateway-run",
        "accounting_policy": "separate-exclusive-workload-embedding-reservation/v1",
        "account_id": "original-synthetic-account", "credential_generation": 1,
        "artifact_sha256": identity["binary_sha256"], "configuration_sha256": launch_ref["sha256"],
        "runtime_sha256": identity["runtime_sha256"], "gateway_sha256": hashlib.sha256(b"gateway_sha256").hexdigest(),
        "contract_sha256": review["input_refs"]["workload_contract"]["sha256"],
        "corpus_sha256": review["input_refs"]["corpus"]["sha256"],
        "allocation_path": "/original-reviewed-inputs/provider_allocation.json",
        "allocation_sha256": review["input_refs"]["provider_allocation"]["sha256"],
        "preparation_contract_path": "/original-reviewed-inputs/preparation_contract.json",
        "preparation_contract_sha256": review["input_refs"]["preparation_contract"]["sha256"],
        "typed_delta_path": "/original-reviewed-inputs/typed_delta.json",
        "typed_delta_sha256": review["input_refs"]["typed_delta"]["sha256"], "bindings": gateway_bindings,
        "endpoint": "https://api.openai.com/v1", "gateway_endpoint": "http://127.0.0.1:43129/v1",
        "upstream_ips": ["8.8.8.8"], "synthetic_corpus_only": True,
        "transport_response_byte_cap": 8388608, "bound_version": "utf8-wire-plus-protocol-v1"}
    manifest_ref = captured("/gateway-manifest.json", original(manifest) + b"\n", "gateway-manifest", raw=True)
    outer_ref = captured(root + "/outer-launch.json", {"format": "recall-gateway-launch-inputs/v2",
        "manifest": manifest_ref, "spec": launch_ref, "input_inventory": inputs_ref}, "gateway-launch-inputs")
    wall = v.timestamp("2026-10-07T01:00:00Z")
    clock = {"wall_start": wall, "mono_start": 1000., "deadline_wall": wall + 7800,
             "deadline_mono": 8800., "time_cap_seconds": 7800, "boot_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
    anchor = {key: hashlib.sha256(key.encode()).hexdigest() for key in v.HASH_FIELDS}
    anchor.update(format="recall-gateway-launch-anchor/v1", run_nonce=identity["run_nonce"], source_commit=identity["source_commit"],
        artifact_sha256=identity["binary_sha256"], paired_artifact_sha256=plan["paired_artifact_sha256"],
        runtime_sha256=identity["runtime_sha256"], configuration_sha256=identity["outer_configuration_sha256"],
        allocation_sha256=manifest["allocation_sha256"], proposal_sha256=manifest["allocation_sha256"],
        corpus_sha256=manifest["corpus_sha256"], manifest_sha256=manifest_ref["sha256"],
        bindings_sha256=hashlib.sha256(original(gateway_bindings)).hexdigest(),
        command_sha256=hashlib.sha256(original(launch["command"])).hexdigest(),
        input_inventory_sha256=inputs_ref["sha256"], clock=clock,
        caps={"physical_dispatches": 2100, "maximum_nano_usd": 3_000_000_000}, parent={"network_namespace": "net:[200]",
            "process": {"pid": 39, "ppid": 38, "pgrp": 39, "session": 39, "start_ticks": 90}})
    anchor_ref = captured("/launch-anchor.json", anchor, "gateway-anchor")
    review["controller_policy"]["expected_anchor"] = {key: copy.deepcopy(anchor[key]) for key in v.STATIC_ANCHOR_FIELDS}
    controller = {"pid": 40, "ppid": 39, "pgrp": 40, "session": 40, "start_ticks": 100}
    bootstrap = {"pid": 41, "ppid": 40, "pgrp": 40, "session": 40, "start_ticks": 101}
    context = {"format": "recall-gateway-launch-context/v1", "anchor": anchor, "anchor_sha256": anchor_ref["sha256"],
        "process": controller, "network": {"namespace": "net:[201]", "parent_namespace": "net:[200]", "interfaces": ["lo"],
            "ipv4_routes": 0, "ipv6_loopback_routes": 0}, "observed_wall": wall + 1, "observed_mono": 1001., "captures": {}}
    context["network"]["ipv6_loopback_routes"] = 1
    ipv6 = b"00000000000000000000000000000001 80 00000000000000000000000000000000 00 00000000000000000000000000000000 00000000 00000000 00000000 00000000 lo\n"
    for label, body in (("dev", b"Inter-| Receive\n face | bytes\n lo: 0 0\n"), ("ipv4", b"Iface Destination Gateway\n"), ("ipv6", ipv6)):
        path = f'/work/gateway-context-{anchor_ref["sha256"]}.{label}.txt'
        ref = captured(path, body, "gateway-network-" + label, raw=True)
        context["captures"][label] = {"path": path, "sha256": ref["sha256"]}
    context_ref = captured(root + "/context.json", context, "gateway-context")
    isolation = {"format": "recall-gateway-isolation/v1", "network_namespace": "net:[201]", "parent_network_namespace": "net:[200]",
        "interfaces": ["lo"], "ipv4_routes": 0, "provider_credentials_present": False}
    isolation_ref = captured("/work/isolation.json", isolation, "startup-isolation")
    backend_config = ('[storage.elephant_db]\nsnapshot_path="' + root + '/service/state.snapshot"\n' if backend == "elephant-db" else
        '[storage.surreal_db]\nurl="ws://127.0.0.1:43131"\nnamespace="primary"\ndatabase="fixture"\n')
    runtime_raw = ('[runtime]\ningest_enabled=true\n[storage]\nbackend="' + backend + '"\n' + backend_config
                   + '[multimodal]\nasset_root="' + root + '/media"\n').encode()
    runtime_ref = captured(root + "/service/.elephant/elephant_config.toml", runtime_raw, "runtime-configuration-toml", raw=True)
    auth, _, unframed = authority_fixture(plan, configs)
    def rpc_capture(index, name, arguments, result):
        request = {"method": "POST", "path": "/mcp/full", "port": 43130, "space": None, "credential_role": "operator",
            "body": {"jsonrpc": "2.0", "id": index, "method": "tools/call", "params": {"name": name, "arguments": arguments}}}
        body = {"jsonrpc": "2.0", "id": index, "result": {"content": [{"type": "text", "text": original(result).decode()}], "isError": False}}
        return request, {"status": 200, "framing_complete": True, "error_type": None, "body": body,
                         "response_headers": {"content-type": ["application/json"]}}, original(body)
    schema_index = None
    if backend == "elephant-db":
        unframed = {index if index < 2 else index + 1: value for index, value in unframed.items()}
        auth["checks"] = {key: [index if index < 2 else index + 1 for index in indices] for key, indices in auth["checks"].items()}
        schema_index = 2
        unframed[schema_index] = rpc_capture(schema_index, "initialize_runtime", {"backend": "elephant-db",
            "snapshot_path": root + "/service/state.snapshot", "ingest_enabled": False, "extractor_required": False,
            "multimodal_asset_root": root + "/media"}, {"configured": True, "storage_ready": True, "backend": "elephant-db"})
    worker_index = len(unframed) + 1
    recipe = worker_fixture()[0]
    recipe["backend"] = backend
    worker = worker_fixture(recipe, index=worker_index)[1][worker_index]
    worker[0].update(port=43130, space=None)
    unframed[worker_index] = worker
    for row in plan["spaces"]:
        name, processing = row["name"], row["processing"]
        recipes = {}
        for operation, binding in processing["bindings"].items():
            profile = processing["profiles"][binding["profile_id"]]
            connection = processing["connections"][profile["connection_id"]]
            recipes[operation] = {"provenance": {"config": {"space_id": name, "config_id": configs[name]["id"],
                "generation": 1, "policy_id": processing["policy_id"]}, "operation": operation, "binding_id": binding["id"],
                "profile_id": profile["id"]}, "partition": {"space_id": name, "provider": "openai", "account_id": connection["account_id"],
                "endpoint": connection["endpoint"], "profile_id": profile["id"], "model": profile["model"], "model_version": profile["model_version"],
                "dimensions": profile["dimensions"], "preprocessing": "text-v1"}, "max_total_http_attempts": 3, "max_inputs_per_call": 100,
                "initial_segment_bytes": None, "max_split_depth": None}
        body = {"space": name, "storage_ready": True, "processing_generation": 1, "embedding_execution_recipes": recipes,
            "approved_operations": {operation: True for operation in recipes}, "available_operations": {operation: True for operation in recipes}}
        unframed[len(unframed) + 1] = ({"method": "GET", "path": f"/v1/spaces/{name}/status", "space": name,
            "credential_role": "service-" + name, "body": None, "port": 43130},
            {"status": 200, "framing_complete": True, "error_type": None, "body": body}, original(body))
    capture_refs = []
    for index, (request, response, raw) in sorted(unframed.items()):
        request_raw = b"" if request["body"] is None else original(request["body"])
        request.update(request_index=index, request_body_base64=base64.b64encode(request_raw).decode(),
            body_sha256=hashlib.sha256(request_raw).hexdigest(), harness_attempt=1, timeout_seconds=15,
            started_monotonic_ns=1010_000_000_000 + index * 1_000_000)
        response.update(request_index=index, raw_body_base64=base64.b64encode(raw).decode(), body_sha256=hashlib.sha256(raw).hexdigest(),
            finished_monotonic_ns=request["started_monotonic_ns"] + 100_000)
        request_path, response_path = root + f"/captures/{index:03}.request.json", root + f"/captures/{index:03}.response.json"
        req = captured(request_path, request, "startup-request")
        res = captured(response_path, response, "startup-response")
        capture_refs.append({"request_index": index, "request_path": request_path, "response_path": response_path,
                             "request_sha256": req["sha256"], "response_sha256": res["sha256"]})
    commands, _, _ = credential_fixture(backend)
    processes, proof_refs, sequence = {}, [], 0
    for number, row in commands.items():
        service = row["exit_code"] is None
        if service and row["purpose"] == "recall-authenticated-service":
            row.update(command=["/opt/recall/bin/elephant", "serve", "--server-only", "--shutdown-timeout-seconds", "60"],
                       cwd=root + "/service", environment_names=["HOME", "JWT_SECRET_FILE", "PATH"])
        started = 1002_000_000_000 + number * 10_000_000
        pid, start_ticks = 100 + number, 200 + number
        born = {"pid": pid, "ppid": 41, "pgrp": pid, "session": pid, "start_ticks": start_ticks}
        logs = [root + f"/logs/{number:02}.{suffix}" for suffix in ("stdout", "stderr")]
        row.update(index=number, pid=pid, started_monotonic_ns=started, logs=logs,
                   log_sha256=None if service else {path: hashlib.sha256(b"").hexdigest() for path in logs})
        intent = {"argv": row["command"], "cwd": row["cwd"], "environment_names": row["environment_names"],
            "started_ns": started + 1000, "deadline_ns": 8800_000_000_000, "log_paths": logs}
        binary = {"path": row["command"][0], "sha256": identity["paired_artifact_sha256"] if row["purpose"] == "paired-persistent-server"
                  else identity["binary_sha256"], "device": 1, "inode": 2,
                  "bytes": 100, "mode": 33261, "mtime_ns": 3, "ctime_ns": 4}
        proof = {"index": number, "intent": intent, "launch_binary": binary, "identity": born, "events": [], "errors": [],
            "exit_observed": None, "exit_code": None, "reaped": False, "reaped_ns": None,
            "logs": [{"path": path, "ref": None, "complete": False, "closure": None} for path in logs]}
        def event(action, data):
            nonlocal sequence
            sequence += 1
            ns = started + 2000 + len(proof["events"]) * 1000
            raw_event = {"format": "recall-installed-process-event/v2", "sequence": sequence, "process": number,
                         "action": action, "observed_ns": ns + 1, "data": copy.deepcopy(data)}
            path = root + f"/events/{sequence:03}.json"
            reference = captured(path, raw_event, "installed-process-event")
            proof["events"].append({"action": action, "observed_ns": ns, "data": copy.deepcopy(data), "capture": {"path": path, "ref": reference}})
        event("intent", intent)
        event("launch-binary", binary)
        event("popen-requested", {"initialization_deadline_ns": started + 5_000_000_000})
        stat_raw = original_stat(pid, 41, pid, start_ticks)
        before_path, after_path = root + f"/events/p{number}-before.stat", root + f"/events/p{number}-after.stat"
        before = captured(before_path, stat_raw, "process-stat", raw=True)
        after = captured(after_path, stat_raw, "process-stat", raw=True)
        event("launched", {"identity": born, "stat": {"before": {"path": before_path, "ref": before},
            "after": {"path": after_path, "ref": after}}, "executable": {**binary, "target": binary["path"]}, "executable_error": None})
        if service:
            processes["paired" if row["purpose"] == "paired-persistent-server" else "recall"] = {
                "pid": pid, "ppid": 41, "process_group": pid, "session": pid, "start_ticks": start_ticks,
                "executable": binary["path"], "executable_sha256": binary["sha256"]}
        else:
            exited = {"code": 0, "waitid_status": 0, "waitid_kind": 1, "observed_ns": started + 5500}
            proof["exit_observed"] = exited
            event("exit-observed", exited)
            for log in proof["logs"]:
                closure = {"eof": True, "error_type": None, "closed_ns": started + 6500 + len(proof["events"]) * 100}
                log.update(ref=captured(log["path"], b"", "process-log", raw=True), complete=True, closure=closure)
                event("log-closed", {"path": log["path"], **closure})
            proof.update(reaped=True, exit_code=0, reaped_ns=started + 9000)
            event("reaped", {"code": 0, "reaped_ns": proof["reaped_ns"]})
        proof_refs.append(captured(root + f"/proof-{number}.json", proof, "startup-process-proof"))
    command_path = root + "/commands.json"
    command_ref = captured(command_path, {"format": "recall-runtime-process-commands/v1", "processes": list(commands.values())}, "startup-command-journal")
    command_inventory = captured(root + "/command-inventory.json", {"format": "recall-dynamic-startup-command-inventory/v2",
        "commands": command_ref, "process_proofs": proof_refs}, "startup-command-inventory")
    receipt = {"format": "recall-runtime-bootstrap/v1", "status": "bootstrapped", "qualification_scope": "authenticated-clean-persistent-startup-only",
        "qualified": False, "product_acceptance": False, **{key: plan[key] for key in v.BOOTSTRAP_IDENTITIES - {"configuration_sha256"}},
        "configuration_sha256": identity["outer_configuration_sha256"], "authentication_enabled": True,
        "clean_persistent_store": True, "schema_explicitly_initialized": True,
        "schema_operation": {"kind": "initialize_runtime" if backend == "elephant-db" else "migrate", "capture_index": schema_index,
                             "process_index": None if backend == "elephant-db" else 9},
        "launch_context_sha256": context_ref["sha256"], "isolation_sha256": isolation_ref["sha256"], "plan_sha256": plan_ref["sha256"],
        "allocation_started_monotonic_ns": 1000_000_000_000, "started_monotonic_ns": 1002_000_000_000,
        "observed_monotonic_ns": 1011_000_000_000, "original_deadline_monotonic_ns": 8800_000_000_000,
        "whole_deadline_monotonic_ns": 8800_000_000_000, "space_requests_sha256": hashlib.sha256(original(plan["spaces"])).hexdigest(),
        "observed_configurations_sha256": hashlib.sha256(original(configs)).hexdigest(), "processes": processes, "bootstrap_process": bootstrap,
        "process_commands": {"path": command_path, "sha256": command_ref["sha256"]}, "checks": auth["checks"], "captures": capture_refs,
        "configurations": configs, "token_files": {name: root + "/private/service-" + name + ".token" for name in configs},
        "runtime_config_sha256": runtime_ref["sha256"]}
    receipt_ref = captured(root + "/bootstrap.json", receipt, "runtime-bootstrap")
    current_process = {}
    for name, who in {"controller": controller, "bootstrap": bootstrap, **processes}.items():
        group = who.get("pgrp", who.get("process_group"))
        raw = original_stat(who["pid"], who["ppid"], group, who["start_ticks"])
        # Bootstrap is in its inherited group/session; stat fixture preserves it.
        stat_ref = captured(root + f"/{name}.stat", raw, "process-stat", raw=True)
        current_process[name] = {"identity": who, "stat": stat_ref, "stat_after": stat_ref,
            "executable": who.get("executable", "/usr/bin/python3"), "executable_sha256": who.get("executable_sha256", "e" * 64)}
    current_ref = captured(root + "/current.json", {"format": "recall-dynamic-current-processes/v2",
        "observed_monotonic_ns": 1012_000_000_000, "observed_wall": wall + 12, "processes": current_process,
        "current_network_namespace": "net:[201]", "boot_id": clock["boot_id"], "input_inventory": inputs_ref, "anchor": anchor_ref}, "current-processes")
    cleanup_ref = captured(root + "/cleanup.json", {"format": "recall-dynamic-cleanup-obligations/v2", "run_nonce": identity["run_nonce"],
        "backend": backend, "original_deadline_monotonic_ns": 8800_000_000_000, "processes": processes}, "cleanup-obligations")
    configs_ref = captured(root + "/configs.json", configs, "space-configurations")
    inventory = {"format": "recall-dynamic-capture-inventory/v2", "files": [{"path": path, "ref": ref} for path, ref in sorted(files.items())]}
    inventory_ref = retain(original(inventory), "capture-inventory", raws)
    observed = {"format": "recall-dynamic-runtime-evidence/v2", "identity": identity, "bootstrap": receipt_ref,
        "installed_offline": retain(original(installed), "installed-offline-evidence", raws), "actual_runtime_configuration": runtime_ref,
        "actual_space_configurations": configs_ref, "controller": context_ref, "current_processes": current_ref,
        "capture_inventory": inventory_ref, "command_inventory": command_inventory, "cleanup_obligations": cleanup_ref,
        "outer_launch": outer_ref}
    review_raw = original(review)
    reviewed = {"review": review, "raw_sha256": hashlib.sha256(review_raw).hexdigest(), "raw_bytes": review_raw}
    wrapper = {"context": context, "raw_sha256": context_ref["sha256"], "raw_bytes": raws[context_ref["sha256"]],
               "observed_wall": wall + 13, "observed_monotonic_ns": 1013_000_000_000}
    return reviewed, observed, raws, wrapper


class LazyOriginals(Mapping):
    def __init__(self, items):
        self.items, self.lookups = items, []
    def __len__(self):
        return len(self.items)
    def __iter__(self):
        return iter(self.items)
    def __getitem__(self, key):
        self.lookups.append(key)
        return self.items[key]


def channel_fixture():
    """Original in-memory G export shapes, composed with F's real predicate."""
    provider = ProviderFixture()
    bundle_ref, provider_raw, identity, context_wrapper, configs, observed, bounds = provider.materialize()
    raws = dict(provider_raw.values)
    bundle = json.loads(raws[bundle_ref["sha256"]])
    begin, close = observed["provider_cohort_begin"], observed["provider_accounting"]
    root = "cohort-01/"
    exported_files = [{"path": root + "bundle.json", "ref": bundle_ref}]
    for key, filename in (("begin", "begin.json"), ("close", "close.json"), ("baseline", "baseline.json"),
            ("phase_clock", "phase-clock.json"), ("sealed", "sealed.json"),
            ("capture_inventory", "capture-inventory.json"), ("dispatch_records", "dispatch-records.json")):
        exported_files.append({"path": root + filename, "ref": bundle[key]})
    kinds = {"request.original.json": "provider-request", "request.forwarded.json": "provider-request",
             "intent.json": "provider-dispatch-intent", "response.body": "provider-response", "capture.json": "provider-capture"}
    for capture in json.loads(raws[bundle["capture_inventory"]["sha256"]]):
        for filename, sha in capture["files"].items():
            exported_files.append({"path": root + f'dispatch-{capture["dispatch_id"]:06d}/' + filename,
                "ref": {"sha256": sha, "bytes": len(raws[sha]), "kind": kinds[filename]}})
    context = context_wrapper["context"]
    helper = "tests/recall_preview_semantics/gateway/accounting_channel.py"
    identity.update(source_commit="a" * 40, artifact_sha256="d" * 64, binary_sha256="b" * 64,
        endpoint="http://127.0.0.1:43130", run_nonce="original-runtime-123", preparation_nonce="original-preparation-123",
        rendered_map_sha256=hashlib.sha256(b"rendered_map_sha256").hexdigest())
    bridge = "tests/recall_preview_semantics/gateway/cohort_bridge.py"
    review = {"helper_sha256": {helper: "6" * 64, bridge: "7" * 64}, "input_refs": {}, "expires_at": "2026-10-07T04:00:00Z"}
    caller = {"format": "recall-dynamic-workload-phase/v2", "execution_authorized": True, "provider_dispatch_authorized": True,
        "phase": "preparation", "provider_phase": "preparation", "expires_at": "2026-10-07T04:00:00Z",
        "output_directory": "/work/preparation", "fixture_timestamp": "2026-10-07T00:00:00Z", "nonce": identity["preparation_nonce"],
        **{key: identity[key] for key in ("source_commit", "artifact_sha256", "binary_sha256", "backend", "endpoint")},
        "provider_bridge_path": "/inputs/original-source/" + bridge, "provider_bridge_sha256": "7" * 64,
        "space_credentials": {name: "service-" + name for name in configs},
        "credential_files": {"service-" + name: f'/work/bootstrap-{identity["run_nonce"]}-{identity["backend"]}/private/service-{name}.token'
                             for name in configs}}
    readonly_inputs = {"format": "recall-gateway-inputs/v1", "files": []}
    for name, static_name in (("contract", "workload_contract"), ("corpus", "corpus"), ("provider_allocation", "provider_allocation"),
            ("preparation_contract", "preparation_contract"), ("typed_delta", "typed_delta"), ("rendered_map", "rendered_map")):
        # F's fixture identities intentionally use sha(field-name bytes). These
        # literal original test inputs retain exactly those same hashes.
        body = (static_name + "_sha256").encode()
        ref = retain(body, "original-" + static_name, raws)
        assert ref["sha256"] == identity[static_name + "_sha256"]
        review["input_refs"][static_name] = ref
        caller[name + "_path"] = "/inputs/" + static_name + ".json"
        caller[name + "_sha256"] = ref["sha256"]
        readonly_inputs["files"].append({"path": static_name + ".json", "bytes": len(body), "sha256": ref["sha256"]})
    review["input_refs"]["workload_intents"] = retain(original({"format": "recall-workload-intents/v2", "backends": {
        identity["backend"]: {"fixture_timestamp": caller["fixture_timestamp"], "preparation_nonce": identity["preparation_nonce"],
                              "measurement_nonce": "original-measurement-123"}}}), "workload-intents", raws)
    facts = {"device": 2049, "inode": 789, "uid": 1000, "mode": 0o700}
    channel = {"format": "recall-provider-accounting-channel/v2", "anchor_sha256": context["anchor_sha256"]}
    records = {"sidecar": {"format": "recall-gateway-accounting-channel/v1", "anchor_sha256": context["anchor_sha256"],
        "path": "/gateway-accounting", "directory": facts, "helper_sha256": "6" * 64, "read_only": True},
        "directory": {"format": "recall-provider-accounting-directory/v2", "path": "/gateway-accounting", **facts},
        "mountinfo": b"41 30 8:1 /original /gateway-accounting ro,nosuid - ext4 /dev/original rw\n",
        "index": {"format": "recall-gateway-accounting-index/v1", "cohort_id": 1, "backend": "elephant-db",
            "phase": "preparation", "caller_sha256": hashlib.sha256(original(caller)).hexdigest(),
            "begin_sha256": hashlib.sha256(original(begin)).hexdigest(), "close_sha256": hashlib.sha256(original(close)).hexdigest(),
            "reference": bundle_ref, "files": exported_files}, "caller": caller}
    paths = {}
    for key, kind in (("sidecar", "provider-accounting-sidecar"), ("directory", "provider-accounting-directory"),
                      ("mountinfo", "provider-mountinfo"), ("index", "provider-accounting-index"), ("caller", "workload-phase-manifest")):
        channel[key] = retain(records[key] if key == "mountinfo" else original(records[key]), kind, raws)
        paths["/work/preparation/channel-" + key] = channel[key]
    reference = retain(original(channel), "provider-accounting-channel", raws)
    paths["/work/preparation/channel.json"] = reference
    return reference, bundle_ref, raws, review, context_wrapper, identity, begin, close, paths, configs, observed, bounds, readonly_inputs


def prepared_fixture(backend="elephant-db"):
    """One complete original synthetic fifty-source M/L/F evidence chain."""
    reviewed, runtime, raws, initial_wrapper = runtime_fixture(prepared=True, backend=backend)
    component = v.validate_runtime(reviewed, runtime, raws, initial_wrapper)
    review, context = reviewed["review"], initial_wrapper["context"]
    identity = {**runtime["identity"], "preparation_nonce": "original-preparation-123",
                **{name + "_sha256": ref["sha256"] for name, ref in review["input_refs"].items() if name in PREP_ORIGINALS}}
    before = json.loads(raws[runtime["actual_space_configurations"]["sha256"]])
    after = copy.deepcopy(before)
    for row in after.values():
        row["processing"]["index_generation"] = 50
        row["updated_at"] = "2026-10-07T01:00:24Z"
    paths, intents, responses, accepted, publication, immutable = {}, [], [], [], [], []
    def save(path, value, kind, *, binary=False):
        ref = retain(value if binary else original(value), kind, raws)
        paths[path] = ref
        return ref
    def capture(method, path, space, body, response, status=200):
        index = len(intents) + 1
        payload, response_bytes = b"" if body is None else original(body), original(response)
        intent = {"format": "recall-workload-preparation-request/v1", "request_index": index,
            "method": method, "path": path, "space": space, "credential_role": "service-" + space,
            "request_body": body, "request_body_base64": base64.b64encode(payload).decode(),
            "request_sha256": hashlib.sha256(payload).hexdigest(), "started_monotonic_ns": 1020_000_000_000 + index * 10_000_000,
            "harness_attempt": 1, "state": "intent-only; no physical-send claim"}
        request_ref = save(f"/work/preparation/captures/{index:04}.request.json", intent, "preparation-intent")
        result = {key: intent[key] for key in ("method", "path", "space", "request_body", "request_sha256", "request_index")}
        result.update(request_intent_sha256=request_ref["sha256"], elapsed_ms=1, error_type=None,
            response={"framing_complete": True, "status": status, "body": response,
                "raw_body_base64": base64.b64encode(response_bytes).decode(), "body_sha256": hashlib.sha256(response_bytes).hexdigest(),
                "bytes": len(response_bytes), "raw_body_complete": True})
        response_ref = save(f"/work/preparation/captures/{index:04}.json", result, "preparation-response")
        intents.append({"request_index": index, "intent": request_ref, "response": response_ref})
        responses.append(result)
        return response_ref
    before_refs = {name: capture("GET", "/v1/spaces/" + name, name, None, row) for name, row in before.items()}
    for source in CORPUS["sources"]:
        capture("GET", f'/v1/spaces/{source["space"]}/sources/{v.quote(source["key"], safe="")}/head', source["space"], None,
            {"error_code": "not_found", "message": "original absent head", "retryable": False}, 404)
    fixtures = []
    for number in range(50):
        source, accept, records, rendered, security, partition = fixture(number)
        body = v.typed_source_request(source, identity, TYPED)
        substitutions = {accept["source_id"]: body["source"]["id"], accept["doc_id"]: body["doc"]["id"]}
        def replace(value):
            if type(value) is dict: return {key: replace(item) for key, item in value.items()}
            if type(value) is list: return [replace(item) for item in value]
            return substitutions.get(value, value) if type(value) is str else value
        accept, records = replace(accept), replace(records)
        capture("POST", "/v1/sources/ingest", source["space"], body, accept, 202)
        accepted.append(accept)
        fixtures.append((source, accept, records, rendered, security, partition))
    for source, accept, records, rendered, security, partition in fixtures:
        doc_path = "/v1/docs/" + v.quote(accept["doc_id"], safe="")
        routes = {"operation": "/v1/operations/" + v.quote(accept["operation"]["operation_id"], safe=""),
            "head": f'/v1/spaces/{source["space"]}/sources/{v.quote(source["key"], safe="")}/head',
            "source": "/v1/sources/" + v.quote(accept["source_id"], safe=""), "doc": doc_path,
            "nodes": doc_path + "/chunks", "pipeline": doc_path + "/pipeline-status",
            "immutable_revision": doc_path + "/revisions/" + v.quote(records["immutable_revision"]["id"], safe="")}
        for label, route in routes.items():
            ref = capture("GET", route, source["space"], None, records[label])
            if label == "immutable_revision":
                immutable.append({"space": source["space"], "key": source["key"], "revision": source["revision"], "response": ref})
        revision = records["immutable_revision"]
        publication.append({"space": source["space"], "key": source["key"], "revision": source["revision"],
            "source_id": accept["source_id"], "doc_id": accept["doc_id"], "internal_doc_revision_id": revision["id"],
            "operation_id": records["operation"]["operation_id"], "generation": records["operation"]["generation"],
            "source_sha256": source["sha256"], "embeddable_node_ids": [node["id"] for node in records["nodes"] if node["role"] in ("paragraph", "chunk")],
            "rendered_sha256": rendered["rendered_sha256"], "rendered_utf8_bytes": rendered["rendered_utf8_bytes"],
            "public_profile_marker": partition, "original_revision_record": {"id": revision["id"], "text_sha256": revision["text_hash"],
                "renderer": revision["renderer"], "response_body_canonical_sha256": hashlib.sha256(original(revision)).hexdigest()},
            "original_private_manifest": "unmet: not exposed by these public reads",
            "physical_vector_inventory": "unmet: node markers and completed operation are not a raw index inventory"})
    after_refs = {name: capture("GET", "/v1/spaces/" + name, name, None, row) for name, row in after.items()}
    observed = {"format": "recall-workload-preparation-observation/v1", "phase": "preparation",
        **{key: identity[key] for key in ("source_commit", "artifact_sha256", "binary_sha256", "backend", "endpoint", "corpus_sha256",
            "preparation_contract_sha256", "provider_allocation_sha256", "typed_delta_sha256", "rendered_map_sha256")},
        "nonce": identity["preparation_nonce"], "contract_sha256": identity["workload_contract_sha256"],
        "http_requests": 454, "acceptance_post_attempts": 50, "accepted": accepted, "published_inventory": publication,
        "source_count": 50, "query_warmups": 0, "harness_retries": 0, "run_error_type": None, "qualified": False,
        "product_acceptance": False, "capture_verdict": "passed", "proof_limits": [
            "public embedding markers are not raw physical vectors", "original private publication manifests are not returned by these APIs"]}
    provider = ProviderFixture(backend)
    provider.identity = identity
    provider.anchor = context["anchor"]
    provider.clock = provider.anchor["clock"]
    wall = provider.clock["wall_start"]
    provider.phase.update(run={key: provider.clock[key] for key in ("wall_start", "mono_start", "boot_id")},
        phase_start={"wall": wall + 19, "mono": 1019.}, deadline_mono=4619.)
    provider.authority.update(bindings_sha256=provider.anchor["bindings_sha256"],
        configuration_sha256=identity["outer_configuration_sha256"], allocation_sha256=identity["provider_allocation_sha256"],
        contract_sha256=identity["workload_contract_sha256"], corpus_sha256=identity["corpus_sha256"],
        preparation_contract_sha256=identity["preparation_contract_sha256"], typed_delta_sha256=identity["typed_delta_sha256"])
    for number, prior in enumerate(provider.baseline_entries, 1):
        prior.update(started=wall + number, finished=wall + number + 0.5)
    for number, call in enumerate(provider.calls, 1):
        call["binding"]["identity"] = copy.deepcopy(provider.authority)
        call["binding"]["scope"]["profile_id"] = "embedding"
        call["intent"]["binding"] = call["binding"]
        call["row"].update(started=wall + 21 + number / 2, finished=wall + 21.2 + number / 2)
        start = 1021 + number / 2
        call["capture"]["transport"].update(started_mono=start, request_started_mono=start + 0.01,
            read_completed_mono=start + 0.1, deadline_mono=start + 120)
    provider.observed = observed
    bundle_ref, provider_raw, _, _, _, observed, _ = provider.materialize()
    raws.update(provider_raw.values)
    begin, close = observed["provider_cohort_begin"], observed["provider_accounting"]
    bundle = json.loads(raws[bundle_ref["sha256"]])
    caller = {"format": "recall-dynamic-workload-phase/v2", "execution_authorized": True, "provider_dispatch_authorized": True,
        "phase": "preparation", "provider_phase": "preparation", "expires_at": review["expires_at"],
        "output_directory": "/work/preparation", "fixture_timestamp": TYPED["fixture_timestamp"], "nonce": identity["preparation_nonce"],
        **{key: identity[key] for key in ("source_commit", "artifact_sha256", "binary_sha256", "backend", "endpoint")},
        "provider_bridge_path": "/inputs/original-source/tests/recall_preview_semantics/gateway/cohort_bridge.py",
        "provider_bridge_sha256": "6" * 64, "space_credentials": {name: "service-" + name for name in before},
        "credential_files": {"service-" + name: f'/work/bootstrap-{identity["run_nonce"]}-{identity["backend"]}/private/service-{name}.token'
                             for name in before}}
    for name, static_name in (("contract", "workload_contract"), *((name, name) for name in PREP_ORIGINALS if name != "workload_contract")):
        caller[name + "_path"] = "/inputs/" + static_name + ".json"
        caller[name + "_sha256"] = review["input_refs"][static_name]["sha256"]
    caller_ref = save("/work/preparation.manifest.json", caller, "workload-phase-manifest")
    cohort_root = f'cohort-{provider.cohort_id:02d}/'
    exported_files = [{"path": cohort_root + "bundle.json", "ref": bundle_ref}]
    for key, filename in (("begin", "begin.json"), ("close", "close.json"), ("baseline", "baseline.json"),
            ("phase_clock", "phase-clock.json"), ("sealed", "sealed.json"),
            ("capture_inventory", "capture-inventory.json"), ("dispatch_records", "dispatch-records.json")):
        exported_files.append({"path": cohort_root + filename, "ref": bundle[key]})
    kinds = {"request.original.json": "provider-request", "request.forwarded.json": "provider-request",
             "intent.json": "provider-dispatch-intent", "response.body": "provider-response", "capture.json": "provider-capture"}
    for capture_row in json.loads(raws[bundle["capture_inventory"]["sha256"]]):
        for filename, sha in capture_row["files"].items():
            exported_files.append({"path": cohort_root + f'dispatch-{capture_row["dispatch_id"]:06d}/' + filename,
                "ref": {"sha256": sha, "bytes": len(raws[sha]), "kind": kinds[filename]}})
    facts = {"device": 2049, "inode": 789, "uid": 1000, "mode": 0o700}
    channel = {"format": "recall-provider-accounting-channel/v2", "anchor_sha256": context["anchor_sha256"], "caller": caller_ref}
    for label, kind, value in (
        ("sidecar", "provider-accounting-sidecar", {"format": "recall-gateway-accounting-channel/v1", "anchor_sha256": context["anchor_sha256"],
            "path": "/gateway-accounting", "directory": facts, "helper_sha256": "6" * 64, "read_only": True}),
        ("directory", "provider-accounting-directory", {"format": "recall-provider-accounting-directory/v2", "path": "/gateway-accounting", **facts}),
        ("mountinfo", "provider-mountinfo", b"41 30 8:1 /original /gateway-accounting ro,nosuid - ext4 /dev/original rw\n"),
        ("index", "provider-accounting-index", {"format": "recall-gateway-accounting-index/v1", "cohort_id": provider.cohort_id, "backend": identity["backend"],
            "phase": "preparation", "caller_sha256": hashlib.sha256(original(caller)).hexdigest(),
            "begin_sha256": hashlib.sha256(original(begin)).hexdigest(), "close_sha256": hashlib.sha256(original(close)).hexdigest(),
            "reference": bundle_ref, "files": exported_files})):
        channel[label] = save("/work/preparation/provider-channel-" + label, value, kind, binary=label == "mountinfo")
    preparation = {"format": "recall-dynamic-preparation-evidence/v2", "identity": identity,
        "observation": save("/work/preparation/observation.json", observed, "preparation-observation"),
        "configuration_before": save("/work/preparation/before.json", before_refs, "preparation-configurations"),
        "configuration_after": save("/work/preparation/after.json", after_refs, "preparation-configurations"),
        "original_intents": save("/work/preparation/intents.json", {"format": "recall-dynamic-preparation-intents/v2", "requests": intents}, "preparation-intents"),
        "immutable_revisions": save("/work/preparation/revisions.json", immutable, "preparation-immutable-inventory"),
        "provider_cohort_begin": bundle["begin"], "provider_accounting": bundle_ref,
        "provider_accounting_channel": save("/work/preparation/channel.json", channel, "provider-accounting-channel"), "controller": runtime["controller"]}
    preparation["capture_inventory"] = retain(original({"format": "recall-dynamic-capture-inventory/v2", "files": [
        {"path": path, "ref": ref} for path, ref in paths.items()]}), "capture-inventory", raws)
    initial_inventory = json.loads(raws[runtime["capture_inventory"]["sha256"]])
    current_files = {row["path"]: row["ref"] for row in initial_inventory["files"]}
    receipt = json.loads(raws[runtime["bootstrap"]["sha256"]])
    current_pairs = []
    for row in receipt["captures"][-3:]:
        request = json.loads(raws[row["request_sha256"]])
        response = json.loads(raws[row["response_sha256"]])
        index = 120 + len(current_pairs)
        request.update(request_index=index, started_monotonic_ns=1026_000_000_000 + len(current_pairs) * 1_000_000)
        response.update(request_index=index, finished_monotonic_ns=request["started_monotonic_ns"] + 100_000)
        pair = {"request_index": index}
        for label, value in (("request", request), ("response", response)):
            pair[label] = retain(original(value), "startup-" + label, raws)
            current_files[f'/work/current/{index}.{label}.json'] = pair[label]
        current_pairs.append(pair)
    process_observation = json.loads(raws[runtime["current_processes"]["sha256"]])
    process_observation.update(observed_monotonic_ns=1027_000_000_000, observed_wall=wall + 27)
    current_ref = retain(original(process_observation), "current-processes", raws)
    current = {"format": "recall-dynamic-current-runtime-custody/v2", "current_processes": current_ref,
        "capture_inventory": retain(original({"format": "recall-dynamic-capture-inventory/v2", "files": [
            {"path": path, "ref": ref} for path, ref in current_files.items()]}), "capture-inventory", raws),
        "cleanup_obligations": runtime["cleanup_obligations"], "outer_launch": runtime["outer_launch"],
        "worker_recipe_capture": current_pairs[0],
        "provider_recipe_captures": dict(zip(before, current_pairs[1:]))}
    chain = {"format": "recall-dynamic-runtime-chain/v2", "component": retain(original(component), "runtime-component", raws),
        "evidence": retain(original(runtime), "runtime-evidence", raws), "validated_monotonic_ns": initial_wrapper["observed_monotonic_ns"],
        "validated_wall": initial_wrapper["observed_wall"], "current_custody": retain(original(current), "runtime-current-custody", raws)}
    preparation["runtime_component"] = retain(original(chain), "runtime-validation-chain", raws)
    wrapper = {**initial_wrapper, "observed_monotonic_ns": 1028_000_000_000, "observed_wall": wall + 28}
    return reviewed, preparation, component, raws, wrapper


class DynamicPredicateControls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.patches = [patch("builtins.open", side_effect=AssertionError("unexpected file I/O")),
                       patch("socket.socket", side_effect=AssertionError("unexpected network")),
                       patch("subprocess.Popen", side_effect=AssertionError("unexpected child"))]
        for item in cls.patches:
            item.start()

    @classmethod
    def tearDownClass(cls):
        for item in reversed(cls.patches):
            item.stop()

    def test_original_map_stays_lazy_and_rechecks_mutation(self):
        items = {}
        first = retain(b"original", "raw-control", items)
        retain(b"unrelated-provider-body", "raw-control", items)
        lazy = LazyOriginals(items)
        evidence = v.Evidence(lazy)
        self.assertEqual(lazy.lookups, [])
        self.assertEqual(evidence.read(first, "raw-control"), b"original")
        self.assertEqual(lazy.lookups, [first["sha256"]])
        items[first["sha256"]] = b"changed!"
        with self.assertRaisesRegex(ValueError, "dynamic_raw_hash_size"):
            evidence.read(first, "raw-control")

    def test_reference_kind_length_and_original_hash_are_independent(self):
        items = {}
        ref = retain(b"original", "one-kind", items)
        for wrong in ({**ref, "bytes": 7}, {**ref, "bytes": True}, {**ref, "kind": "other-kind"}, {**ref, "sha256": "0" * 64}):
            with self.subTest(wrong=wrong):
                with self.assertRaises(ValueError):
                    v.Evidence(items).read(wrong, "one-kind")
        evidence = v.Evidence(items)
        evidence.read(ref, "one-kind")
        with self.assertRaisesRegex(ValueError, "dynamic_reference_kind_alias"):
            evidence.read({**ref, "kind": "other-kind"}, "other-kind")

    def test_same_empty_logs_keep_distinct_paths_and_one_kind(self):
        items = {}
        ref = retain(b"", "process-log", items)
        inventory = {"format": "recall-dynamic-capture-inventory/v2", "files": [
            {"path": "/work/original.stdout", "ref": ref}, {"path": "/work/original.stderr", "ref": ref}]}
        entry = retain(original(inventory), "capture-inventory", items)
        evidence = v.Evidence(items)
        evidence.inventory(entry)
        self.assertEqual(len(evidence.paths), 2)
        inventory["files"].append(inventory["files"][0])
        duplicate = retain(original(inventory), "capture-inventory", items)
        with self.assertRaisesRegex(ValueError, "dynamic_duplicate_capture_path"):
            v.Evidence(items).inventory(duplicate)

    def test_strict_json_never_accepts_duplicate_nonfinite_or_bad_utf8(self):
        for raw in (b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":1e999}', b'"\xff"'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                v.strict_json(raw)
        self.assertFalse(v.integer(True))
        self.assertFalse(v.exact({"x": True}, {"x": 1}))

    def test_actual_stat_identity_and_live_state(self):
        expected = {"pid": 42, "ppid": 4, "pgrp": 42, "session": 42, "start_ticks": 777}
        raw = b"42 (name with ) brackets) S 4 42 42 " + b"0 " * 15 + b"777 0 0\n"
        self.assertEqual(v.process_identity(raw, expected), expected)
        for wrong in (raw.replace(b") S", b") Z"), raw.replace(b"777", b"778")):
            with self.assertRaises(ValueError):
                v.process_identity(wrong, expected)

    def test_original_controller_parent_and_namespace_fields_are_exact(self):
        reviewed, observed, raws, wrapper = runtime_fixture()
        plan = json.loads(raws[reviewed["review"]["bootstrap_plans"]["elephant-db"]["sha256"]])
        for mutation, error in (("parent-missing", "dynamic_fields"), ("parent-extra", "dynamic_fields"),
                                ("parent-pid-bool", "dynamic_process_numbers"),
                                ("parent-ppid-negative", "dynamic_process_numbers"),
                                ("parent-start-zero", "dynamic_process_numbers"),
                                ("parent-namespace", "dynamic_network_namespace"),
                                ("current-namespace", "dynamic_network_namespace"),
                                ("controller-pgrp-bool", "dynamic_process_numbers")):
            context = copy.deepcopy(wrapper["context"])
            if mutation == "parent-missing": context["anchor"]["parent"].pop("process")
            elif mutation == "parent-extra": context["anchor"]["parent"]["uid"] = 1000
            elif mutation == "parent-pid-bool": context["anchor"]["parent"]["process"]["pid"] = True
            elif mutation == "parent-ppid-negative": context["anchor"]["parent"]["process"]["ppid"] = -1
            elif mutation == "parent-start-zero": context["anchor"]["parent"]["process"]["start_ticks"] = 0
            elif mutation == "parent-namespace":
                context["anchor"]["parent"]["network_namespace"] = context["network"]["parent_namespace"] = "not-a-namespace"
            elif mutation == "current-namespace": context["network"]["namespace"] = "another-invalid-namespace"
            else: context["process"]["pgrp"] = True
            context["anchor_sha256"] = hashlib.sha256(original(context["anchor"])).hexdigest()
            raw = original(context)
            reference = retain(raw, "gateway-context", raws)
            current = {**wrapper, "context": context, "raw_sha256": reference["sha256"], "raw_bytes": raw}
            evidence = v.Evidence(raws)
            evidence.inventory(observed["capture_inventory"])
            with self.subTest(mutation=mutation), self.assertRaisesRegex(ValueError, "^" + error + "$"):
                v.context_evidence(current, reference, evidence, reviewed["review"], observed["identity"], plan)

    def test_original_outer_launch_raws_paths_and_current_inventory_are_required(self):
        reviewed, observed, raws, wrapper = runtime_fixture()
        original_outer = json.loads(raws[observed["outer_launch"]["sha256"]])
        plan = json.loads(raws[reviewed["review"]["bootstrap_plans"]["elephant-db"]["sha256"]])
        processes = json.loads(raws[observed["current_processes"]["sha256"]])
        inventory = json.loads(raws[observed["capture_inventory"]["sha256"]])
        self.assertTrue(raws[original_outer["manifest"]["sha256"]].endswith(b"\n"))
        self.assertTrue(raws[original_outer["spec"]["sha256"]].endswith(b"\n"))
        for member in ("wrapper", "manifest", "spec", "input_inventory"):
            reference = observed["outer_launch"] if member == "wrapper" else original_outer[member]
            current = copy.deepcopy(inventory)
            current["files"] = [row for row in current["files"] if row["ref"] != reference]
            with self.subTest(member=member), self.assertRaisesRegex(ValueError, "^dynamic_outer_capture_membership$"):
                v.outer_launch(observed["outer_launch"], v.Evidence(raws), current, processes, wrapper["context"],
                               observed["identity"], plan, reviewed["review"])
        for member, path in (("manifest", "/gateway-manifest.json"), ("spec", "/gateway-launch.json")):
            current = copy.deepcopy(inventory)
            next(row for row in current["files"] if row["path"] == path)["path"] = "/work/copied-" + member
            with self.subTest(member=member), self.assertRaisesRegex(ValueError, "^dynamic_outer_original_fixed_path$"):
                v.outer_launch(observed["outer_launch"], v.Evidence(raws), current, processes, wrapper["context"],
                               observed["identity"], plan, reviewed["review"])
        for member, error in (("manifest", "dynamic_outer_original_manifest_hash"),
                              ("spec", "dynamic_outer_original_spec_hash")):
            outer = copy.deepcopy(original_outer)
            outer[member] = retain(raws[outer[member]["sha256"]].rstrip(b"\n"), outer[member]["kind"], raws)
            reference = retain(original(outer), "gateway-launch-inputs", raws)
            current = copy.deepcopy(inventory)
            for row in current["files"]:
                if row["ref"] == observed["outer_launch"]: row["ref"] = reference
                elif row["ref"] == original_outer[member]: row["ref"] = outer[member]
            with self.subTest(member=member), self.assertRaisesRegex(ValueError, "^" + error + "$"):
                v.outer_launch(reference, v.Evidence(raws), current, processes, wrapper["context"],
                               observed["identity"], plan, reviewed["review"])

    def test_outer_launch_parsed_fields_cannot_be_replaced_with_copied_hashes(self):
        reviewed, observed, originals, wrapper = runtime_fixture()
        initial_outer = json.loads(originals[observed["outer_launch"]["sha256"]])
        plan = json.loads(originals[reviewed["review"]["bootstrap_plans"]["elephant-db"]["sha256"]])
        processes = json.loads(originals[observed["current_processes"]["sha256"]])
        cases = (
            ("spec-bindings", "dynamic_outer_original_spec_bindings"),
            ("command", "dynamic_outer_original_command_hash"),
            ("environment-key", "dynamic_outer_environment"), ("environment-size", "dynamic_outer_environment"),
            ("retry", "dynamic_outer_original_queue_recipe"), ("inputs-hash", "dynamic_outer_original_inventory_hash"),
            ("inputs-ref", "dynamic_outer_same_input_inventory"), ("inputs-directory", "dynamic_outer_inputs_directory"),
            ("manifest-extra", "dynamic_outer_manifest_fields"), ("manifest-account", "dynamic_outer_original_account_profile"),
            ("manifest-nonce", "dynamic_outer_original_nonce"), ("manifest-configuration", "dynamic_outer_original_manifest_binding"),
            ("manifest-cap-bool", "dynamic_outer_original_capture_scope"), ("manifest-allocation", "dynamic_outer_original_allocation"),
            ("manifest-bindings", "dynamic_outer_original_bindings_hash"), ("manifest-endpoint", "dynamic_outer_original_endpoint"),
            ("manifest-upstream", "dynamic_outer_upstream_address"), ("manifest-dimensions-bool", "dynamic_outer_embedding_profile"))
        for mutation, error in cases:
            raws, context, identity = dict(originals), copy.deepcopy(wrapper["context"]), dict(observed["identity"])
            outer = copy.deepcopy(initial_outer)
            manifest = json.loads(raws[outer["manifest"]["sha256"]])
            spec = json.loads(raws[outer["spec"]["sha256"]])
            if mutation == "spec-bindings": spec["bindings"][0]["id"] += "_changed"
            elif mutation == "command": spec["command"].append("--changed")
            elif mutation == "environment-key": spec["environment"]["UNREVIEWED"] = "value"
            elif mutation == "environment-size": spec["environment"]["RUST_LOG"] = "x" * 1025
            elif mutation == "retry": spec["environment"]["PIPELINE_WORK_TRANSIENT_RETRY_MAX_ATTEMPTS"] = "4"
            elif mutation == "inputs-hash": spec["inputs"]["inventory_sha256"] = "f" * 64
            elif mutation == "inputs-ref": outer["input_inventory"] = {**outer["input_inventory"], "kind": "other-inventory"}
            elif mutation == "inputs-directory": spec["inputs"]["directory"] = "/original/../replaced"
            elif mutation == "manifest-extra": manifest["source_commit"] = identity["source_commit"]
            elif mutation == "manifest-account": manifest["account_id"] = "different-account"
            elif mutation == "manifest-nonce": manifest["run_id"] += "changed"
            elif mutation == "manifest-configuration": pass
            elif mutation == "manifest-cap-bool": manifest["transport_response_byte_cap"] = True
            elif mutation == "manifest-allocation": manifest["allocation_sha256"] = "a" * 64
            elif mutation == "manifest-bindings": manifest["bindings"][0]["id"] += "_changed"
            elif mutation == "manifest-endpoint": manifest["gateway_endpoint"] = "http://127.0.0.1:43131/v1"
            elif mutation == "manifest-upstream": manifest["upstream_ips"] = ["127.0.0.1"]
            else: manifest["bindings"][0]["dimensions"] = True
            outer["spec"] = retain(original(spec) + b"\n", "gateway-launch-spec", raws)
            identity["outer_configuration_sha256"] = context["anchor"]["configuration_sha256"] = outer["spec"]["sha256"]
            manifest["configuration_sha256"] = "a" * 64 if mutation == "manifest-configuration" else outer["spec"]["sha256"]
            outer["manifest"] = retain(original(manifest) + b"\n", "gateway-manifest", raws)
            context["anchor"]["manifest_sha256"] = outer["manifest"]["sha256"]
            reference = retain(original(outer), "gateway-launch-inputs", raws)
            inventory = json.loads(raws[observed["capture_inventory"]["sha256"]])
            changes = [(observed["outer_launch"], reference), *[(initial_outer[key], outer[key]) for key in initial_outer if key != "format"]]
            for row in inventory["files"]:
                for old, new in changes:
                    if row["ref"] == old:
                        row["ref"] = new
                        break
            with self.subTest(mutation=mutation), self.assertRaisesRegex(ValueError, "^" + error + "$"):
                v.outer_launch(reference, v.Evidence(raws), inventory, processes, context, identity, plan, reviewed["review"])

    def test_original_primary_manifest_variants_retain_separate_exact_fields(self):
        reviewed, observed, raws, wrapper = runtime_fixture()
        outer = json.loads(raws[observed["outer_launch"]["sha256"]])
        cohort = json.loads(raws[outer["manifest"]["sha256"]])
        for settlement in (False, True):
            manifest = {key: value for key, value in cohort.items() if key not in {
                "accounting_policy", "credential_generation", "allocation_path", "allocation_sha256", "contract_sha256",
                "preparation_contract_path", "preparation_contract_sha256", "typed_delta_path", "typed_delta_sha256"}}
            manifest.update(format="recall-gateway-manifest/v2" if settlement else "recall-gateway-manifest/v1",
                proposal_sha256="c" * 64, catalog_sha256="9b40bf9b99a93b3f6d1ba7cf0b402fd46b01b4aac42205c9681c8e894a03e2ce",
                corpus_sha256="fa6a84149483d3511fd559ac9a13e89d6c1c041ae97d6f15084b52b7d7f21c1b")
            manifest["bindings"] = copy.deepcopy(manifest["bindings"])
            for binding in manifest["bindings"]: binding["space"] = binding["space"].replace("qualification-", "semantics-")
            if settlement:
                manifest.update(accounting_policy="full-context-terminal-v2", rate_version="sol-standard-long-max-2026-10-06",
                    credential_generation=1, bound_version="provider-full-context-v1",
                    reasoning_sdk_sha256="105229842f1a221277513653d5396c8e36a116384dde406182f2e173cdd0f8c3",
                    reasoning_projection_version="sdk-0.8.51-reasoning-projection-v1")
            anchor = {**wrapper["context"]["anchor"], "allocation_sha256": None,
                "proposal_sha256": manifest["proposal_sha256"], "corpus_sha256": manifest["corpus_sha256"],
                "bindings_sha256": hashlib.sha256(original(manifest["bindings"])).hexdigest()}
            self.assertEqual(v.gateway_manifest(manifest, anchor, observed["identity"]), manifest)
            changed = {**manifest, "allocation_sha256": "a" * 64}
            with self.assertRaisesRegex(ValueError, "^dynamic_outer_manifest_fields$"):
                v.gateway_manifest(changed, anchor, observed["identity"])
            if not settlement:
                with self.assertRaisesRegex(ValueError, "^dynamic_outer_manifest_fields$"):
                    v.gateway_manifest({**manifest, "credential_generation": 1}, anchor, observed["identity"])

    def test_static_review_preserves_original_bytes_and_returns_detached_plan(self):
        review, raws, plan = static_fixture()
        result = v.validate_static(review, raws, "elephant-db")
        self.assertEqual(result, plan)
        result["spaces"][0]["name"] = "changed"
        self.assertEqual(v.validate_static(review, raws, "elephant-db"), plan)
        review["raw_bytes"] += b" "
        with self.assertRaisesRegex(ValueError, "dynamic_parameter_raw_identity"):
            v.validate_static(review, raws, "elephant-db")

    def test_future_output_and_wider_static_budget_refuse(self):
        for change in (lambda item: item["limits"].update(runtime_raw_bytes=2**40),
                       lambda item: item["controller_policy"].update(process_pid=123)):
            review, raws, _ = static_fixture()
            change(review["review"])
            review["raw_bytes"] = original(review["review"])
            review["raw_sha256"] = hashlib.sha256(review["raw_bytes"]).hexdigest()
            with self.assertRaises(ValueError):
                v.validate_static(review, raws, "elephant-db")

    def test_static_anchor_requires_every_immutable_input_and_no_future_output(self):
        expected_keys = {"format", "run_nonce", "source_commit", "proposal_sha256", "corpus_sha256",
            "allocation_sha256", "artifact_sha256", "paired_artifact_sha256", "runtime_sha256", "gateway_sha256",
            "bindings_sha256", "bridge_sha256", "context_helper_sha256", "command_sha256", "caps"}
        self.assertEqual(v.STATIC_ANCHOR_FIELDS, expected_keys)
        for key in expected_keys | {"manifest_sha256", "configuration_sha256", "input_inventory_sha256", "clock", "parent"}:
            review, raws, _ = static_fixture()
            anchor = review["review"]["controller_policy"]["expected_anchor"]
            if key in expected_keys:
                del anchor[key]
            else:
                anchor[key] = "0" * 64
            review["raw_bytes"] = original(review["review"])
            review["raw_sha256"] = hashlib.sha256(review["raw_bytes"]).hexdigest()
            with self.subTest(field=key), self.assertRaisesRegex(ValueError, "dynamic_controller_policy"):
                v.validate_static(review, raws, "elephant-db")

    def test_static_anchor_and_paired_original_executable_crossbinding(self):
        for mutation in ("paired-plan", "paired-anchor", "binary", "format", "cap", "cap-type", "allocation", "budget-type"):
            review, raws, plan = static_fixture()
            policy = review["review"]["controller_policy"]
            anchor = policy["expected_anchor"]
            if mutation == "paired-plan":
                plan["paired_artifact_sha256"] = "7" * 64
                review["review"]["bootstrap_plans"]["elephant-db"] = retain(original(plan), "bootstrap-plan", raws)
            elif mutation == "paired-anchor":
                anchor["paired_artifact_sha256"] = "7" * 64
            elif mutation == "binary":
                anchor["artifact_sha256"] = review["review"]["identities"]["elephant-db"]["artifact_sha256"]
            elif mutation == "format":
                anchor["format"] = "unreviewed"
            elif mutation == "cap":
                anchor["caps"]["physical_dispatches"] += 1
            elif mutation == "cap-type":
                anchor["caps"]["physical_dispatches"] = 2100.0
            elif mutation == "allocation":
                anchor["allocation_sha256"] = None
            else:
                policy["maximum_backend_seconds"] = 7800.0
            review["raw_bytes"] = original(review["review"])
            review["raw_sha256"] = hashlib.sha256(review["raw_bytes"]).hexdigest()
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                v.validate_static(review, raws, "elephant-db")
        review, raws, _ = static_fixture()
        policy = review["review"]["controller_policy"]
        policy["expected_anchor"].update(allocation_sha256=None,
            caps={"physical_dispatches": 432, "maximum_nano_usd": 75_000_000_000})
        policy["maximum_backend_seconds"] = 2700
        review["raw_bytes"] = original(review["review"])
        review["raw_sha256"] = hashlib.sha256(review["raw_bytes"]).hexdigest()
        self.assertEqual(v.validate_static(review, raws, "elephant-db")["backend"], "elephant-db")

    def test_static_original_classification_scope_and_helper_recipe(self):
        for mutate in (
                lambda plan: plan.update(service_scopes=["read:status"]),
                lambda plan: plan["service_scopes"].append("manage:grant"),
                lambda plan: plan["spaces"][0]["security"].update(level=True),
                lambda plan: plan["spaces"][0]["security"].update(level=7, policy_id="business-policy"),
                lambda plan: plan["spaces"][0]["processing"].update(index_generation=1),
                lambda plan: plan["spaces"][0]["processing"].update(generation=True),
                lambda plan: plan["spaces"][0]["processing"].update(bindings={}),
                lambda plan: plan.update(dependencies={"scripts/unreviewed.py": "6" * 64})):
            review, raws, plan = static_fixture()
            mutate(plan)
            review["review"]["bootstrap_plans"]["elephant-db"] = retain(original(plan), "bootstrap-plan", raws)
            review["raw_bytes"] = original(review["review"])
            review["raw_sha256"] = hashlib.sha256(review["raw_bytes"]).hexdigest()
            with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                v.validate_static(review, raws, "elephant-db")

    def test_original_config_creation_and_all_grant_cas_transitions(self):
        receipt, plan, captures = authority_fixture()
        v.startup_proofs(receipt, plan, captures)
        v.startup_authority(receipt, plan, captures)
        for mutation in ("create-body", "create-role", "revoke-cas", "grant-active", "grant-scope", "grant-generation", "readonly-body", "drop-check"):
            receipt, plan, captures = authority_fixture()
            creation = next(index for index, (request, _, _) in captures.items() if request["method"] == "POST")
            revoke = next(index for index, (request, _, _) in captures.items()
                          if request["method"] == "PUT" and type(request["body"]) is dict and request["body"].get("active") is False)
            if mutation == "create-body":
                captures[creation][0]["body"]["security"]["level"] = 2
            elif mutation == "create-role":
                captures[creation][0]["credential_role"] = "operator"
            elif mutation == "revoke-cas":
                captures[revoke][0]["body"]["expected_generation"] = None
            elif mutation in ("grant-active", "grant-scope", "grant-generation"):
                request, response, _ = captures[revoke]
                if mutation == "grant-active":
                    response["body"]["active"] = True
                elif mutation == "grant-scope":
                    response["body"]["scopes"].append("manage:grant")
                else:
                    response["body"]["generation"] = "service-created"
                captures[revoke] = request, response, original(response["body"])
            elif mutation == "readonly-body":
                captures[receipt["checks"]["read_only_write_denial"][0]][0]["body"]["revision"] = "another-attempt"
            else:
                receipt["checks"]["grant_readback"].pop()
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                v.startup_authority(receipt, plan, captures)

    def test_original_cli_role_scope_and_lifetime_for_both_backends(self):
        for backend in ("elephant-db", "surreal-db"):
            v.credential_commands(*credential_fixture(backend))
        for mutation in ("scope", "global", "lifetime", "environment", "missing", "extra"):
            commands, plan, receipt = credential_fixture()
            if mutation == "scope":
                commands[1]["command"][-1] = "write:source"
            elif mutation == "global":
                commands[3]["command"] += ["--global-admin"]
            elif mutation == "lifetime":
                commands[1]["command"][8] = "3601"
            elif mutation == "environment":
                commands[3]["environment_names"].append("SKIP_AUTH")
            elif mutation == "missing":
                del commands[2]
            else:
                commands[9] = copy.deepcopy(commands[1])
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                v.credential_commands(commands, plan, receipt)

    def test_worker_recipe_requires_same_unrestricted_actual_instance(self):
        recipe, captures = worker_fixture()
        plan = {"backend": "elephant-db"}
        self.assertEqual(v.effective_worker_recipe(captures, plan), recipe)
        for mutation in ("disabled", "allowed", "retry", "summary", "worker", "instance", "authority"):
            recipe, captures = worker_fixture()
            if mutation == "disabled":
                recipe["work_queue"]["disabled_kinds"] = ["render_doc"]
            elif mutation == "allowed":
                recipe["work_queue"]["allowed_kinds"] = ["render_doc"]
            elif mutation == "retry":
                recipe["work_queue"]["transient_retry_max_attempts"] = True
            elif mutation == "summary":
                recipe["work_queue"]["doc_tree"]["create_summaries"] = True
            elif mutation == "worker":
                recipe["embed_worker"]["enabled"] = False
            elif mutation == "instance":
                recipe["instance_id"] = "00000000-0000-0000-0000-000000000002"
            if mutation == "instance":
                captures.update(worker_fixture(recipe, 2)[1])
            else:
                captures = worker_fixture(recipe)[1]
            if mutation == "authority":
                captures[1][0]["credential_role"] = "service-qualification-household"
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                v.effective_worker_recipe(captures, plan)

    def test_original_isolation_carrier_is_bound_to_same_gateway(self):
        raw = original({"format": "recall-gateway-isolation/v1", "network_namespace": "net:[201]",
            "parent_network_namespace": "net:[200]", "interfaces": ["lo"], "ipv4_routes": 0, "provider_credentials_present": False})
        items = {}
        ref = retain(raw, "startup-isolation", items)
        evidence = v.Evidence(items)
        evidence.paths["/work/isolation.json"] = ref
        receipt = {"isolation_sha256": ref["sha256"]}
        context = {"network": {"namespace": "net:[201]", "parent_namespace": "net:[200]"}}
        v.original_isolation(receipt, context, evidence)
        context["network"]["namespace"] = "net:[202]"
        with self.assertRaisesRegex(ValueError, "dynamic_original_isolation"):
            v.original_isolation(receipt, context, evidence)

    def test_lazy_file_boundary_errors_never_disclose_original_path(self):
        review, raws, _ = static_fixture()
        class UnavailableOriginal(LazyOriginals):
            def __getitem__(self, key):
                raise OSError("private-location-must-not-escape")
        with self.assertRaisesRegex(ValueError, "^dynamic_invalid_evidence$"):
            v.validate_static(review, UnavailableOriginal(raws), "elephant-db")

    def test_complete_runtime_composes_actual_installed_predicate_and_original_custody(self):
        reviewed, observed, raws, wrapper = runtime_fixture()
        lazy = LazyOriginals(raws)
        component = v.validate_runtime(reviewed, observed, lazy, wrapper)
        self.assertEqual(component["format"], "recall-installed-runtime-component/v2")
        self.assertFalse(component["product_acceptance"])
        self.assertEqual(len(component["required_cases"]), 19)
        self.assertEqual(component["identity"]["artifact_sha256"], "d" * 64)
        self.assertEqual(component["identity"]["binary_sha256"], "b" * 64)
        self.assertIn("processes", component["remaining_cleanup_obligations"])
        self.assertGreater(len(lazy.lookups), 100)

    def test_complete_runtime_refuses_changed_raw_originals_and_current_custody(self):
        for mutate in ("installed-source", "current-birth", "whole-deadline", "worker-instance", "grant-cas", "isolation"):
            reviewed, observed, raws, wrapper = runtime_fixture()
            if mutate == "installed-source":
                carrier = json.loads(raws[observed["installed_offline"]["sha256"]])
                carrier["identity"]["source_commit"] = "0" * 40
                observed["installed_offline"] = retain(original(carrier), "installed-offline-evidence", raws)
            elif mutate == "current-birth":
                current = json.loads(raws[observed["current_processes"]["sha256"]])
                current["processes"]["recall"]["identity"]["start_ticks"] += 1
                observed["current_processes"] = retain(original(current), "current-processes", raws)
            elif mutate == "whole-deadline":
                receipt = json.loads(raws[observed["bootstrap"]["sha256"]])
                receipt["whole_deadline_monotonic_ns"] += 1
                observed["bootstrap"] = retain(original(receipt), "runtime-bootstrap", raws)
            else:
                inventory = json.loads(raws[observed["capture_inventory"]["sha256"]])
                for row in inventory["files"]:
                    ref = row["ref"]
                    if mutate == "isolation" and ref["kind"] == "startup-isolation":
                        raws[ref["sha256"]] += b" "
                        break
                    if ref["kind"] != "startup-request":
                        continue
                    request = json.loads(raws[ref["sha256"]])
                    body = request["body"]
                    if mutate == "worker-instance" and type(body) is dict and body.get("params", {}).get("name") == "get_runtime_config":
                        raws[ref["sha256"]] += b" "
                        break
                    if mutate == "grant-cas" and type(body) is dict and body.get("active") is False:
                        raws[ref["sha256"]] += b" "
                        break
                else:
                    self.fail("independent mutation did not select its original capture")
            with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                v.validate_runtime(reviewed, observed, raws, wrapper)

    def test_original_channel_composes_with_actual_provider_predicate(self):
        ref, bundle, raws, review, wrapper, identity, begin, close, paths, configs, observed, bounds, inputs = channel_fixture()
        lazy = LazyOriginals(raws)
        evidence = v.Evidence(lazy)
        evidence.paths.update(paths)
        index = v.accounting_channel(ref, bundle, evidence, review, wrapper["context"], identity, begin, close, inputs)
        result = provider_evidence_validator.validate_provider_accounting(bundle, lazy, identity, wrapper, configs, observed, bounds)
        self.assertEqual(index["reference"], bundle)
        self.assertEqual(result["begin"], begin)
        self.assertEqual(result["close"], close)
        self.assertTrue(all(item["ref"]["sha256"] in lazy.lookups for item in index["files"]))

    def test_complete_prepared_fixture_composes_original_runtime_publication_channel_and_provider(self):
        for backend in ("elephant-db", "surreal-db"):
            with self.subTest(backend=backend):
                reviewed, prepared, runtime, raws, wrapper = prepared_fixture(backend)
                result = v.validate_prepared(reviewed, prepared, runtime, LazyOriginals(raws), wrapper)
                self.assertEqual(result["format"], "recall-prepared-fixture-component/v2")
                self.assertEqual(result["identity"]["backend"], backend)
                self.assertEqual(result["source_count"], 50)
                self.assertEqual(result["sources_per_space"], {"qualification-household": 25, "qualification-business": 25})
                self.assertEqual(len(result["published_inventory"]), 50)
                self.assertFalse(result["product_acceptance"])
                self.assertEqual(result["remaining_cleanup_obligations"], runtime["remaining_cleanup_obligations"])
                self.assertEqual(set(result["remaining_cleanup_obligations"]["processes"]),
                                 {"recall"} if backend == "elephant-db" else {"recall", "paired"})
                # Repeat the same originals at the later premeasurement boundary.
                self.assertEqual(v.validate_prepared(reviewed, prepared, runtime, LazyOriginals(raws),
                                 {**wrapper, "observed_monotonic_ns": wrapper["observed_monotonic_ns"] + 1,
                                  "observed_wall": wrapper["observed_wall"] + 0.001}), result)

    def test_complete_prepared_fixture_refuses_changed_original_publication_or_channel(self):
        for mutation in ("missing-channel", "caller-original", "submission-original", "published-original", "current-worker"):
            reviewed, prepared, runtime, raws, wrapper = prepared_fixture()
            if mutation == "missing-channel":
                prepared.pop("provider_accounting_channel")
            elif mutation == "caller-original":
                channel = json.loads(raws[prepared["provider_accounting_channel"]["sha256"]])
                raws[channel["caller"]["sha256"]] += b" "
            elif mutation == "published-original":
                observation = json.loads(raws[prepared["observation"]["sha256"]])
                observation["published_inventory"][0]["source_sha256"] = "0" * 64
                prepared["observation"] = retain(original(observation), "preparation-observation", raws)
            elif mutation == "submission-original":
                intents = json.loads(raws[prepared["original_intents"]["sha256"]])
                raws[intents["requests"][52]["intent"]["sha256"]] += b" "
            else:
                chain = json.loads(raws[prepared["runtime_component"]["sha256"]])
                current = json.loads(raws[chain["current_custody"]["sha256"]])
                raws[current["worker_recipe_capture"]["response"]["sha256"]] += b" "
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                v.validate_prepared(reviewed, prepared, runtime, LazyOriginals(raws), wrapper)

    def test_current_recipe_refs_must_belong_to_current_inventory_and_actual_port(self):
        for role in ("worker", "qualification-household", "qualification-business"):
            for mutation in ("request-outside-current", "response-outside-current", "wrong-port", "bool-port"):
                reviewed, prepared, runtime, raws, wrapper = prepared_fixture()
                chain = json.loads(raws[prepared["runtime_component"]["sha256"]])
                current = json.loads(raws[chain["current_custody"]["sha256"]])
                pair = current["worker_recipe_capture"] if role == "worker" else current["provider_recipe_captures"][role]
                current_inventory = json.loads(raws[current["capture_inventory"]["sha256"]])
                if mutation.endswith("outside-current"):
                    ref = pair[mutation.split("-", 1)[0]]
                    removed = next(row for row in current_inventory["files"] if row["ref"] == ref)
                    current_inventory["files"].remove(removed)
                    # Keep the exact valid bytes AND path in the preparation
                    # inventory. The union still contains them, so only the
                    # independently required current membership can refuse.
                    prep_inventory = json.loads(raws[prepared["capture_inventory"]["sha256"]])
                    prep_inventory["files"].append(removed)
                    prepared["capture_inventory"] = retain(original(prep_inventory), "capture-inventory", raws)
                    expected_error = "dynamic_preparation_current_recipe_capture_path"
                else:
                    old_ref = pair["request"]
                    request = json.loads(raws[old_ref["sha256"]])
                    request["port"] = 43131 if mutation == "wrong-port" else True
                    pair["request"] = retain(original(request), "startup-request", raws)
                    for row in current_inventory["files"]:
                        if row["ref"] == old_ref:
                            row["ref"] = pair["request"]
                    expected_error = "dynamic_startup_original_port"
                current["capture_inventory"] = retain(original(current_inventory), "capture-inventory", raws)
                chain["current_custody"] = retain(original(current), "runtime-current-custody", raws)
                prepared["runtime_component"] = retain(original(chain), "runtime-validation-chain", raws)
                with self.subTest(role=role, mutation=mutation), self.assertRaisesRegex(ValueError, expected_error):
                    v.validate_prepared(reviewed, prepared, runtime, LazyOriginals(raws), wrapper)

    def test_prepared_current_outer_launch_cannot_borrow_or_replace_originals(self):
        for mutation in ("missing-wrapper", "missing-manifest", "missing-spec", "missing-input_inventory", "replacement-wrapper"):
            reviewed, prepared, runtime, raws, wrapper = prepared_fixture()
            chain = json.loads(raws[prepared["runtime_component"]["sha256"]])
            current = json.loads(raws[chain["current_custody"]["sha256"]])
            inventory = json.loads(raws[current["capture_inventory"]["sha256"]])
            outer = json.loads(raws[current["outer_launch"]["sha256"]])
            if mutation == "replacement-wrapper":
                current["outer_launch"] = retain(original(outer) + b"\n", "gateway-launch-inputs", raws)
                inventory["files"].append({"path": "/work/current/replaced-outer.json", "ref": current["outer_launch"]})
                error = "dynamic_outer_current_original_changed"
            else:
                member = mutation.removeprefix("missing-")
                reference = current["outer_launch"] if member == "wrapper" else outer[member]
                inventory["files"] = [row for row in inventory["files"] if row["ref"] != reference]
                # Original runtime inventory and all raw bytes remain available.
                error = "dynamic_outer_capture_membership"
            current["capture_inventory"] = retain(original(inventory), "capture-inventory", raws)
            chain["current_custody"] = retain(original(current), "runtime-current-custody", raws)
            prepared["runtime_component"] = retain(original(chain), "runtime-validation-chain", raws)
            with self.subTest(mutation=mutation), self.assertRaisesRegex(ValueError, "^" + error + "$"):
                v.validate_prepared(reviewed, prepared, runtime, LazyOriginals(raws), wrapper)

    def test_channel_caller_raw_reference_and_canonical_hash_are_distinct(self):
        ref, bundle, raws, review, wrapper, identity, begin, close, paths, _, _, _, inputs = channel_fixture()
        channel = json.loads(raws[ref["sha256"]])
        caller_ref = channel["caller"]
        channel["caller"] = retain(raws[caller_ref["sha256"]] + b"\n", "workload-phase-manifest", raws)
        self.assertNotEqual(channel["caller"]["sha256"], caller_ref["sha256"])
        paths["/work/preparation/channel-caller"] = channel["caller"]
        ref = retain(original(channel), "provider-accounting-channel", raws)
        paths["/work/preparation/channel.json"] = ref
        evidence = v.Evidence(LazyOriginals(raws))
        evidence.paths.update(paths)
        self.assertEqual(v.accounting_channel(ref, bundle, evidence, review, wrapper["context"], identity, begin, close, inputs)
                         ["caller_sha256"], caller_ref["sha256"])

    def test_channel_original_mount_flags_device_duplicate_and_shadow_refuse(self):
        directory = {"device": 2049}
        good = b"41 30 8:1 /original /gateway-accounting ro,nosuid - ext4 /dev/original rw\n"
        v.accounting_readonly_mount(good, directory)
        for bad in (good.replace(b"ro,nosuid", b"rw,nosuid"), good.replace(b"ro,nosuid", b"ro,rw"),
                    good.replace(b"8:1", b"8:2"), good + good,
                    good + good.replace(b"/gateway-accounting ", b"/gateway-accounting/child "),
                    good.replace(b"/gateway-accounting", b"/unrelated"), good.replace(b" - ext4", b" ext4")):
            with self.subTest(raw=bad), self.assertRaises(ValueError):
                v.accounting_readonly_mount(bad, directory)

    def test_channel_substituted_original_records_and_membership_refuse(self):
        for mutation in ("anchor", "helper", "inode", "uid-type", "permissions", "directory-path", "sidecar-only",
                         "index-begin", "index-cohort", "index-bundle", "missing-file", "duplicate-file", "file-kind",
                         "file-hash", "file-path", "not-in-inventory", "caller-hash", "caller-identity", "caller-input",
                         "caller-input-path", "caller-phase", "caller-credentials", "caller-authorization"):
            ref, bundle, raws, review, wrapper, identity, begin, close, paths, _, _, _, inputs = channel_fixture()
            channel = json.loads(raws[ref["sha256"]])
            sidecar = json.loads(raws[channel["sidecar"]["sha256"]])
            directory = json.loads(raws[channel["directory"]["sha256"]])
            index = json.loads(raws[channel["index"]["sha256"]])
            caller = json.loads(raws[channel["caller"]["sha256"]])
            if mutation == "anchor": channel["anchor_sha256"] = "0" * 64
            elif mutation == "helper": sidecar["helper_sha256"] = "0" * 64
            elif mutation == "inode": directory["inode"] += 1
            elif mutation == "uid-type": directory["uid"] = True
            elif mutation == "permissions":
                directory["mode"] = sidecar["directory"]["mode"] = 0o770
            elif mutation == "directory-path": directory["path"] += "/other"
            elif mutation == "sidecar-only": directory = sidecar["directory"]
            elif mutation == "index-begin": index["begin_sha256"] = "0" * 64
            elif mutation == "index-cohort": index["cohort_id"] = 3
            elif mutation == "index-bundle": index["reference"] = {**bundle, "sha256": "0" * 64}
            elif mutation == "missing-file": index["files"].pop()
            elif mutation == "duplicate-file": index["files"][-1] = copy.deepcopy(index["files"][0])
            elif mutation == "file-kind": index["files"][-1]["ref"]["kind"] = "unreviewed-provider-kind"
            elif mutation == "file-hash": index["files"][-1]["ref"]["sha256"] = "0" * 64
            elif mutation == "file-path": index["files"][-1]["path"] = "cohort-01/../secret"
            elif mutation == "caller-hash": index["caller_sha256"] = "0" * 64
            elif mutation == "caller-identity": caller["source_commit"] = "0" * 40
            elif mutation == "caller-input": caller["contract_sha256"] = "0" * 64
            elif mutation == "caller-input-path": caller["corpus_path"] = "/work/unrelated.json"
            elif mutation == "caller-phase": caller["phase"] = "measurement"
            elif mutation == "caller-credentials": caller["credential_files"]["service-qualification-household"] = "/work/foreign.token"
            elif mutation == "caller-authorization": caller["provider_dispatch_authorized"] = False
            if mutation.startswith("caller-") and mutation != "caller-hash":
                index["caller_sha256"] = hashlib.sha256(original(caller)).hexdigest()
            for key, value in (("sidecar", sidecar), ("directory", directory), ("index", index), ("caller", caller)):
                channel[key] = retain(original(value), channel[key]["kind"], raws)
                paths["/work/preparation/channel-" + key] = channel[key]
            ref = retain(original(channel), "provider-accounting-channel", raws)
            paths["/work/preparation/channel.json"] = ref
            if mutation == "not-in-inventory": paths.pop("/work/preparation/channel-directory")
            evidence = v.Evidence(LazyOriginals(raws))
            evidence.paths.update(paths)
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                v.accounting_channel(ref, bundle, evidence, review, wrapper["context"], identity, begin, close, inputs)

    def test_publication_requires_actual_original_lineage_and_full_inventory(self):
        value = v.public_publication(*fixture())
        self.assertEqual(value["revision"], CORPUS["sources"][0]["revision"])
        self.assertEqual(len(value["embeddable_node_ids"]), 2)
        self.assertTrue(value["physical_vector_inventory"].startswith("unmet:"))
        for mutate in (
                lambda rows: rows["source"].update(hash="0" * 64),
                lambda rows: rows["head"].update(state="pending"),
                lambda rows: rows["operation"].update(generation="new-generation"),
                lambda rows: rows["immutable_revision"].update(text_hash="0" * 64),
                lambda rows: rows["immutable_revision"].update(supersedes="doc_revision:older"),
                lambda rows: rows["nodes"][1].update(space_id="another-space"),
                lambda rows: rows["nodes"][1].update(subjects_state="complete"),
                lambda rows: rows["nodes"].pop(),
                lambda rows: rows["pipeline"].update(nodes_count=True),
                lambda rows: rows["pipeline"]["chunks"].update(embedded=1)):
            data = fixture()
            mutate(data[2])
            with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                v.public_publication(*data)

    def test_root_terminal_lf_cannot_expand_original_paragraph_or_chunk(self):
        data = fixture()
        data[2]["nodes"][1]["span"]["end"] += 1
        with self.assertRaisesRegex(ValueError, "dynamic_preparation_independent_span_inventory"):
            v.public_publication(*data)
        data = fixture()
        data[2]["nodes"][2]["span"]["end"] = True
        with self.assertRaises(ValueError):
            v.public_publication(*data)

    def test_all_fifty_independent_rendered_fixtures_have_exact_public_lineage(self):
        identities = set()
        for index in range(50):
            value = v.public_publication(*fixture(index))
            identities.add((value["space"], value["key"]))
        self.assertEqual(len(identities), 50)
        self.assertEqual(sum(space == "qualification-household" for space, _ in identities), 25)

    def test_only_storage_epoch_and_update_timestamp_can_change_configuration(self):
        before = {"id": "space_config:original", "updated_at": "2026-10-07T00:00:00Z",
                  "processing": {"generation": 7, "index_generation": 0, "bindings": {"document": "original"}}}
        after = copy.deepcopy(before)
        after["updated_at"] = "2026-10-07T00:01:00Z"
        after["processing"]["index_generation"] = 51
        self.assertEqual(v.stable_configuration(before), v.stable_configuration(after))
        after["processing"]["generation"] = 8
        self.assertNotEqual(v.stable_configuration(before), v.stable_configuration(after))
        after["processing"]["index_generation"] = True
        with self.assertRaises(ValueError):
            v.stable_configuration(after)

    def test_original_tool_json_and_sse_identity(self):
        result = {"jsonrpc": "2.0", "id": 13, "result": {"content": [{"type": "text", "text": '{"ok":true,"data":{"configured":true}}'}]}}
        raw = original(result)
        for body, media in ((raw, "application/json"), (b"event: message\ndata: " + raw + b"\n\n", "text/event-stream")):
            self.assertEqual(v.original_rpc(body, media, 13)[1], {"configured": True})
            with self.assertRaises(ValueError):
                v.original_rpc(body, media, True)
        with self.assertRaises(ValueError):
            v.original_rpc(b"data: " + raw + b"\n", "text/event-stream", 13)

    def test_fabricated_pass_summary_cannot_enter_either_public_component(self):
        review, raws, _ = static_fixture()
        for invoke in (lambda: v.validate_runtime(review, {"verdict": "passed"}, raws, {}),
                       lambda: v.validate_prepared(review, {"verdict": "passed"}, {"verdict": "passed"}, raws, {})):
            with self.assertRaisesRegex(ValueError, "^dynamic_"):
                invoke()


if __name__ == "__main__":
    unittest.main()
