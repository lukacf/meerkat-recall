"""Validate observed startup custody; never launch processes or grant authority.

D reviews immutable plans and producer source before output exists. C and G
produce observations later. Every consumer uses the same original deadline.
"""
from __future__ import annotations

import base64
import importlib.util
import os
from pathlib import Path
import stat
import time

from primary_provider_driver_contract import encode, require, sha

C_HELPER = "scripts/qualification/runtime_package_bootstrap_contract.py"
G_HELPER = "tests/recall_preview_semantics/gateway/launch_context.py"
OBSERVATION_CONTRACTS = {
    "runtime_receipt": {"format": "recall-runtime-bootstrap/v1",
                        "qualification_scope": "authenticated-clean-persistent-startup-only",
                        "qualified": False, "product_acceptance": False},
    "gateway_context": {"format": "recall-gateway-launch-context/v1"},
    "runtime_custody": {"format": "recall-runtime-custody/v2",
                        "runtime_evidence_format": "recall-dynamic-runtime-evidence/v2",
                        "runtime_component_scope": "installed-offline-and-current-runtime"},
}


def load_producer(root, relative, expected_hash):
    path = root / relative
    require(path.resolve() == path and path.is_file() and not path.is_symlink(),
            "reviewed producer helper changed")
    source = path.read_bytes()
    require(sha(source) == expected_hash, "reviewed producer helper changed")
    specification = importlib.util.spec_from_file_location("primary_" + path.stem, path)
    module = importlib.util.module_from_spec(specification)
    exec(compile(source, str(path), "exec", dont_inherit=True), module.__dict__)
    return module


def read_private(path, maximum=16777216):
    path = Path(path)
    require(path.is_absolute() and path.parent.resolve() == path.parent
            and path.is_relative_to("/work"), "observation outside private work mount")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(descriptor)
        require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid()
                and info.st_mode & 0o077 == 0 and info.st_size <= maximum,
                "observation is not a bounded private regular file")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            raw = stream.read(maximum + 1)
        require(len(raw) <= maximum, "observation grew beyond bound")
        return raw
    finally:
        os.close(descriptor)


def original_deadline(receipt, context, now_ns):
    clock = context["anchor"]["clock"]
    start = receipt["allocation_started_monotonic_ns"]
    deadline = receipt["original_deadline_monotonic_ns"]
    whole = receipt["whole_deadline_monotonic_ns"]
    # G seconds originate from its monotonic clock; C retains the original
    # integer allocation. Conversion tolerance is one microsecond, never time.
    require(abs(whole - int(clock["deadline_mono"] * 10**9)) <= 1000,
            "whole-run deadline differs from original gateway allocation")
    require(start >= int(clock["mono_start"] * 10**9) - 1000
            and start <= receipt["started_monotonic_ns"] <= receipt["observed_monotonic_ns"] <= now_ns < deadline <= whole
            and deadline - start <= 2700 * 10**9, "startup reset or exhausted original allocation")
    return deadline / 10**9


def captures(receipt, contract, reader=read_private):
    observations, total = {}, 0
    for reference in receipt["captures"]:
        request_raw = reader(reference["request_path"], 1048576)
        response_raw = reader(reference["response_path"], 1048576)
        total += len(request_raw) + len(response_raw)
        require(total <= contract.LIMITS["capture_bytes"], "startup capture total exceeded")
        require(sha(request_raw) == reference["request_sha256"]
                and sha(response_raw) == reference["response_sha256"], "original startup capture hash changed")
        request, response = contract.strict_json(request_raw), contract.strict_json(response_raw)
        index = reference["request_index"]
        require(request["request_index"] == response["request_index"] == index
                and request["harness_attempt"] == 1, "original startup request substituted")
        request_body = base64.b64decode(request["request_body_base64"], validate=True)
        response_body = base64.b64decode(response["raw_body_base64"], validate=True)
        require(len(request_body) <= contract.LIMITS["request_bytes"]
                and len(response_body) <= contract.LIMITS["response_bytes"]
                and sha(request_body) == request["body_sha256"]
                and sha(response_body) == response["body_sha256"]
                and request_body == (b"" if request["body"] is None else encode(request["body"])),
                "startup original wire body changed")
        require(receipt["started_monotonic_ns"] <= request["started_monotonic_ns"]
                <= response["finished_monotonic_ns"] <= receipt["observed_monotonic_ns"],
                "startup capture belongs to another clock allocation")
        # Incomplete readiness polls may remain in the raw journal. A referenced
        # proof must be a complete actual response with matching raw JSON bytes.
        observations[index] = (request, response, response_body)
    return observations


def check_proofs(receipt, plan, contract, observations):
    names = {row["name"] for row in plan["spaces"]}
    per_space = contract.CHECKS - {"unauthenticated_readiness", "ready"}
    denial = {"missing_grant", "wrong_space", "bootstrap_data_denial", "read_only_write_denial", "grant_revocation"}
    for check, indices in receipt["checks"].items():
        spaces = set()
        for index in indices:
            request, response, raw = observations[index]
            require(response["framing_complete"] is True and response["error_type"] is None
                    and request["port"] == 43130, "startup proof is suppressed, truncated or foreign")
            require(contract.strict_json(raw) == response["body"], "proof body differs from actual raw JSON response")
            name = request["space"]
            spaces.add(name)
            if check == "unauthenticated_readiness":
                require(request["credential_role"] is None and request["path"] == "/v1/readiness"
                        and response["status"] == 401, "unauthenticated readiness proof differs")
                continue
            if check == "ready":
                require(request["path"] == "/v1/readiness" and request["credential_role"] == "operator"
                        and response["status"] == 200 and response["body"].get("worker_ready") is True
                        and response["body"].get("semantic_ingest_ready") is True,
                        "actual worker readiness proof absent")
                continue
            require(name in names, "proof belongs to another named space")
            role = request["credential_role"]
            if check in denial:
                contract.denied(response, 403, "forbidden_space")
                if check == "wrong_space":
                    require(role in {"service-" + other for other in names - {name}}
                            and request["path"] == "/v1/spaces/" + name, "wrong-space proof has no crossed caller")
                else:
                    expected_role = ("bootstrap-" if check == "bootstrap_data_denial" else
                                     "read-only-" if check == "read_only_write_denial" else "service-") + name
                    require(role == expected_role and request["path"].startswith(f"/v1/spaces/{name}/sources"),
                            "denial proof used another authority or route")
                    require(request["method"] == ("PUT" if check == "read_only_write_denial" else "GET"),
                            "denial proof used another operation")
            else:
                require(response["status"] == 200 and request["method"] == "GET", "actual readback missing")
                if check in {"named_config_readback", "ordinary_config_readback", "grant_restoration"}:
                    require(request["path"] == "/v1/spaces/" + name
                            and role == ("bootstrap-" if check == "named_config_readback" else "service-") + name
                            and response["body"] == receipt["configurations"][name], "original configuration readback differs")
                elif check in {"ordinary_empty_inventory", "final_empty_inventory"}:
                    require(request["path"] == f"/v1/spaces/{name}/sources?limit=100" and role == "service-" + name
                            and response["body"].get("sources") == [] and response["body"].get("next_cursor") is None,
                            "original fresh inventory not empty")
                elif check == "grant_readback":
                    grant = response["body"]
                    require(role == "bootstrap-" + name and grant["space_id"] == name
                            and request["path"] == f"/v1/spaces/{name}/grants/" + grant["subject"]
                            and grant["subject"] in {"primary-service-" + name, "primary-read-only-" + name}
                            and bool(grant["generation"]), "original current grant readback differs")
        if check in per_space:
            require(spaces == names, "startup proof missing an original named space: " + check)


def startup_execution(receipt, plan, contract, observations, reader=read_private):
    """Join declared initialization to actual captured command/wire evidence."""
    reference = receipt["process_commands"]
    raw = reader(reference["path"], 262144)
    require(sha(raw) == reference["sha256"], "original process command journal changed")
    journal = contract.strict_json(raw)
    contract.fields(journal, {"format", "processes"})
    require(journal["format"] == "recall-runtime-process-commands/v1"
            and isinstance(journal["processes"], list)
            and 1 <= len(journal["processes"]) <= contract.LIMITS["processes"], "bounded actual startup commands absent")
    commands = {}
    for number, row in enumerate(journal["processes"], 1):
        contract.fields(row, {"index", "purpose", "command", "cwd", "environment_names", "logs",
                              "started_monotonic_ns", "pid", "exit_code", "log_sha256"})
        require(row["index"] == number and type(row["pid"]) is int and row["pid"] > 0
                and receipt["started_monotonic_ns"] <= row["started_monotonic_ns"] <= receipt["observed_monotonic_ns"]
                and isinstance(row["command"], list) and row["command"]
                and all(type(value) is str for value in row["command"])
                and isinstance(row["environment_names"], list)
                and row["environment_names"] == sorted(set(row["environment_names"])),
                "original startup command identity differs")
        require(Path(row["cwd"]).is_relative_to(plan["run_directory"])
                and row["logs"] == [plan["run_directory"] + f"/logs/{number:02}.{suffix}"
                                    for suffix in ("stdout", "stderr")], "original startup paths changed")
        if row["exit_code"] is None:
            require(row["log_sha256"] is None, "live service logs are not final evidence")
        else:
            require(type(row["exit_code"]) is int and row["exit_code"] == 0
                    and isinstance(row["log_sha256"], dict) and set(row["log_sha256"]) == set(row["logs"]),
                    "startup command failed or final logs absent")
            for path in row["logs"]:
                require(sha(reader(path, contract.LIMITS["log_bytes_per_stream"])) == row["log_sha256"][path],
                        "completed startup command original log bytes changed")
        commands[number] = row
    for name, process in receipt["processes"].items():
        matched = [row for row in commands.values() if row["pid"] == process["pid"]]
        require(len(matched) == 1, "original service command not unique")
        row = matched[0]
        expected = (["/opt/recall/bin/elephant", "serve", "--server-only", "--shutdown-timeout-seconds", "60"]
                    if name == "recall" else ["/opt/recall/bin/recall-surreal", "start", "--bind",
                                             "127.0.0.1:43131", "file:" + plan["run_directory"] + "/paired/database.db"])
        require(row["exit_code"] is None and row["command"] == expected
                and row["purpose"] == ("recall-authenticated-service" if name == "recall" else "paired-persistent-server"),
                "service adopted a different executable or command")
    operation = receipt["schema_operation"]
    if plan["backend"] == "surreal-db":
        row = commands.get(operation["process_index"])
        require(row is not None and row["purpose"] == "explicit-fresh-schema"
                and row["command"] == ["/opt/recall/bin/elephant", "migrate"] and row["exit_code"] == 0,
                "declared schema migration lacks original successful command")
    else:
        from primary_provider_driver_transport import sse_rpc
        request, response, body = observations[operation["capture_index"]]
        expected_arguments = {"backend": "elephant-db", "snapshot_path": plan["run_directory"] + "/service/state.snapshot",
                              "ingest_enabled": False, "extractor_required": False,
                              "multimodal_asset_root": plan["run_directory"] + "/media"}
        require(request["method"] == "POST" and request["path"] == "/mcp/full" and request["port"] == 43130
                and request["credential_role"] == "operator" and request["space"] is None
                and request["body"]["method"] == "tools/call"
                and request["body"]["params"] == {"name": "initialize_runtime", "arguments": expected_arguments}
                and response["status"] == 200 and response["framing_complete"] is True and response["error_type"] is None,
                "declared initialization lacks original successful operator call")
        media = response.get("response_headers", {}).get("content-type", [])
        reply = (sse_rpc(body, contract.strict_json, request["body"]["id"])
                 if len(media) == 1 and media[0].split(";", 1)[0].strip() == "text/event-stream"
                 else contract.strict_json(body))
        require(reply == response["body"] and reply.get("jsonrpc") == "2.0"
                and reply.get("id") == request["body"]["id"] and "error" not in reply,
                "initialization reply differs from actual original wire result")
        result = reply["result"]
        texts = [entry["text"] for entry in result.get("content", []) if entry.get("type") == "text"]
        require(result.get("isError") is not True and len(texts) == 1, "initialization tool result is unsuccessful")
        value = contract.strict_json(texts[0].encode())
        if value.get("ok") is True:
            value = value["data"]
        require(value.get("configured") is True and value.get("storage_ready") is True
                and value.get("backend") == "elephant-db", "actual initialized storage state missing")


def live_ancestry(receipt, context, gateway):
    bootstrap = receipt["bootstrap_process"]
    require(gateway.process_identity(bootstrap["pid"]) == bootstrap
            and bootstrap["ppid"] == context["process"]["pid"]
            and os.getppid() == bootstrap["pid"], "collector is not the original live bootstrap child")
    for service in receipt["processes"].values():
        actual = gateway.process_identity(service["pid"])
        require(actual == {"pid": service["pid"], "ppid": bootstrap["pid"], "pgrp": service["process_group"],
                           "session": service["session"], "start_ticks": service["start_ticks"]}
                and service["ppid"] == bootstrap["pid"]
                and os.readlink(f'/proc/{service["pid"]}/exe') == service["executable"],
                "original service process or parent changed")


def validate_observations(root, manifest, loaded, review):
    require(review["observation_contracts"] == OBSERVATION_CONTRACTS, "dynamic producer contracts unreviewed")
    contract = load_producer(root, C_HELPER, review["dependencies"][C_HELPER])
    gateway = load_producer(root, G_HELPER, review["dependencies"][G_HELPER])
    plan = contract.validate_plan(loaded["bootstrap_plan"])
    receipt = contract.validate_receipt(loaded["runtime_receipt"], plan)
    for key in contract.IDENTITIES:
        require(receipt[key] == manifest["nonce" if key == "run_nonce" else key], "actual startup identity changed: " + key)
    require(plan["dependencies"] == review["dependencies"], "startup producer source identity changed")
    expected = review["expected_launch"]
    require(set(expected) == (gateway.HASH_FIELDS - {"manifest_sha256", "configuration_sha256"})
            | {"source_commit", "run_nonce", "allocation_sha256"}
            and expected["allocation_sha256"] is None, "complete reviewed original launch identities required")
    for key in ("source_commit", "artifact_sha256", "paired_artifact_sha256", "runtime_sha256"):
        require(expected[key] == manifest[key], "review and startup launch identity disagree")
    require(expected["run_nonce"] == manifest["nonce"], "launch run nonce changed")
    reference = manifest["observations"]["gateway_context"]
    context = gateway.read_context(reference["path"], reference["sha256"], expected=expected)
    require(context["anchor"]["configuration_sha256"] == manifest["configuration_sha256"]
            and context["anchor"]["input_inventory_sha256"] is not None,
            "actual outer launch or immutable input mount missing")
    require(context == loaded["gateway_context"] and receipt["launch_context_sha256"] == reference["sha256"],
            "startup and collector observed different original launches")
    require(receipt["plan_sha256"] == manifest["inputs"]["bootstrap_plan"]["sha256"], "startup used another original plan")
    require(receipt["token_files"] == manifest["token_files"], "ordinary credentials substituted after startup")
    require(sha(read_private("/work/isolation.json", 131072)) == receipt["isolation_sha256"],
            "original startup isolation capture changed")
    require(sha(read_private(plan["run_directory"] + "/service/.elephant/elephant_config.toml", 1048576))
            == receipt["runtime_config_sha256"], "retained runtime configuration changed after startup")
    for request in plan["spaces"]:
        name = request["name"]
        require(loaded["configurations"][name]["create_request"] == request, "reviewed named-space request changed")
        contract.validate_config(request, receipt["configurations"][name], "primary-bootstrap-" + name)
        loaded["configurations"][name]["configuration"] = receipt["configurations"][name]
    proof = captures(receipt, contract)
    check_proofs(receipt, plan, contract, proof)
    startup_execution(receipt, plan, contract, proof)
    live_ancestry(receipt, context, gateway)
    return original_deadline(receipt, context, time.monotonic_ns())


def revalidate_current_observations(root, manifest, loaded, review):
    """Evidence loading consumes time; recheck the original live chain afterward."""
    gateway = load_producer(root, G_HELPER, review["dependencies"][G_HELPER])
    reference = manifest["observations"]["gateway_context"]
    context = gateway.read_context(reference["path"], reference["sha256"], expected=review["expected_launch"])
    require(context == loaded["gateway_context"], "original controller changed during evidence validation")
    live_ancestry(loaded["runtime_receipt"], context, gateway)
    deadline = original_deadline(loaded["runtime_receipt"], context, time.monotonic_ns())
    require(deadline == loaded["original_deadline"], "runtime validation replaced original allocation")
