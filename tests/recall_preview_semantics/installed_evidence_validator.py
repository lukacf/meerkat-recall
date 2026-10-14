"""Pure replay of original installed Linux evidence; no producer or I/O authority.

Case names and producer verdict strings are never evidence. Callers supply
independently reviewed static inputs and a map of original SHA256-bound bytes.
"""
from __future__ import annotations

import base64
from collections.abc import Mapping
import datetime as dt
import hashlib
import json
import math
from pathlib import PurePosixPath
import re
import tomllib
from urllib.parse import quote

CASE_SHA256 = "c791d9cdf2293e83ff393c8ece8811878c9d4cc915589d4ced2ff8b5cc890719"
CASES = ["verified_installed_binary", "absent_service", "bootstrap_unready", "fresh_cli_auth",
         "runtime_initialized_without_workers", "two_named_spaces", "http_auth_denied",
         "mcp_auth_denied", "wrong_space_denied", "mcp_positive", "grant_revocation",
         "source_acceptance_and_idempotence", "pending_is_not_complete", "input_bounds",
         "named_export", "replacement_accepted", "restore_original_heads", "graceful_restart",
         "read_only_capabilities"]
IDENTITY = {"source_commit", "artifact_sha256", "binary_sha256", "backend"}
# Installed plans retain their original role-named pins. Runtime reviews use
# canonical repository paths so the orchestrator can verify the same source bytes.
INSTALLED_HELPER_PATHS = {
    "driver": "scripts/qualification/installed_linux_offline.py",
    "process_capture": "scripts/qualification/installed_process_capture.py",
    "collector": "scripts/qualification/recall_preview.py",
    "installer": "scripts/release/artifact.py",
}
REFERENCES = {"reviewed_plan", "artifact_manifest", "observation", "capture_inventory",
              "command_inventory", "controller", "terminal_cleanup"}
DATA_SCOPES = ["read:status", "read:space_config", "write:space_config", "read:source", "write:source",
               "read:doc:raw", "read:doc:metadata", "read:entity", "manage:grant", "manage:provider"]
INSPECTION_STREAMS = (("snapshot-inspection", 32 * 1024**2 + 1), ("process-log", 1024**2))


def require(condition, suffix):
    if not condition:
        raise ValueError("dynamic_installed_" + suffix)


def encode(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False,
                      separators=(",", ":")).encode()


def same(left, right):
    return type(left) is type(right) and encode(left) == encode(right)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def integer(value, minimum=0):
    return type(value) is int and value >= minimum


def fields(value, names, suffix):
    require(type(value) is dict and set(value) == set(names), suffix)
    return value


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "duplicate_json_key")
            result[key] = value
        return result
    require(type(raw) is bytes, "original_bytes")
    try:
        result = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs)
        encode(result)
        return result
    except (ValueError, TypeError, UnicodeError, RecursionError) as error:
        raise ValueError("dynamic_installed_invalid_json") from error


def checksum(value, length=64):
    require(type(value) is str and re.fullmatch("[0-9a-f]{" + str(length) + "}", value), "checksum")
    return value


def private_path(value):
    require(type(value) is str and value.startswith("/") and "\x00" not in value
            and str(PurePosixPath(value)) == value and ".." not in PurePosixPath(value).parts,
            "original_path")
    return value


class Evidence:
    def __init__(self, raw):
        require(isinstance(raw, Mapping), "raw_map")
        self.raw = raw
        self.paths = {}

    def read(self, reference, kind=None, maximum=8 * 1024**2):
        fields(reference, {"sha256", "bytes", "kind"}, "reference_fields")
        checksum(reference["sha256"])
        require(integer(reference["bytes"]) and reference["bytes"] <= maximum
                and type(reference["kind"]) is str and reference["kind"]
                and (kind is None or reference["kind"] == kind), "reference_bound_kind")
        raw = self.raw.get(reference["sha256"])
        require(type(raw) is bytes and len(raw) == reference["bytes"]
                and digest(raw) == reference["sha256"], "original_hash_length")
        return raw

    def json(self, reference, kind=None, maximum=8 * 1024**2):
        return strict_json(self.read(reference, kind, maximum))

    def original(self, wrapped, kind=None, maximum=8 * 1024**2):
        fields(wrapped, {"path", "ref"}, "original_reference")
        path = private_path(wrapped["path"])
        require(path in self.paths and same(self.paths[path], wrapped["ref"]), "inventory_membership")
        return self.read(wrapped["ref"], kind, maximum)

    def inventory(self, reference):
        value = fields(self.json(reference, "capture-inventory"), {"format", "files"}, "inventory_fields")
        require(value["format"] == "recall-dynamic-capture-inventory/v2"
                and type(value["files"]) is list and 0 < len(value["files"]) <= 16384, "inventory_format")
        total = 0
        for row in value["files"]:
            fields(row, {"path", "ref"}, "inventory_row")
            path = private_path(row["path"])
            require(path not in self.paths, "duplicate_inventory_path")
            self.read(row["ref"])
            self.paths[path] = row["ref"]
            total += row["ref"]["bytes"]
            require(total <= 100663296, "installed_capture_bound")


def process_stat(raw):
    try:
        before, after = raw.decode("ascii").rsplit(") ", 1)
        parts = after.split()
        require(len(parts) >= 20, "process_stat_length")
        result = {"pid": int(before.split(" (", 1)[0]), "ppid": int(parts[1]),
                  "pgrp": int(parts[2]), "session": int(parts[3]), "start_ticks": int(parts[19])}
        require(all(integer(value, 0 if key == "ppid" else 1) for key, value in result.items()), "process_identity")
        return result, parts[0]
    except (UnicodeError, ValueError, IndexError) as error:
        raise ValueError("dynamic_installed_process_stat") from error


def controller(reference, evidence, observation):
    value = evidence.json(reference, "installed-controller")
    require(value.get("format") == "recall-installed-controller/v2", "controller_format")
    captures = value["captures"]
    before, state = process_stat(evidence.original(captures["before"], "process-stat", 8192))
    after, after_state = process_stat(evidence.original(captures["after"], "process-stat", 8192))
    require(same(before, after) and same(before, value["identity"])
            and state not in {"Z", "X", "x"} and after_state not in {"Z", "X", "x"}, "controller_identity")
    require(integer(value["observed_ns"], 1) and integer(value["deadline_ns"], 1)
            and value["observed_ns"] < value["deadline_ns"]
            and value["deadline_ns"] - value["observed_ns"] <= 300_000_000_000, "controller_clock")
    isolation = observation["isolation"]
    require(re.fullmatch(r"net:\[\d+\]", value["network_namespace"])
            and value["network_namespace"] == isolation["network_namespace"]
            and re.fullmatch(r"net:\[\d+\]", isolation["parent_network_namespace"])
            and isolation["parent_network_namespace"] != value["network_namespace"], "controller_namespace")
    dev = evidence.original(captures["network_dev"], "network-dev", 65536).decode("ascii")
    require({line.split(":", 1)[0].strip() for line in dev.splitlines() if ":" in line} == {"lo"}, "controller_interfaces")
    routes = evidence.original(captures["network_ipv4"], "network-route", 65536).decode("ascii").splitlines()
    require(len(routes) == 1 and routes[0].split()[0] == "Iface", "controller_ipv4_routes")
    ipv6 = evidence.original(captures["network_ipv6"], "network-ipv6-route", 65536).decode("ascii").splitlines()
    require(all(line.split()[-1] == "lo" for line in ipv6), "controller_ipv6_routes")
    boot = evidence.original(captures["boot_id"], "boot-id", 128).decode("ascii").strip()
    require(re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", boot), "controller_boot")
    return value


def original_allocation_clock(owner, run):
    expiry = dt.datetime.fromisoformat(run["expires_at"].replace("Z", "+00:00"))
    require(expiry.tzinfo is not None and expiry.utcoffset() is not None, "expiry_timezone")
    delta = expiry.astimezone(dt.timezone.utc) - dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
    expires_ns = (delta.days * 86400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1000
    clock = fields(owner["allocation_clock"], {"mono_before_ns", "wall_ns", "mono_after_ns", "expires_wall_ns", "total_seconds"}, "allocation_clock_fields")
    require(all(integer(value, 1) for value in clock.values()) and clock["total_seconds"] == 300
            and clock["mono_before_ns"] <= clock["mono_after_ns"] <= owner["observed_ns"]
            and clock["expires_wall_ns"] == expires_ns and clock["wall_ns"] < expires_ns, "original_allocation_expiry")
    expected_deadline = clock["mono_before_ns"] + min(300_000_000_000, expires_ns - clock["wall_ns"])
    require(owner["deadline_ns"] == expected_deadline and clock["mono_after_ns"] < expected_deadline,
            "original_allocation_deadline")
    before, after, wall = (owner[key] for key in ("wall_sample_before_ns", "wall_sample_after_ns", "wall_time_ns"))
    require(all(integer(value, 1) for value in (before, after, wall))
            and clock["mono_after_ns"] <= before <= after == owner["observed_ns"] < expected_deadline
            and wall < expires_ns, "controller_wall_bracket")


def process_streams(intent, ordinary_limit, *, allow_inspection=False):
    """Independently recognize F's sole explicit pre-spawn stream exception."""
    if "streams" not in intent:
        return (("process-log", ordinary_limit),) * 2
    require(allow_inspection, "installed_stream_extension")
    fields(intent, {"argv", "cwd", "environment_names", "started_ns", "deadline_ns", "log_paths", "streams"},
           "inspection_intent_fields")
    argv = intent["argv"]
    require(type(argv) is list and len(argv) == 8 and all(type(arg) is str for arg in argv)
            and argv[1:4] == ["snapshot", "inspect", "--input"]
            and argv[5:] == ["--json", "--include-records", "--compact-records"], "inspection_command")
    private_path(argv[4])
    paths, streams = intent["log_paths"], intent["streams"]
    require(type(paths) is list and len(paths) == 2 and paths[0] != paths[1]
            and type(streams) is list and len(streams) == 2, "inspection_stream_inventory")
    for path, stream, (kind, limit) in zip(paths, streams, INSPECTION_STREAMS):
        private_path(path)
        fields(stream, {"path", "kind", "maximum_bytes", "reserved_bytes"}, "inspection_stream_fields")
        require(same(stream, {"path": path, "kind": kind, "maximum_bytes": limit, "reserved_bytes": limit}),
                "inspection_original_stream_reservation")
    return INSPECTION_STREAMS


def _validate_process(row, evidence, owner, binary_sha, require_terminal, sequences, births,
                      log_limit=1024**2, allow_inspection=False):
    require(type(row) is dict and integer(row["index"], 1) and not row["errors"], "process_snapshot")
    index = row["index"]
    if require_terminal:
        require(row["reaped"] is True and type(row["exit_code"]) is int and row["exit_code"] == 0, "process_terminal")
    else:
        require(row["reaped"] is False and row["exit_code"] is None and row["reaped_ns"] is None
                and row["exit_observed"] is None, "process_live_terminal_absent")
    intent = row["intent"]
    streams = process_streams(intent, log_limit, allow_inspection=allow_inspection)
    require(type(intent["argv"]) is list and intent["argv"]
            and all(type(arg) is str for arg in intent["argv"])
            and integer(intent["started_ns"], 1) and integer(intent["deadline_ns"], 1)
            and owner["observed_ns"] <= intent["started_ns"] < intent["deadline_ns"]
            and intent["deadline_ns"] <= owner["deadline_ns"], "process_intent_clock")
    private_path(intent["cwd"])
    require(type(intent["environment_names"]) is list
            and intent["environment_names"] == sorted(set(intent["environment_names"]))
            and "SKIP_AUTH" not in intent["environment_names"], "process_environment")
    binary = row["launch_binary"]
    require(binary["path"] == intent["argv"][0] and binary["sha256"] == binary_sha
            and integer(binary["bytes"], 1), "process_binary")
    events = row["events"]
    require(type(events) is list and 0 < len(events) <= 128, "process_event_bound")
    actions = {}
    previous = intent["started_ns"]
    for event in events:
        require(integer(event["observed_ns"], 1) and previous <= event["observed_ns"] <= owner["deadline_ns"], "process_event_clock")
        previous = event["observed_ns"]
        captured = strict_json(evidence.original(event["capture"], "installed-process-event"))
        require(captured.get("format") == "recall-installed-process-event/v2"
                and captured["process"] == index and captured["action"] == event["action"]
                and same(captured["data"], event["data"])
                and event["observed_ns"] <= captured["observed_ns"] <= owner["deadline_ns"]
                and integer(captured["sequence"], 1) and captured["sequence"] not in sequences,
                "original_process_event")
        sequences.add(captured["sequence"])
        actions.setdefault(event["action"], []).append(event)
    required_actions = ("intent", "launch-binary", "popen-requested", "launched") + (("exit-observed", "reaped") if require_terminal else ())
    for action in required_actions:
        require(len(actions.get(action, [])) == 1, "process_required_event")
    require(same(actions["intent"][0]["data"], intent)
            and same(actions["launch-binary"][0]["data"], binary), "process_original_intent")
    if "streams" in intent:
        order = [event["action"] for event in events]
        require(order.index("intent") < order.index("launch-binary") < order.index("popen-requested")
                < order.index("launched"), "inspection_pre_spawn_intent")
    launched = actions["launched"][0]["data"]
    before, _ = process_stat(evidence.original(launched["stat"]["before"], "process-stat", 8192))
    after, state = process_stat(evidence.original(launched["stat"]["after"], "process-stat", 8192))
    require(same(before, after) and same(before, row["identity"]) and same(before, launched["identity"])
            and before["ppid"] == owner["identity"]["pid"]
            and before["pid"] == before["pgrp"] == before["session"], "original_child_birth")
    birth = (before["pid"], before["start_ticks"])
    require(birth not in births, "duplicate_child_birth")
    births.add(birth)
    executable = launched["executable"]
    if executable is None:
        require(state == "Z" and launched["executable_error"] == "FileNotFoundError", "missing_live_executable")
    else:
        require(executable["target"] == binary["path"] and all(executable[key] == binary[key]
            for key in ("sha256", "device", "inode", "bytes", "mode", "mtime_ns", "ctime_ns")), "actual_child_executable")
    if require_terminal:
        exited = actions["exit-observed"][0]
        reaped = actions["reaped"][0]
        require(same(exited["data"], row["exit_observed"]) and exited["data"]["code"] == 0
                and reaped["data"]["code"] == 0 and reaped["data"]["reaped_ns"] == row["reaped_ns"]
                and exited["observed_ns"] <= reaped["observed_ns"], "exit_before_reap")
        for event in actions.get("signal", []):
            signal = event["data"]
            require(signal["signal"] in (9, 15) and signal["outcome"] in {"requested", "sent", "group-absent"}, "signal_shape")
            require(event["observed_ns"] <= reaped["observed_ns"], "signal_after_reap")
            if signal["signal"] == 9 and signal["outcome"] != "group-absent":
                require(signal["phase"] == "post-exit-descendant-cleanup"
                        and event["observed_ns"] >= exited["observed_ns"], "forced_live_termination")
    else:
        require(state not in {"Z", "X", "x"} and executable is not None
                and not any(actions.get(action) for action in ("exit-observed", "reaped", "signal")), "process_live_state")
    require(len(row["logs"]) == 2 and [log["path"] for log in row["logs"]] == intent["log_paths"], "process_log_identity")
    for log, (kind, maximum) in zip(row["logs"], streams):
        if require_terminal:
            require(log["complete"] is True and log["closure"]["eof"] is True
                    and log["closure"]["error_type"] is None, "process_log_incomplete")
            evidence.original({"path": log["path"], "ref": log["ref"]}, kind, maximum)
            require(any(event["data"] == {"path": log["path"], **log["closure"]}
                        for event in actions.get("log-closed", [])), "original_log_closure")
        else:
            require(log["complete"] is False and log["ref"] is None and log["closure"] is None,
                    "live_log_not_terminal")
    return {"identity": dict(row["identity"]), "require_terminal": require_terminal,
            "argv": list(intent["argv"]), "started_ns": intent["started_ns"], "reaped_ns": row["reaped_ns"]}


def process_journal(reference, evidence, owner, binary_sha):
    journal = evidence.json(reference, "installed-process-journal")
    require(journal.get("format") == "recall-installed-process-journal/v2", "journal_format")
    rows = journal["processes"]
    require(type(rows) is list and 0 < len(rows) <= 16 and journal["attempted_processes"] == len(rows), "journal_count")
    sequences = set()
    births = set()
    for index, row in enumerate(rows, 1):
        require(row["index"] == index, "process_index")
        _validate_process(row, evidence, owner, binary_sha, True, sequences, births)
    captured_sequences = set()
    case_events = []
    for reference in evidence.paths.values():
        if reference["kind"] != "installed-process-event":
            continue
        event = evidence.json(reference, "installed-process-event")
        require(event.get("format") == "recall-installed-process-event/v2"
                and integer(event["sequence"], 1) and event["sequence"] not in captured_sequences,
                "duplicate_original_event")
        captured_sequences.add(event["sequence"])
        if event["process"] is None:
            require(event["action"] == "case-observed" and event["data"]["observed_ns"] <= event["observed_ns"] <= owner["deadline_ns"], "original_case_event")
            case_events.append(event)
        else:
            require(event["sequence"] in sequences, "omitted_original_process_event")
    require(len(captured_sequences) <= 512 and captured_sequences == set(range(1, len(captured_sequences) + 1))
            and same([event["data"] for event in sorted(case_events, key=lambda event: event["sequence"])], journal["case_proofs"]), "complete_original_event_journal")
    require(integer(journal["terminal_observed_ns"], 1)
            and journal["terminal_observed_ns"] <= owner["deadline_ns"], "journal_terminal_clock")
    return journal


def rpc_body(raw, request):
    try:
        value = strict_json(raw)
    except ValueError:
        normalized = raw.replace(b"\r\n", b"\n")
        require(normalized.endswith(b"\n\n"), "sse_incomplete")
        candidates = []
        for event in normalized.split(b"\n\n"):
            lines = [line[5:].lstrip(b" ") for line in event.split(b"\n") if line.startswith(b"data:")]
            if lines:
                candidates.append(strict_json(b"\n".join(lines)))
        matches = [value for value in candidates if type(value) is dict and value.get("id") == request.get("id")]
        require(len(matches) == 1, "sse_response_identity")
        value = matches[0]
    require(type(value) is dict and set(value) == {"jsonrpc", "id", "result"} and value.get("jsonrpc") == "2.0"
            and type(value.get("id")) is int and type(request.get("id")) is int
            and value["id"] == request["id"] and "error" not in value, "rpc_protocol_identity")
    return value["result"]


def tool_body(row, *, error=False):
    result = row["body"]
    require(type(result) is dict and (result.get("isError") is True) == error, "tool_outcome")
    blocks = result.get("content")
    require(type(blocks) is list and len(blocks) == 1 and blocks[0].get("type") == "text"
            and type(blocks[0].get("text")) is str, "tool_content")
    value = strict_json(blocks[0]["text"].encode())
    if type(value) is dict and value.get("ok") is True:
        return value["data"]
    return value


class Traffic:
    def __init__(self, evidence, journal, owner):
        self.rows = {}
        for path, reference in evidence.paths.items():
            match = re.search(r"/captures/http-([0-9]{3})\.json$", path)
            if not match:
                continue
            index = int(match[1])
            require(index not in self.rows, "duplicate_http_index")
            row = evidence.json(reference, "installed-http", 2 * 1024**2)
            require(type(row) is dict and row.get("method") in {"GET", "POST", "PUT"}
                    and type(row.get("path")) is str and row["path"].startswith("/"), "http_request")
            try:
                raw = base64.b64decode(row["raw_body_base64"], validate=True)
            except (ValueError, TypeError) as error:
                raise ValueError("dynamic_installed_http_base64") from error
            require(len(raw) <= 262144 and digest(raw) == row["body_sha256"], "http_body_identity")
            start, end, deadline = (row.get(key) for key in
                ("started_monotonic_ns", "finished_monotonic_ns", "deadline_monotonic_ns"))
            cap = 120 if row["path"].startswith("/v1/system/snapshot/") else 5
            require(all(integer(value, 1) for value in (start, end, deadline))
                    and owner["observed_ns"] <= start <= end <= deadline <= owner["deadline_ns"]
                    and deadline - start <= cap * 1_000_000_000
                    and type(row["duration_ms"]) in (int, float) and math.isfinite(row["duration_ms"])
                    and 0 <= row["duration_ms"] <= cap * 1000, "http_clock")
            if row.get("transport_error_type") is not None:
                require(row["transport_error_type"] in {"ConnectionRefusedError", "ConnectionResetError"} and row["method"] == "GET"
                        and row["path"] == "/v1/health" and row["credential_role"] is None
                        and row["space"] is None and row["request"] is None and row["raw_body_complete"] is False,
                        "http_transport_failure")
                if row["transport_error_type"] == "ConnectionRefusedError":
                    require(row["status"] is None and raw == b"", "refused_health_framing")
                body = None
            else:
                require(row["raw_body_complete"] is True and "capture_error" not in row
                        and type(row["status"]) is int and 100 <= row["status"] <= 599, "http_framing")
                body = rpc_body(raw, row["request"]) if row["path"] == "/mcp/full" and row["status"] == 200 else strict_json(raw)
            self.rows[index] = {**row, "body": body, "index": index}
        require(0 < len(self.rows) <= 128 and sorted(self.rows) == list(range(1, len(self.rows) + 1)), "http_inventory")
        self.groups = {}
        used = set()
        previous = owner["observed_ns"]
        for proof in journal["case_proofs"]:
            fields(proof, {"case", "observed_ns", "request_indices", "process_indices"}, "case_proof_fields")
            case = proof["case"]
            require(case in CASES and case not in self.groups and integer(proof["observed_ns"], 1)
                    and previous <= proof["observed_ns"] <= owner["deadline_ns"], "case_original_order")
            previous = proof["observed_ns"]
            indices = proof["request_indices"]
            require(type(indices) is list and all(type(index) is int and index in self.rows for index in indices)
                    and indices == sorted(set(indices)) and not used.intersection(indices), "case_original_requests")
            require(all(self.rows[index]["finished_monotonic_ns"] <= proof["observed_ns"] for index in indices), "case_before_response")
            used.update(indices)
            self.groups[case] = [self.rows[index] for index in indices]
        require(set(self.groups) == set(CASES) and used == set(self.rows), "case_request_coverage")

    def matches(self, case, method, path, role, space, request=None):
        return [row for row in self.groups[case] if row["method"] == method and row["path"] == path
                and row["credential_role"] == role and row["space"] == space
                and (request is None or same(row["request"], request))]

    def one(self, case, method, path, role, space, request=None):
        rows = self.matches(case, method, path, role, space, request)
        require(len(rows) == 1, "case_missing_or_duplicate_response")
        return rows[0]

    def tool(self, case, name, role="operator", space=None):
        rows = [row for row in self.groups[case] if row["method"] == "POST" and row["path"] == "/mcp/full"
                and row["credential_role"] == role and row["space"] == space and row["status"] == 200
                and row["request"].get("method") == "tools/call"
                and row["request"].get("params", {}).get("name") == name]
        require(len(rows) == 1, "case_missing_or_duplicate_tool")
        return rows[0]


def denial(row, status, code, canaries):
    require(row["status"] == status, "denial_status")
    body = fields(row["body"], {"error_code", "message", "retryable"}, "denial_payload")
    require(body["error_code"] == code and body["retryable"] is False
            and type(body["message"]) is str and len(body["message"]) <= 512, "denial_classification")
    wire = encode(body).decode()
    require(all(text not in wire for text in canaries if text), "denial_disclosure")


def mcp_data_denial(row, code, canaries, arguments):
    """Replay the original requested data-tool denial, including its caller args."""
    require(row["status"] == 200, "mcp_denial_status")
    request = row["request"]
    require(request.get("method") == "tools/call"
            and same(request.get("params"), {"name": "search_scoped", "arguments": arguments}),
            "mcp_denial_original_arguments")
    result = fields(row["body"], {"content", "isError"}, "mcp_denial_result")
    require(result["isError"] is True, "mcp_denial_outcome")
    blocks = result["content"]
    require(type(blocks) is list and len(blocks) == 1, "mcp_denial_blocks")
    block = fields(blocks[0], {"type", "text"}, "mcp_denial_block_fields")
    require(block["type"] == "text" and type(block["text"]) is str, "mcp_denial_text")
    denial({"status": 200, "body": strict_json(block["text"].encode())}, 200, code, canaries)


def mcp_anonymous_case(rows, space, query, canaries):
    """Public status-only discovery precedes a real anonymous data-tool denial."""
    require(len(rows) == 2 and all(row["method"] == "POST" and row["path"] == "/mcp/full"
            and row["credential_role"] is None and row["space"] == space for row in rows),
            "mcp_anonymous_original_requests")
    discovery, denied = sorted(rows, key=lambda row: row["index"])
    require(discovery["status"] == 200 and discovery["request"].get("method") == "tools/list"
            and same(discovery["request"].get("params"), {}), "mcp_public_discovery_request")
    result = fields(discovery["body"], {"tools"}, "mcp_public_discovery_fields")
    require(type(result["tools"]) is list and len(result["tools"]) == 1, "mcp_public_tool_count")
    tool = fields(result["tools"][0], {"name", "description", "inputSchema"}, "mcp_public_tool_fields")
    require(tool["name"] == "get_runtime_status" and type(tool["description"]) is str
            and 0 < len(tool["description"]) <= 512 and same(tool["inputSchema"], {}),
            "mcp_public_status_only")
    wire = encode(result).decode()
    require(all(value not in wire for value in canaries if value), "mcp_public_disclosure")
    mcp_data_denial(denied, "unauthorized", canaries,
                    {"query": query, "ranking_mode": "lexical", "limit": 1})


def configuration(row, space):
    require(row["status"] == 200, "configuration_status")
    value = row["body"]
    require(type(value) is dict and value.get("space_id") == space["id"]
            and type(value.get("id")) is str and value["id"], "configuration_identity")
    expected = {"security": {"policy_id": space["policy_id"], "level": space["level"], "labels": [], "handling": []},
                "embedding_provider": "openai", "embedding_model": "installed-unapproved-" + space["id"],
                "embedding_dims": space["dimensions"], "purpose": space["extraction_purpose"], "skill": space["extraction_skill"]}
    require(all(same(value.get(key), item) for key, item in expected.items()), "configuration_requested_values")
    processing = dict(value["processing"])
    require(integer(processing.pop("index_generation", None)), "configuration_index_epoch")
    require(same(processing, {"generation": 1, "policy_id": space["policy_id"], "connections": {}, "profiles": {},
                             "bindings": {}, "active_embedding_profile_id": None, "pending_reindex": None}), "configuration_metadata_authority")
    result = dict(value)
    result.pop("updated_at", None)
    result["processing"] = processing
    return result


def source_path(space, key):
    return "/v1/spaces/" + quote(space, safe="") + "/sources/" + quote(key, safe="")


def source_request(source, generation=None):
    return {"revision": source["revision"], "text": source["text"], "media_type": "text/plain",
            "source_url": source.get("source_url", "https://example.invalid/qualification"),
            "observed_at": "2026-10-06T20:11:00Z", "labels": source.get("labels", {}),
            "expected_generation": generation}


def verify_cases(traffic, journal, corpus, cases, artifact, observation, evidence):
    spaces = corpus["spaces"]
    require([space["id"] for space in spaces] == cases["spaces"] and len(spaces) == 2, "frozen_spaces")
    home, business = cases["spaces"]
    canaries = [source["text"] for source in corpus["sources"]]
    rows = journal["processes"]
    version = [row for row in rows if row["intent"]["argv"][1:] == ["--version"]]
    require(len(version) == 1, "version_command")
    version_raw = evidence.original({"path": version[0]["logs"][0]["path"], "ref": version[0]["logs"][0]["ref"]}, "process-log")
    require(version_raw.decode().strip() == "meerkat-recall " + artifact["version"], "installed_version")
    absent = traffic.one("absent_service", "GET", "/v1/health", None, None)
    require(absent.get("transport_error_type") == "ConnectionRefusedError", "original_absent_service")
    bootstrap = [row for row in rows if row["intent"]["argv"][1:3] == ["service-token", "bootstrap"]]
    issued = [row for row in rows if row["intent"]["argv"][1:3] == ["service-token", "issue"]]
    require(len(bootstrap) == 1 and len(issued) == 5, "fresh_cli_commands")
    def argument(row, flag):
        args = row["intent"]["argv"]
        require(args.count(flag) == 1 and args.index(flag) + 1 < len(args), "cli_argument")
        return args[args.index(flag) + 1]
    key = argument(bootstrap[0], "--secret-file")
    roles = {"operator", "service", "household", "bootstrap-" + home, "bootstrap-" + business}
    require({argument(row, "--subject") for row in issued} == {"installed-" + role for role in roles}
            and len({argument(row, "--output") for row in issued}) == 5, "fresh_cli_roles")
    for row in issued:
        require(argument(row, "--secret-file") == key and argument(row, "--expires-in") == "900"
                and argument(row, "--issuer") == "installed-offline" and argument(row, "--audience") == "recall-installed"
                and bootstrap[0]["reaped_ns"] < row["intent"]["started_ns"], "fresh_cli_key_chain")
        args = row["intent"]["argv"]
        role = argument(row, "--subject").removeprefix("installed-")
        def values(flag):
            return [args[index + 1] for index, value in enumerate(args[:-1]) if value == flag]
        expected_scopes = DATA_SCOPES
        if role == "operator":
            require(args.count("--global-admin") == 1 and not values("--space") and not values("--bootstrap-space"), "operator_scope")
            expected_scopes = ["manage:grant", "manage:provider", "read:space_config", "write:space_config",
                               "read:status", "read:system", "write:system"]
        elif role.startswith("bootstrap-"):
            require("--global-admin" not in args and not values("--space")
                    and values("--bootstrap-space") == [role.removeprefix("bootstrap-")]
                    and len(values("--clearances-file")) == 1, "bootstrap_scope")
            expected_scopes = ["read:space_config", "write:space_config", "manage:grant", "manage:provider"]
        else:
            require("--global-admin" not in args and not values("--bootstrap-space")
                    and values("--space") == ([home] if role == "household" else [home, business])
                    and len(values("--clearances-file")) == 1, "ordinary_scope")
        require(values("--scope") == expected_scopes, "cli_exact_scopes")
    unready = traffic.one("bootstrap_unready", "GET", "/v1/readiness", "operator", None)
    require(unready["status"] == 503 and unready["body"].get("configured") is False
            and unready["body"].get("ready") is False, "bootstrap_unready")
    health = traffic.matches("bootstrap_unready", "GET", "/v1/health", None, None)
    require(any(row["status"] == 200 and row["index"] < unready["index"] for row in health), "bootstrap_health_before_readiness")
    initialize = traffic.tool("runtime_initialized_without_workers",
                              "initialize_runtime" if observation["backend"] == "elephant-db" else "configure_runtime")
    arguments = initialize["request"]["params"]["arguments"]
    require(arguments.get("ingest_enabled") is False and arguments.get("extractor_required") is False, "worker_disable_request")
    status = tool_body(initialize)
    require(status.get("configured") is True and status.get("storage_ready") is True
            and status.get("backend") == observation["backend"], "actual_runtime_initialization")
    if observation["backend"] == "surreal-db":
        require(sum(row["intent"]["argv"][1:] == ["migrate"] for row in rows) == 1, "explicit_native_migration")
    original_configs, original_grants = {}, {}
    for space in spaces:
        name = space["id"]
        created = traffic.one("two_named_spaces", "POST", "/v1/spaces", "bootstrap-" + name, name)
        require(created["status"] == 201 and created["request"]["name"] == name, "named_space_creation")
        config = traffic.one("two_named_spaces", "GET", "/v1/spaces/" + name, "service", name)
        original_configs[name] = configuration(config, space)
        require(type(config["body"]["processing"]["index_generation"]) is int
                and config["body"]["processing"]["index_generation"] == 0, "initial_configuration_epoch")
        for key in ("security", "embedding_provider", "embedding_model", "embedding_dims", "purpose", "skill"):
            require(same(created["request"][key], config["body"][key]), "named_request_readback")
        grant = traffic.one("two_named_spaces", "PUT", f"/v1/spaces/{name}/grants/installed-service", "bootstrap-" + name, name,
                            {"active": True, "scopes": DATA_SCOPES, "expected_generation": None})
        require(grant["status"] == 200 and type(grant["body"]["generation"]) is str and grant["body"]["generation"], "initial_service_grant")
        original_grants[name] = grant["body"]["generation"]
    unauthenticated = traffic.one("http_auth_denied", "GET", f"/v1/spaces/{home}/status", None, None)
    denial(unauthenticated, 401, "unauthorized", canaries)
    positive = traffic.one("http_auth_denied", "GET", f"/v1/spaces/{home}/status", "household", home)
    require(positive["status"] == 200 and positive["index"] < unauthenticated["index"], "same_household_positive")
    home_source = next(row for row in cases["sources"] if row["space"] == home)
    home_query = next(query["query"] for query in corpus["queries"] if query["id"] == home_source["query_id"])
    mcp_anonymous_case(traffic.groups["mcp_auth_denied"], home, home_query, canaries)
    for role, selected in (("household", business), ("service", home)):
        denial(traffic.one("wrong_space_denied", "GET", f"/v1/spaces/{business}/status", role, selected), 403, "forbidden_space", canaries)
    for space in (home, business):
        initializations = [row for row in traffic.groups["mcp_positive"] if row["space"] == space
                           and row["credential_role"] == "service" and row["request"].get("method") == "initialize"]
        require(len(initializations) == 1 and initializations[0]["status"] == 200
                and same(initializations[0]["request"]["params"], {"protocolVersion": "2025-03-26", "capabilities": {},
                         "clientInfo": {"name": "installed-two-space-client", "version": "1"}}), "original_mcp_initialize")
        positive = [row for row in traffic.groups["mcp_positive"] if row["space"] == space
                    and row["credential_role"] == "service" and row["request"].get("method") == "tools/list"]
        require(len(positive) == 1 and positive[0]["status"] == 200
                and initializations[0]["index"] < positive[0]["index"]
                and positive[0]["body"].get("isError") is not True
                and any(tool.get("name") == "search_scoped" for tool in positive[0]["body"]["tools"]), "mcp_positive_inventory")
    grants = traffic.matches("grant_revocation", "PUT", f"/v1/spaces/{home}/grants/installed-service", "bootstrap-" + home, home)
    require(len(grants) == 2 and grants[0]["request"]["active"] is False and grants[1]["request"]["active"] is True
            and all(row["status"] == 200 for row in grants)
            and grants[0]["request"]["expected_generation"] == original_grants[home]
            and all(row["request"]["scopes"] == DATA_SCOPES for row in grants)
            and grants[1]["request"]["expected_generation"] == grants[0]["body"]["generation"], "grant_revoke_restore_cas")
    denied = traffic.one("grant_revocation", "GET", f"/v1/spaces/{home}/status", "service", home)
    denial(denied, 403, "forbidden_space", canaries)
    require(denied["index"] == grants[0]["index"] + 1 and denied["index"] < grants[1]["index"], "next_request_revocation")
    baseline, operations = {}, {}
    for expected in cases["sources"]:
        space, key = expected["space"], expected["key"]
        original = [source for source in corpus["sources"] if source["space"] == space and source["key"] == key
                    and source["revision"] == expected["baseline_revision"]]
        require(len(original) == 1, "frozen_source")
        accepted = traffic.matches("source_acceptance_and_idempotence", "PUT", source_path(space, key), "service", space,
                                   source_request(original[0]))
        require(len(accepted) == 2 and all(row["status"] == 202 for row in accepted)
                and same(accepted[0]["body"], accepted[1]["body"]), "source_idempotent_receipt")
        receipt = accepted[0]["body"]
        require(receipt["desired_revision"] == expected["baseline_revision"] and receipt["published_revision"] is None
                and all(type(receipt.get(field)) is str and receipt[field] for field in ("operation_id", "internal_source_id", "generation")), "accepted_unpublished_identity")
        head = traffic.one("source_acceptance_and_idempotence", "GET", source_path(space, key) + "/head", "service", space)
        require(head["status"] == 200 and head["body"]["desired_revision"] == expected["baseline_revision"]
                and head["body"]["state"] == "pending", "pending_head")
        operation = traffic.one("source_acceptance_and_idempotence", "GET", "/v1/operations/" + quote(receipt["operation_id"], safe=""), "service", space)
        require(operation["status"] == 200 and operation["body"]["state"] == "pending", "pending_operation")
        queries = [query for query in corpus["queries"] if query["id"] == expected["query_id"] and query["space"] == space]
        require(len(queries) == 1, "frozen_query")
        request = {"query": queries[0]["query"], "limit": queries[0]["k"], "ranking_mode": "lexical",
                   "source_revision_constraints": {key: expected["baseline_revision"]}}
        search = traffic.one("source_acceptance_and_idempotence", "POST", f"/v1/spaces/{space}/search", "service", space, request)
        require(search["status"] == 200 and search["body"]["partial"] is True and search["body"]["results"] == []
                and search["body"]["coverage"][key]["state"] == "pending", "pending_not_retrievable")
        mcp = traffic.tool("source_acceptance_and_idempotence", "search_scoped", "service", space)
        require(same(mcp["request"]["params"]["arguments"], request)
                and same(tool_body(mcp), search["body"]), "http_mcp_pending_equality")
        baseline[space], operations[space] = head["body"], operation["body"]
        replacement = [source for source in corpus["sources"] if source["space"] == space and source["key"] == key
                       and source["revision"] == expected["replacement_revision"]]
        require(len(replacement) == 1, "frozen_replacement")
        accepted_new = traffic.one("replacement_accepted", "PUT", source_path(space, key), "service", space,
                                   source_request(replacement[0], head["body"]["generation"]))
        require(accepted_new["status"] == 202 and accepted_new["body"]["desired_revision"] == expected["replacement_revision"]
                and accepted_new["body"]["published_revision"] is None, "replacement_receipt")
        for stage in ("restore_original_heads", "graceful_restart"):
            restored_head = traffic.one(stage, "GET", source_path(space, key) + "/head", "service", space)
            restored_operation = traffic.one(stage, "GET", operation["path"], "service", space)
            require(restored_head["status"] == restored_operation["status"] == 200
                    and same(restored_head["body"], baseline[space]) and same(restored_operation["body"], operations[space]), "original_restored_state")
    denial(traffic.one("input_bounds", "POST", f"/v1/spaces/{home}/search", "service", home,
                       {"query": "original invalid-limit control", "limit": 51, "ranking_mode": "lexical"}), 400, "invalid_request", canaries)
    business_source = next(row for row in cases["sources"] if row["space"] == business)
    denial(traffic.one("input_bounds", "GET", source_path(business, business_source["key"]) + "/head", "household", business), 403, "forbidden_space", canaries)
    business_query = next(query["query"] for query in corpus["queries"] if query["id"] == business_source["query_id"])
    mcp_data_denial(traffic.one("input_bounds", "POST", "/mcp/full", "household", business),
                    "forbidden_space", canaries, {"query": business_query, "ranking_mode": "lexical",
                    "source_revision_constraints": {business_source["key"]: business_source["baseline_revision"]}})
    exported = traffic.one("named_export", "POST", "/v1/system/snapshot/export", "operator", None,
                           {"path": "baseline", "name": "installed-baseline", "retention_seconds": 3600, "quiesce_timeout_seconds": 30})
    require(exported["status"] == 200 and exported["body"].get("success") is True
            and type(exported["body"].get("manifest")) is dict, "named_export")
    restored = traffic.one("restore_original_heads", "POST", "/v1/system/snapshot/restore", "operator", None, {"path": "baseline"})
    require(restored["status"] == 200 and restored["body"].get("success") is True, "named_restore")
    for stage in ("restore_original_heads", "graceful_restart"):
        for space in spaces:
            current = configuration(traffic.one(stage, "GET", "/v1/spaces/" + space["id"], "service", space["id"]), space)
            require(same(current, original_configs[space["id"]]), "restored_configuration_identity")
    reopened = traffic.groups["graceful_restart"]
    services = [row for row in rows if row["intent"]["argv"][1:] == ["serve", "--server-only", "--shutdown-timeout-seconds", "60"]]
    require(len(services) == (2 if observation["backend"] == "elephant-db" else 3), "service_restart_count")
    require(services[-2]["reaped_ns"] < services[-1]["intent"]["started_ns"]
            and services[-1]["intent"]["started_ns"] <= min(row["started_monotonic_ns"] for row in reopened), "graceful_restart_order")
    readonly = traffic.tool("read_only_capabilities", "configure_runtime")
    require(same(readonly["request"]["params"]["arguments"], {"runtime_mode": "read_only"}), "readonly_request")
    listed = [row for row in traffic.groups["read_only_capabilities"] if row["path"] == "/mcp/full"
              and row["credential_role"] == "service" and row["space"] == home and row["request"].get("method") == "tools/list"]
    require(len(listed) == 1 and listed[0]["status"] == 200, "readonly_tool_list")
    names = {tool["name"] for tool in listed[0]["body"]["tools"]}
    require("search_scoped" in names and "retract_source" not in names, "readonly_inventory")
    refused = traffic.tool("read_only_capabilities", "retract_source", "service", home)
    denial({"status": 200, "body": tool_body(refused, error=True)}, 200, "capability_disabled", canaries)
    home_source = next(row for row in cases["sources"] if row["space"] == home)
    metadata = traffic.one("read_only_capabilities", "GET", source_path(home, home_source["key"]) + "/head", "service", home)
    require(metadata["status"] == 200 and metadata["body"]["desired_revision"] == home_source["baseline_revision"], "readonly_metadata")


def _validate_installed(reviewed_static, installed_carrier, raw_evidence):
    fields(reviewed_static, {"review", "raw_sha256", "raw_bytes"}, "review_parameter")
    require(type(reviewed_static["raw_bytes"]) is bytes
            and digest(reviewed_static["raw_bytes"]) == reviewed_static["raw_sha256"]
            and same(strict_json(reviewed_static["raw_bytes"]), reviewed_static["review"]), "review_original_bytes")
    review = reviewed_static["review"]
    require(review.get("format") == "recall-dynamic-component-review/v2"
            and review.get("approved") is True and review.get("reviewer") == "qualification", "independent_review")
    carrier = fields(installed_carrier, {"format", "identity", *REFERENCES}, "carrier_fields")
    require(carrier["format"] == "recall-installed-offline-evidence/v2", "carrier_format")
    identity = fields(carrier["identity"], IDENTITY, "identity_fields")
    checksum(identity["source_commit"], 40)
    for key in ("artifact_sha256", "binary_sha256"):
        checksum(identity[key])
    require(identity["backend"] in ("elephant-db", "surreal-db"), "native_backend")
    approved_identity = review["identities"][identity["backend"]]
    require(all(same(identity[key], approved_identity[key]) for key in IDENTITY), "reviewed_candidate_identity")
    evidence = Evidence(raw_evidence)
    evidence.inventory(carrier["capture_inventory"])
    refs = review["input_refs"]
    plan = evidence.json(refs["installed_plan"], "installed-plan")
    case_raw = evidence.read(refs["installed_cases"], "installed-case-file", 1048576)
    corpus_raw = evidence.read(refs["installed_corpus"], "installed-corpus", 1048576)
    original_review = evidence.json(refs["installed_review"], "installed-review", 1048576)
    run = evidence.json(refs["installed_run_manifest"], "installed-manifest", 1048576)
    require(digest(case_raw) == CASE_SHA256, "frozen_case_file")
    cases, corpus = strict_json(case_raw), strict_json(corpus_raw)
    require(cases["required_cases"] == CASES and plan.get("format") == "recall-installed-linux-plan/v1"
            and same(plan["expectations"], cases) and plan["expectations_sha256"] == CASE_SHA256
            and plan["corpus_sha256"] == digest(corpus_raw), "reviewed_original_plan")
    require(original_review.get("format") == "recall-installed-linux-review/v1"
            and original_review.get("approved") is True and original_review.get("reviewer") == "qualification", "original_offline_review")
    for field in ("expectations_sha256", "corpus_sha256", "driver_sha256", "process_capture_sha256", "collector_sha256", "installer_sha256"):
        require(original_review.get(field) == plan[field], "original_plan_pin")
    for name, path in INSTALLED_HELPER_PATHS.items():
        require(checksum(review["helper_sha256"][path]) == plan[name + "_sha256"], "original_helper_pin")
    copied = fields(evidence.json(carrier["reviewed_plan"], "installed-reviewed-plan"),
                    {"format", "plan", "review", "case_file", "corpus"}, "copied_inputs_fields")
    require(copied["format"] == "recall-installed-reviewed-inputs/v2" and same(copied["plan"], plan), "copied_original_plan")
    for field, key, kind in (("review", "installed_review", "installed-review"),
                              ("case_file", "installed_cases", "installed-case-file"), ("corpus", "installed_corpus", "installed-corpus")):
        require(same(copied[field]["ref"], refs[key]), "copied_original_input_ref")
        evidence.original(copied[field], kind, 1048576)
    require(sum(same(reference, refs["installed_run_manifest"]) for reference in evidence.paths.values()) == 1,
            "original_run_manifest_capture")
    require(run.get("format") == "recall-installed-linux-run/v1" and run.get("execution_authorized") is True
            and run["expectation_review_sha256"] == refs["installed_review"]["sha256"], "original_run_allocation")
    for key, source in (("source_commit", "source_commit"), ("artifact_sha256", "archive_sha256"),
                        ("binary_sha256", "binary_sha256"), ("backend", "backend")):
        require(identity[key] == run[source], "original_run_identity")
    observation = evidence.json(carrier["observation"], "installed-observation", 2 * 1024**2)
    require(observation.get("format") == "recall-installed-linux-observation/v1"
            and observation.get("product_acceptance") is False and same(observation["plan"], plan), "observation_scope")
    for key in ("source_commit", "binary_sha256", "backend", "archive_sha256"):
        require(observation[key] == run[key], "observation_original_candidate")
    require(observation["run_manifest_sha256"] == digest(json.dumps(run, ensure_ascii=False, sort_keys=True, allow_nan=False).encode()),
            "observation_original_run")
    require(type(observation["cases"]) is list and len(observation["cases"]) == 19
            and {row["case"] for row in observation["cases"]} == set(CASES), "observation_case_inventory")
    artifact = evidence.json(carrier["artifact_manifest"], "installed-artifact-manifest", 1048576)
    require(artifact.get("format") == "meerkat-recall-native/v1" and artifact.get("target") == "x86_64-unknown-linux-gnu"
            and artifact.get("source_commit") == identity["source_commit"]
            and artifact.get("binary_sha256") == identity["binary_sha256"]
            and same(artifact, observation["artifact_manifest"]), "original_artifact_provenance")
    owner = controller(carrier["controller"], evidence, observation)
    original_allocation_clock(owner, run)
    journal = process_journal(carrier["command_inventory"], evidence, owner, identity["binary_sha256"])
    cleanup = evidence.json(carrier["terminal_cleanup"], "installed-cleanup")
    require(cleanup.get("format") == "recall-installed-process-cleanup/v2"
            and cleanup["remaining_child_pid"] is None and cleanup["remaining_command_pids"] == []
            and cleanup["reaped_indices"] == list(range(1, len(journal["processes"]) + 1))
            and integer(cleanup["observed_ns"], 1) and journal["terminal_observed_ns"] <= cleanup["observed_ns"] <= owner["deadline_ns"]
            and type(cleanup["original_deadline_ns"]) is int and cleanup["original_deadline_ns"] == owner["deadline_ns"]
            and cleanup["original_expires_at"] == run["expires_at"], "original_terminal_cleanup")
    configurations = [reference for reference in evidence.paths.values() if reference["kind"] == "installed-runtime-configuration"]
    require(len(configurations) == 1 and configurations[0]["sha256"] == observation["runtime_configuration_sha256"], "original_saved_configuration")
    saved = tomllib.loads(evidence.read(configurations[0], "installed-runtime-configuration", 1048576).decode())
    require(saved.get("runtime", {}).get("mode") == "read_only"
            and saved["runtime"].get("ingest_enabled") is False
            and saved.get("storage", {}).get("backend") == identity["backend"], "saved_configuration_toml")
    traffic = Traffic(evidence, journal, owner)
    services = [row for row in journal["processes"] if row["intent"]["argv"][1:] ==
                ["serve", "--server-only", "--shutdown-timeout-seconds", "60"]]
    for request in traffic.rows.values():
        live = sum(row["intent"]["started_ns"] <= request["started_monotonic_ns"]
                   and request["finished_monotonic_ns"] <= row["exit_observed"]["observed_ns"] for row in services)
        absent = request in traffic.groups["absent_service"]
        require(live == (0 if absent else 1), "http_live_original_service")
    require(observation["http_attempts"] == len(traffic.rows) and observation["processes"] == len(journal["processes"]), "complete_observation_counts")
    verify_cases(traffic, journal, corpus, cases, artifact, observation, evidence)
    return {"identity": dict(identity), "required_cases": list(CASES)}


def validate_installed(reviewed_static, installed_carrier, raw_evidence):
    """Replay all 19 frozen predicates, or refuse with a stable error code.

    This returns only originally evidenced native metadata identities. It does
    not certify a paired server, semantic provider, runtime package or live run.
    """
    try:
        return _validate_installed(reviewed_static, installed_carrier, raw_evidence)
    except ValueError as error:
        if str(error).startswith("dynamic_installed_"):
            raise
        raise ValueError("dynamic_installed_invalid_evidence") from error
    except (KeyError, TypeError, IndexError, UnicodeError, OverflowError, RecursionError) as error:
        raise ValueError("dynamic_installed_invalid_evidence") from error


def validate_process_proof(snapshot, raw_evidence, expected_binary_sha256,
                           controller_identity, deadline_ns, *, require_terminal):
    """Validate original F process proof without adopting any live process.

    The caller separately binds original command/path inventory and rechecks live
    custody. This function hashes every referenced original byte and checks the
    shared launch/exit/log predicates under the current-runtime 4 MiB per-stream
    bound. Only the fixed snapshot inspection command may declare the original
    pre-spawn 32 MiB + newline stdout reservation and 1 MiB stderr reservation.
    Its report retains the snapshot-inspection kind. Installed nineteen-case
    validation retains its original 1 MiB bound and rejects this extension.
    Live snapshots cannot claim terminal logs. This proves output custody, not
    report semantics or physical erasure; the caller owns the shared reservation.
    """
    try:
        require(type(require_terminal) is bool and integer(deadline_ns, 1), "process_arguments")
        checksum(expected_binary_sha256)
        fields(controller_identity, {"pid", "ppid", "pgrp", "session", "start_ticks"}, "controller_identity_fields")
        evidence = Evidence(raw_evidence)
        streams = process_streams(snapshot["intent"], 4 * 1024**2, allow_inspection=True)
        report = snapshot["logs"][0] if streams == INSPECTION_STREAMS else None
        report_sha = report["ref"]["sha256"] if report is not None and report["ref"] is not None else None
        def register(value):
            if type(value) is dict:
                if set(value) == {"path", "ref"}:
                    path = private_path(value["path"])
                    require(path not in evidence.paths or same(evidence.paths[path], value["ref"]), "process_original_path_replaced")
                    ref = value["ref"]
                    maximum = 8 * 1024**2
                    if ref.get("kind") == "snapshot-inspection":
                        require(report is not None and path == report["path"]
                                and same(ref, report["ref"]), "inspection_original_report_role")
                        maximum = INSPECTION_STREAMS[0][1]
                    if report_sha is not None and ref.get("sha256") == report_sha:
                        require(ref.get("kind") == "snapshot-inspection", "inspection_stream_kind_alias")
                    evidence.read(ref, maximum=maximum)
                    evidence.paths[path] = value["ref"]
                else:
                    for item in value.values():
                        register(item)
            elif type(value) is list:
                for item in value:
                    register(item)
        register(snapshot)
        # Stat references live inside original event bodies, not the snapshot's
        # summaries. Bind those bytes before validating their decoded identity.
        for event in snapshot["events"]:
            register(strict_json(evidence.read(event["capture"]["ref"], "installed-process-event")))
        for log in snapshot["logs"]:
            if log["ref"] is not None:
                register({"path": log["path"], "ref": log["ref"]})
        owner = {"identity": controller_identity, "observed_ns": 0, "deadline_ns": deadline_ns}
        return _validate_process(snapshot, evidence, owner, expected_binary_sha256,
                                 require_terminal, set(), set(), log_limit=4 * 1024**2, allow_inspection=True)
    except ValueError as error:
        if str(error).startswith("dynamic_installed_"):
            raise
        raise ValueError("dynamic_installed_invalid_process_proof") from error
    except (KeyError, TypeError, IndexError, UnicodeError, OverflowError, RecursionError) as error:
        raise ValueError("dynamic_installed_invalid_process_proof") from error
