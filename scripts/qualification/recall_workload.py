#!/usr/bin/env python3
"""Bounded read-only D28 workload capture; source preparation is not execution.

Fifty callers, four original requests each, 50 TOTAL prepared sources across two
spaces. No service start, fixture preparation, mutation, retry or provider probe.
The independent D workload adapter owns all correctness and latency verdicts.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import datetime as dt
import hashlib
import importlib.util
import ipaddress
import json
from pathlib import Path
import re
import sys
import time
import urllib.parse

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("workload_installed_helpers", Path(__file__).with_name("installed_linux_offline.py"))
helpers = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helpers)
require = helpers.require
profiles = helpers.module("recall_closed_workload_profiles", ROOT / "tests/recall_preview_semantics/workload_profiles.py",
                          expected_sha256="aa55b0a29cc0663425c889ef7206f17d48613eb6263e39e100919857babe4f13")
CAP = 262144
MAX_TRANSPORT_BODY = 270336
HEADER_CAP = 32768
WIRE_CAP = 2097152
FIXED = {"callers": 50, "requests_per_caller": 4, "request_count": 200, "source_count": 50,
         "request_timeout_ms": 8000, "duration_budget_ms": 120000,
         "response_budget_bytes": CAP, "p95_ms": 2000, "p99_ms": 5000, "wire_limit": 50}


def sha(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def inventory():
    return {"format": "recall-workload-driver-inventory/v1", "fixed": FIXED,
            "driver_sha256": helpers.file_digest(Path(__file__)),
            "helpers_sha256": helpers.file_digest(Path(helpers.__file__)),
            "collector_sha256": helpers.file_digest(Path(helpers.capture.__file__)),
            "profiles_sha256": helpers.file_digest(ROOT / profiles.SELF),
            "scope": "source only; requires D corpus, schedule, adapter, preparation and isolated provider accounting",
            "product_acceptance": False}


def strict_uint(value, maximum, label):
    require(isinstance(value, bytes) and re.fullmatch(rb"[0-9]+", value), f"invalid {label}")
    number = int(value)
    require(number <= maximum, f"{label} exceeds bound")
    return number


async def read_frame(reader, observed, *, body_limit=CAP):
    """Bound one actual HTTP/1.x response with an outer absolute asyncio deadline.

    This asynchronous transport deliberately reuses D's strict JSON parser, not
    its main-thread SIGALRM context manager. It records the body prefix on error.
    The default D28 limit stays fixed; an independently reviewed MCP consumer
    may explicitly allow its bounded JSON-RPC envelope up to 270336 bytes.
    """
    require(type(body_limit) is int and 1 <= body_limit <= MAX_TRANSPORT_BODY,
            "invalid explicit transport body limit")
    wire_bytes = 0

    async def exact(count):
        nonlocal wire_bytes
        require(wire_bytes + count <= WIRE_CAP, "wire response exceeds bound")
        try:
            value = await reader.readexactly(count)
        except asyncio.IncompleteReadError as error:
            wire_bytes += len(error.partial)
            raise
        wire_bytes += len(value)
        return value

    async def line(bound):
        nonlocal wire_bytes
        value = await reader.readuntil(b"\r\n")
        wire_bytes += len(value)
        require(len(value) <= bound and wire_bytes <= WIRE_CAP, "HTTP line exceeds bound")
        return value[:-2]

    try:
        first = await exact(1)
        observed["first_response_byte_ns"] = time.monotonic_ns()
        status_line = first + await line(4096)
        match = re.fullmatch(rb"HTTP/1\.[01] ([1-5][0-9][0-9])(?: [^\r\n]*)?", status_line)
        require(match is not None, "invalid HTTP response status")
        observed["status"] = int(match[1])
        require(observed["status"] >= 200, "unexpected informational response")
        headers = {}
        header_bytes = len(status_line)
        while True:
            field = await line(HEADER_CAP)
            header_bytes += len(field) + 2
            require(header_bytes <= HEADER_CAP, "HTTP headers exceed bound")
            if not field:
                break
            require(not field.startswith((b" ", b"\t")) and b":" in field, "invalid HTTP header")
            name, value = field.split(b":", 1)
            require(re.fullmatch(rb"[!#$%&'*+.^_`|~0-9A-Za-z-]+", name) is not None, "invalid header name")
            value = value.strip(b" \t")
            require(all(byte >= 32 or byte == 9 for byte in value), "invalid header value")
            headers.setdefault(name.lower(), []).append(value)
        lengths, transfers = headers.get(b"content-length", []), headers.get(b"transfer-encoding", [])
        require(not (lengths and transfers) and len(lengths) <= 1 and len(transfers) <= 1, "ambiguous HTTP framing")
        observed["response_headers"] = {name.decode("ascii"): [value.decode("latin1") for value in values]
                                        for name, values in headers.items()}

        async def body_exact(count):
            nonlocal wire_bytes
            require(len(observed["raw_body"]) + count <= body_limit, "response body exceeds bound")
            remaining = count
            while remaining:
                chunk = await reader.read(min(8192, remaining))
                require(bool(chunk), "incomplete HTTP response body")
                wire_bytes += len(chunk)
                require(wire_bytes <= WIRE_CAP, "wire response exceeds bound")
                observed["raw_body"] += chunk
                remaining -= len(chunk)

        if transfers:
            require(transfers[0].lower() == b"chunked", "unsupported transfer encoding")
            while True:
                size_line = await line(256)
                size = size_line.split(b";", 1)[0]
                require(re.fullmatch(rb"[0-9a-fA-F]+", size), "invalid chunk size")
                count = int(size, 16)
                if count == 0:
                    trailers = 0
                    while True:
                        trailer = await line(8192)
                        trailers += len(trailer) + 2
                        require(trailers <= 8192, "HTTP trailers exceed bound")
                        if not trailer:
                            break
                        require(b":" in trailer and not trailer.startswith((b" ", b"\t")), "invalid HTTP trailer")
                        require(trailer.split(b":", 1)[0].lower() not in (b"content-length", b"transfer-encoding"),
                                "framing header appeared in trailer")
                    break
                await body_exact(count)
                require(await exact(2) == b"\r\n", "missing chunk terminator")
        elif lengths:
            length = strict_uint(lengths[0], body_limit, "Content-Length")
            await body_exact(length)
        else:
            # EOF framing is valid only when the transport actually closes.
            while True:
                chunk = await reader.read(min(8192, body_limit + 1 - len(observed["raw_body"])))
                if not chunk:
                    break
                wire_bytes += len(chunk)
                observed["raw_body"] += chunk
                require(len(observed["raw_body"]) <= body_limit and wire_bytes <= WIRE_CAP, "response body exceeds bound")
        observed["framing_complete"] = True
        observed["body"] = helpers.capture.strict_json_body(observed["raw_body"])
    finally:
        observed["wire_bytes"] = wire_bytes


def endpoint(value):
    parsed = urllib.parse.urlsplit(value)
    require(parsed.scheme == "http" and parsed.hostname and not parsed.username and not parsed.password
            and parsed.path in ("", "/") and not parsed.query and not parsed.fragment,
            "explicit installed loopback HTTP origin required")
    try:
        address = ipaddress.ip_address(parsed.hostname)
    except ValueError as error:
        raise ValueError("literal loopback address required; no ambient DNS") from error
    require(address.is_loopback, "workload origin must be the supplied installed loopback service")
    require(parsed.port is not None and 1 <= parsed.port <= 65535, "explicit workload port required")
    return parsed.hostname, parsed.port


def validate_schedule(contract, corpus):
    selected = profiles.profile_for_contract(contract)
    profiles.validate_corpus(corpus, selected)
    require(contract.get("format") == "recall-preview-workload/v2", "unknown D workload contract")
    for key, value in selected.fixed().items():
        if key == "source_count":
            continue
        field = "concurrent_callers" if key == "callers" else key
        require(type(contract.get(field)) is int and contract[field] == value, f"root-frozen {field} differs")
    spaces = contract["spaces"]
    require(isinstance(spaces, list) and len(spaces) == 2 and len(set(spaces)) == 2
            and all(isinstance(space, str) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", space) for space in spaces),
            "two distinct bounded named spaces required")
    identities = {(row["space"], row["key"]) for row in corpus["sources"]}
    require(len(identities) == 50 and {space for space, _ in identities} == set(spaces), "50 TOTAL workload sources required")
    queries = {row["id"]: row for row in corpus["queries"]}
    require(len(queries) == len(corpus["queries"]), "duplicate frozen query identity")
    schedule = corpus["schedule"]
    require(len(schedule) == selected.requests, "exact closed-profile request schedule required")
    caller_ids = set(selected.caller_ids())
    requests, ordinals = set(), set()
    for request in schedule:
        require(isinstance(request["id"], str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,96}", request["id"]), "bounded request identity required")
        require(request["id"] not in requests, "duplicate request identity")
        requests.add(request["id"])
        worker, ordinal = request["caller_id"], request["ordinal"]
        require(isinstance(worker, str) and worker in caller_ids
                and type(ordinal) is int and 0 <= ordinal < 4, "request outside frozen caller schedule")
        require((worker, ordinal) not in ordinals, "duplicate caller ordinal")
        ordinals.add((worker, ordinal))
        query = queries[request["query_id"]]
        require(query["space"] in spaces and query["space"] == request["space"], "query changed named space")
        require(isinstance(query["query"], str) and bool(query["query"].strip()) and query["wire_limit"] == 50,
                "original query or wire cutoff changed")
        require(query.get("ranking_mode") == "semantic", "no lexical substitute for governed load")
    require(ordinals == {(worker, ordinal) for worker in caller_ids for ordinal in range(4)}, "caller schedule is incomplete")
    require(sha(json.dumps(schedule, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()) == contract["schedule_sha256"],
            "independent frozen schedule digest differs")
    require(contract["sources_per_space"] == {space: sum(identity[0] == space for identity in identities) for space in spaces},
            "independent source distribution differs")
    require({row["space"] for row in schedule} == set(spaces), "schedule omitted a named space")
    if selected.id == profiles.SEMANTIC140_ID:
        require(corpus.get("profile_id") == selected.id
                and contract["corpus_sha256"] == selected.hashes["corpus"], "closed corpus/profile differs")
        require(all(sum(row["space"] == space for row in schedule) == 280 for space in spaces),
                "closed semantic140 named-space request distribution differs")
    return queries


def rendered_map_index(rendered, corpus, corpus_sha256):
    """Consume D's exact static expectations, never normalize an observation."""
    require(type(rendered) is dict and rendered.get("format") == "recall-independent-rendered-map/v1"
            and rendered.get("reviewer") == "qualification (independent D lane)"
            and rendered.get("execution_authorized") is False and rendered.get("product_acceptance") is False
            and rendered.get("corpus_sha256") == corpus_sha256, "independent rendered map identity differs")
    renderer, tree = rendered["renderer"], rendered["tree"]
    require(renderer["name"] == "elephant.renderer.text" and renderer["version"] == "1.0.0"
            and renderer["source_path"] == "crates/pipeline/src/renderer/default.rs"
            and tree["source_path"] == "crates/pipeline/src/scheduler/evidence_work_kinds.rs",
            "rendered map describes another renderer/tree")
    for owner in (renderer, tree):
        require(type(owner.get("source_sha256")) is str and re.fullmatch(r"[0-9a-f]{64}", owner["source_sha256"]),
                "rendered map has no exact reviewed source hash")
    configuration = {"max_chunk_size": 512, "chunk_overlap": 50, "create_paragraphs": True,
                     "detect_sections": False, "semantic_chunking": True, "create_summaries": False}
    require(canonical(tree["configuration"]) == canonical(configuration), "rendered map changes the frozen tree recipe")
    originals = {(source["space"], source["key"], source["revision"]): source for source in corpus["sources"]}
    require(len(originals) == len(corpus["sources"]) and type(rendered["source_count"]) is int
            and rendered["source_count"] == len(originals) and type(rendered["sources"]) is list
            and len(rendered["sources"]) == len(originals), "rendered map coverage differs")
    fields = {"space", "source_key", "revision", "input_sha256", "input_utf8_bytes", "rendered_text",
              "rendered_sha256", "rendered_utf8_bytes", "root_span", "paragraph_spans", "chunk_spans"}
    index = {}
    for row in rendered["sources"]:
        require(type(row) is dict and set(row) == fields, "rendered expectation fields differ")
        key = (row["space"], row["source_key"], row["revision"])
        require(key in originals and key not in index, "missing, foreign or duplicate rendered identity")
        source = originals[key]
        raw = source["text"].encode("utf-8")
        require(0 < len(raw) < 512 and "\r" not in source["text"] and "\n" not in source["text"]
                and source["text"] == source["text"].rstrip(), "original input is outside D's reviewed single-line recipe")
        require(row["input_sha256"] == source["sha256"] == sha(raw)
                and type(row["input_utf8_bytes"]) is int and row["input_utf8_bytes"] == len(raw),
                "rendered map changes original input bytes")
        require(type(row["rendered_text"]) is str and row["rendered_text"] == source["text"] + "\n",
                "independent rendered bytes differ from D's exact reviewed projection")
        expected = row["rendered_text"].encode("utf-8")
        require(row["rendered_sha256"] == sha(expected) and type(row["rendered_utf8_bytes"]) is int
                and row["rendered_utf8_bytes"] == len(expected), "rendered byte identity differs")
        for name, spans, expected_spans in (
            ("root", [row["root_span"]], [{"start": 0, "end": len(expected)}]),
            ("paragraph", row["paragraph_spans"], [{"start": 0, "end": len(raw)}]),
            ("chunk", row["chunk_spans"], [{"start": 0, "end": len(raw)}]),
        ):
            require(type(spans) is list and all(type(span) is dict and set(span) == {"start", "end"}
                    and type(span["start"]) is int and type(span["end"]) is int for span in spans)
                    and spans == expected_spans, "rendered map changes exact " + name + " spans")
        index[key] = row
    return index


def measurement_binding(manifest, loaded):
    """Pin the prepared fixture and G/D typed allocation without granting it."""
    require(manifest.get("provider_phase") == "measurement", "explicit measurement provider phase required")
    require(re.fullmatch(r"[a-zA-Z0-9_-]{16,64}", manifest.get("nonce", "")), "fresh bounded measurement nonce required")
    selected = profiles.profile_for_manifest(manifest)
    if selected.id == profiles.SEMANTIC140_ID or "contract" in loaded:
        require(profiles.profile_for_contract(loaded["contract"]) is selected, "contract/manifest profile differs")
    fixture = loaded["prepared_fixture"]
    require(fixture.get("qualified") is True, "prepared fixture has no independent qualification")
    for key in ("source_commit", "artifact_sha256", "binary_sha256", "backend", "endpoint", "corpus_sha256",
                "preparation_contract_sha256", "typed_delta_sha256", "provider_allocation_sha256", "rendered_map_sha256"):
        require(fixture.get(key) == manifest[key], f"prepared fixture {key} differs")
    typed, delta = loaded["preparation_contract"], loaded["typed_delta"]
    require(typed.get("format") == "recall-workload-typed-preparation/v1"
            and typed["corpus_sha256"] == manifest["corpus_sha256"], "typed preparation corpus differs")
    require(delta.get("format") == "recall-workload-allocation-representation-delta/v1"
            and delta["base_allocation_sha256"] == manifest["provider_allocation_sha256"]
            and delta["preparation_contract_sha256"] == manifest["preparation_contract_sha256"]
            and delta["corpus_sha256"] == manifest["corpus_sha256"], "original typed allocation identity differs")


def measurement_begin(manifest, begin):
    require(isinstance(begin, dict) and len(canonical(begin)) <= 65536, "invalid G cohort begin receipt")
    require(begin.get("format") == "recall-exclusive-cohort-begin/v1" and begin.get("authority") == "exclusive_gateway_cohort"
            and begin.get("phase") == "measurement" and begin.get("backend") == manifest["backend"]
            and begin.get("allocation_sha256") == manifest["provider_allocation_sha256"], "wrong original measurement cohort")
    require(type(begin.get("cohort_id")) is int and begin["cohort_id"] > 0
            and re.fullmatch(r"[0-9a-f]{64}", begin.get("baseline_sha256", "")), "invalid original cohort identity")


def measurement_close(begin, accounting, *, profile_id=profiles.LEGACY_ID):
    selected = profiles.profile(profile_id)
    require(isinstance(accounting, dict) and accounting.get("verdict") in ("passed", "failed", "unmet"), "invalid G accounting verdict")
    if accounting["verdict"] != "passed":
        return
    require(begin is not None and accounting.get("format") == "recall-exclusive-cohort-accounting/v1"
            and type(accounting.get("cohort_id")) is int
            and accounting.get("cohort_id") == begin["cohort_id"]
            and accounting.get("before_sha256") == begin["baseline_sha256"], "G close differs from original measurement cohort")
    require(accounting.get("admission_closed") is True and type(accounting.get("unsettled_dispatches")) is int
            and accounting["unsettled_dispatches"] == 0
            and accounting.get("per_request_association") == "unmet", "G close is unsettled or fabricates request attribution")
    for name in ("after_sha256", "capture_inventory_sha256", "sealed_receipt_sha256"):
        require(re.fullmatch(r"[0-9a-f]{64}", accounting.get(name, "")), "G sealed evidence identity absent")
    count, reserved = accounting.get("physical_dispatch_reservations"), accounting.get("reserved_nano_usd")
    require(type(count) is int and 0 < count <= selected.measurement_attempts and type(reserved) is int and 0 < reserved <= selected.money,
            "G measurement accounting exceeds frozen bounds or lacks physical evidence")
    require(reserved == count * 1_064_960, "physical attempt reservations differ from the frozen no-refund allocation")


def reviewed_inputs(manifest_path):
    manifest = helpers.read_json(manifest_path)
    require(manifest.get("format") == "recall-workload-run/v1" and manifest.get("execution_authorized") is True,
            "workload execution has not been allocated")
    require(dt.datetime.now(dt.timezone.utc) < helpers.utc(manifest["expires_at"]), "workload allocation expired")
    require(manifest.get("provider_dispatch_authorized") is True, "governed query dispatch is not allocated")
    endpoint(manifest["endpoint"])
    require(manifest.get("backend") in ("elephant-db", "surreal-db"), "explicit workload backend required")
    for key, length in (("source_commit", 40), ("artifact_sha256", 64), ("binary_sha256", 64)):
        require(isinstance(manifest.get(key), str) and re.fullmatch(f"[0-9a-f]{{{length}}}", manifest[key]), f"trusted {key} required")
    loaded = {}
    names = ("contract", "corpus", "prepared_fixture", "provider_allocation", "preparation_contract", "typed_delta", "rendered_map")
    for name in ("review", *names):
        path = Path(manifest[name + "_path"])
        require(path.is_absolute() and helpers.file_digest(path) == manifest[name + "_sha256"], f"exact {name} custody differs")
        loaded[name] = helpers.read_json(path)
    review = loaded["review"]
    require(review.get("format") == "recall-workload-review/v1" and review.get("reviewer") == "qualification"
            and review.get("approved") is True, "independent workload source/cases not approved")
    for key in ("driver_sha256", "helpers_sha256", "collector_sha256"):
        require(review.get(key) == inventory()[key], f"reviewed {key} differs")
    for name in names:
        require(review.get(name + "_sha256") == manifest[name + "_sha256"], f"unreviewed {name}")
    selected = profiles.profile_for_manifest(manifest)
    if selected.id == profiles.SEMANTIC140_ID:
        require(review.get("profiles_sha256") == inventory()["profiles_sha256"], "closed-profile source is unreviewed")
    measurement_binding(manifest, loaded)
    require(loaded["contract"]["corpus_sha256"] == manifest["corpus_sha256"], "D workload corpus hash differs")
    queries = validate_schedule(loaded["contract"], loaded["corpus"])
    rendered_map_index(loaded["rendered_map"], loaded["corpus"], manifest["corpus_sha256"])
    # D supplies the public adapter; absence is a prerequisite failure, not a
    # local replacement oracle or permission to fabricate per-request verdicts.
    adapter_path = ROOT / "tests/recall_preview/workload.py"
    require(adapter_path.is_file() and helpers.file_digest(adapter_path) == review.get("adapter_sha256"),
            "independent D workload adapter is missing or changed")
    adapter = helpers.module("independent_d_workload", adapter_path)
    for name in ("request", "workload_response", "evaluate_cohort"):
        require(callable(getattr(adapter, name, None)), f"independent D {name} interface missing")
    return manifest, loaded, queries, adapter, accounting_bridge(manifest, review)


def accounting_bridge(manifest, review):
    """Load G's separately reviewed authority; D's evaluator grants no budget.

    Root named these two public functions; the G-owned implementation and its
    exclusive-cohort allocation remain external prerequisites. Missing source
    refuses before any workload socket, without a local authorization substitute.
    """
    path = Path(manifest.get("provider_bridge_path", ""))
    custody = ROOT / "tests/recall_preview_semantics/gateway"
    require(path.is_absolute() and path == path.resolve() and path.is_relative_to(custody)
            and path.is_file(), "reviewed G exclusive-cohort bridge is unavailable")
    digest = helpers.file_digest(path)
    require(digest == manifest.get("provider_bridge_sha256") == review.get("provider_bridge_sha256"),
            "G exclusive-cohort bridge custody differs")
    bridge = helpers.module("governed_workload_accounting", path)
    for name in ("authorize_provider_allocation", "provider_accounting"):
        require(callable(getattr(bridge, name, None)), f"G exclusive-cohort {name} interface missing")
    return bridge


class Workload:
    @property
    def profile(self):
        return profiles.profile_for_contract(self.loaded["contract"])

    def __init__(self, manifest, loaded, queries, adapter, bridge):
        self.manifest, self.loaded, self.queries, self.adapter = manifest, loaded, queries, adapter
        require(profiles.profile_for_manifest(manifest) is self.profile, "workload profile binding differs")
        self.bridge = bridge
        self.provider_cohort_begin = None
        self.host, self.port = endpoint(manifest["endpoint"])
        self.root = Path(manifest["output_directory"])
        require(self.root.is_absolute() and self.root == self.root.resolve() and not self.root.exists(), "new canonical private output directory required")
        self.root.mkdir(mode=0o700)
        (self.root / "captures").mkdir(mode=0o700)
        self.tokens = {}
        require(set(manifest["space_credentials"]) == set(loaded["contract"]["spaces"]), "explicit role for each selected space required")
        roles = set(manifest["space_credentials"].values())
        require(all(isinstance(role, str) and re.fullmatch(r"[a-z][a-z0-9_]{0,63}", role) for role in roles), "invalid credential role")
        require(set(manifest["credential_files"]) == roles, "exact explicit credential roles required")
        for role, name in manifest["credential_files"].items():
            path = Path(name)
            require(path.is_absolute() and not path.is_symlink() and path.is_file()
                    and path.stat().st_mode & 0o077 == 0 and path.stat().st_size <= 16384, "private bounded credential file required")
            token = path.read_text().strip()
            require(re.fullmatch(r"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", token), "invalid signed credential file")
            self.tokens[role] = token
        self.rows = []
        self.started_ns = 0
        self.active_callers = 0
        self.peak_callers = 0
        self.capture_bytes = 0
        self.callers = {}
        self.barrier_released_ms = None

    async def request(self, scheduled):
        query = self.queries[scheduled["query_id"]]
        request_body = self.adapter.request(query)
        require(request_body["query"] == query["query"] and request_body["limit"] == 50
                and request_body["ranking_mode"] == "semantic", "independent original request differs")
        payload = canonical(request_body)
        role = self.manifest["space_credentials"][scheduled["space"]]
        require(len(payload) <= 32768, "frozen request exceeds driver bound")
        path = "/v1/spaces/" + urllib.parse.quote(scheduled["space"], safe="") + "/search"
        host_header = f"[{self.host}]:{self.port}" if ":" in self.host else f"{self.host}:{self.port}"
        wire = (f"POST {path} HTTP/1.1\r\nHost: {host_header}\r\nAuthorization: Bearer {self.tokens[role]}\r\n"
                f"X-Space-Id: {scheduled['space']}\r\nContent-Type: application/json\r\nAccept: application/json\r\n"
                f"Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n").encode() + payload
        observed = {"raw_body": b"", "framing_complete": False, "body": None, "status": None}
        started = time.monotonic_ns()
        row = {"id": scheduled["id"], "caller_id": scheduled["caller_id"], "ordinal": scheduled["ordinal"],
               "space": scheduled["space"], "query_id": scheduled["query_id"], "request_body": request_body,
               "request_sha256": sha(payload), "credential_role": role,
               "started_ms": (started - self.started_ns) / 1e6,
               "oracle_verdict": "failed", "provider_attempt_ids": None, "evaluation_fallbacks": []}
        writer = None
        self.active_callers += 1
        self.peak_callers = max(self.peak_callers, self.active_callers)
        try:
            require(dt.datetime.now(dt.timezone.utc) < helpers.utc(self.manifest["expires_at"]), "allocation expired before request")
            async with asyncio.timeout(8):
                reader, writer = await asyncio.open_connection(self.host, self.port, limit=HEADER_CAP)
                observed["connected_ns"] = time.monotonic_ns()
                writer.write(wire)
                await writer.drain()
                observed["client_write_complete_ns"] = time.monotonic_ns()
                await read_frame(reader, observed)
        except (Exception, asyncio.CancelledError) as error:
            row["capture_error"] = type(error).__name__
            if isinstance(error, asyncio.CancelledError):
                row["run_deadline_cancelled"] = True
        finally:
            row["ended_ms"] = (time.monotonic_ns() - self.started_ns) / 1e6
            self.active_callers -= 1
            if writer is not None:
                writer.close()
                try:
                    async with asyncio.timeout(0.1):
                        await writer.wait_closed()
                except asyncio.CancelledError:
                    row["capture_error"] = "CancelledError"
                    row["run_deadline_cancelled"] = True
                except Exception:
                    row["connection_cleanup_failed"] = True
        raw = observed.pop("raw_body")[:CAP]
        for key in ("connected_ns", "client_write_complete_ns", "first_response_byte_ns"):
            if key in observed:
                row[key.replace("_ns", "_ms")] = (observed.pop(key) - self.started_ns) / 1e6
        response = {**observed, "raw_body_base64": base64.b64encode(raw).decode(), "body_sha256": sha(raw),
                    "raw_body_complete": observed["framing_complete"], "bytes": len(raw)}
        row["response_bytes"] = len(raw)
        row["response"] = response
        if "capture_error" not in row and not row.get("connection_cleanup_failed"):
            try:
                verdict = self.adapter.workload_response(query, response, self.loaded["corpus"])
                require(verdict.get("verdict") in ("passed", "failed"), "independent response verdict malformed")
                row["oracle_verdict"] = verdict["verdict"]
                row["independent_response_verdict"] = verdict
            except Exception as error:
                row["oracle_error_type"] = type(error).__name__
        encoded = canonical(row)
        self.capture_bytes += len(encoded)
        require(self.capture_bytes <= self.profile.row_capture_bytes, "private capture budget exceeded")
        helpers.private_new(self.root / "captures" / (row["id"] + ".json"), encoded)
        self.rows.append(row)
        if row.get("run_deadline_cancelled"):
            raise asyncio.CancelledError

    async def measure(self):
        gate = asyncio.Event()
        ready = asyncio.Queue()

        async def caller(worker):
            scheduled = sorted((row for row in self.loaded["corpus"]["schedule"] if row["caller_id"] == worker), key=lambda row: row["ordinal"])
            self.callers[worker] = {"id": worker, "ready_ms": (time.monotonic_ns() - self.started_ns) / 1e6}
            await ready.put(worker)
            try:
                await gate.wait()
                for row in scheduled:
                    await self.request(row)
            finally:
                self.callers[worker]["finished_ms"] = (time.monotonic_ns() - self.started_ns) / 1e6

        self.started_ns = time.monotonic_ns()
        tasks = [asyncio.create_task(caller(worker)) for worker in self.profile.caller_ids()]
        try:
            async with asyncio.timeout(120):
                for _ in self.profile.caller_ids():
                    await ready.get()
                self.barrier_released_ms = (time.monotonic_ns() - self.started_ns) / 1e6
                gate.set()
                await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    def report(self):
        events = [(row[field], change) for row in self.rows for field, change in (("started_ms", 1), ("ended_ms", -1))]
        current = peak = 0
        for _, change in sorted(events):
            current += change
            peak = max(peak, current)
        sent = [(row[field], change) for row in self.rows if "client_write_complete_ms" in row
                for field, change in (("client_write_complete_ms", 1), ("ended_ms", -1))]
        current = physical_peak = 0
        for _, change in sorted(sent):
            current += change
            physical_peak = max(physical_peak, current)
        observation = {"format": "recall-workload-observation/v1", "measured": self.started_ns > 0,
            "source_commit": self.manifest["source_commit"], "artifact_sha256": self.manifest["artifact_sha256"],
            "binary_sha256": self.manifest["binary_sha256"], "backend": self.manifest["backend"], "endpoint": self.manifest["endpoint"],
            "workload_contract_sha256": sha(json.dumps(self.loaded["contract"], sort_keys=True, separators=(",", ":"), allow_nan=False).encode()),
            "contract_sha256": self.manifest["contract_sha256"], "corpus_sha256": self.manifest["corpus_sha256"],
            "prepared_fixture_sha256": self.manifest["prepared_fixture_sha256"],
            **{key: self.manifest[key] for key in ("provider_phase", "nonce", "preparation_contract_sha256", "typed_delta_sha256", "provider_allocation_sha256", "rendered_map_sha256")},
            "callers": [self.callers[key] for key in sorted(self.callers)], "barrier_released_ms": self.barrier_released_ms,
            "peak_callers": self.peak_callers, "peak_in_flight": peak, "peak_sent_waiting_response": physical_peak,
            "overlap_definition": "client request lifetimes and write-complete/response-complete intervals; not server execution concurrency",
            "sources_per_space": {space: len({row["key"] for row in self.loaded["corpus"]["sources"] if row["space"] == space})
                                  for space in self.loaded["contract"]["spaces"]},
            "source_count_origin": "independently qualified preparation receipt and frozen corpus; no invented live inventory",
            "requests": sorted(self.rows, key=lambda row: (row["caller_id"], row["ordinal"])),
            "product_acceptance": False, "fallback_executed": False, "retries": 0,
            "provider_cohort_begin": self.provider_cohort_begin,
            "provider_request_association": "unmet; exclusive cohort establishes totals only"}
        if self.profile.id == profiles.SEMANTIC140_ID:
            observation["profile_id"] = self.profile.id
        try:
            observation["independent_verdict"] = self.adapter.evaluate_cohort(observation)
        except Exception as error:
            observation["independent_verdict"] = {"verdict": "failed", "error_type": type(error).__name__}
        try:
            with helpers.capture.total_deadline(130):
                observation["provider_accounting"] = self.bridge.provider_accounting(
                    self.manifest, observation, self.loaded["provider_allocation"])
            measurement_close(self.provider_cohort_begin, observation["provider_accounting"], profile_id=self.profile.id)
        except Exception as error:
            observation["provider_accounting"] = {"verdict": "unmet", "error_type": type(error).__name__}
        observation["collector_verdict"] = "passed" if all(
            observation[key].get("verdict") == "passed" for key in ("independent_verdict", "provider_accounting")) else "failed"
        return observation

    async def run(self):
        failure = None
        try:
            # All local input/credential checks have completed. G must durably
            # admit the exact exclusive cohort before the first network send.
            # Its terminal function also runs after a refused/partial begin so
            # an original durable hold cannot silently become a fresh budget.
            with helpers.capture.total_deadline(10):
                begin = self.bridge.authorize_provider_allocation(
                    self.manifest, self.loaded["contract"], self.loaded["provider_allocation"])
            measurement_begin(self.manifest, begin)
            self.provider_cohort_begin = begin
            await self.measure()
        except (Exception, asyncio.CancelledError) as error:
            failure = type(error).__name__
        observation = self.report()
        if failure:
            observation["run_error_type"] = failure
            observation["independent_verdict"] = {"verdict": "failed"}
            observation["collector_verdict"] = "failed"
        raw = canonical(observation)
        # The historical standalone collector retains its original row bound;
        # the closed successor also bounds its combined observation before write.
        if self.profile.id == profiles.SEMANTIC140_ID:
            require(len(raw) <= self.profile.observation_bytes, "combined observation budget exceeded")
        helpers.private_new(self.root / "observation.json", raw)
        return 0 if observation["collector_verdict"] == "passed" else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("inventory")
    run = commands.add_parser("run")
    run.add_argument("--manifest", type=Path, required=True)
    run.add_argument("--execute-reviewed", action="store_true", required=True)
    args = parser.parse_args()
    if args.command == "inventory":
        print(canonical(inventory()).decode())
        return 0
    require(sys.version_info >= (3, 11), "async absolute request deadlines require Python 3.11+")
    return asyncio.run(Workload(*reviewed_inputs(args.manifest)).run())


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print(canonical({"verdict": "unmet-prerequisite", "error_type": type(error).__name__, "product_acceptance": False}).decode(), file=sys.stderr)
        sys.exit(78)
