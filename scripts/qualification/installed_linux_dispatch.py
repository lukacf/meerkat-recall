#!/usr/bin/env python3
"""Optional installed loopback dispatch packet; never model/relevance acceptance.

Source preparation only until a separate D review and root execution allocation.
The exact physical count (three per backend) predates any candidate observations.
"""
from __future__ import annotations

import argparse
import copy
import datetime as dt
import http.server
import importlib.util
from pathlib import Path
import re
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("installed_linux_core", Path(__file__).with_name("installed_linux_offline.py"))
core = importlib.util.module_from_spec(spec)
spec.loader.exec_module(core)
CASES = ROOT / "tests/recall_preview_semantics/installed_linux_dispatch_cases.json"


def plan():
    result = core.plan()
    result["format"] = "recall-installed-linux-dispatch-plan/v1"
    result["expectations"] = core.read_json(CASES)
    result["expectations_sha256"] = core.file_digest(CASES)
    result["core_driver_sha256"] = result["driver_sha256"]
    result["driver_sha256"] = core.file_digest(Path(__file__))
    core.require(result["expectations"]["spaces"] == [row["id"] for row in core.fixtures.corpus()["spaces"]],
                 "dispatch spaces differ from frozen corpus")
    for space, query_id in zip(result["expectations"]["spaces"], result["expectations"]["queries"]):
        core.require(sum(row["space"] == space and row["id"] == query_id for row in core.fixtures.corpus()["queries"]) == 1,
                     "original dispatch query is absent or ambiguous")
    return result


def validate_manifest(path, now=None):
    manifest = core.read_json(path)
    core.require(manifest.get("format") == "recall-installed-linux-dispatch-run/v1"
                 and manifest.get("execution_authorized") is True, "dispatch execution is not allocated")
    core.require(manifest.get("backend") in ("elephant-db", "surreal-db"), "explicit native backend required")
    for key, length in (("source_commit", 40), ("archive_sha256", 64), ("binary_sha256", 64), ("expectation_review_sha256", 64)):
        core.require(isinstance(manifest.get(key), str) and re.fullmatch(f"[0-9a-f]{{{length}}}", manifest[key]),
                     f"exact trusted {key} required")
    core.require((now or dt.datetime.now(dt.timezone.utc)) < core.utc(manifest["expires_at"]), "allocation expired")
    for key in ("archive", "run_directory", "expectation_review"):
        core.require(Path(manifest[key]).is_absolute(), f"{key} must be absolute")
    review_path = Path(manifest["expectation_review"])
    core.require(core.file_digest(review_path) == manifest["expectation_review_sha256"], "D review digest differs")
    review = core.read_json(review_path)
    core.require(review.get("format") == "recall-installed-linux-dispatch-review/v1"
                 and review.get("reviewer") == "qualification" and review.get("approved") is True,
                 "independent dispatch expectations are not approved")
    inventory = plan()
    for key in ("expectations_sha256", "corpus_sha256", "driver_sha256", "core_driver_sha256", "collector_sha256", "installer_sha256"):
        core.require(review.get(key) == inventory[key], f"reviewed {key} differs")
    return manifest, inventory


def credential_reference(space, generation):
    suffix = "HOUSEHOLD" if space == "qualification-household" else "BUSINESS"
    core.require(space in ("qualification-household", "qualification-business") and generation in (1, 2),
                 "unknown synthetic credential")
    return f"INSTALLED_DISPATCH_SPY_{suffix}_{generation}"


def synthetic_credential(space, generation):
    return "synthetic-loopback-only-" + credential_reference(space, generation)


def processing_config(space, endpoint):
    name = space["id"]
    profile = name + "-profile"
    return {"generation": 1, "index_generation": 0, "policy_id": space["policy_id"],
        "connections": {"connection": {"id": "connection", "provider": "openai", "account_id": name + "-spy-account",
            "endpoint": endpoint, "credential_ref": credential_reference(name, 1), "credential_generation": 1, "revoked": False}},
        "profiles": {profile: {"id": profile, "connection_id": "connection", "model": name + "-spy-model",
            "model_version": "controlled-transport-v1", "dimensions": space["dimensions"], "preprocessing": "text-v1"}},
        "bindings": {operation: {"id": name + "-" + operation, "profile_id": profile, "revoked": False}
                     for operation in ("query_embedding", "document_embedding")},
        "active_embedding_profile_id": profile, "pending_reindex": None}


class TransportSpy:
    """Two owned listeners, one frozen chronology; no guessed retrieval winners."""
    def __init__(self, owner):
        self.owner = owner
        self.lock = threading.Lock()
        self.requests = []
        self.failed = False
        self.listeners = []
        self.threads = []
        self.endpoints = {}
        self.connections = set()

    def observe(self, space, method, path, headers, raw, framing_complete=True):
        with self.lock:
            sequence = len(self.requests)
            auth = headers.get_all("Authorization", [])
            known = {"Bearer " + synthetic_credential(row["space"], row["credential_generation"])
                     for row in self.owner.cases["expected_outbound"]}
            receipt = {"sequence": sequence + 1, "space": space, "method": method, "path": path,
                "authorization": auth if all(value in known for value in auth) else ["<unexpected credential redacted>"],
                "authorization_sha256": core.digest(core.encoded(auth)),
                "raw_body_base64": core.base64.b64encode(raw).decode(), "body_sha256": core.digest(raw),
                "framing_complete": framing_complete, "monotonic_seconds": time.monotonic(),
                "classification": "controlled transport spy; no semantic verdict"}
            self.requests.append(receipt)
            try:
                core.require(sequence < self.owner.cases["limits"]["physical_embedding_calls"], "extra physical embedding attempt")
                expected = self.owner.cases["expected_outbound"][sequence]
                core.require(framing_complete and method == "POST" and path == "/v1/embeddings", "wrong spy transport or framing")
                core.require(space == expected["space"], "cross-space outbound order")
                core.require(auth == ["Bearer " + synthetic_credential(space, expected["credential_generation"])],
                             "wrong space or rotated credential")
                query_id = self.owner.cases["queries"][self.owner.cases["spaces"].index(space)]
                query = next(row["query"] for row in core.fixtures.corpus()["queries"] if row["id"] == query_id)
                dims = next(row["dimensions"] for row in core.fixtures.corpus()["spaces"] if row["id"] == space)
                body = core.capture.strict_json_body(raw)
                core.require(body == {"model": space + "-spy-model", "input": [query], "dimensions": dims},
                             "original query/model/dimensions differ at physical edge")
                # Shape-valid, finite, nonzero and deliberately meaningless.
                response = {"object": "list", "model": body["model"], "data": [
                    {"index": 0, "object": "embedding", "embedding": [1.0] + [0.0] * (dims - 1)}],
                    "usage": {"prompt_tokens": 1, "total_tokens": 1}}
                receipt["valid"] = True
                return response
            except Exception:
                self.failed = True
                receipt["valid"] = False
                raise
            finally:
                self.owner.save(f"spy-{sequence + 1:03d}.json", core.encoded(receipt))

    def start(self):
        spy = self
        for selected_space in self.owner.cases["spaces"]:
            class Handler(http.server.BaseHTTPRequestHandler):
                def setup(self):
                    self.request.settimeout(spy.owner.cases["limits"]["spy_request_seconds"])
                    super().setup()
                    self.dispatched = False
                    with spy.lock:
                        spy.connections.add(self.request)

                def finish(self):
                    try:
                        super().finish()
                    finally:
                        with spy.lock:
                            spy.connections.discard(self.request)
                            if not self.dispatched:
                                spy.failed = True

                def log_message(self, *_args):
                    pass

                def dispatch(self):
                    self.dispatched = True
                    raw = b""
                    complete = False
                    try:
                        lengths = self.headers.get_all("Content-Length", [])
                        bound = spy.owner.cases["limits"]["spy_request_bytes"]
                        if len(lengths) == 1 and re.fullmatch(r"[0-9]+", lengths[0]) and not self.headers.get_all("Transfer-Encoding", []):
                            length = int(lengths[0])
                            if length <= bound:
                                deadline = time.monotonic() + spy.owner.cases["limits"]["spy_request_seconds"]
                                try:
                                    while len(raw) < length:
                                        remaining = deadline - time.monotonic()
                                        if remaining <= 0:
                                            break
                                        self.request.settimeout(remaining)
                                        chunk = self.rfile.read1(min(8192, length - len(raw)))
                                        if not chunk:
                                            break
                                        raw += chunk
                                    complete = len(raw) == length
                                except OSError:
                                    complete = False
                        reply = spy.observe(self.server.selected_space, self.command, self.path, self.headers, raw, complete)
                        status = 200
                    except Exception:
                        # A failing transport is never retried into an acceptance.
                        status, reply = 400, {"error": {"message": "controlled transport fixture rejected request"}}
                        spy.failed = True
                    payload = core.encoded(reply)
                    try:
                        self.send_response(status)
                        self.send_header("Content-Type", "application/json")
                        self.send_header("Content-Length", str(len(payload)))
                        self.send_header("Connection", "close")
                        self.end_headers()
                        self.wfile.write(payload)
                    except OSError:
                        spy.failed = True

                do_POST = do_GET = do_PUT = do_DELETE = do_PATCH = do_HEAD = do_OPTIONS = dispatch

            listener = http.server.HTTPServer(("127.0.0.1", 0), Handler)
            listener.selected_space = selected_space
            self.listeners.append(listener)
            self.endpoints[selected_space] = f"http://127.0.0.1:{listener.server_port}/v1"
            thread = threading.Thread(target=listener.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
            thread.start()
            self.threads.append(thread)

    def count(self):
        with self.lock:
            core.require(not self.failed, "controlled transport failed")
            return len(self.requests)

    def close(self):
        with self.lock:
            connections = list(self.connections)
        for connection in connections:
            try:
                connection.shutdown(core.socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()
        for listener in self.listeners:
            listener.shutdown()
            listener.server_close()
        for thread in self.threads:
            thread.join(timeout=3)
        core.require(not any(thread.is_alive() for thread in self.threads), "owned spy listener did not stop")


class DispatchDriver(core.Driver):
    process_capture_manifest_format = "recall-installed-linux-dispatch-run/v1"
    process_capture_cases = CASES

    def __init__(self, manifest, inventory, isolation, *, original_manifest_raw=None):
        super().__init__(manifest, inventory, isolation, original_manifest_raw=original_manifest_raw)
        self.capture_lock = threading.Lock()
        self.spy = TransportSpy(self)
        self.report["format"] = "recall-installed-linux-dispatch-observation/v1"

    def save(self, name, raw):
        with self.capture_lock:
            super().save(name, raw)

    def stored_config(self, space, expected):
        result = self.http("GET", f"/v1/spaces/{space['id']}", "service", space["id"])
        core.require(result["status"] == 200, "stored approved profile unavailable")
        body = result["body"]
        core.require(body["space_id"] == space["id"] and isinstance(body["id"], str), "stored profile space differs")
        core.require(body["security"] == {"policy_id": space["policy_id"], "level": space["level"], "labels": [], "handling": []},
                     "stored profile security differs")
        core.require(body["processing"] == expected, "stored processing authority differs")
        core.require(body["embedding_provider"] == "openai" and body["embedding_model"] == space["id"] + "-spy-model"
                     and body["embedding_dims"] == space["dimensions"], "stored legacy profile projection differs")
        return body

    def replace(self, space, current, change):
        self.stored_config(space, current)
        updated = copy.deepcopy(current)
        updated["generation"] += 1
        change(updated)
        result = self.http("PUT", f"/v1/spaces/{space['id']}", self.bootstrap_role(space["id"]), space["id"], {"processing": updated})
        core.require(result["status"] == 200, "approved configuration CAS failed")
        self.stored_config(space, updated)
        return updated

    def query(self, space, token, *, rejected=False):
        index = self.cases["spaces"].index(space)
        query = next(row["query"] for row in core.fixtures.corpus()["queries"] if row["id"] == self.cases["queries"][index])
        arguments = {"query": query, "use_vector": True, "limit": 1}
        if rejected:
            return self.denied_tool("search_entities", arguments, token=token, space=space,
                                    code="provider_unapproved")
        result = self.tool("search_entities", arguments, token=token, space=space)
        core.require(result["items"] == [], "empty-space transport control invented an entity")
        return result

    def execute(self):
        try:
            self.execute_cases()
        finally:
            # Cleanup is inside the core runner's failure/receipt boundary.
            try:
                self.stop(require_clean=False)
            finally:
                try:
                    self.spy.close()
                finally:
                    self.report["physical_embedding_attempts"] = len(self.spy.requests)
                    self.report["spy_capture_failed"] = self.spy.failed

    def execute_cases(self):
        with core.capture.total_deadline(self.remaining()):
            manifest = core.artifact.verify(Path(self.manifest["archive"]), self.manifest["archive_sha256"], self.root / "installed")
        self.report["artifact_manifest"] = manifest
        core.require(manifest["target"] == "x86_64-unknown-linux-gnu" and manifest["source_commit"] == self.manifest["source_commit"],
                     "installed target/source differs")
        core.require(manifest["binary_sha256"] == self.manifest["binary_sha256"] == core.file_digest(self.binary), "installed binary differs")
        prefix = self.process(["--version"])
        core.require((self.root / "captures" / f"{prefix}.stdout").read_text().strip() == f"meerkat-recall {manifest['version']}",
                     "installed version differs")
        self.record("verified_installed_binary")
        self.credentials()
        for row in self.cases["expected_outbound"]:
            self.environment[credential_reference(row["space"], row["credential_generation"])] = synthetic_credential(row["space"], row["credential_generation"])
        self.spy.start()
        self.launch()
        self.rpc("operator", None, "initialize", {"protocolVersion": "2025-03-26", "capabilities": {},
            "clientInfo": {"name": "installed-dispatch-spy", "version": "1"}})
        if self.manifest["backend"] == "elephant-db":
            status = self.tool("initialize_runtime", {"backend": "elephant-db", "ingest_enabled": False,
                "extractor_required": False, "snapshot_path": str(self.root / "service/state.snapshot"),
                "multimodal_asset_root": str(self.root / "media")})
        else:
            self.stop()
            self.environment.update({"ELEPHANT_DB": "surreal-db", "SURREALDB_URL": "rocksdb://" + str(self.root / "service/native-store"),
                "SURREALDB_NAMESPACE": "installed_dispatch", "SURREALDB_DATABASE": "isolated_fixture",
                "SURREALDB_USER": "root", "SURREALDB_PASS": "synthetic-offline-only"})
            self.process(["migrate"])
            self.launch()
            status = self.tool("configure_runtime", {"ingest_enabled": False, "extractor_required": False,
                "multimodal_asset_root": str(self.root / "media")})
        core.require(status["configured"] and status["storage_ready"] and status["backend"] == self.manifest["backend"],
                     "actual requested runtime is unavailable")
        spaces = core.fixtures.corpus()["spaces"]
        configurations = {}
        for space in spaces:
            name = space["id"]
            processing = processing_config(space, self.spy.endpoints[name])
            created = self.http("POST", "/v1/spaces", self.bootstrap_role(name), name, {"name": name,
                "security": {"policy_id": space["policy_id"], "level": space["level"], "labels": [], "handling": []},
                "embedding_provider": "openai", "embedding_model": name + "-spy-model", "embedding_dims": space["dimensions"],
                "purpose": space["extraction_purpose"], "skill": space["extraction_skill"], "processing": processing})
            core.require(created["status"] == 201, "approved spy space creation failed")
            for role in (["service", "household"] if name == self.cases["spaces"][0] else ["service"]):
                grant = self.http("PUT", f"/v1/spaces/{name}/grants/installed-{role}", self.bootstrap_role(name), name,
                    {"active": True, "scopes": self.data_scopes, "expected_generation": None})
                core.require(grant["status"] == 200, "actual spy service grant missing")
            configurations[name] = processing
            self.stored_config(space, processing)
        self.record("two_distinct_profiles", processing=configurations)
        core.require(self.spy.count() == 0, "configuration or bootstrap dispatched a provider")
        home, business = self.cases["spaces"]
        self.query(home, "household")
        core.require(self.spy.count() == 1, "household positive did not dispatch exactly once")
        self.query(business, "service")
        core.require(self.spy.count() == 2, "business positive did not dispatch exactly once")
        self.record("approved_dispatch")
        business_query = next(row["query"] for row in core.fixtures.corpus()["queries"] if row["id"] == self.cases["queries"][1])
        self.denied_tool("search_entities", {"query": business_query, "use_vector": True, "limit": 1},
                         token="household", space=business, code="forbidden_space")
        core.require(self.spy.count() == 2, "wrong-space request dispatched")
        self.record("wrong_space_zero_dispatch")
        current = configurations[home]
        original_binding = copy.deepcopy(current["bindings"]["query_embedding"])
        current = self.replace(spaces[0], current, lambda value: value["bindings"].pop("query_embedding"))
        self.query(home, "household", rejected=True)
        core.require(self.spy.count() == 2, "missing approval dispatched")
        self.record("missing_approval_zero_dispatch")
        current = self.replace(spaces[0], current, lambda value: value["bindings"].update(query_embedding=copy.deepcopy(original_binding)))
        current = self.replace(spaces[0], current, lambda value: value["bindings"]["query_embedding"].update(revoked=True))
        self.query(home, "household", rejected=True)
        core.require(self.spy.count() == 2, "revoked approval dispatched")
        self.record("revoked_approval_zero_dispatch")
        current = self.replace(spaces[0], current, lambda value: value["bindings"].update(query_embedding=copy.deepcopy(original_binding)))
        before = copy.deepcopy(current)
        current = self.replace(spaces[0], current, lambda value: value["connections"]["connection"].update(
            credential_ref=credential_reference(home, 2), credential_generation=2))
        core.require(current["profiles"] == before["profiles"] and current["bindings"] == before["bindings"], "credential rotation changed profile identity")
        self.query(home, "household")
        core.require(self.spy.count() == 3, "rotated credential did not dispatch exactly once")
        self.stored_config(spaces[1], configurations[business])
        self.record("credential_rotation_distinct", before=before, after=current)
        self.stop()
        self.spy.close()
        core.require(self.spy.count() == 3 and core.file_digest(self.binary) == self.manifest["binary_sha256"], "final dispatch custody differs")
        self.report["physical_embedding_calls"] = self.spy.count()
        core.require({row["case"] for row in self.report["cases"]} == set(self.cases["required_cases"]), "dispatch cases missing")
        self.report["subset_observation"] = "passed; requires independent D transport-only verdict"

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("plan")
    run = commands.add_parser("run")
    run.add_argument("--manifest", type=Path, required=True)
    run.add_argument("--execute-reviewed", action="store_true", required=True)
    run.add_argument("--parent-network-namespace", required=True)
    args = parser.parse_args()
    if args.command == "plan":
        print(core.encoded(plan()).decode())
        return 0
    core.require(sys.version_info >= (3, 12), "artifact installation requires Python 3.12+")
    manifest, inventory = validate_manifest(args.manifest)
    isolation = core.offline_namespace(args.parent_network_namespace)
    core.enable_loopback()
    return DispatchDriver(manifest, inventory, isolation,
        original_manifest_raw=core.process_capture.read_bounded(args.manifest, 1024 * 1024)).run()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print(core.encoded({"status": "unmet-prerequisite", "error_type": type(error).__name__, "product_acceptance": False}).decode(), file=sys.stderr)
        sys.exit(78)
