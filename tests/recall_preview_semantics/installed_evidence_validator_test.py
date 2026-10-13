"""Independent literal transcript controls; never installed product acceptance."""
import base64
import copy
import datetime as dt
import hashlib
import importlib.util
import json
from pathlib import Path
import unittest

HERE = Path(__file__).parent
ROOT = HERE.parents[1]
SPEC = importlib.util.spec_from_file_location("installed_oracle", HERE / "installed_evidence_validator.py")
ORACLE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ORACLE)
CASE_RAW = (HERE / "installed_linux_cases.json").read_bytes()
CORPUS_RAW = (ROOT / "tests/recall_preview/corpus.json").read_bytes()
CASE = json.loads(CASE_RAW)
CORPUS = json.loads(CORPUS_RAW)
SCOPES = ["read:status", "read:space_config", "write:space_config", "read:source", "write:source",
          "read:doc:raw", "read:doc:metadata", "read:entity", "manage:grant", "manage:provider"]


def wire(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def stat(pid, ppid, group, start, state="S"):
    return (f"{pid} (literal fixture) " + " ".join([state, str(ppid), str(group), str(group)]
            + ["0"] * 15 + [str(start)]) + "\n").encode()


class Transcript:
    """Literal original-wire fixture independent from producer and oracle code."""
    def __init__(self, backend="elephant-db"):
        self.raw, self.files, self.http, self.processes, self.proofs = {}, {}, [], [], []
        self.now, self.deadline = 10_000_000_000, 310_000_000_000
        self.root, self.sequence = "/offline/original", 0
        self.last_request = self.last_process = 0
        self.identity = {"source_commit": "a" * 40, "artifact_sha256": "d" * 64,
                         "binary_sha256": "b" * 64, "backend": backend}
        self.controller_identity = {"pid": 41, "ppid": 40, "pgrp": 41, "session": 41, "start_ticks": 100}
        self.owner = {"format": "recall-installed-controller/v2", "identity": self.controller_identity,
                      "observed_ns": self.now, "deadline_ns": self.deadline, "wall_time_ns": 1791330000000000000,
                      "network_namespace": "net:[101]", "executable": {"sha256": "e" * 64}, "captures": {}}
        originals = {"before": (stat(41, 40, 41, 100), "process-stat"),
                     "after": (stat(41, 40, 41, 100), "process-stat"),
                     "network_dev": (b"Inter-| Receive\n face | bytes\n lo: 0 0\n", "network-dev"),
                     "network_ipv4": (b"Iface Destination Gateway\n", "network-route"),
                     "network_ipv6": (b"", "network-ipv6-route"),
                     "boot_id": (b"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee\n", "boot-id")}
        for name, (raw, kind) in originals.items():
            self.owner["captures"][name] = self.capture("owner-" + name, raw, kind)
        self.pins = {name + "_sha256": str(index) * 64 for index, name in enumerate(
            ("driver", "process_capture", "collector", "installer"), 1)}
        self.plan = {"format": "recall-installed-linux-plan/v1", "expectations": CASE,
                     "expectations_sha256": sha(CASE_RAW), "corpus_sha256": sha(CORPUS_RAW),
                     **self.pins, "execution": "not authorized by a plan", "product_acceptance": False}
        self.offline_review = {"format": "recall-installed-linux-review/v1", "approved": True,
                               "reviewer": "qualification", **self.pins,
                               "expectations_sha256": sha(CASE_RAW), "corpus_sha256": sha(CORPUS_RAW)}
        self.run = {"format": "recall-installed-linux-run/v1", "execution_authorized": True,
                    "backend": backend, "source_commit": "a" * 40, "archive_sha256": "d" * 64,
                    "binary_sha256": "b" * 64, "expectation_review_sha256": sha(wire(self.offline_review)),
                    "expires_at": "2026-10-07T20:11:00Z", "run_directory": self.root}
        self.owner["allocation_clock"] = {"mono_before_ns": self.now, "mono_after_ns": self.now,
            "wall_ns": self.owner["wall_time_ns"], "total_seconds": 300,
            "expires_wall_ns": int(dt.datetime(2026, 10, 7, 20, 11, tzinfo=dt.timezone.utc).timestamp()) * 1_000_000_000}
        self.owner["wall_sample_before_ns"] = self.now
        self.owner["wall_sample_after_ns"] = self.now
        self.artifact = {"format": "meerkat-recall-native/v1", "target": "x86_64-unknown-linux-gnu",
                         "source_commit": "a" * 40, "binary_sha256": "b" * 64, "version": "0.1.0"}

    def tick(self):
        self.now += 1_000_000
        return self.now

    def reference(self, raw, kind):
        result = {"sha256": sha(raw), "bytes": len(raw), "kind": kind}
        self.raw[result["sha256"]] = raw
        return result

    def capture(self, name, raw, kind):
        path = self.root + "/captures/" + name
        reference = self.reference(raw, kind)
        self.files[path] = reference
        return {"path": path, "ref": reference}

    def event(self, process, action, data):
        snapshot_time = self.tick()
        self.sequence += 1
        original = {"format": "recall-installed-process-event/v2", "sequence": self.sequence,
                    "process": process["index"], "action": action, "data": copy.deepcopy(data), "observed_ns": self.tick()}
        capture = self.capture(f"event-{self.sequence:04}.json", wire(original), "installed-process-event")
        process["events"].append({"action": action, "observed_ns": snapshot_time, "data": copy.deepcopy(data), "capture": capture})

    def process(self, args, live=False, stdout=b"", stderr=b"", inspection=False):
        index = len(self.processes) + 1
        paths = [self.root + f"/captures/p{index}.{suffix}" for suffix in ("stdout", "stderr")]
        intent = {"argv": [self.root + "/installed/meerkat-recall", *args], "cwd": self.root + "/service",
                  "environment_names": ["HOME", "PATH"], "started_ns": self.tick(), "deadline_ns": self.deadline,
                  "log_paths": paths}
        if inspection:
            intent["streams"] = [
                {"path": paths[0], "kind": "snapshot-inspection", "maximum_bytes": 33554433, "reserved_bytes": 33554433},
                {"path": paths[1], "kind": "process-log", "maximum_bytes": 1048576, "reserved_bytes": 1048576},
            ]
        binary = {"path": intent["argv"][0], "sha256": "b" * 64, "device": 1, "inode": 2,
                  "bytes": 100, "mode": 33261, "mtime_ns": 3, "ctime_ns": 4}
        identity = {"pid": 100 + index, "ppid": 41, "pgrp": 100 + index, "session": 100 + index, "start_ticks": 200 + index}
        process = {"index": index, "intent": intent, "launch_binary": binary, "identity": identity,
                   "events": [], "errors": [], "exit_observed": None, "exit_code": None,
                   "reaped": False, "reaped_ns": None,
                   "logs": [{"path": path, "ref": None, "complete": False, "closure": None} for path in paths]}
        self.processes.append(process)
        self.event(process, "intent", intent)
        self.event(process, "launch-binary", binary)
        self.event(process, "popen-requested", {"initialization_deadline_ns": self.deadline})
        raw = stat(identity["pid"], 41, identity["pid"], identity["start_ticks"])
        before = self.capture(f"p{index}-before.stat", raw, "process-stat")
        after = self.capture(f"p{index}-after.stat", raw, "process-stat")
        self.event(process, "launched", {"identity": identity, "stat": {"before": before, "after": after},
                                         "executable": {**binary, "target": binary["path"]}, "executable_error": None})
        if not live:
            self.finish(process, stdout, stderr)
        return process

    def finish(self, process, stdout=b"", stderr=b""):
        exited = {"code": 0, "waitid_status": 0, "waitid_kind": 1, "observed_ns": self.tick()}
        process["exit_observed"] = exited
        self.event(process, "exit-observed", exited)
        for index, (log, raw) in enumerate(zip(process["logs"], (stdout, stderr))):
            closure = {"eof": True, "error_type": None, "closed_ns": self.tick()}
            kind = "snapshot-inspection" if index == 0 and "streams" in process["intent"] else "process-log"
            reference = self.reference(raw, kind)
            self.files[log["path"]] = reference
            log.update(ref=reference, complete=True, closure=closure)
            self.event(process, "log-closed", {"path": log["path"], **closure})
        process.update(reaped=True, exit_code=0, reaped_ns=self.tick())
        self.event(process, "reaped", {"code": 0, "reaped_ns": process["reaped_ns"]})

    def request(self, method, path, role=None, space=None, request=None, status=200, response=None, absent=False):
        start = self.tick()
        raw = b"" if absent else wire(response)
        row = {"method": method, "path": path, "space": space, "credential_role": role, "request": copy.deepcopy(request),
               "status": None if absent else status, "raw_body_base64": base64.b64encode(raw).decode(),
               "body_sha256": sha(raw), "raw_body_complete": not absent,
               "started_monotonic_ns": start, "finished_monotonic_ns": self.tick(),
               "deadline_monotonic_ns": start + 5_000_000_000, "duration_ms": 1.0}
        if absent:
            row["transport_error_type"] = "ConnectionRefusedError"
        self.http.append(row)
        return row

    def rpc(self, method, params, result, role="service", space=None):
        request_id = len(self.http) + 1
        return self.request("POST", "/mcp/full", role, space,
                            {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params},
                            response={"jsonrpc": "2.0", "id": request_id, "result": result})

    def tool(self, name, arguments, result, role="operator", space=None, error=False):
        return self.rpc("tools/call", {"name": name, "arguments": arguments},
                        {"content": [{"type": "text", "text": wire(result).decode()}], "isError": error}, role, space)

    def case(self, name):
        proof = {"case": name, "observed_ns": self.tick(),
                            "request_indices": list(range(self.last_request + 1, len(self.http) + 1)),
                            "process_indices": list(range(self.last_process + 1, len(self.processes) + 1))}
        self.proofs.append(proof)
        self.sequence += 1
        self.capture(f"event-{self.sequence:04}.json", wire({"format": "recall-installed-process-event/v2",
                     "sequence": self.sequence, "process": None, "action": "case-observed", "data": proof,
                     "observed_ns": self.tick()}), "installed-process-event")
        self.last_request, self.last_process = len(self.http), len(self.processes)

    def denied(self, method, path, role=None, space=None, code="forbidden_space", status=403, request=None):
        return self.request(method, path, role, space, request, status,
                            {"error_code": code, "message": "literal denied", "retryable": False})

    def populate(self):
        self.process(["--version"], stdout=b"meerkat-recall 0.1.0\n")
        self.case("verified_installed_binary")
        self.request("GET", "/v1/health", absent=True)
        self.case("absent_service")
        self.process(["service-token", "bootstrap", "--secret-file", "/offline/key"])
        for role in ("operator", "service", "household", *["bootstrap-" + name for name in CASE["spaces"]]):
            scopes = SCOPES
            if role == "operator":
                extra = ["--global-admin"]
                scopes = ["manage:grant", "manage:provider", "read:space_config", "write:space_config",
                          "read:status", "read:system", "write:system"]
            elif role.startswith("bootstrap-"):
                extra = ["--bootstrap-space", role.removeprefix("bootstrap-"), "--clearances-file", "/offline/clearance"]
                scopes = ["read:space_config", "write:space_config", "manage:grant", "manage:provider"]
            else:
                extra = ["--clearances-file", "/offline/clearance"]
                for name in CASE["spaces"][:1 if role == "household" else 2]:
                    extra += ["--space", name]
            for scope in scopes:
                extra += ["--scope", scope]
            self.process(["service-token", "issue", "--secret-file", "/offline/key", "--subject", "installed-" + role,
                          "--expires-in", "900", "--issuer", "installed-offline", "--audience", "recall-installed",
                          "--output", "/offline/" + role + ".token", *extra])
        self.case("fresh_cli_auth")
        serve_args = ["serve", "--server-only", "--shutdown-timeout-seconds", "60"]
        service = self.process(serve_args, live=True)
        self.request("GET", "/v1/health", response={"status": "ok"})
        self.request("GET", "/v1/readiness", "operator", status=503, response={"configured": False, "ready": False})
        self.case("bootstrap_unready")
        if self.identity["backend"] == "surreal-db":
            self.finish(service)
            self.process(["migrate"])
            service = self.process(serve_args, live=True)
        self.tool("initialize_runtime" if self.identity["backend"] == "elephant-db" else "configure_runtime",
                  {"ingest_enabled": False, "extractor_required": False},
                  {"configured": True, "storage_ready": True, "backend": self.identity["backend"]})
        self.case("runtime_initialized_without_workers")
        configs = {}
        for space in CORPUS["spaces"]:
            name = space["id"]
            config = {"space_id": name, "id": "space_config:" + name, "updated_at": "original-time",
                      "security": {"policy_id": space["policy_id"], "level": space["level"], "labels": [], "handling": []},
                      "embedding_provider": "openai", "embedding_model": "installed-unapproved-" + name,
                      "embedding_dims": space["dimensions"], "purpose": space["extraction_purpose"], "skill": space["extraction_skill"],
                      "processing": {"generation": 1, "index_generation": 0, "policy_id": space["policy_id"],
                                     "connections": {}, "profiles": {}, "bindings": {}, "active_embedding_profile_id": None, "pending_reindex": None}}
            configs[name] = config
            self.request("POST", "/v1/spaces", "bootstrap-" + name, name, {"name": name, **{key: value for key, value in config.items()
                         if key not in {"id", "space_id", "updated_at"}}}, 201, {})
            self.request("PUT", f"/v1/spaces/{name}/grants/installed-service", "bootstrap-" + name, name,
                         {"active": True, "scopes": SCOPES, "expected_generation": None}, response={"generation": "g1"})
            self.request("GET", "/v1/spaces/" + name, "service", name, response=config)
        self.case("two_named_spaces")
        home, business = CASE["spaces"]
        self.request("GET", f"/v1/spaces/{home}/status", "household", home, response={})
        self.denied("GET", f"/v1/spaces/{home}/status", status=401, code="unauthorized")
        self.case("http_auth_denied")
        self.denied("POST", "/mcp/full", space=home, status=401, code="unauthorized",
                    request={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}})
        self.case("mcp_auth_denied")
        self.denied("GET", f"/v1/spaces/{business}/status", "household", business)
        self.denied("GET", f"/v1/spaces/{business}/status", "service", home)
        self.case("wrong_space_denied")
        for space in (home, business):
            self.rpc("initialize", {"protocolVersion": "2025-03-26", "capabilities": {},
                     "clientInfo": {"name": "installed-two-space-client", "version": "1"}}, {}, space=space)
            self.rpc("tools/list", {}, {"tools": [{"name": "search_scoped"}]}, space=space)
        self.case("mcp_positive")
        grant = f"/v1/spaces/{home}/grants/installed-service"
        self.request("PUT", grant, "bootstrap-" + home, home, {"active": False, "scopes": SCOPES, "expected_generation": "g1"}, response={"generation": "g2"})
        self.denied("GET", f"/v1/spaces/{home}/status", "service", home)
        self.request("PUT", grant, "bootstrap-" + home, home, {"active": True, "scopes": SCOPES, "expected_generation": "g2"}, response={"generation": "g3"})
        self.case("grant_revocation")
        heads, operations = {}, {}
        for expected in CASE["sources"]:
            name, key = expected["space"], expected["key"]
            source = next(row for row in CORPUS["sources"] if row["space"] == name and row["key"] == key and row["revision"] == expected["baseline_revision"])
            path = "/v1/spaces/" + name + "/sources/" + key.replace("/", "%2F")
            request = {"revision": source["revision"], "text": source["text"], "media_type": "text/plain",
                       "source_url": "https://example.invalid/qualification", "observed_at": "2026-10-06T20:11:00Z",
                       "labels": {}, "expected_generation": None}
            receipt = {"desired_revision": source["revision"], "published_revision": None, "operation_id": "op-" + name,
                       "internal_source_id": "source-" + name, "generation": "generation-1"}
            self.request("PUT", path, "service", name, request, 202, receipt)
            self.request("PUT", path, "service", name, request, 202, receipt)
            heads[name] = {"desired_revision": source["revision"], "state": "pending", "generation": "generation-1"}
            operations[name] = {"id": receipt["operation_id"], "state": "pending"}
            self.request("GET", path + "/head", "service", name, response=heads[name])
            self.request("GET", "/v1/operations/" + receipt["operation_id"], "service", name, response=operations[name])
            query = next(row for row in CORPUS["queries"] if row["id"] == expected["query_id"])
            query_request = {"query": query["query"], "limit": query["k"], "ranking_mode": "lexical", "source_revision_constraints": {key: source["revision"]}}
            pending = {"partial": True, "results": [], "coverage": {key: {"state": "pending"}}}
            self.request("POST", f"/v1/spaces/{name}/search", "service", name, query_request, response=pending)
            self.tool("search_scoped", query_request, pending, "service", name)
        self.case("source_acceptance_and_idempotence")
        self.case("pending_is_not_complete")
        self.denied("GET", f"/v1/spaces/{business}/sources/business%2Foriole/head", "household", business)
        self.denied("POST", "/mcp/full", "household", business)
        self.denied("POST", f"/v1/spaces/{home}/search", "service", home, code="invalid_request", status=400,
                    request={"query": "original invalid-limit control", "limit": 51, "ranking_mode": "lexical"})
        self.case("input_bounds")
        self.request("POST", "/v1/system/snapshot/export", "operator", request={"path": "baseline", "name": "installed-baseline",
                     "retention_seconds": 3600, "quiesce_timeout_seconds": 30}, response={"success": True, "manifest": {"id": "registered-backup"}})
        self.case("named_export")
        for expected in CASE["sources"]:
            name, key = expected["space"], expected["key"]
            source = next(row for row in CORPUS["sources"] if row["space"] == name and row["key"] == key and row["revision"] == expected["replacement_revision"])
            request = {"revision": source["revision"], "text": source["text"], "media_type": "text/plain",
                       "source_url": "https://example.invalid/qualification", "observed_at": "2026-10-06T20:11:00Z",
                       "labels": {}, "expected_generation": "generation-1"}
            self.request("PUT", "/v1/spaces/" + name + "/sources/" + key.replace("/", "%2F"), "service", name, request, 202,
                         {"desired_revision": source["revision"], "published_revision": None})
        self.case("replacement_accepted")
        self.request("POST", "/v1/system/snapshot/restore", "operator", request={"path": "baseline"}, response={"success": True})
        def read_originals():
            for expected in CASE["sources"]:
                name, key = expected["space"], expected["key"]
                self.request("GET", "/v1/spaces/" + name + "/sources/" + key.replace("/", "%2F") + "/head", "service", name, response=heads[name])
                self.request("GET", "/v1/operations/op-" + name, "service", name, response=operations[name])
            for name, config in configs.items():
                self.request("GET", "/v1/spaces/" + name, "service", name, response=config)
        read_originals()
        self.case("restore_original_heads")
        self.finish(service)
        service = self.process(serve_args, live=True)
        read_originals()
        self.case("graceful_restart")
        self.tool("configure_runtime", {"runtime_mode": "read_only"}, {})
        self.rpc("tools/list", {}, {"tools": [{"name": "search_scoped"}]}, space=home)
        self.tool("retract_source", {}, {"error_code": "capability_disabled", "message": "readonly", "retryable": False}, "service", home, True)
        self.request("GET", f"/v1/spaces/{home}/sources/home%2Fheat/head", "service", home, response=heads[home])
        self.case("read_only_capabilities")
        self.finish(service)
        return self

    def seal(self):
        refs = {}
        for name, value, kind in (("installed_plan", self.plan, "installed-plan"),
                                   ("installed_review", self.offline_review, "installed-review"),
                                   ("installed_run_manifest", self.run, "installed-manifest")):
            refs[name] = self.capture(name + ".json", wire(value), kind)["ref"]
        refs["installed_cases"] = self.capture("original-cases.json", CASE_RAW, "installed-case-file")["ref"]
        refs["installed_corpus"] = self.capture("original-corpus.json", CORPUS_RAW, "installed-corpus")["ref"]
        copied = {"format": "recall-installed-reviewed-inputs/v2", "plan": self.plan,
                  "review": {"path": self.root + "/captures/installed_review.json", "ref": refs["installed_review"]},
                  "case_file": {"path": self.root + "/captures/original-cases.json", "ref": refs["installed_cases"]},
                  "corpus": {"path": self.root + "/captures/original-corpus.json", "ref": refs["installed_corpus"]}}
        carrier = {"format": "recall-installed-offline-evidence/v2", "identity": copy.deepcopy(self.identity)}
        carrier["reviewed_plan"] = self.capture("reviewed-inputs.json", wire(copied), "installed-reviewed-plan")["ref"]
        carrier["artifact_manifest"] = self.capture("artifact-provenance.json", wire(self.artifact), "installed-artifact-manifest")["ref"]
        carrier["controller"] = self.capture("controller.json", wire(self.owner), "installed-controller")["ref"]
        journal = {"format": "recall-installed-process-journal/v2", "processes": self.processes,
                   "attempted_processes": len(self.processes), "case_proofs": self.proofs, "terminal_observed_ns": self.tick(), "complete": True}
        carrier["command_inventory"] = self.capture("process-journal.json", wire(journal), "installed-process-journal")["ref"]
        cleanup = {"format": "recall-installed-process-cleanup/v2", "observed_ns": self.tick(), "remaining_child_pid": None,
                   "remaining_command_pids": [], "reaped_indices": list(range(1, len(self.processes) + 1)), "capture_complete": True,
                   "original_deadline_ns": self.deadline, "original_expires_at": self.run["expires_at"]}
        carrier["terminal_cleanup"] = self.capture("cleanup.json", wire(cleanup), "installed-cleanup")["ref"]
        for number, row in enumerate(self.http, 1):
            self.capture(f"http-{number:03}.json", wire(row), "installed-http")
        config = f'[runtime]\nmode="read_only"\ningest_enabled=false\n[storage]\nbackend="{self.identity["backend"]}"\n'.encode()
        configuration = self.capture("runtime-configuration.toml", config, "installed-runtime-configuration")
        observation = {"format": "recall-installed-linux-observation/v1", "product_acceptance": False,
                       "source_commit": "a" * 40, "archive_sha256": "d" * 64, "binary_sha256": "b" * 64,
                       "backend": self.identity["backend"], "plan": self.plan, "artifact_manifest": self.artifact,
                       "run_manifest_sha256": sha(wire(self.run)), "runtime_configuration_sha256": configuration["ref"]["sha256"],
                       "isolation": {"network_namespace": "net:[101]", "parent_network_namespace": "net:[100]"},
                       "cases": [{"case": proof["case"], "observation": "passed"} for proof in self.proofs],
                       "http_attempts": len(self.http), "processes": len(self.processes)}
        carrier["observation"] = self.reference(wire(observation), "installed-observation")
        inventory = {"format": "recall-dynamic-capture-inventory/v2", "files": [{"path": path, "ref": ref} for path, ref in sorted(self.files.items())]}
        carrier["capture_inventory"] = self.reference(wire(inventory), "capture-inventory")
        review = {"format": "recall-dynamic-component-review/v2", "reviewer": "qualification", "approved": True,
                  "identities": {self.identity["backend"]: copy.deepcopy(self.identity)}, "input_refs": refs,
                  "helper_sha256": {
                      "scripts/qualification/installed_linux_offline.py": self.pins["driver_sha256"],
                      "scripts/qualification/installed_process_capture.py": self.pins["process_capture_sha256"],
                      "scripts/qualification/recall_preview.py": self.pins["collector_sha256"],
                      "scripts/release/artifact.py": self.pins["installer_sha256"],
                  }}
        reviewed = {"review": review, "raw_sha256": sha(wire(review)), "raw_bytes": wire(review)}
        return reviewed, carrier, self.raw


class InstalledEvidenceTests(unittest.TestCase):
    def test_complete_original_nineteen_transcript_for_both_native_backends(self):
        for backend in ("elephant-db", "surreal-db"):
            with self.subTest(backend=backend):
                fixture = Transcript(backend).populate()
                result = ORACLE.validate_installed(*fixture.seal())
                self.assertEqual(result, {"identity": fixture.identity, "required_cases": CASE["required_cases"]})
                self.assertNotIn("verdict", result)

    def test_case_names_and_pass_flags_do_not_replace_wire_predicates(self):
        for change in ("suffix", "published", "wrong-space", "readonly", "framing", "idempotence", "restore", "config", "query"):
            with self.subTest(change=change):
                fixture = Transcript().populate()
                if change in ("suffix", "query"):
                    row = next(row for row in fixture.http if row["method"] == ("PUT" if change == "suffix" else "POST")
                               and row["path"].endswith("home%2Fheat" if change == "suffix" else "/search"))
                    row["request"]["text" if change == "suffix" else "query"] += " altered"
                else:
                    candidates = [row for row in fixture.http if row["raw_body_complete"]]
                    if change == "published":
                        row = next(row for row in candidates if row["status"] == 202)
                    elif change == "wrong-space":
                        row = next(row for row in candidates if row["status"] == 403)
                    elif change == "readonly":
                        row = next(row for row in candidates if (row["request"] or {}).get("params", {}).get("name") == "retract_source")
                    elif change == "restore":
                        row = [row for row in candidates if row["method"] == "GET" and row["path"].endswith("home%2Fheat/head")][-2]
                    elif change == "config":
                        row = next(row for row in candidates if row["method"] == "GET" and row["path"] == "/v1/spaces/qualification-household")
                    elif change == "idempotence":
                        row = [row for row in candidates if row["status"] == 202][1]
                    else:
                        row = next(row for row in candidates if row["status"] == 202)
                    body = json.loads(base64.b64decode(row["raw_body_base64"]))
                    if change == "published": body["published_revision"] = "heat-v1"
                    elif change == "wrong-space": row["status"] = 200
                    elif change == "readonly": body["result"]["isError"] = False
                    elif change == "restore": body["desired_revision"] = "heat-v2"
                    elif change == "config": body["embedding_dims"] = 1
                    elif change == "idempotence": body["operation_id"] = "different-operation"
                    else: row["raw_body_complete"] = False
                    raw = wire(body)
                    row["raw_body_base64"], row["body_sha256"] = base64.b64encode(raw).decode(), sha(raw)
                with self.assertRaisesRegex(ValueError, "^dynamic_installed_"):
                    ORACLE.validate_installed(*fixture.seal())

    def test_original_process_identity_terminal_and_event_bytes_are_required(self):
        for change in ("unreaped", "nonzero", "foreign-parent", "reused-birth", "false-event", "log-incomplete", "late", "skip-auth"):
            with self.subTest(change=change):
                fixture = Transcript().populate()
                row = fixture.processes[0]
                if change == "unreaped": row["reaped"] = False
                elif change == "nonzero": row["exit_code"] = 1
                elif change == "foreign-parent": row["identity"]["ppid"] = 999
                elif change == "reused-birth": fixture.processes[1]["identity"] = copy.deepcopy(row["identity"])
                elif change == "false-event": row["events"][0]["data"]["cwd"] = "/another-place"
                elif change == "log-incomplete": row["logs"][0]["complete"] = False
                elif change == "late": row["events"][-1]["observed_ns"] = fixture.deadline + 1
                else: row["intent"]["environment_names"].append("SKIP_AUTH")
                with self.assertRaisesRegex(ValueError, "^dynamic_installed_"):
                    ORACLE.validate_installed(*fixture.seal())

    def test_hash_replacement_and_unproven_extra_identity_refuse(self):
        reviewed, carrier, raw = Transcript().populate().seal()
        missing = dict(raw)
        del missing[carrier["controller"]["sha256"]]
        with self.assertRaisesRegex(ValueError, "dynamic_installed_original_hash_length"):
            ORACLE.validate_installed(reviewed, carrier, missing)
        expanded = copy.deepcopy(carrier)
        expanded["identity"]["paired_binary_sha256"] = "f" * 64
        with self.assertRaisesRegex(ValueError, "dynamic_installed_identity_fields"):
            ORACLE.validate_installed(reviewed, expanded, raw)

    def test_shared_original_process_predicate_accepts_live_and_terminal_exactly(self):
        fixture = Transcript()
        row = fixture.process(["serve"], live=True)
        result = ORACLE.validate_process_proof(row, fixture.raw, "b" * 64, fixture.controller_identity,
                                               fixture.deadline, require_terminal=False)
        self.assertEqual(result["identity"], row["identity"])
        with self.assertRaisesRegex(ValueError, "dynamic_installed_process_terminal"):
            ORACLE.validate_process_proof(row, fixture.raw, "b" * 64, fixture.controller_identity,
                                          fixture.deadline, require_terminal=True)
        fixture.finish(row)
        ORACLE.validate_process_proof(row, fixture.raw, "b" * 64, fixture.controller_identity,
                                      fixture.deadline, require_terminal=True)
        with self.assertRaisesRegex(ValueError, "dynamic_installed_process_live_terminal_absent"):
            ORACLE.validate_process_proof(row, fixture.raw, "b" * 64, fixture.controller_identity,
                                          fixture.deadline, require_terminal=False)

    def test_original_installed_and_current_runtime_log_bounds_stay_distinct(self):
        for length in (1024**2 + 1, 4 * 1024**2, 4 * 1024**2 + 1):
            with self.subTest(current_runtime_log_bytes=length):
                fixture = Transcript()
                row = fixture.process(["migrate"], stdout=b"x" * length)
                if length <= 4 * 1024**2:
                    ORACLE.validate_process_proof(row, fixture.raw, "b" * 64,
                        fixture.controller_identity, fixture.deadline, require_terminal=True)
                else:
                    with self.assertRaisesRegex(ValueError, "dynamic_installed_reference_bound_kind"):
                        ORACLE.validate_process_proof(row, fixture.raw, "b" * 64,
                            fixture.controller_identity, fixture.deadline, require_terminal=True)
        fixture = Transcript().populate()
        row = next(row for row in fixture.processes if "serve" in row["intent"]["argv"])
        log = row["logs"][0]
        log["ref"] = fixture.reference(b"x" * (1024**2 + 1), "process-log")
        fixture.files[log["path"]] = log["ref"]
        with self.assertRaisesRegex(ValueError, "dynamic_installed_reference_bound_kind"):
            ORACLE.validate_installed(*fixture.seal())

    def test_snapshot_report_original_role_has_exact_independent_stream_bounds(self):
        args = ["snapshot", "inspect", "--input", "/offline/original/bundle", "--json", "--include-records", "--compact-records"]
        for length in (4 * 1024**2 + 1, 33554433):
            fixture = Transcript()
            row = fixture.process(args, stdout=b"r" * length, stderr=b"e" * 1048576, inspection=True)
            result = ORACLE.validate_process_proof(row, fixture.raw, "b" * 64,
                fixture.controller_identity, fixture.deadline, require_terminal=True)
            self.assertEqual(result["argv"][1:], args)
            self.assertTrue(result["require_terminal"])
        for stream, length in ((0, 33554434), (1, 1048577)):
            fixture = Transcript()
            row = fixture.process(args, stdout=b"report", inspection=True)
            log = row["logs"][stream]
            raw = (b"r" if stream == 0 else b"e") * length
            log["ref"] = fixture.reference(raw, "snapshot-inspection" if stream == 0 else "process-log")
            fixture.files[log["path"]] = log["ref"]
            with self.subTest(stream=stream), self.assertRaisesRegex(ValueError, "dynamic_installed_reference_bound_kind"):
                ORACLE.validate_process_proof(row, fixture.raw, "b" * 64,
                    fixture.controller_identity, fixture.deadline, require_terminal=True)

    def test_snapshot_stream_reservation_command_and_roles_are_not_caller_options(self):
        args = ["snapshot", "inspect", "--input", "/offline/original/bundle", "--json", "--include-records", "--compact-records"]
        for mutation in ("maximum", "reservation", "bool-maximum", "bool-reservation", "stdout-role", "stderr-role",
                         "path", "missing-stderr", "extra-field", "ordinary-command", "extra-output-flag", "relative-input"):
            fixture = Transcript()
            row = fixture.process(args, stdout=b"original-report", inspection=True)
            intent = row["intent"]
            expected = "inspection_original_stream_reservation"
            if mutation == "maximum": intent["streams"][0]["maximum_bytes"] += 1
            elif mutation == "reservation": intent["streams"][0]["reserved_bytes"] -= 1
            elif mutation == "bool-maximum": intent["streams"][1]["maximum_bytes"] = True
            elif mutation == "bool-reservation": intent["streams"][1]["reserved_bytes"] = True
            elif mutation == "stdout-role": intent["streams"][0]["kind"] = "process-log"
            elif mutation == "stderr-role": intent["streams"][1]["kind"] = "snapshot-inspection"
            elif mutation == "path": intent["streams"][0]["path"] = intent["log_paths"][1]
            elif mutation == "missing-stderr":
                intent["streams"].pop()
                expected = "inspection_stream_inventory"
            elif mutation == "extra-field":
                intent["streams"][0]["refundable"] = True
                expected = "inspection_stream_fields"
            elif mutation == "ordinary-command":
                intent["argv"][1] = "serve"
                expected = "inspection_command"
            elif mutation == "extra-output-flag":
                intent["argv"] += ["--output", "/offline/original/report"]
                expected = "inspection_command"
            else:
                intent["argv"][4] = "relative-bundle"
                expected = "original_path"
            with self.subTest(mutation=mutation), self.assertRaisesRegex(ValueError, "dynamic_installed_" + expected):
                ORACLE.validate_process_proof(row, fixture.raw, "b" * 64,
                    fixture.controller_identity, fixture.deadline, require_terminal=True)

    def test_snapshot_streams_require_original_intent_eof_reap_and_consistent_kind(self):
        args = ["snapshot", "inspect", "--input", "/offline/original/bundle", "--json", "--include-records", "--compact-records"]
        for mutation, expected in (("late-intent", "process_original_intent"), ("eof", "process_log_incomplete"),
                                   ("reap", "process_terminal"), ("kind-alias", "inspection_stream_kind_alias"),
                                   ("unbound-role", "inspection_original_report_role")):
            fixture = Transcript()
            row = fixture.process(args, stdout=b"report", inspection=mutation != "late-intent")
            if mutation == "late-intent":
                row["intent"]["streams"] = [
                    {"path": row["logs"][0]["path"], "kind": "snapshot-inspection", "maximum_bytes": 33554433, "reserved_bytes": 33554433},
                    {"path": row["logs"][1]["path"], "kind": "process-log", "maximum_bytes": 1048576, "reserved_bytes": 1048576},
                ]
                row["logs"][0]["ref"] = fixture.reference(b"report", "snapshot-inspection")
            elif mutation == "eof": row["logs"][0]["closure"]["eof"] = False
            elif mutation == "reap": row["reaped"] = False
            elif mutation == "kind-alias": row["logs"][1]["ref"] = fixture.reference(b"report", "process-log")
            else: row["logs"][1]["ref"] = fixture.reference(b"different-report", "snapshot-inspection")
            with self.subTest(mutation=mutation), self.assertRaisesRegex(ValueError, "dynamic_installed_" + expected):
                ORACLE.validate_process_proof(row, fixture.raw, "b" * 64,
                    fixture.controller_identity, fixture.deadline, require_terminal=True)
        fixture = Transcript().populate()
        row = fixture.processes[0]
        row["intent"]["streams"] = []
        with self.assertRaisesRegex(ValueError, "dynamic_installed_installed_stream_extension"):
            ORACLE.validate_installed(*fixture.seal())

    def test_snapshot_reservation_must_precede_original_spawn(self):
        fixture = Transcript()
        row = fixture.process(["snapshot", "inspect", "--input", "/offline/original/bundle",
                               "--json", "--include-records", "--compact-records"],
                              stdout=b"report", inspection=True)
        original_intent = row["events"].pop(0)
        row["events"].insert(2, original_intent)
        # Retain consistent original event hashes, data and monotonic samples;
        # only the order now claims reservation after the spawn request.
        for event in row["events"]:
            event["observed_ns"] = fixture.now
            captured = json.loads(fixture.raw[event["capture"]["ref"]["sha256"]])
            captured["observed_ns"] = fixture.now
            event["capture"]["ref"] = fixture.reference(wire(captured), "installed-process-event")
        with self.assertRaisesRegex(ValueError, "dynamic_installed_inspection_pre_spawn_intent"):
            ORACLE.validate_process_proof(row, fixture.raw, "b" * 64,
                fixture.controller_identity, fixture.deadline, require_terminal=True)

    def test_original_expiry_and_cleanup_deadlines_cannot_be_refreshed(self):
        for change in ("expired", "clock-reversed", "extended-cleanup"):
            with self.subTest(change=change):
                fixture = Transcript().populate()
                if change == "expired":
                    fixture.run["expires_at"] = "2020-01-01T00:00:00Z"
                elif change == "clock-reversed":
                    fixture.owner["wall_sample_before_ns"] = fixture.owner["wall_sample_after_ns"] + 1
                reviewed, carrier, raw = fixture.seal()
                if change == "extended-cleanup":
                    cleanup = json.loads(raw[carrier["terminal_cleanup"]["sha256"]])
                    cleanup["original_deadline_ns"] += 1
                    carrier["terminal_cleanup"] = fixture.reference(wire(cleanup), "installed-cleanup")
                with self.assertRaisesRegex(ValueError, "^dynamic_installed_"):
                    ORACLE.validate_installed(reviewed, carrier, raw)


if __name__ == "__main__":
    unittest.main()
