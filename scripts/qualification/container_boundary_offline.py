#!/usr/bin/env python3
"""Independent bounded clients for the allocated container lifecycle extension.

Source construction is not execution. The base container driver owns every
runtime/resource; independent qualification owns expectations and acceptance.
"""
from __future__ import annotations

import asyncio
import base64
import importlib.util
from pathlib import Path
import time

BASE_PATH = Path(__file__).with_name("container_offline.py")
_spec = importlib.util.spec_from_file_location("boundary_container_base", BASE_PATH)
base = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(base)
require = base.require
ROOT = base.ROOT
CASES_PATH = ROOT / "tests/recall_preview_semantics/container_boundary_cases.json"
PARSER_PATH = ROOT / "scripts/qualification/recall_workload.py"
CONTRACT_PATH = ROOT / "tests/recall_preview/container-boundary-contract.json"
PHYSICAL_ORACLE_PATH = Path(__file__).with_name("snapshot_report_oracle.py")
BODY_CAP = 270336
# Compile this pure helper's original bounded bytes; no cached bytecode or
# runtime/container imports are needed by installed full-workload consumers.
_INSPECTION_PATH = Path(__file__).with_name("snapshot_inspection_capture.py")
with _INSPECTION_PATH.open("rb") as _inspection_stream:
    _inspection_source = _inspection_stream.read(65537)
require(0 < len(_inspection_source) <= 65536, "bounded original inspection helper required")
_inspection_spec = importlib.util.spec_from_file_location("boundary_inspection_capture", _INSPECTION_PATH)
inspection_capture = importlib.util.module_from_spec(_inspection_spec)
exec(compile(_inspection_source, str(_INSPECTION_PATH), "exec", dont_inherit=True), inspection_capture.__dict__)
INSPECTION_CAP = inspection_capture.INSPECTION_CAP
BOUNDARY_DEPENDENCIES = ("scripts/qualification/container_offline.py",
    "scripts/qualification/snapshot_inspection_capture.py",
    "scripts/qualification/physical_inventory.py",
    "scripts/qualification/native_private_membership.py",
    "scripts/qualification/recall_workload.py", "tests/recall_preview/container-boundary-contract.json",
    "scripts/qualification/snapshot_report_oracle.py", "tests/recall_preview/PHYSICAL_EXPORT_EXPECTATIONS.md",
    "scripts/qualification/container_graph_runtime.py",
    "scripts/qualification/container_graph_budget.py",
    "scripts/qualification/container_graph_helper.py",
    "scripts/qualification/Dockerfile.graph-helper",
    "tests/recall_preview_semantics/full_workload_transport.py",
    "tests/recall_preview_semantics/full_workload_spy.py",
    "tests/recall_preview_semantics/full_workload_graph.py")
MAX_REQUESTS = 1000
MAX_CAPTURE_BYTES = 128 * 1024 * 1024
# Reserve the base command collector's independent 32 MiB hard ceiling.
EVIDENCE_CAP = MAX_CAPTURE_BYTES - 32 * base.MAX_COMMAND_BYTES
MAX_REQUEST_BYTES = 2_100_000
REQUEST_SECONDS = 5
RESTORE_SECONDS = 120
JOURNAL_CAPTURE_CAP = 128 * 1024


def graph_inputs(allocation, review):
    """Verify original D-reviewed recipe bytes before import or allocation."""
    if "graph_plan" not in allocation:
        require("graph_plan_sha256" not in allocation, "Graph plan identity is incomplete")
        return None
    path = Path(allocation["graph_plan"])
    plan = base.read_private_json(path)
    require(base.digest(path.read_bytes()) == allocation.get("graph_plan_sha256")
            == review.get("graph_plan_sha256"), "independent original Graph plan hash differs")
    require(review.get("graph_case_contract") == "cb07-two-source-complete-evidence-v1",
            "revised independent CB07 contract approval is missing")
    require(base.IMAGE.fullmatch(allocation.get("graph_helper_image_id", ""))
            and allocation["graph_helper_image_id"] == review.get("graph_helper_image_id"),
            "exact independently reviewed local Graph helper image required")
    require(type(plan) is dict and set(plan) == {"format", "space", "configuration", "spy", "positive",
            "response_budget", "arithmetic", "positive_query", "empty_query", "query_binding_id"}
            and plan["format"] == "recall-container-graph-plan/v1", "Graph plan field set differs")
    space = plan["space"]
    require(isinstance(space, str) and base.re.fullmatch(r"[A-Za-z0-9_-]{1,128}", space)
            and plan["configuration"].get("name") == space, "Graph named-space configuration differs")
    for field in ("positive_query", "empty_query", "query_binding_id"):
        require(type(plan[field]) is str and 0 < len(plan[field].encode()) <= 1024,
                "explicit bounded Graph query/binding required")
    require(type(plan["response_budget"]) is list and len(plan["response_budget"]) == 2,
            "CB07 requires two original bounded Graph sources")
    inputs, hashes = {}, {}
    def load(descriptor, canonical):
        expected_fields = {"path", "file_sha256"} | ({"canonical_sha256"} if canonical else set())
        require(type(descriptor) is dict and set(descriptor) == expected_fields,
                "exact original Graph recipe descriptor required")
        relative = Path(descriptor["path"])
        path = ROOT / relative
        require(not relative.is_absolute() and relative.parts[:3] == ("tests", "recall_preview", "graph-recipes")
                and len(relative.parts) == 4 and path.resolve() == path and path.is_file(),
                "Graph input escaped reviewed literal directory")
        raw = path.read_bytes()
        require(len(raw) <= 8 * 1024 * 1024 and base.digest(raw) == descriptor["file_sha256"],
                "original Graph recipe bytes changed")
        value = base.strict_json(raw)
        if canonical:
            # GraphFixture and ControlledSpy consume the original manifest's
            # UTF-8 compact JSON identity, without a terminal LF. The container
            # report encoder adds LF and escapes non-ASCII; it is not this codec.
            canonical_raw = base.json.dumps(value, sort_keys=True, separators=(",", ":"),
                                            ensure_ascii=False, allow_nan=False).encode("utf-8")
            require(base.digest(canonical_raw) == descriptor["canonical_sha256"],
                    "Graph canonical recipe identity differs")
        hashes[str(relative)] = descriptor["file_sha256"]
        return value
    for key in ("spy", "positive", "arithmetic"):
        inputs[key] = load(plan[key], key != "arithmetic")
    inputs["response_budget"] = [load(row, True) for row in plan["response_budget"]]
    require(review.get("graph_input_sha256") == hashes, "Graph transitive literal input review differs")
    for recipe in [inputs["positive"], *inputs["response_budget"]]:
        require(recipe.get("source_request", {}).get("source", {}).get("space_id") == space,
                "Graph recipe changed its original space")
    arithmetic = inputs["arithmetic"]
    require(arithmetic.get("format") == "recall-independent-search-budget-arithmetic/v1"
            and arithmetic.get("source_count") == 2 and arithmetic.get("ranking_mode") == "lexical"
            and arithmetic["limit20"]["required_http_status"] == 413
            and arithmetic["limit20"]["quote_only_response_bytes_lower_bound"] > 262144
            and arithmetic["limit10"]["full_response_bytes_upper_bound"] < 262144,
            "independent two-source response-budget proof differs")
    constraints = {recipe["source_request"]["source"]["external_id"]:
                   recipe["source_request"]["intake"]["revision_id"] for recipe in inputs["response_budget"]}
    require(arithmetic["source_revision_constraints"] == constraints,
            "response budget constraints differ from original literal sources")
    return plan, inputs


def physical_oracle():
    """D's sole independent predicate implementation, loaded after hash review."""
    spec = importlib.util.spec_from_file_location("boundary_reviewed_physical_oracle", PHYSICAL_ORACLE_PATH)
    require(spec is not None and spec.loader is not None, "reviewed physical interpreter unavailable")
    module = importlib.util.module_from_spec(spec)
    base.sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def checked_journal_tar(raw):
    """Read only payload-free intent files. Never open a key or membership file."""
    require(len(raw) <= JOURNAL_CAPTURE_CAP, "journal intent capture bound exceeded")
    entries = {}
    with base.tarfile.open(fileobj=base.io.BytesIO(raw), mode="r:") as archive:
        members = archive.getmembers()
        require(len(members) <= 17, "journal intent member bound exceeded")
        for member in members:
            name = Path(member.name)
            require(not name.is_absolute() and name.parts and ".." not in name.parts and name.parts[0] == "erasures",
                    "journal capture escaped original intent directory")
            if member.isdir():
                require(name.parts == ("erasures",), "nested journal directory refused")
                continue
            require(member.isfile() and len(name.parts) == 2 and name.suffix == ".json"
                    and member.size <= 4096 and name.name not in entries,
                    "journal intent must be a unique bounded regular file")
            identity = name.stem
            require(str(base.uuid.UUID(identity)) == identity, "journal filename is not an exact UUID")
            stream = archive.extractfile(member)
            require(stream is not None, "journal intent bytes unavailable")
            body = stream.read(4097)
            require(len(body) == member.size <= 4096, "journal intent capture truncated")
            intent = base.strict_json(body)
            require(isinstance(intent, dict) and set(intent) == {"intent_id", "space_id", "source_id", "recorded_at"}
                    and intent["intent_id"] == identity and isinstance(intent["space_id"], str)
                    and isinstance(intent["source_id"], str) and intent["source_id"].startswith("source:"),
                    "journal intent shape or identity differs")
            base.uuid.UUID(intent["source_id"].partition(":")[2])
            base.utc(intent["recorded_at"])
            entries[name.name] = {"filename": name.name, "intent": intent, "sha256": base.digest(body),
                                  "body_base64": base64.b64encode(body).decode(), "bytes": len(body)}
    require(len(entries) <= 16, "journal intent count bound exceeded")
    return entries


def framing_parser():
    """Call only after the complete transitive source review has been checked."""
    spec = importlib.util.spec_from_file_location("boundary_reviewed_frame_parser", PARSER_PATH)
    require(spec is not None and spec.loader is not None, "reviewed async parser unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    async def parse(reader, observed):
        await module.read_frame(reader, observed, body_limit=BODY_CAP)
    return parse


class IndependentClients:
    """One asyncio loop, separate connections and complete absolute deadlines.

    No process SIGALRM, inherited mutable driver counters, proxy, redirects,
    ambient credentials, DNS target, or shared HTTP connection is used.
    """
    def __init__(self, port, directory, tokens, read_frame, mcp_value, deadline_ns,
                 *, connect=None, clock_ns=time.monotonic_ns, reserve=None, reserve_capture=None):
        require(type(port) is int and 1 <= port <= 65535, "owned loopback port required")
        self.port = port
        self.directory = Path(directory)
        self.tokens = dict(tokens)
        self.read_frame = read_frame
        self.mcp_value = mcp_value
        self.deadline_ns = deadline_ns
        self.connect = connect or asyncio.open_connection
        self.clock_ns = clock_ns
        self.count = 0
        self.capture_bytes = 0
        self.active = 0
        self.peak_active = 0
        self.records = []
        self.reserve = reserve or (lambda: None)
        self.reserve_capture = reserve_capture or (lambda _bytes: None)

    async def request(self, channel, method, path, role=None, space=None, body=None,
                      *, request_id=None, restore=False):
        require(channel.replace("-", "").isalnum() and len(channel) <= 64, "bounded capture channel required")
        require(method in ("GET", "POST"), "boundary clients only read or call reviewed tools/restore")
        require(path.startswith(("/v1/", "/mcp/")) and all(32 < ord(char) < 127 for char in path),
                "explicit local API path required")
        require(self.count < MAX_REQUESTS, "independent HTTP request bound exceeded")
        now = self.clock_ns()
        timeout = min(RESTORE_SECONDS if restore else REQUEST_SECONDS, (self.deadline_ns - now) / 1e9)
        require(timeout > 0, "allocated container deadline expired")
        token = self.tokens[role] if role else None
        for value in (token, space):
            require(value is None or (isinstance(value, str) and value and all(32 < ord(char) < 127 for char in value)),
                    "invalid synthetic HTTP header")
        payload = b"" if body is None else base.encoded(body)
        require(len(payload) <= MAX_REQUEST_BYTES, "boundary request exceeds allocated bytes")
        self.reserve()
        self.count += 1
        identity = self.count
        headers = [f"{method} {path} HTTP/1.1", f"Host: 127.0.0.1:{self.port}",
                   "Connection: close", "Accept: application/json, text/event-stream"]
        if token:
            headers.append("Authorization: Bearer " + token)
        if space:
            headers.append("X-Space-Id: " + space)
        if body is not None:
            headers += ["Content-Type: application/json", "Content-Length: " + str(len(payload))]
        wire = ("\r\n".join(headers) + "\r\n\r\n").encode("ascii") + payload
        observed = {"raw_body": b"", "framing_complete": False, "body": None, "status": None}
        record = {"id": identity, "channel": channel, "method": method, "path": path,
                  "credential_role": role, "space": space, "request_body": body,
                  "request_sha256": base.digest(payload), "started_ns": now,
                  "deadline_ns": now + int(timeout * 1e9), "transport_error": None}
        writer = None
        self.active += 1
        self.peak_active = max(self.peak_active, self.active)
        try:
            async with asyncio.timeout(timeout):
                reader, writer = await self.connect("127.0.0.1", self.port, limit=32768)
                record["connected_ns"] = self.clock_ns()
                writer.write(wire)
                await writer.drain()
                record["client_write_complete_ns"] = self.clock_ns()
                try:
                    await self.read_frame(reader, observed)
                except (ValueError, UnicodeError):
                    # S's frame parser intentionally parses JSON after framing.
                    # A complete MCP SSE body uses its separately reviewed parser.
                    if request_id is None or not observed["framing_complete"]:
                        raise
                    types = observed.get("response_headers", {}).get("content-type", [])
                    require(len(types) == 1 and types[0].split(";", 1)[0].strip().lower() == "text/event-stream",
                            "non-JSON response is not complete MCP SSE")
                    observed["body"] = self.mcp_value(observed["raw_body"], request_id)
                if request_id is not None and observed["status"] == 200:
                    observed["body"] = self.mcp_value(observed["raw_body"], request_id)
        except Exception as error:
            record["transport_error"] = type(error).__name__
        finally:
            if writer is not None:
                writer.close()
                try:
                    async with asyncio.timeout(0.25):
                        await writer.wait_closed()
                except Exception:
                    record["close_complete"] = False
                else:
                    record["close_complete"] = True
            self.active -= 1
            raw = observed.pop("raw_body")
            record.update(observed, finished_ns=self.clock_ns(),
                          raw_body_base64=base64.b64encode(raw).decode(),
                          body_sha256=base.digest(raw), body_bytes=len(raw))
            record["elapsed_ms"] = (record["finished_ns"] - record["started_ns"]) / 1e6
            output = base.encoded(record)
            require(self.capture_bytes + len(output) <= MAX_CAPTURE_BYTES, "independent HTTP capture byte budget exhausted")
            self.reserve_capture(len(output))
            self.capture_bytes += len(output)
            name = f"boundary-http-{identity:03d}-{channel}.json"
            base.private_new(self.directory / name, output)
            record["capture"] = name
            self.records.append(record)
        return record


def complete_response(row):
    require(row["transport_error"] is None and row["framing_complete"] is True,
            "incomplete or failed transport cannot establish a product outcome")
    require(row["finished_ns"] <= row["deadline_ns"] + 300_000_000,
            "response exceeded request deadline and bounded close allowance")
    return row["body"]


def public_error(value, code, retryable):
    require(isinstance(value, dict) and set(value) == {"error_code", "message", "retryable"},
            "public error shape includes unexpected data")
    require(value["error_code"] == code and value["retryable"] is retryable,
            "public error category or retryability changed")
    require(isinstance(value["message"], str) and len(value["message"]) <= 512,
            "public error message exceeds published bound")


def observed_restore_window(restore, samples):
    """Report a sampled actual barrier, never a continuous-refusal proof."""
    complete_response(restore)
    require(restore["status"] == 200 and restore["body"].get("success") is True,
            "actual registered restore did not succeed")
    overlapping = [row for row in samples if row["started_ns"] >= restore["started_ns"]
                   and row["finished_ns"] <= restore["finished_ns"]]
    blocked = []
    data = []
    for row in overlapping:
        body = complete_response(row)
        if row["channel"] == "readiness":
            if row["status"] == 503 and isinstance(body, dict) and body.get("ready") is False and body.get("recovery_ready") is False:
                blocked.append(row)
        elif row["channel"].startswith(("source-a", "source-b")):
            if row["status"] == 503:
                public_error(body, "not_ready", True)
                data.append(row)
            elif row["channel"].endswith("mcp") and row["status"] == 200:
                success, value = mcp_result(row)
                if not success and value.get("error_code") == "not_ready":
                    public_error(value, "not_ready", True)
                    data.append(row)
    simultaneous = [(ready["capture"], row["capture"]) for ready in blocked for row in data
                    if max(ready["started_ns"], row["started_ns"]) <= min(ready["finished_ns"], row["finished_ns"])]
    return {"restore_capture": restore["capture"], "restore_interval_ns": [restore["started_ns"], restore["finished_ns"]],
            "overlapping_captures": [row["capture"] for row in overlapping],
            "observed_recovery_blocked": bool(blocked), "contemporaneous_refusals": simultaneous,
            "continuous_barrier_proof": False, "verdict": "pass" if simultaneous else "unmet",
            "reason": "Only the fully captured overlapping sample intervals are qualified." if simultaneous else
                      "No contemporaneous readiness recovery barrier and typed data refusal was observed."}


def mcp_result(row):
    value = complete_response(row)
    require(row["status"] == 200 and isinstance(value, dict) and "error" not in value,
            "MCP transport refusal is not a successful tool response")
    result = value.get("result")
    require(isinstance(result, dict), "MCP result missing")
    content = result.get("content", [])
    require(isinstance(content, list) and len(content) == 1 and isinstance(content[0], dict)
            and content[0].get("type") == "text" and isinstance(content[0].get("text"), str),
            "one complete typed MCP result required")
    texts = [content[0]["text"]]
    body = base.strict_json(texts[0])
    if result.get("isError") is True:
        return False, body
    require(isinstance(body, dict) and body.get("ok") is True and "data" in body,
            "successful tool lacks its typed data envelope")
    return True, body["data"]


async def call_tool(clients, channel, name, arguments, role, space):
    request_id = 10_000 + clients.count + 1
    return await clients.request(channel, "POST", "/mcp/full", role, space,
        {"jsonrpc": "2.0", "id": request_id, "method": "tools/call",
         "params": {"name": name, "arguments": arguments}}, request_id=request_id)


checked_inspection = inspection_capture.checked_inspection

class ContainerBoundaryDriver(base.ContainerDriver):
    """Observe the base driver's actual owned lifecycle with independent clients."""
    def __init__(self, allocation, inventory, boundary_inventory, *, commands=None, utilities=None, parser=None):
        self.evidence_capture_bytes = 0
        self.graph_capture_held = 0
        self.graph_budget = None
        super().__init__(allocation, inventory, commands=commands, utilities=utilities)
        self.service_scopes += ["read:doc:raw", "read:doc:metadata", "write:doc"]
        self.boundary_inventory = boundary_inventory
        self.parser = parser or framing_parser()
        self.boundary_cases = {name: {"verdict": "not_executed"}
                               for name in boundary_inventory["expectations"]["required_cases"]}
        self.clients = None
        self.restore_phase = "clean"
        self.boundary_failure = None
        self.total_http_calls = 0
        self.inspection_reports = {}
        self.inspection_capture_bytes = 0
        self.metadata_canaries = {}
        self.physical_fixture = None
        self.erasure_acceptance = None
        self.erasure_started_at = None
        self.before_erasure_intents = None
        self.physical_export_comparison = {"verdict": "not_executed"}
        self.physical_files = {}
        self.physical_file_evidence = {}
        self.physical_replay_started_at = None
        self.graph_spy = None
        self.graph_plan_inputs = None
        if "graph_plan" in allocation:
            review = base.read_private_json(Path(allocation["boundary_review"]))
            self.graph_plan_inputs = graph_inputs(allocation, review)
            self.environment["RECALL_BOUNDARY_SPY_KEY"] = "synthetic-controlled-provider-key"

    require = staticmethod(require)
    base_root = ROOT
    base_encode = staticmethod(base.encoded)
    base_decode = staticmethod(base.strict_json)
    base_digest = staticmethod(base.digest)
    base_private_new = staticmethod(base.private_new)
    evidence_cap = EVIDENCE_CAP
    graph_mcp_result = staticmethod(mcp_result)
    graph_complete_response = staticmethod(complete_response)

    @staticmethod
    async def graph_call_tool(clients, query, space):
        return await call_tool(clients, "graph-search-mcp", "search_scoped", query, "graph-service", space)

    def graph_cases(self):
        if self.graph_plan_inputs is None:
            for name in ("CC04", "CB07", "CB09"):
                self.unmet(name, "Original independently reviewed Graph plan and literal inputs are unavailable.")
            return
        spec = importlib.util.spec_from_file_location("container_reviewed_graph_runtime",
            ROOT / "scripts/qualification/container_graph_runtime.py")
        require(spec is not None and spec.loader is not None, "reviewed owned Graph adapter unavailable")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        plan, inputs = self.graph_plan_inputs
        require(plan["space"] not in self.cases["spaces"], "Graph fixture must preserve original named-space baselines")
        auth = [(self.volumes["auth"], base.AUTH_ROOT, False)]
        arguments = ["meerkat-recall", "service-token", "issue", "--secret-file", base.AUTH_ROOT + "/signing.key",
            "--subject", "container-graph-service", "--expires-in", "1800", "--issuer", "container-offline",
            "--audience", "recall-container", "--clearances-file", base.AUTH_ROOT + "/clearances.json",
            "--space", plan["space"], "--output", base.AUTH_ROOT + "/graph-service.token"]
        for scope in module.GRAPH_SCOPES:
            arguments += ["--scope", scope]
        container = self.once("auth-graph-service", arguments, mounts=auth)
        token = self.copy(container, base.AUTH_ROOT + "/graph-service.token", 16384).decode().strip()
        require(token.count(".") == 2 and len(token) < 16384, "CLI did not issue original Graph JWT")
        self.tokens["graph-service"] = token
        self.commands.secrets.append(token)
        self.independent_clients().tokens["graph-service"] = token
        asyncio.run(module.run(self, plan, inputs))
        self.verify_configurations()

    def reserve_capture(self, amount):
        if self.graph_budget is not None:
            state = self.graph_budget.change(written=amount, allow_expired=True)
            self.evidence_capture_bytes = state["written"]
            return
        require(type(amount) is int and amount >= 0
                and self.evidence_capture_bytes + self.graph_capture_held + amount <= EVIDENCE_CAP,
                "combined private capture budget exhausted")
        self.evidence_capture_bytes += amount

    def save(self, name, value):
        self.reserve_capture(len(base.encoded(value)))
        return super().save(name, value)

    def reserve_http(self):
        if self.graph_budget is not None:
            state = self.graph_budget.change(calls=1)
            self.total_http_calls = state["calls"]
            return
        require(self.total_http_calls < MAX_REQUESTS, "combined HTTP request budget exhausted")
        self.total_http_calls += 1

    def http(self, *args, **kwargs):
        self.reserve_http()
        if len(args) >= 4 and args[0:2] == ("POST", "/v1/sources/ingest"):
            body = kwargs.get("body")
            if isinstance(body, dict) and body.get("ingest_mode") == "metadata":
                body = dict(body)
                identity = (args[3], body["name"], body["intake"]["revision_id"])
                canary = "urn:recall:qualification:" + base.digest(base.encoded([self.allocation["run_id"], *identity]))
                require("uri" not in body, "physical fixture cannot replace an existing URI")
                body["uri"] = canary
                self.metadata_canaries[identity] = canary
                kwargs = {**kwargs, "body": body}
        result = super().http(*args, **kwargs)
        if len(args) >= 2 and args[0] == "POST" and args[1].endswith(":erase") and result["status"] == 202:
            require(self.erasure_acceptance is None, "physical fixture erasure accepted twice")
            self.erasure_acceptance = result["body"]
        return result

    def independent_clients(self):
        if self.clients is None:
            self.clients = IndependentClients(self.port, self.root, self.tokens, self.parser, self.mcp_value,
                int((self.commands.started + base.MAX_SECONDS) * 1e9), reserve=self.reserve_http,
                reserve_capture=self.reserve_capture)
        require(self.clients.port == self.port, "observer port changed without a fresh connection context")
        return self.clients

    def launch(self, *args, **kwargs):
        result = super().launch(*args, **kwargs)
        if self.clients is not None:
            self.clients.port = self.port
        return result

    def boundary_case(self, name, action):
        row = self.boundary_cases[name]
        require(row["verdict"] == "not_executed", "boundary case repeated")
        row["started_ns"] = time.monotonic_ns()
        try:
            observation = action()
            verdict = observation.pop("verdict", "pass")
            require(verdict in ("pass", "fail", "unmet"), "invalid boundary verdict")
            row.update(verdict=verdict, observations=observation)
        except Exception as error:
            row.update(verdict="fail", failure_type=type(error).__name__)
            self.boundary_failure = type(error).__name__
            raise
        finally:
            row["finished_ns"] = time.monotonic_ns()
            self.save("boundary-case-" + name + ".json", row)
        return row

    def unmet(self, name, reason):
        return self.boundary_case(name, lambda: {"verdict": "unmet", "reason": reason})

    async def error_pair(self, rest_path, rest_body, tool, arguments, code, status, retryable,
                         *, space=None, role="service", method="POST", transport=False):
        clients = self.independent_clients()
        space = space or self.cases["spaces"][0]
        rest = await clients.request("error-rest", method, rest_path, role, space, rest_body)
        mcp = await call_tool(clients, "error-mcp", tool, arguments, role, space)
        require(rest["status"] == status, "HTTP error status differs from frozen taxonomy")
        public_error(complete_response(rest), code, retryable)
        if transport:
            require(mcp["status"] == status, "MCP middleware refusal changed transport status")
            public_error(complete_response(mcp), code, retryable)
        else:
            success, value = mcp_result(mcp)
            require(not success, "MCP error request returned a successful tool result")
            public_error(value, code, retryable)
        # Reject synthetic bearer secrets and private source payload in errors.
        hidden = [*self.tokens.values(), *self.metadata_canaries.values()]
        for value in getattr(self, "baseline", {}).values():
            hidden += [base.encoded(value["source"]).decode().strip(), value["source"].get("id", "")]
        hidden += [value.get("source_id", "") for value in getattr(self, "accepted", {}).values()]
        for row in (rest, mcp):
            raw = base64.b64decode(row["raw_body_base64"]).decode("utf-8")
            require(all(not secret or secret not in raw for secret in hidden), "public error disclosed fixture data")
        return {"error_code": code, "captures": [rest["capture"], mcp["capture"]]}

    async def search_pair(self, space, key, revision, state):
        query = {"query": "boundary-no-evidence", "ranking_mode": "lexical", "limit": 1,
                 "source_revision_constraints": {key: revision}}
        clients = self.independent_clients()
        rest = await clients.request("coverage-rest", "POST", f"/v1/spaces/{space}/search", "service", space, query)
        mcp = await call_tool(clients, "coverage-mcp", "search_scoped", query, "service", space)
        success, tool_data = mcp_result(mcp)
        require(rest["status"] == 200 and success, "coverage became a transport/tool error")
        values = [complete_response(rest), tool_data]
        for value in values:
            require(isinstance(value, dict) and set(value) == {"results", "coverage", "partial"}, "search DTO changed")
            require(value["results"] == [] and value["partial"] is True, "incomplete metadata fixture fabricated evidence")
            coverage = value["coverage"][key]
            require(coverage["state"] == state and coverage["requested_revision"] == revision, "original coverage changed")
            if state == "missing":
                require(coverage["published_revision"] is None and coverage["desired_revision"] is None, "absent source acquired authority")
            if state == "pending":
                require(coverage["published_revision"] is None and coverage["desired_revision"] == revision, "pending source falsely published")
            if state == "stale":
                require(coverage["published_revision"] == self.baseline[space]["head"]["published_revision"]
                        and coverage["desired_revision"] == self.baseline[space]["head"]["desired_revision"], "stale coverage hid actual authority")
        require(values[0] == values[1], "HTTP/MCP source coverage differs")
        return {"captures": [rest["capture"], mcp["capture"]], "response": values[0]}

    def accept(self, space, key, revision, generation=None):
        accepted = super().accept(space, key, revision, generation)
        if revision == "metadata-v1" and space == self.cases["spaces"][0] and self.boundary_cases["CC02"]["verdict"] == "not_executed":
            self.boundary_case("CC02", lambda: asyncio.run(self.search_pair(space, key, revision, "pending")))
        return accepted

    def unchanged(self):
        for space, accepted in self.accepted.items():
            require(self.published(space, self.cases["source_key"], accepted) == self.baseline[space],
                    "negative boundary case altered original head, operation or source")

    def pending_restart(self):
        result = super().pending_restart()
        first, second = self.cases["spaces"]
        key = self.cases["source_key"]
        query = {"query": "boundary-no-evidence", "ranking_mode": "lexical", "limit": 1}
        search_path = f"/v1/spaces/{first}/search"
        self.boundary_case("CB01", lambda: asyncio.run(self.error_pair(search_path, query, "search_scoped", query,
            "unauthorized", 401, False, role=None, transport=True)))
        # Independently authorized original named-space reader remains positive.
        own = self.http("POST", search_path, "one-space", first, query)
        require(own["status"] == 200, "same credential own-space positive failed")
        self.boundary_case("CB02", lambda: asyncio.run(self.error_pair(f"/v1/spaces/{second}/search", query,
            "search_scoped", query, "forbidden_space", 403, False, space=second, role="one-space", transport=True)))
        missing_key = "synthetic/never-accepted-boundary"
        self.boundary_case("CB04", lambda: asyncio.run(self.error_pair(self.source_path(first, missing_key) + "/head",
            None, "get_source_head", {"external_source_id": missing_key}, "not_found", 404, False, method="GET")))
        self.boundary_case("CC01", lambda: asyncio.run(self.search_pair(first, missing_key, "absent-v1", "missing")))
        self.boundary_case("CC03", lambda: asyncio.run(self.search_pair(first, key, "never-current-revision", "stale")))
        mutation = {"external_source_id": key, "expected_generation": "never-current-generation",
                    "idempotency_key": "stale-boundary-withdrawal"}
        self.boundary_case("CB05", lambda: asyncio.run(self.error_pair(self.source_path(first, key) + ":retract",
            {k: v for k, v in mutation.items() if k != "external_source_id"}, "retract_source", mutation,
            "conflict_generation", 409, False)))
        invalid = {**query, "limit": 51}
        self.boundary_case("CB08", lambda: asyncio.run(self.error_pair(search_path, invalid, "search_scoped", invalid,
            "invalid_request", 400, False)))
        self.boundary_case("CB03", self.disabled_pair)
        self.unchanged()
        self.boundary_case("CC05", self.retracted_coverage)
        self.graph_cases()
        return result

    def disabled_pair(self):
        before = self.tool("get_runtime_status", {})
        original_mode = before.get("runtime_mode")
        require(original_mode in ("local_memory", "shared_memory", "maintenance"), "original writable mode unavailable")
        changed = self.tool("configure_runtime", {"runtime_mode": "read_only"})
        require(changed.get("runtime_mode") == "read_only", "actual read-only mode was not configured")
        first = self.cases["spaces"][0]
        key = self.cases["source_key"]
        arguments = {"external_source_id": key, "expected_generation": self.baseline[first]["head"]["generation"],
                     "idempotency_key": "disabled-boundary-withdrawal"}
        try:
            result = asyncio.run(self.error_pair(self.source_path(first, key) + ":retract",
                {k: v for k, v in arguments.items() if k != "external_source_id"}, "retract_source", arguments,
                "capability_disabled", 403, False))
            positive = self.http("POST", f"/v1/spaces/{first}/search", "service", first,
                                {"query": "boundary-no-evidence", "ranking_mode": "lexical", "limit": 1})
            require(positive["status"] == 200, "read-only mode disabled permitted reads")
            return result
        finally:
            restored = self.tool("configure_runtime", {"runtime_mode": original_mode})
            require(restored.get("runtime_mode") == original_mode, "original runtime mode not restored")

    def retracted_coverage(self):
        space = self.cases["spaces"][0]
        key = "synthetic/boundary-retraction"
        accepted = self.accept(space, key, "withdraw-v1")
        published = self.published(space, key, accepted)
        response = self.http("POST", self.source_path(space, key) + ":retract", "service", space,
                             {"expected_generation": published["head"]["generation"], "idempotency_key": "boundary-retract"})
        require(response["status"] == 202, "real source retraction failed")
        receipt = response["body"]
        end = time.monotonic() + min(60, self.commands.remaining())
        while time.monotonic() < end:
            operation = self.operation(space, receipt["operation_id"])
            if operation.get("state") == "complete":
                require(operation.get("lifecycle") == "retracted", "completed retraction has wrong lifecycle")
                return asyncio.run(self.search_pair(space, key, "withdraw-v1", "retracted"))
            require(operation.get("state") in ("pending", "running"), "source retraction failed")
            time.sleep(min(0.1, self.commands.remaining()))
        raise ValueError("retraction did not complete within the bounded deadline")

    async def observe_restore(self, backup):
        clients = self.independent_clients()
        first, second = self.cases["spaces"]
        stop = asyncio.Event()
        samples = []
        ready_before = await clients.request("before-readiness", "GET", "/v1/readiness", "operator")
        ready_body = complete_response(ready_before)
        require(ready_before["status"] == 200 and ready_body.get("ready") is True, "restore positive readiness missing")
        before_b = await clients.request("before-source-b", "GET", "/v1/sources/" +
            base.urllib.parse.quote(self.accepted[second]["source_id"], safe=""), "service", second)
        before_b_body = complete_response(before_b)
        require(before_b["status"] == 200, "independent B positive missing before restore")
        if self.restore_phase == "replay":
            require(before_b_body == self.baseline[second]["source"], "independent B changed before replay")
            before_a = await clients.request("before-source-a", "GET", "/v1/sources/" +
                base.urllib.parse.quote(self.accepted[first]["source_id"], safe=""), "service", first)
            complete_response(before_a)
            require(before_a["status"] == 404, "erased A was visible before replay")
        end = time.monotonic() + min(RESTORE_SECONDS, self.commands.remaining())

        async def observe(subject):
            ordinal = 0
            while time.monotonic() < end:
                if subject == "readiness":
                    row = await clients.request("readiness", "GET", "/v1/readiness", "operator")
                else:
                    space = first if subject == "source-a" else second
                    source_id = self.accepted[space]["source_id"]
                    if ordinal % 2 == 0:
                        row = await clients.request(subject + "-rest", "GET", "/v1/sources/" +
                            base.urllib.parse.quote(source_id, safe=""), "service", space)
                    else:
                        row = await call_tool(clients, subject + "-mcp", "get_source", {"id": source_id}, "service", space)
                    if row["status"] == 200:
                        value = complete_response(row)
                        success = True
                        if ordinal % 2:
                            success, value = mcp_result(row)
                        if success and subject == "source-a" and self.restore_phase == "replay":
                            raise ValueError("erased source reappeared to an independent public client")
                        if success and subject == "source-b":
                            require(value in (before_b_body, self.baseline[second]["source"]), "independent B payload mixed revisions during restore")
                complete_response(row)
                samples.append(row)
                ordinal += 1
                if stop.is_set():
                    return
                # Reserve enough calls for restore completion and the remaining
                # base lifecycle; budget exhaustion is not an observed barrier.
                if self.total_http_calls >= MAX_REQUESTS - 150:
                    return
                await asyncio.sleep(0.025)

        tasks = [asyncio.create_task(observe(subject)) for subject in ("readiness", "source-a", "source-b")]
        await asyncio.sleep(0)
        try:
            if self.restore_phase == "replay":
                self.physical_replay_started_at = base.dt.datetime.now(base.dt.timezone.utc).isoformat()
            restored = await clients.request("restore", "POST", "/v1/system/snapshot/import", "operator",
                                             body={"path": backup["path"]}, restore=True)
        finally:
            stop.set()
            await asyncio.gather(*tasks)
        window = observed_restore_window(restored, samples)
        window["before_readiness"] = ready_before["capture"]
        window["before_b"] = before_b["capture"]
        window["samples"] = [row["capture"] for row in samples]
        window["peak_connections_including_restore"] = clients.peak_active
        return window

    def restore(self, backup):
        if self.restore_phase == "replay":
            self.capture_original_erasure()
        name = "restore_observation" if self.restore_phase == "clean" else "replay_observation"
        row = self.boundary_case(name, lambda: asyncio.run(self.observe_restore(backup)))
        if self.restore_phase == "replay":
            self.boundary_case("CB10", lambda: {"verdict": row["verdict"], **row["observations"]})
        return self.wait_ready()

    def erase_restore_replay(self):
        self.before_erasure_intents = self.journal_intents("before-erasure")
        self.erasure_started_at = base.dt.datetime.now(base.dt.timezone.utc).isoformat()
        self.restore_phase = "replay"
        try:
            result = super().erase_restore_replay()
        finally:
            self.restore_phase = "clean"
        first = self.cases["spaces"][0]
        self.boundary_case("CC06", lambda: asyncio.run(self.search_pair(first, self.cases["source_key"],
            self.accepted[first]["operation"]["desired_revision"], "erased")))
        result["post_replay_inspection"] = self.backup("post-replay-inspection", 300)["inspection"]
        require(self.physical_fixture is not None, "actual pre-replay erasure capture missing")
        self.physical_fixture["snapshots"]["replay"] = self.physical_snapshot_reference("post-replay-inspection")
        self.capture_physical_files("replay")
        self.save("physical-fixture.json", self.physical_fixture)
        result["physical_fixture"] = "physical-fixture.json"
        return result

    def journal_intents(self, name):
        root = base.RECALL_ROOT + "/recovery"
        container = self.once("original-journal-observation", ["-c",
            'test -d "$1/erasures" && test ! -L "$1/erasures" && test -f "$1/journal-id" && test ! -L "$1/journal-id"',
            "read-original-intents", root], mounts=[(self.volumes["recovery"], root, True)], entrypoint="/bin/sh")
        identity = self.copy(container, root + "/journal-id", 512).decode().strip()
        require(identity == self.original_journal["journal-id"], "intent directory belongs to another journal")
        _, raw, _ = self.commands.run(["container", "cp", container + ":" + root + "/erasures", "-"],
                                      secret=True, output_limit=JOURNAL_CAPTURE_CAP, timeout=30)
        entries = checked_journal_tar(raw)
        self.save("journal-intents-" + name + ".json", {"journal_id": identity, "entries": entries})
        return entries

    def capture_original_erasure(self):
        require(self.physical_fixture is None and self.erasure_acceptance is not None
                and self.before_erasure_intents is not None, "original erasure observations missing or repeated")
        first, second = self.cases["spaces"]
        completed = self.operation(first, self.erasure_acceptance["operation_id"])
        require(completed.get("state") == "complete" and completed.get("lifecycle") == "erased"
                and completed.get("cleanup_state") == "complete", "physical capture preceded actual completed erasure")
        completed_observed_at = base.dt.datetime.now(base.dt.timezone.utc).isoformat()
        self.backup("post-erasure-inspection", 300)
        after = self.journal_intents("after-erasure")
        for name, original in self.before_erasure_intents.items():
            require(after.get(name) == original, "original erasure journal intent changed")
        source_a = self.accepted[first]["source_id"]
        new_intents = [row["intent"] for name, row in after.items() if name not in self.before_erasure_intents
                       and row["intent"]["space_id"] == first and row["intent"]["source_id"] == source_a]
        require(len(new_intents) == 1, "exact new original source erasure intent required")
        require(base.utc(self.erasure_started_at) <= base.utc(new_intents[0]["recorded_at"])
                <= base.utc(completed_observed_at), "journal intent is outside actual observed erasure interval")
        def source_observations(space):
            accepted = self.accepted[space]
            revision = accepted["operation"]["desired_revision"]
            return {"space_id": space, "source_id": accepted["source_id"], "external_key": self.cases["source_key"],
                "revision_id": revision, "canary": self.metadata_canaries[(space, self.cases["source_key"], revision)],
                "accepted_receipt": accepted["operation"], "completed_baseline": self.baseline[space]}
        self.physical_fixture = {"format": "recall-physical-fixture/v1", "backend": self.allocation["backend"],
            "A": source_observations(first), "B": source_observations(second),
            "erasure": {"accepted": self.erasure_acceptance, "completed": completed},
            "journal": {"journal_id": self.original_journal["journal-id"],
                "reference_key_fingerprint": self.original_journal["reference-key-fingerprint"],
                "before_intents": list(self.before_erasure_intents.values()), "after_intents": list(after.values()),
                "original_erasure_intent": new_intents[0], "erase_started_at": self.erasure_started_at,
                "erase_completed_observed_at": completed_observed_at},
            "snapshots": {"pre": self.physical_snapshot_reference("baseline"),
                          "post": self.physical_snapshot_reference("post-erasure-inspection")}}
        self.capture_physical_files("post")

    def physical_snapshot_reference(self, name):
        # Collection metadata remains in inspection_reports. The existing
        # independent fixture contract takes these exact four original fields.
        return {key: value for key, value in self.inspection_reports[name].items()
                if key in {"capture", "sha256", "bytes", "manifest"}}

    def read_inspection_bytes(self, name):
        evidence = self.inspection_reports[name]
        path = self.root / evidence["capture"]
        require(path.is_file() and path.stat().st_size == evidence["bytes"] <= INSPECTION_CAP,
                "original inspection capture changed")
        raw = path.read_bytes()
        require(base.digest(raw) == evidence["sha256"], "original inspection capture digest changed")
        return raw

    def inspect_backup(self, backup, name):
        require(name.replace("-", "").isalnum() and name not in self.inspection_reports,
                "unique bounded offline inspection name required")
        # The only mounted volume is this run's backup volume, read-only. No
        # storage connection, auth token, source journal or live PV is available.
        # Capture stdout in a private tmpfs file so Docker log limits cannot
        # silently truncate a valid report. Arguments are positional, not code.
        output_path = "/tmp/recall-snapshot-inspection.json"
        container = self.once("offline-snapshot-inspection", ["-c",
            'umask 077; set -C; exec /usr/local/bin/meerkat-recall snapshot inspect --input "$1" --json --include-records --compact-records > "$2"',
            "inspect-owned-backup", backup["path"], output_path],
            mounts=[(self.volumes["backups"], base.RECALL_ROOT + "/backups", True)], entrypoint="/bin/sh")
        raw = self.copy(container, output_path, INSPECTION_CAP)
        report = checked_inspection(raw, backup["manifest"], expected_format="recall-snapshot-inspection-v2")
        require(self.inspection_capture_bytes + len(raw) <= 3 * INSPECTION_CAP,
                "offline inspection inventory capture budget exhausted")
        self.reserve_capture(len(raw))
        self.inspection_capture_bytes += len(raw)
        capture = "physical-inspection-" + name + ".json"
        base.private_new(self.root / capture, raw)
        evidence = {"capture": capture, "sha256": base.digest(raw), "bytes": len(raw),
                    "backup_manifest_sha256": base.digest(base.encoded(backup["manifest"])),
                    "manifest": backup["manifest"],
                    "row_count": report["row_count"], "table_counts": report["table_counts"],
                    "scope": "complete quiesced export inventory; physical source/PV/media/backup verdict remains independent"}
        self.inspection_reports[name] = evidence
        return evidence

    def backup(self, name, seconds):
        backup = super().backup(name, seconds)
        # Starting a one-shot inspector could legitimately exceed the original
        # five-second expiry. That case proves actual registered file deletion;
        # use the unexpired baseline/post-erasure exports for typed inventory.
        if name != "short-retention":
            backup["inspection"] = self.inspect_backup(backup, name)
        if name == "baseline":
            self.capture_physical_files("pre")
        return backup

    def physical_inventory_module(self):
        path = ROOT / "scripts/qualification/physical_inventory.py"
        spec = importlib.util.spec_from_file_location("boundary_physical_inventory", path)
        require(spec is not None and spec.loader is not None, "physical inventory interpreter unavailable")
        module = importlib.util.module_from_spec(spec)
        raw = path.read_bytes()
        require(base.digest(raw) == self.boundary_inventory["dependency_sha256"][str(path.relative_to(ROOT))],
                "reviewed physical inventory source changed")
        exec(compile(raw, str(path), "exec", dont_inherit=True), module.__dict__)
        return module

    def private_membership_module(self):
        path = ROOT / "scripts/qualification/native_private_membership.py"
        raw = path.read_bytes()
        require(base.digest(raw) == self.boundary_inventory["dependency_sha256"][str(path.relative_to(ROOT))],
                "reviewed native private inspection source changed")
        spec = importlib.util.spec_from_file_location("boundary_native_private_membership", path)
        require(spec is not None and spec.loader is not None, "native private inspection interpreter unavailable")
        module = importlib.util.module_from_spec(spec)
        exec(compile(raw, str(path), "exec", dont_inherit=True), module.__dict__)
        return module

    def capture_private_membership(self, phase):
        """Fixed read-only native inspection with Recall stopped, before DB stop."""
        interpreter = self.private_membership_module()
        remaining = EVIDENCE_CAP - self.evidence_capture_bytes - self.graph_capture_held
        maximum = min(interpreter.RAW_CAP, INSPECTION_CAP, remaining)
        require(maximum > 0, "native inspection has no original remaining evidence allocation")
        timeout = min(30, int(self.commands.remaining()))
        require(timeout > 0, "native inspection original deadline expired")
        environment = {key: self.environment[key] for key in ("SURREALDB_USER", "SURREALDB_PASS")}
        output, status = "/tmp/recall-private-native.json", "/tmp/recall-private-status.txt"
        container = self.once("native-private-inspection", ["-c",
            'umask 077; set -C; exec curl --silent --show-error --fail --noproxy "*" '
            '--proto "=http" --max-time "$2" --max-filesize "$3" '
            '--user "$SURREALDB_USER:$SURREALDB_PASS" '
            '--header "Accept: application/json" --header "Content-Type: application/surrealql" '
            '--header "Surreal-NS: offline" --header "Surreal-DB: fixture" '
            '--data-binary "$1" --output "$4" --write-out "%{http_code}" '
            'http://paired-store:8000/sql > "$5"',
            "inspect-owned-native-membership", interpreter.QUERY, str(timeout), str(maximum), output, status],
            network=self.network, environment=environment, entrypoint="/bin/sh")
        code = self.copy(container, status, 3)
        require(code == b"200", "native private inspection HTTP status was not complete success")
        raw = self.copy(container, output, maximum)
        self.reserve_capture(len(raw) + len(code))
        name = "physical-private-native-" + phase + ".json"
        base.private_new(self.root / name, raw)
        return raw, {"capture": name, "sha256": base.digest(raw), "bytes": len(raw), "http_status": 200,
                     "query_sha256": base.digest(interpreter.QUERY.encode()), "writer_stopped": True}

    def capture_physical_files(self, phase):
        require(phase in ("pre", "post", "replay", "expired") and phase not in self.physical_files,
                "original physical phase repeated or unknown")
        interpreter = self.physical_inventory_module()
        self.stop()
        private = self.capture_private_membership(phase) if self.database else None
        if self.database:
            self.stop(self.database)
        # The stopped original service and paired store are the only product
        # writers. The helper has no network and all mounts are read-only.
        mounts = [(self.volumes[key], "/physical/" + key, True)
                  for key in ("data", "database", "backups")]
        mounts.append((self.volumes["recovery"], "/physical/journal", True))
        # GNU find emits NUL-delimited actual type/device/inode/size/link/path
        # tuples to existing private tmpfs, avoiding Docker log truncation.
        # No shell-expanded paths, journal traversal, symlink following or
        # candidate parser participates in this independent metadata capture.
        metadata_path = "/tmp/recall-physical-metadata.bin"
        container = self.once("physical-volume-capture", ["-c",
            'umask 077; set -eC; test ! -L /physical/journal/history-commitments; '
            'if test -e /physical/journal/history-commitments; then '
            'test -d /physical/journal/history-commitments; printf present; else printf absent; fi '
            '> /tmp/recall-history-presence.txt; '
            'test ! -L /physical/journal/imported-history-origins.json; '
            'if test -e /physical/journal/imported-history-origins.json; then '
            'test -f /physical/journal/imported-history-origins.json; printf present; else printf absent; fi '
            '> /tmp/recall-origin-presence.txt; '
            "exec find /physical/data /physical/database /physical/backups "
            "-printf '%y\\0%D\\0%i\\0%s\\0%n\\0%p\\0' > \"$1\"",
            "capture-owned-metadata", metadata_path], mounts=mounts,
                              user="0:0", entrypoint="/bin/sh")
        journal_id = self.copy(container, "/physical/journal/journal-id", 512).decode().strip()
        require(journal_id == self.original_journal["journal-id"], "physical capture journal changed")
        captures, evidence, files, directories = {}, {}, {}, set()
        for root in interpreter.ROOTS:
            path = "/physical/journal/backups" if root == "registry" else "/physical/" + root
            remaining = EVIDENCE_CAP - self.evidence_capture_bytes - self.graph_capture_held
            require(remaining >= 10240, "physical capture has no remaining original evidence allocation")
            _, raw, _ = self.commands.run(["container", "cp", container + ":" + path, "-"],
                secret=True, output_limit=min(interpreter.CAPTURE_CAP, remaining), timeout=60)
            files.update(interpreter.archive(raw, root, directory_inventory=directories))
            self.reserve_capture(len(raw))
            name = "physical-files-" + phase + "-" + root + ".tar"
            base.private_new(self.root / name, raw)
            captures[root] = raw
            evidence[root] = {"capture": name, "sha256": base.digest(raw), "bytes": len(raw)}
        remaining = EVIDENCE_CAP - self.evidence_capture_bytes - self.graph_capture_held
        require(remaining > 0, "physical metadata has no remaining original evidence allocation")
        metadata_raw = self.copy(container, metadata_path, min(interpreter.CAPTURE_CAP, remaining))
        interpreter.file_metadata(metadata_raw, files, directories)
        self.reserve_capture(len(metadata_raw))
        name = "physical-files-" + phase + "-metadata.bin"
        base.private_new(self.root / name, metadata_raw)
        evidence["metadata"] = {"capture": name, "sha256": base.digest(metadata_raw), "bytes": len(metadata_raw)}
        # Only these exact non-secret custody files may leave the journal.
        # reference-key.json is never opened, copied, hashed or parsed.
        membership = {}
        for field, filename in (("index", "reference-membership-index.json"),
                                ("fingerprint", "reference-key-fingerprint")):
            raw = self.copy(container, "/physical/journal/" + filename, JOURNAL_CAPTURE_CAP)
            self.reserve_capture(len(raw))
            name = "physical-membership-" + phase + "-" + field + ".json"
            base.private_new(self.root / name, raw)
            membership[field] = raw
            evidence["membership_" + field] = {"capture": name, "sha256": base.digest(raw), "bytes": len(raw)}
        remaining = EVIDENCE_CAP - self.evidence_capture_bytes - self.graph_capture_held
        require(remaining >= 10240, "membership capture has no original remaining allocation")
        _, raw, _ = self.commands.run(["container", "cp", container + ":/physical/journal/reference-memberships", "-"],
            secret=True, output_limit=min(interpreter.CAPTURE_CAP, remaining), timeout=60)
        interpreter.archive(raw, "reference-memberships")
        self.reserve_capture(len(raw))
        name = "physical-membership-" + phase + "-files.tar"
        base.private_new(self.root / name, raw)
        membership["files"] = raw
        evidence["membership_files"] = {"capture": name, "sha256": base.digest(raw), "bytes": len(raw)}
        presence = self.copy(container, "/tmp/recall-history-presence.txt", 7)
        require(presence in (b"present", b"absent"), "native history presence capture incomplete")
        self.reserve_capture(len(presence))
        membership["history"] = None
        if presence == b"present":
            remaining = EVIDENCE_CAP - self.evidence_capture_bytes - self.graph_capture_held
            require(remaining >= 10240, "history capture has no original remaining allocation")
            _, raw, _ = self.commands.run(["container", "cp", container + ":/physical/journal/history-commitments", "-"],
                secret=True, output_limit=min(interpreter.CAPTURE_CAP, remaining), timeout=60)
            interpreter.archive(raw, "history-commitments")
            self.reserve_capture(len(raw))
            name = "physical-history-" + phase + ".tar"
            base.private_new(self.root / name, raw)
            membership["history"] = raw
            evidence["history_files"] = {"capture": name, "sha256": base.digest(raw), "bytes": len(raw)}
        evidence["history_presence"] = presence.decode()
        presence = self.copy(container, "/tmp/recall-origin-presence.txt", 7)
        require(presence in (b"present", b"absent"), "native restore origin presence capture incomplete")
        self.reserve_capture(len(presence))
        membership["origins"] = None
        if presence == b"present":
            remaining = EVIDENCE_CAP - self.evidence_capture_bytes - self.graph_capture_held
            require(remaining > 0, "restore origins have no original remaining evidence allocation")
            raw = self.copy(container, "/physical/journal/imported-history-origins.json",
                            min(interpreter.ORIGIN_CAP, remaining))
            self.reserve_capture(len(raw))
            name = "physical-restore-origins-" + phase + ".json"
            base.private_new(self.root / name, raw)
            membership["origins"] = raw
            evidence["restore_origins"] = {"capture": name, "sha256": base.digest(raw), "bytes": len(raw),
                                           "mac_verified_by_collector": False}
        evidence["restore_origin_presence"] = presence.decode()
        observed_at = base.dt.datetime.now(base.dt.timezone.utc).isoformat()
        maintenance_started_at = (getattr(self, "erasure_started_at", None) if phase == "post" else
                                  getattr(self, "physical_replay_started_at", None) if phase == "replay" else None)
        self.physical_files[phase] = {"archives": captures, "observed_at": observed_at,
                                     "stopped": True, "journal_id": journal_id, "membership": membership,
                                     "metadata": metadata_raw, "maintenance_started_at": maintenance_started_at}
        self.physical_file_evidence[phase] = {"archives": evidence, "observed_at": observed_at,
                                             "stopped": True, "journal_id": journal_id,
                                             "maintenance_started_at": maintenance_started_at}
        if private is not None:
            self.physical_files[phase]["private_membership"] = private[0]
            self.physical_file_evidence[phase]["private_membership"] = private[1]
        self.save("physical-files-" + phase + ".json", self.physical_file_evidence[phase])
        if self.database:
            self.commands.run(["start", self.database])
            self.wait_database()
        self.commands.run(["start", self.server])
        self.wait_ready()

    def capture_physical_expiry(self):
        interpreter = self.physical_inventory_module()
        require("pre" in self.physical_files, "original pre-erasure inventory missing")
        deadlines = [interpreter.retention_deadline(capture, self.original_journal["journal-id"])
                     for capture in self.physical_files.values()]
        deadline = max((value for value in deadlines if value is not None), default=None)
        if deadline is not None:
            now = base.dt.datetime.now(base.dt.timezone.utc)
            # No renewed clock, shortened retention, or manual deletion. A long
            # original deadline remains observable as incomplete evidence.
            if (deadline - now).total_seconds() < self.commands.remaining() - 150:
                while base.dt.datetime.now(base.dt.timezone.utc) <= deadline:
                    self.commands.remaining()
                    base.time.sleep(min(1, self.commands.remaining()))
                self.stop()
                if self.database:
                    self.stop(self.database)
                    self.commands.run(["start", self.database])
                    self.wait_database()
                self.commands.run(["start", self.server])
                self.wait_ready()  # Existing startup retention cleanup owns removal.
        self.capture_physical_files("expired")

    def unavailable_pair(self):
        if self.allocation["backend"] != "surreal-db":
            return {"verdict": "unmet", "reason": "No existing supported embedded storage-failure control produces unavailable."}
        first = self.cases["spaces"][1]  # Independent B survives the base erasure.
        query = {"query": "boundary-no-evidence", "ranking_mode": "lexical", "limit": 1}
        positive = self.http("POST", f"/v1/spaces/{first}/search", "service", first, query)
        require(positive["status"] == 200, "native healthy positive failed before paired server stop")
        self.stop(self.database)
        try:
            return asyncio.run(self.error_pair(f"/v1/spaces/{first}/search", query, "search_scoped", query,
                                              "unavailable", 503, True, space=first))
        finally:
            self.commands.run(["start", self.database])
            self.wait_database()
            self.wait_ready()

    def execute(self):
        super().execute()
        self.boundary_case("CB06", self.unavailable_pair)
        self.capture_physical_expiry()
        self.boundary_case("physical_erasure_inventory", self.evaluate_physical_exports)

    def evaluate_physical_exports(self):
        require(self.physical_fixture is not None, "actual physical fixture receipt missing")
        result = physical_oracle().evaluate_export_bytes(self.read_inspection_bytes("baseline"),
            self.read_inspection_bytes("post-erasure-inspection"), self.read_inspection_bytes("post-replay-inspection"),
            self.physical_fixture)
        require(isinstance(result, dict) and result.get("verdict") in ("pass", "fail", "unmet")
                and result.get("scope") == "exported_inventory" and result.get("product_acceptance") is False,
                "independent export evaluator returned an invalid component verdict")
        self.physical_export_comparison = result
        self.save("physical-export-comparison.json", result)
        if result["verdict"] == "fail":
            physical = {"verdict": "fail", "reason": "independent_export_component_failed"}
        elif not getattr(self, "physical_files", None):
            physical = {"verdict": "unmet", "reason": "original_stopped_volume_captures_missing"}
        else:
            physical = self.physical_inventory_module().evaluate(self.physical_files, self.physical_fixture, result,
                oracle=physical_oracle(), original_report=self.read_inspection_bytes("baseline"),
                private_oracle=self.private_membership_module() if self.allocation["backend"] == "surreal-db" else None,
                phase_reports={"post": self.read_inspection_bytes("post-erasure-inspection"),
                               "replay": self.read_inspection_bytes("post-replay-inspection")})
        self.save("physical-volume-comparison.json", physical)
        return {"verdict": physical["verdict"], "captured_exports": self.inspection_reports,
            "captured_physical_volumes": getattr(self, "physical_file_evidence", {}),
            "export_comparison": result, "volume_comparison": physical,
            "reason": "Complete original PV/WAL/media/previous snapshots and registered backup evidence are evaluated independently."}


    def finish(self, failure=None):
        if self.graph_budget is not None:
            state = self.graph_budget.change(allow_expired=True)
            self.total_http_calls = state["calls"]
            self.evidence_capture_bytes = state["written"]
        original = super().finish(failure)
        report = {"format": "recall-container-boundary-observations/v1", "backend": self.allocation["backend"],
            "run_id": self.allocation["run_id"], "inventory": self.boundary_inventory,
            "base_observations": "observations.json", "base_cases_passed": original["product_acceptance"],
            "cases": self.boundary_cases, "failure_type": failure or self.boundary_failure,
            "combined_http_calls": self.total_http_calls,
            "offline_inspection_captures": self.inspection_reports,
            "offline_inspection_bytes": self.inspection_capture_bytes,
            "private_evidence_bytes_before_final_report": self.evidence_capture_bytes,
            "independent_capture_bytes": self.clients.capture_bytes if self.clients else 0,
            "product_acceptance": original["product_acceptance"] and self.boundary_failure is None
                and all(row["verdict"] == "pass" for row in self.boundary_cases.values())}
        self.save("boundary-observations.json", report)
        return report


def plan():
    cases = base.strict_json(CASES_PATH.read_bytes())
    dependencies = {path: base.digest((ROOT / path).read_bytes()) if (ROOT / path).is_file() else None
                    for path in BOUNDARY_DEPENDENCIES}
    return {"format": "recall-container-boundary-plan/v1", "expectations": cases,
            "expectations_sha256": base.digest(CASES_PATH.read_bytes()),
            "driver_sha256": base.digest(Path(__file__).read_bytes()), "dependency_sha256": dependencies,
            "base_plan": base.plan(), "product_acceptance": False}


def validate_allocation(path, now=None):
    allocation, inventory = base.validate_allocation(path, now)
    boundary = plan()
    review_path = Path(allocation["boundary_review"])
    review = base.read_private_json(review_path)
    require(base.digest(review_path.read_bytes()) == allocation["boundary_review_sha256"], "boundary review digest changed")
    require(review.get("format") == "recall-container-boundary-review/v1" and review.get("reviewer") == "qualification"
            and review.get("approved") is True, "independent boundary review required")
    for field in ("driver_sha256", "expectations_sha256", "dependency_sha256"):
        require(review.get(field) == boundary[field], "boundary source or expectation review changed")
    require(all(base.HEX64.fullmatch(value or "") for value in boundary["dependency_sha256"].values()),
            "boundary source dependencies missing")
    graph_inputs(allocation, review)
    contract = base.strict_json(CONTRACT_PATH.read_bytes())
    require(contract["format"] == "recall-container-boundary-expectations/v1"
            and [case["id"] for case in contract["error_cases"]] == [f"CB{i:02}" for i in range(1, 11)]
            and [case["id"] for case in contract["coverage_cases"]] == [f"CC{i:02}" for i in range(1, 7)],
            "independent boundary case contract changed")
    return allocation, inventory, boundary


def main(argv=None):
    parser = base.argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    commands.add_parser("plan", help="render inert source inventory")
    run = commands.add_parser("run", help="run only the centrally allocated and independently reviewed extension")
    run.add_argument("--allocation", required=True, type=Path)
    args = parser.parse_args(argv)
    if args.action == "plan":
        print(base.encoded(plan()).decode(), end="")
        return 0
    allocation, inventory, boundary = validate_allocation(args.allocation)
    directory = Path(allocation["run_directory"])
    directory.mkdir(mode=0o700, parents=False)
    driver = None
    failure = None
    cancellation = base.WorkCancellation()
    prior_term = base.signal.getsignal(base.signal.SIGTERM)
    base.signal.signal(base.signal.SIGTERM, cancellation.request)
    try:
        commands = base.DockerCommands(allocation, directory, cancellation=cancellation)
        driver = ContainerBoundaryDriver(allocation, inventory, boundary, commands=commands)
        driver.execute()
        cancellation.check()
    except BaseException as error:
        failure = type(error).__name__
        if driver is None:
            base.private_new(directory / "startup-failure.json", base.encoded({"failure_type": failure, "product_acceptance": False}))
    finally:
        cancellation.begin_cleanup()
        try:
            report = driver.finish(failure) if driver else {"product_acceptance": False}
        finally:
            base.signal.signal(base.signal.SIGTERM, prior_term)
    print(base.encoded({"report_directory": str(directory), "product_acceptance": report["product_acceptance"]}).decode(), end="")
    return 0 if report["product_acceptance"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
