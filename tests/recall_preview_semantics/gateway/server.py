"""Host-side AF_UNIX gateway. Only the sandbox's loopback bridge can reach it."""
from __future__ import annotations

from http.server import BaseHTTPRequestHandler
import datetime
import hashlib
import os
from pathlib import Path
import time
import uuid
import socketserver
import threading

from cohort_profile import SEMANTIC140
from capture import write_new
from ledger import sync_directory
from wire import Denied, MAX_BODY_BYTES, encode, require, validate


# A diagnostic vocabulary, not an authorization allowlist. Unknown reasons are
# retained only as "unclassified_denial"; never copy an arbitrary exception.
DENIAL_CODES = frozenset("""
invalid_http expectation_not_supported unsupported_http_version ambiguous_http_header
unsupported_http_framing unapproved_host unsupported_content_type invalid_content_length
request_body_limit incomplete_request cohort_control_response_limit gateway_dispatch_incomplete
invalid_capability approval_changed_or_revoked approval_expired unsupported_path
unapproved_context_policy unapproved_transform_mode unapproved_model unsupported_fields
invalid_json_value duplicate_json_key nonfinite_json invalid_json request_object_required
text_required schema_depth_limit remote_schema_reference text_content_required
nontext_content remote_annotations operation_path_mismatch unapproved_dimensions
unsupported_encoding embedding_batch_limit empty_embedding_input embedding_input_limit
output_token_limit server_storage_forbidden unapproved_service_tier unapproved_reasoning
unsupported_include invalid_stream invalid_tool_parallelism invalid_sampling
state_reference_forbidden truncation_forbidden input_item_limit input_item_object_required
unsupported_role incomplete_history_item unsupported_phase opaque_reasoning_forbidden
unsupported_input_type tool_limit hosted_tool_forbidden invalid_tool_schema
unsupported_tool_choice unsupported_verbosity unsupported_output_format invalid_output_schema
generation_input_limit money_limit dispatch_count_limit concurrency_limit operation_count_limit
run_time_limit clock_rollback boot_changed ledger_unavailable unknown_scope run_halted
response_byte_limit unsupported_upstream_encoding incomplete_http_framing invalid_upstream_header
duplicate_reasoning_item duplicate_tool output_schema_required reasoning_provenance_required
reasoning_capture_changed settlement_exact_binding_required settlement_request_hash
settlement_embedding_reserve settlement_output_cap settlement_full_reserve_required
settlement_request_identity settlement_money_integrity settlement_row_integrity
settlement_delivery_unsettled settlement_late_delivery
unclassified_denial
""".split())
DENIAL_STAGES = frozenset({"http_headers", "framing", "authentication", "body",
                           "cohort_control", "wire_validation", "forwarding", "unknown"})
DENIAL_ROUTES = {"/v1/responses": "responses", "/v1/embeddings": "embeddings",
                 "/v1/_qualification/cohort": "cohort_control"}


class DenialCapture:
    """Private bounded diagnostics, independent of dispatch/usage accounting."""
    MAX_EVENTS = 128
    MAX_EVENT_BYTES = 512
    MAX_COUNTER = 2**63 - 1

    def __init__(self, directory):
        self.directory = Path(directory)
        stem = "gateway-denials-" + uuid.uuid4().hex
        self.path = self.directory / (stem + ".jsonl")
        self.receipt_path = self.directory / (stem + ".receipt.json")
        self.fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        self.mutex = threading.Lock()
        self.started_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        self.started = time.monotonic_ns()
        self.count = 0
        self.omitted = 0
        self.size = 0
        self.digest = hashlib.sha256()
        self.failed = False
        self.closed = False
        self.summary = None
        try:
            sync_directory(self.directory)
        except BaseException:
            os.close(self.fd)
            raise

    def record(self, status, code, stage, path):
        with self.mutex:
            if self.closed or self.failed or self.count >= self.MAX_EVENTS:
                self.omitted = min(self.MAX_COUNTER, self.omitted + 1)
                return
            event = {"sequence": self.count + 1,
                     "elapsed_ns": min(self.MAX_COUNTER, max(0, time.monotonic_ns() - self.started)),
                     "status": status if type(status) is int and 100 <= status <= 599 else 0,
                     "code": code if type(code) is str and code in DENIAL_CODES else "unclassified_denial",
                     "stage": stage if type(stage) is str and stage in DENIAL_STAGES else "unknown",
                     "route": DENIAL_ROUTES.get(path, "other") if type(path) is str else "other"}
            try:
                raw = encode(event) + b"\n"
                require(len(raw) <= self.MAX_EVENT_BYTES, "denial_capture_event_bound")
                offset = 0
                while offset < len(raw):
                    written = os.write(self.fd, raw[offset:])
                    if written <= 0:
                        raise OSError("denial_capture_write")
                    self.digest.update(raw[offset:offset + written])
                    self.size += written
                    offset += written
                os.fsync(self.fd)
                self.count += 1
            except Exception:
                # Preserve the original denial and never turn recording failure
                # into admission, forwarding, a retry or a fabricated completion.
                self.failed = True
                self.omitted = min(self.MAX_COUNTER, self.omitted + 1)

    def finish(self):
        with self.mutex:
            if self.closed:
                return self.summary
            self.closed = True
            try:
                os.fsync(self.fd)
            except OSError:
                self.failed = True
            finally:
                os.close(self.fd)
            self.summary = {"format": "recall-gateway-denial-capture/v1",
                            "started_at": self.started_at,
                            "file": {"name": self.path.name, "sha256": self.digest.hexdigest(), "bytes": self.size},
                            "recorded_events": self.count, "omitted_events": self.omitted,
                            "maximum_events": self.MAX_EVENTS, "maximum_event_bytes": self.MAX_EVENT_BYTES,
                            "write_failed": self.failed, "complete": not self.failed and self.omitted == 0,
                            "dispatch_or_usage_evidence": False, "request_or_secret_data_retained": False}
            write_new(self.receipt_path, encode(self.summary))
            sync_directory(self.directory)
            return self.summary


class GatewayServer(socketserver.ThreadingUnixStreamServer):
    daemon_threads = False
    block_on_close = True
    request_queue_size = 4

    def __init__(self, path, policy, forwarder, *, diagnostic_directory=None):
        self.policy = policy
        self.forwarder = forwarder
        if getattr(policy, "profile", None) == SEMANTIC140:
            require(getattr(policy, "cohort", False), "semantic140_cohort_required")
            capacity = SEMANTIC140.connection_slots
            self.request_queue_size = capacity
        else:
            capacity = 64 if getattr(policy, "cohort", False) else 8
            self.request_queue_size = 64 if getattr(policy, "cohort", False) else 4
        self.slots = threading.BoundedSemaphore(capacity)
        self.denial_capture = None
        super().__init__(str(path), Handler)
        if diagnostic_directory is not None:
            try:
                self.denial_capture = DenialCapture(diagnostic_directory)
            except BaseException:
                super().server_close()
                raise

    def capture_denial(self, status, code, stage, path):
        if self.denial_capture is not None:
            self.denial_capture.record(status, code, stage, path)

    def close_denial_capture(self):
        if self.denial_capture is not None:
            return self.denial_capture.finish()
        return None

    def process_request(self, request, address):
        if not self.slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, address)
        except BaseException:
            self.slots.release()
            raise

    def process_request_thread(self, request, address):
        try:
            super().process_request_thread(request, address)
        finally:
            self.slots.release()

    def handle_error(self, request, address):
        # socketserver's default traceback can expose request/secret locals.
        pass


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "RecallQualificationGateway"
    sys_version = ""

    def setup(self):
        self.diagnostic_stage = "http_headers"
        self.request.settimeout(10)
        super().setup()

    def log_message(self, format, *args):
        pass  # Never log paths, headers, prompts, provider bodies or exceptions.

    def send_error(self, code, message=None, explain=None):
        self.deny(code, "invalid_http")

    def deny(self, status, code):
        self.server.capture_denial(status, code, getattr(self, "diagnostic_stage", "unknown"),
                                   getattr(self, "path", None))
        body = encode({"error": {"code": code}})
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.close_connection = True

    def handle_expect_100(self):
        self.deny(417, "expectation_not_supported")
        return False

    def do_POST(self):
        started = False
        self.close_connection = True
        try:
            self.diagnostic_stage = "framing"
            require(self.request_version == "HTTP/1.1", "unsupported_http_version")
            for name in ("Host", "Authorization", "Content-Length", "Content-Type"):
                require(len(self.headers.get_all(name, [])) == 1, "ambiguous_http_header")
            require(not self.headers.get_all("Transfer-Encoding") and not self.headers.get_all("Content-Encoding")
                    and not self.headers.get_all("Expect"), "unsupported_http_framing")
            require(self.headers["Host"] == "127.0.0.1:43129", "unapproved_host")
            require(self.headers["Content-Type"].split(";", 1)[0].strip() == "application/json", "unsupported_content_type")
            length = self.headers["Content-Length"]
            require(length.isascii() and length.isdecimal() and len(length) <= 6, "invalid_content_length")
            count = int(length)
            require(0 < count <= MAX_BODY_BYTES, "request_body_limit")
            control = getattr(self.server.policy, "cohort", False) and self.path == "/v1/_qualification/cohort"
            self.diagnostic_stage = "authentication"
            if control:
                self.server.policy.authenticate_control(self.headers["Authorization"])
            else:
                scope = self.server.policy.authenticate(self.headers["Authorization"])
            self.diagnostic_stage = "body"
            raw = self.rfile.read(count)
            require(len(raw) == count, "incomplete_request")
            if control:
                self.diagnostic_stage = "cohort_control"
                from wire import strict_json
                reply = self.server.policy.control(strict_json(raw), self.server.forwarder.ledger)
                body = encode(reply)
                require(len(body) <= 65536, "cohort_control_response_limit")
                started = True
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(body)
                return
            self.diagnostic_stage = "wire_validation"
            request = validate(raw, self.path, scope, output_cap_mode=self.server.policy.output_cap_mode,
                               context_policy=self.server.policy.context_policy,
                               authorize_reasoning=getattr(self.server.forwarder.ledger, "authorize_reasoning", None))

            def start_response(status, content_type):
                nonlocal started
                require("\r" not in content_type and "\n" not in content_type, "invalid_upstream_header")
                started = True
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Connection", "close")
                self.end_headers()

            def write(block):
                self.wfile.write(block)
                self.wfile.flush()

            self.diagnostic_stage = "forwarding"
            self.server.forwarder.dispatch(request, raw, start_response, write)
        except Denied as error:
            if not started:
                self.deny(403, str(error))
        except Exception:
            if not started:
                self.deny(502, "gateway_dispatch_incomplete")
