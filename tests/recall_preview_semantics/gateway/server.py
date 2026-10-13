"""Host-side AF_UNIX gateway. Only the sandbox's loopback bridge can reach it."""
from __future__ import annotations

from http.server import BaseHTTPRequestHandler
import socketserver
import threading

from cohort_profile import SEMANTIC140
from wire import Denied, MAX_BODY_BYTES, encode, require, validate


class GatewayServer(socketserver.ThreadingUnixStreamServer):
    daemon_threads = False
    block_on_close = True
    request_queue_size = 4

    def __init__(self, path, policy, forwarder):
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
        super().__init__(str(path), Handler)

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
        self.request.settimeout(10)
        super().setup()

    def log_message(self, format, *args):
        pass  # Never log paths, headers, prompts, provider bodies or exceptions.

    def send_error(self, code, message=None, explain=None):
        self.deny(code, "invalid_http")

    def deny(self, status, code):
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
            if control:
                self.server.policy.authenticate_control(self.headers["Authorization"])
            else:
                scope = self.server.policy.authenticate(self.headers["Authorization"])
            raw = self.rfile.read(count)
            require(len(raw) == count, "incomplete_request")
            if control:
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

            self.server.forwarder.dispatch(request, raw, start_response, write)
        except Denied as error:
            if not started:
                self.deny(403, str(error))
        except Exception:
            if not started:
                self.deny(502, "gateway_dispatch_incomplete")
