"""A single fixed, verified TLS upstream; never redirects, proxies or retries."""
from __future__ import annotations

import hashlib
import http.client
import json
import socket
import ssl
import threading
import time

from ledger import RESPONSE_CAP
from wire import Denied, Request, require


class PinnedTLS(http.client.HTTPSConnection):
    def __init__(self, address: str, timeout: float):
        super().__init__("api.openai.com", 443, timeout=timeout, context=ssl.create_default_context())
        self.address = address

    def connect(self):
        # The approved numeric address is used directly; certificate validation
        # and SNI still name the fixed upstream. No runtime DNS or proxy lookup.
        deadline = time.monotonic() + self.timeout
        raw = socket.create_connection((self.address, 443), self.timeout)
        self.sock = raw
        try:
            raw.settimeout(max(0.001, deadline - time.monotonic()))
            self.sock = self._context.wrap_socket(raw, server_hostname="api.openai.com")
        except BaseException:
            raw.close()
            raise


def usage_from_response(body: bytes, streaming: bool) -> tuple[int, int] | None:
    """Advisory usage only; never releases a reservation or authenticates state."""
    try:
        if streaming:
            candidates = []
            # SSE data is inspected without modifying the forwarded bytes.
            for frame in body.replace(b"\r\n", b"\n").split(b"\n\n"):
                data = b"\n".join(line[5:].lstrip(b" ") for line in frame.split(b"\n") if line.startswith(b"data:"))
                if data and data != b"[DONE]":
                    value = json.loads(data)
                    if value.get("type") in ("response.completed", "response.incomplete", "response.failed"):
                        candidates.append(value.get("response", {}))
            value = candidates[-1] if candidates else {}
        else:
            value = json.loads(body)
        usage = value.get("usage", {})
        input_tokens = usage.get("input_tokens", usage.get("prompt_tokens"))
        output_tokens = usage.get("output_tokens", 0)
        if type(input_tokens) is int and type(output_tokens) is int and min(input_tokens, output_tokens) >= 0:
            return input_tokens, output_tokens
    except (ValueError, TypeError, AttributeError):
        pass
    return None


def stream_complete(body: bytes) -> bool:
    try:
        for frame in body.replace(b"\r\n", b"\n").split(b"\n\n"):
            data = b"\n".join(line[5:].lstrip(b" ") for line in frame.split(b"\n") if line.startswith(b"data:"))
            if data and data != b"[DONE]" and json.loads(data).get("type") == "response.completed":
                return True
    except (ValueError, TypeError, AttributeError):
        return False
    return False


def explicit_incomplete(body: bytes) -> bool:
    try:
        value = json.loads(body)
        return isinstance(value, dict) and value.get("status") in ("incomplete", "failed", "cancelled")
    except (ValueError, TypeError):
        return False


def cohort_framing(response):
    """Actual allowlisted framing facts only; never retain credential headers."""
    headers = getattr(response, "headers", None)
    require(headers is not None and callable(getattr(headers, "get_all", None)), "cohort_original_headers_missing")
    fields = {}
    for key, name in (("content_length", "Content-Length"), ("transfer_encoding", "Transfer-Encoding"),
                      ("content_encoding", "Content-Encoding"), ("content_type", "Content-Type")):
        values = headers.get_all(name, [])
        require(type(values) is list and len(values) <= 4
                and all(type(value) is str and len(value.encode()) <= 1024 for value in values),
                "cohort_framing_capture_bound")
        fields[key] = values
    require(len(json.dumps(fields).encode()) <= 4096, "cohort_framing_capture_bound")
    return fields


class Forwarder:
    def __init__(self, ledger, authorize, connection, credential: str, capture):
        self.ledger = ledger
        self.authorize = authorize
        self.connection = connection
        self.credential = credential
        self.capture = capture

    def dispatch(self, request: Request, original: bytes, start_response, write) -> None:
        identifier, seconds = self.ledger.reserve(request, self.authorize)
        transport_started = time.monotonic()
        deadline = transport_started + seconds
        cohort_transport = None
        if getattr(self.ledger, "capture_original_transport", False):
            cohort_transport = {"format": "recall-provider-transport/v2", "started_mono": transport_started,
                                "request_started_mono": None, "deadline_mono": deadline, "read_completed_mono": None,
                                "response_headers": None, "eof_observed": False, "remaining_length": None,
                                "chunked": None, "response_closed": None}
        conn = None
        status = None
        content_type = None
        body = bytearray()
        response_hash = hashlib.sha256()
        complete = False
        recorded = False
        captured = None
        timer = None
        read_completed = None
        try:
            if hasattr(self.ledger, "original_transport_deadline"):
                deadline = min(deadline, self.ledger.original_transport_deadline(identifier))
                require(deadline > transport_started, "dispatch_deadline")
                if cohort_transport is not None:
                    cohort_transport["deadline_mono"] = deadline
            if hasattr(self.ledger, "bind_deadline"):
                self.ledger.bind_deadline(identifier, deadline)
            captured = self.capture(identifier, request, original)
            # Factory construction and connection both occur after durable reserve.
            conn = self.connection(min(seconds, self.ledger.before_forward(self.authorize)))
            def expire():
                # Bound slow-drip headers and body reads as well as socket calls.
                sock = conn.sock
                if sock is not None:
                    try:
                        sock.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
            timer = threading.Timer(max(0.001, deadline - time.monotonic()), expire)
            timer.daemon = True
            timer.start()
            conn.connect()
            remaining = min(deadline - time.monotonic(), self.ledger.before_forward(self.authorize))
            require(remaining > 0, "dispatch_deadline")
            conn.sock.settimeout(remaining)
            if hasattr(self.ledger, "before_request"):
                self.ledger.before_request(request)
                remaining = min(deadline - time.monotonic(), self.ledger.before_forward(self.authorize))
                require(remaining > 0, "dispatch_deadline")
                conn.sock.settimeout(remaining)
            if hasattr(self.ledger, "before_dispatch"):
                self.ledger.before_dispatch(identifier, request)
                remaining = min(deadline - time.monotonic(), self.ledger.before_forward(self.authorize))
                require(remaining > 0, "dispatch_deadline")
                conn.sock.settimeout(remaining)
            if cohort_transport is not None:
                cohort_transport["request_started_mono"] = time.monotonic()
            conn.request("POST", request.path, body=request.body,
                         headers={"Authorization": "Bearer " + self.credential,
                                  "Content-Type": "application/json", "Accept-Encoding": "identity"})
            response = conn.getresponse()
            status = response.status
            if cohort_transport is not None:
                cohort_transport["response_headers"] = cohort_framing(response)
                require(type(getattr(response, "chunked", None)) is bool
                        and callable(getattr(response, "isclosed", None)), "cohort_original_framing_missing")
                cohort_transport["chunked"] = response.chunked
            if hasattr(self.ledger, "validate_response_headers"):
                self.ledger.validate_response_headers(response)
            require(response.getheader("Content-Encoding", "identity") == "identity", "unsupported_upstream_encoding")
            # Redirect statuses are passed to the client but NEVER followed.
            content_type = response.getheader("Content-Type", "application/octet-stream")
            start_response(status, content_type)
            while True:
                remaining = deadline - time.monotonic()
                require(remaining > 0, "dispatch_deadline")
                conn.sock.settimeout(remaining)
                block = response.read1(min(8192, RESPONSE_CAP + 1 - len(body)))
                if not block:
                    # Our deadline watchdog may produce EOF on a close-delimited
                    # response. That local shutdown is not upstream completion.
                    require(time.monotonic() < deadline, "dispatch_deadline")
                    if cohort_transport is not None:
                        cohort_transport["eof_observed"] = True
                        cohort_transport["remaining_length"] = getattr(response, "length", None)
                        cohort_transport["response_closed"] = response.isclosed()
                    # read1() may return EOF without raising IncompleteRead for
                    # a short Content-Length body. A terminal SSE event cannot
                    # override incomplete HTTP framing.
                    require(getattr(response, "length", None) in (None, 0), "incomplete_http_framing")
                    if getattr(response, "chunked", False):
                        require(response.isclosed(), "incomplete_http_framing")
                    read_completed = time.monotonic()
                    require(read_completed < deadline, "dispatch_deadline")
                    complete = True
                    if cohort_transport is not None:
                        cohort_transport["read_completed_mono"] = read_completed
                    break
                require(len(body) + len(block) <= RESPONSE_CAP, "response_byte_limit")
                body.extend(block)
                response_hash.update(block)
                captured.write(block)
                write(block)
            usage = usage_from_response(bytes(body), request.streaming and status < 300)
            outcome = "upstream_error" if status >= 300 else "complete"
            if status < 300 and request.streaming and not stream_complete(bytes(body)):
                outcome, complete = "incomplete", False
            if status < 300 and not request.streaming and explicit_incomplete(bytes(body)):
                outcome, complete = "incomplete", False
            timing = {"deadline_mono": deadline, "read_completed_mono": read_completed} if hasattr(self.ledger, "record_transport") else None
            if cohort_transport is not None:
                timing = cohort_transport
            captured.finish(complete=complete, status=status, response_hash=response_hash.hexdigest(), content_type=content_type, transport=timing)
            if hasattr(self.ledger, "record_transport"):
                timely = self.ledger.record_transport(identifier, read_completed, time.monotonic())
                if not timely:
                    outcome, complete = "incomplete", False
            self.ledger.finish(identifier, outcome=outcome, status=status,
                               response_hash=response_hash.hexdigest(), response_bytes=len(body),
                               usage=usage, halt=not complete)
            recorded = True
        finally:
            if timer is not None:
                timer.cancel()
            try:
                if not recorded:
                    # Includes cancellation, send ambiguity, read/capture failure
                    # and disconnect. Never replace a failed physical dispatch.
                    try:
                        if captured is not None:
                            captured.finish(complete=False, status=status, response_hash=response_hash.hexdigest(),
                                            content_type=content_type, transport=cohort_transport)
                    finally:
                        self.ledger.finish(identifier, outcome="incomplete", status=status,
                                           response_hash=response_hash.hexdigest(), response_bytes=len(body),
                                           usage=None, halt=True)
            finally:
                try:
                    if captured is not None:
                        captured.close()
                finally:
                    if conn is not None:
                        conn.close()
