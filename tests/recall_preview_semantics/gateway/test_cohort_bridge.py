"""Offline control-response boundary: no gateway process or network access."""
from email.message import Message
import http.client
import unittest
from unittest.mock import patch

from cohort_bridge import CohortUnavailable, _call, _strict_json


class Response:
    def __init__(self, data=b'{"verdict":"unmet"}', headers=None, status=200, remaining=0):
        self.data, self.status, self.length = data, status, remaining
        self.headers = Message()
        for name, value in (headers if headers is not None else [
                ("Content-Length", str(len(data))), ("Content-Type", "application/json")]):
            self.headers[name] = value
        self.reads = 0

    def read(self, limit):
        self.reads += 1
        if isinstance(self.data, Exception):
            raise self.data
        return self.data[:limit]


class Connection:
    def __init__(self, response):
        self.response, self.closed = response, False
        self.requests = []

    def request(self, *args, **kwargs):
        self.requests.append((args, kwargs))

    def getresponse(self):
        return self.response

    def close(self):
        self.closed = True


class CohortBridgeTests(unittest.TestCase):
    def call(self, response):
        connection = Connection(response)
        with patch.dict("os.environ", {"RECALL_GATEWAY_COHORT_CONTROL_TOKEN": "offline-control"}), \
                patch("http.client.HTTPConnection", return_value=connection) as factory, \
                patch("socket.create_connection", side_effect=AssertionError("no sockets")):
            try:
                return _call({"action": "close"})
            finally:
                factory.assert_called_once_with("127.0.0.1", 43129, timeout=10)
                self.assertTrue(connection.closed)
                self.assertEqual(len(connection.requests), 1)

    def test_complete_response_preserves_unmet(self):
        self.assertEqual(self.call(Response()), {"verdict": "unmet"})

    def test_ambiguous_or_unbounded_framing_is_rejected_before_body(self):
        base = [("Content-Length", "19"), ("Content-Type", "application/json")]
        cases = [base + [("Content-Length", "19")], base + [("Transfer-Encoding", "chunked")],
                 base + [("Content-Type", "application/json")], base[1:], base[:1]]
        # Keep the raw duplicate headers intact: a mapping would hide ambiguity.
        cases = cases + [[("Content-Length", value), base[1]]
                            for value in ("0", "65537", "999999", "-1", "+19", "١٩")]
        for headers in cases:
            with self.subTest(headers=headers):
                response = Response(headers=headers)
                with self.assertRaises(CohortUnavailable):
                    self.call(response)
                self.assertEqual(response.reads, 0)

    def test_truncated_extra_or_unsettled_body_is_rejected(self):
        for response in (Response(headers=[("Content-Length", "20"), ("Content-Type", "application/json")]),
                         Response(headers=[("Content-Length", "18"), ("Content-Type", "application/json")]),
                         Response(remaining=1)):
            with self.subTest(response=response), self.assertRaises(CohortUnavailable):
                self.call(response)

    def test_duplicate_nonfinite_or_invalid_json_is_rejected(self):
        for data in (b'{"verdict":"passed","verdict":"unmet"}', b'{"nested":{"a":1,"a":2}}',
                     b'{"n":NaN}', b'{"n":Infinity}', b'{"n":1e999}', b'{"s":"\xff"}',
                     b'{} {}', b'[]'):
            with self.subTest(data=data), self.assertRaises(CohortUnavailable):
                self.call(Response(data))
        self.assertEqual(_strict_json(b'{"n":1.5,"s":"ok"}'), {"n": 1.5, "s": "ok"})

    def test_transport_failure_closes_connection_and_never_retries(self):
        for error in (TimeoutError("offline stall"), http.client.IncompleteRead(b"{")):
            with self.subTest(error=error), self.assertRaises(CohortUnavailable):
                self.call(Response(error, [("Content-Length", "19"), ("Content-Type", "application/json")]))


if __name__ == "__main__":
    unittest.main()
