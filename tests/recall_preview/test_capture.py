"""Wire-adapter fixtures only, using actual public schema shapes without HTTP."""

import importlib.util
import base64
import http.client
import io
import json
from pathlib import Path
import signal
import time
import unittest
from unittest import mock
import urllib.error

import fixtures
import oracle

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "qualification" / "recall_preview.py"
# parents[2] is the repository root for tests/recall_preview/test_capture.py.
SPEC = importlib.util.spec_from_file_location("recall_preview_capture", SCRIPT)
capture = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(capture)


def response_fixture():
    corpus = fixtures.corpus()
    query = corpus["queries"][0]
    source = next(row for row in corpus["sources"] if row["revision"] == "heat-v2")
    return query, {"status": 200, "framing_complete": True, "body": {
        "results": [{"kind": "chunk", "text": source["text"], "score": 0.9, "evidence": [{
            "source_id": source["key"], "revision": source["revision"],
            "span": {"start": 0, "end": len(source["text"].encode())}, "quote": source["text"]}]}],
        "coverage": {source["key"]: {"requested_revision": source["revision"],
            "desired_revision": source["revision"], "published_revision": source["revision"], "state": "complete"}},
        "partial": False}}


class CaptureContractTests(unittest.TestCase):
    @staticmethod
    def http_response(body, headers=b"", status=200):
        # Real HTTP parser, no sockets or listeners. HTTPResponse.read(n) can
        # return valid JSON despite a declared length exceeding received bytes.
        class OfflineSocket:
            def makefile(self, _mode):
                return io.BytesIO(f"HTTP/1.1 {status} Fixture\r\n".encode() + headers + b"\r\n" + body)

        response = http.client.HTTPResponse(OfflineSocket())
        response.begin()
        return response

    def collect(self, response, *, max_bytes=4096, as_http_error=False):
        opener = mock.Mock()
        if as_http_error:
            opener.open.side_effect = urllib.error.HTTPError(
                "http://fixture.invalid/search", response.status, "Fixture", response.headers, response)
        else:
            opener.open.return_value = response
        with mock.patch.object(capture.urllib.request, "build_opener", return_value=opener), \
                mock.patch("socket.socket", side_effect=AssertionError("offline fixture forbids sockets")), \
                mock.patch("socket.create_connection", side_effect=AssertionError("offline fixture forbids network")):
            return capture.request_json("http://fixture.invalid/search", "synthetic-test-token",
                                        {"query": "unchanged"}, 1, max_bytes)

    def test_complete_http_keeps_exact_bytes_and_hash_before_normalization(self):
        query, expected = response_fixture()
        body = b" \n" + json.dumps(expected["body"], ensure_ascii=False, indent=2).encode() + b"\n"
        response = self.collect(self.http_response(body, f"Content-Length: {len(body)}\r\n".encode()))
        self.assertTrue(response["framing_complete"])
        self.assertTrue(response["raw_body_complete"])
        self.assertNotIn("capture_error", response)
        self.assertEqual(base64.b64decode(response["raw_body_base64"]), body)
        self.assertEqual(response["body_sha256"], oracle.sha256(body))
        self.assertEqual(response["bytes"], len(body))
        capture.normalize_search(query, response)

    def test_short_content_length_keeps_failed_bytes_and_never_qualifies(self):
        query, expected = response_fixture()
        body = json.dumps(expected["body"]).encode()
        for status in (200, 503):
            with self.subTest(status=status):
                response = self.collect(self.http_response(
                    body, f"Content-Length: {len(body) + 1}\r\n".encode(), status), as_http_error=status != 200)
                self.assertFalse(response["framing_complete"])
                self.assertFalse(response["raw_body_complete"])
                self.assertEqual(base64.b64decode(response["raw_body_base64"]), body)
                self.assertIsNone(response["body"])
                with self.assertRaisesRegex(oracle.Rejected, "incomplete HTTP"):
                    capture.normalize_search(query, response)

    def test_incomplete_chunked_response_is_failed_not_valid_json(self):
        body = b'{"results":[]}'
        # Omit the final zero-size chunk despite a syntactically complete body.
        encoded = f"{len(body):x}\r\n".encode() + body + b"\r\n"
        response = self.collect(self.http_response(encoded, b"Transfer-Encoding: chunked\r\n"))
        self.assertFalse(response["framing_complete"])
        self.assertIn("framing", response["capture_error"])
        self.assertEqual(base64.b64decode(response["raw_body_base64"]), body)

    def test_oversize_retains_only_explicitly_incomplete_bounded_prefix(self):
        response = self.collect(self.http_response(b"x" * 40, b"Content-Length: 40\r\n"), max_bytes=8)
        self.assertFalse(response["raw_body_complete"])
        self.assertFalse(response["framing_complete"])
        self.assertEqual(response["bytes"], 9)
        self.assertEqual(base64.b64decode(response["raw_body_base64"]), b"x" * 8)
        self.assertIn("byte budget", response["capture_error"])

    def test_ambiguous_lengths_and_untrusted_json_are_never_repaired(self):
        for body, headers, reason in [
            (b"{}", b"Content-Length: 2\r\nContent-Length: 2\r\n", "Content-Length"),
            (b"{}", b"Content-Length: -1\r\n", "Content-Length"),
            (b'{"partial":true,"partial":false}', b"", "duplicate"),
            (b'{"score":NaN}', b"", "non-finite"),
            (b'{"score":1e999}', b"", "non-finite"),
            (b'{"value":"\xff"}', b"", "invalid JSON"),
        ]:
            with self.subTest(body=body, headers=headers):
                response = self.collect(self.http_response(body, headers))
                self.assertIn(reason, response["capture_error"])
                self.assertIsNone(response["body"])
                self.assertEqual(base64.b64decode(response["raw_body_base64"]), body)

    def test_total_deadline_stops_a_trickling_or_stalled_response(self):
        before = signal.getsignal(signal.SIGALRM)
        with self.assertRaisesRegex(TimeoutError, "total time budget"):
            with capture.total_deadline(0.01):
                time.sleep(1)
        self.assertEqual(signal.getitimer(signal.ITIMER_REAL)[0], 0)
        self.assertEqual(signal.getsignal(signal.SIGALRM), before)

    def test_consumer_evidence_array_and_coverage_object(self):
        query, response = response_fixture()
        normalized = capture.normalize_search(query, response)
        self.assertEqual(oracle.retrieval(query, normalized, fixtures.corpus())["required_found"], 1)
        self.assertEqual(normalized["results"][0]["evidence"][0]["hash_origin"], "wire-if-present-otherwise-oracle")

    def test_old_flat_coverage_array_is_not_silently_accepted(self):
        query, response = response_fixture()
        response["body"]["coverage"] = list(response["body"]["coverage"].values())
        with self.assertRaisesRegex(oracle.Rejected, "coverage object"):
            capture.normalize_search(query, response)

    def test_missing_constraint_coverage_is_not_space_readiness(self):
        query, response = response_fixture()
        response["body"]["coverage"] = {}
        with self.assertRaisesRegex(oracle.Rejected, "coverage absent"):
            capture.normalize_search(query, response)

    def test_partial_response_is_not_empty_success(self):
        query, response = response_fixture()
        response["body"].update(results=[], partial=True)
        with self.assertRaisesRegex(oracle.Rejected, "partial"):
            capture.normalize_search(query, response)

    def test_forged_quote_remains_unrepaired(self):
        query, response = response_fixture()
        response["body"]["results"][0]["evidence"][0]["quote"] = "Invented boiler date"
        normalized = capture.normalize_search(query, response)
        with self.assertRaisesRegex(oracle.Rejected, "quote differs"):
            oracle.retrieval(query, normalized, fixtures.corpus())


if __name__ == "__main__":
    unittest.main()
