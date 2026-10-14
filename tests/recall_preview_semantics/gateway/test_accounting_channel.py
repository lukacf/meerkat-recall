"""Closed-world original accounting export controls; no sandbox or provider run."""
from pathlib import Path
import http.client
import io
import os
import unittest
from unittest.mock import patch

import accounting_channel as channel
from accounting_export import closed_export, export_root
from launcher import launch_command
import test_cohort as fixtures
from wire import Denied, digest, encode, strict_json
from capture import Capture
from transport import Forwarder
from test_gateway import FakeConnection, FakeResponse


class AccountingTests(unittest.TestCase):
    setUp = fixtures.CohortTests.setUp
    tearDown = fixtures.CohortTests.tearDown
    begin = fixtures.CohortTests.begin
    request = fixtures.CohortTests.request
    forward = fixtures.CohortTests.forward
    close = fixtures.CohortTests.close

    def directory_observation(self):
        root = export_root(self.ledger)
        return channel.encode({"format": "recall-provider-accounting-directory/v2",
                               "path": str(root), **channel.directory_identity(root)})

    def test_channel_returns_original_stat_observation_and_refuses_sidecar_substitution(self):
        root = export_root(self.ledger)
        actual = channel.directory_identity(root)
        anchor = b"original launch anchor"
        sidecar = {"format": channel.CHANNEL_FORMAT, "anchor_sha256": channel.digest(anchor),
                   "path": str(root), "directory": actual.copy(), "read_only": True,
                   "helper_sha256": channel.digest(Path(channel.__file__).read_bytes())}
        mountinfo = f"45 24 8:1 /accounting {root} ro,nosuid - ext4 /dev/root rw\n".encode()
        original_open = Path.open
        def opened(path, *args, **kwargs):
            if str(path) == "/proc/self/mountinfo":
                return io.BytesIO(mountinfo)
            return original_open(path, *args, **kwargs)
        def original_record(_root, relative, _maximum):
            if relative == channel.SIDECAR[1:]:
                return channel.encode(sidecar)
            if relative == "launch-anchor.json":
                return anchor
            raise AssertionError("unexpected original channel record")
        with patch.object(channel, "MOUNT", str(root)), patch.object(Path, "open", opened), \
                patch.object(channel, "read_owned", side_effect=original_record), \
                patch.object(channel, "directory_identity", wraps=channel.directory_identity) as observed:
            original_sidecar, original_mountinfo, directory_raw = channel.observe_channel()
            observed.assert_called_once_with(root)
            self.assertEqual(original_sidecar, channel.encode(sidecar))
            self.assertEqual(original_mountinfo, mountinfo)
            self.assertEqual(channel.decode(directory_raw), {
                "format": "recall-provider-accounting-directory/v2", "path": str(root), **actual})
            self.assertLessEqual(len(directory_raw), 16384)
            sidecar["directory"]["inode"] += 1
            with self.assertRaisesRegex(channel.AccountingUnavailable, "original directory differs"):
                channel.observe_channel()
            self.assertEqual(channel.decode(directory_raw)["inode"], actual["inode"],
                             "returned observation must retain the original actual stat")

    def forward_http(self, wire):
        class MemorySocket:
            def makefile(self, *args, **kwargs):
                return io.BytesIO(wire)
        response = http.client.HTTPResponse(MemorySocket())
        response.begin()
        original, request = self.request()
        connection = FakeConnection(response)
        forwarder = Forwarder(self.ledger, lambda: None, lambda _: connection, "synthetic-only",
                              lambda identifier, req, raw: Capture(self.directory, identifier, req, raw,
                                                                   binding=self.ledger.binding(req.scope)))
        forwarder.dispatch(request, original, lambda *_: None, lambda _: None)
        return connection

    def test_actual_http_framing_and_read_clock_survive_later_capture_completion(self):
        self.begin()
        body = b'{"data":[],"usage":{"prompt_tokens":3,"total_tokens":3}}'
        finish = Capture.finish
        def late_capture(capture, **kwargs):
            self.wall += 121
            self.mono += 121
            return finish(capture, **kwargs)
        with patch("transport.time.monotonic", side_effect=lambda: self.mono), patch.object(Capture, "finish", late_capture):
            connection = self.forward_http(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
                                           + str(len(body)).encode() + b"\r\nX-Private-Ignored: never-captured\r\n\r\n" + body)
        self.assertEqual(len(connection.sent), 1)
        raw = (self.directory / "dispatch-000001/capture.json").read_bytes()
        captured = strict_json(raw)
        proof = captured["transport"]
        self.assertTrue(captured["complete"])
        self.assertEqual(proof["format"], "recall-provider-transport/v2")
        self.assertLess(proof["read_completed_mono"], proof["deadline_mono"])
        self.assertGreater(self.mono, proof["deadline_mono"])
        self.assertEqual(proof["response_headers"], {"content_length": [str(len(body))], "transfer_encoding": [],
            "content_encoding": [], "content_type": ["application/json"]})
        self.assertEqual(set(proof), {"format", "started_mono", "request_started_mono", "deadline_mono",
            "read_completed_mono", "response_headers", "eof_observed", "remaining_length", "chunked", "response_closed"})
        self.assertEqual((proof["chunked"], proof["remaining_length"], proof["response_closed"], proof["eof_observed"]),
                         (False, 0, True, True))
        self.assertNotIn(b"never-captured", raw)
        self.assertEqual((self.directory / "dispatch-000001/response.body").read_bytes(), body)

    def test_real_chunked_end_and_truncated_length_remain_distinct(self):
        self.begin()
        body = b'{"data":[]}'
        self.forward_http(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
                          + f"{len(body):x}\r\n".encode() + body + b"\r\n0\r\n\r\n")
        captured = strict_json((self.directory / "dispatch-000001/capture.json").read_bytes())
        self.assertTrue(captured["transport"]["chunked"])
        self.assertTrue(captured["transport"]["response_closed"])
        self.assertEqual(captured["transport"]["response_headers"]["transfer_encoding"], ["chunked"])
        with self.assertRaisesRegex(Denied, "incomplete_http_framing"):
            self.forward_http(b"HTTP/1.1 200 OK\r\nContent-Length: 999\r\n\r\n" + body)
        incomplete = strict_json((self.directory / "dispatch-000002/capture.json").read_bytes())
        self.assertFalse(incomplete["complete"])
        self.assertIsNone(incomplete["transport"]["read_completed_mono"])
        self.assertTrue(incomplete["transport"]["eof_observed"])
        self.assertGreater(incomplete["transport"]["remaining_length"], 0)
        with self.assertRaises(Denied):
            self.ledger.close_with_evidence("elephant-db", "preparation", self.caller, "c" * 64)

    def test_missing_actual_headers_cannot_be_a_successful_null_proof(self):
        self.begin()
        original, request = self.request()
        response = FakeResponse(b"{}")
        del response.headers
        connection = FakeConnection(response)
        forwarder = Forwarder(self.ledger, lambda: None, lambda _: connection, "synthetic-only",
                              lambda identifier, req, raw: Capture(self.directory, identifier, req, raw,
                                                                   binding=self.ledger.binding(req.scope)))
        with self.assertRaisesRegex(Denied, "cohort_original_headers_missing"):
            forwarder.dispatch(request, original, lambda *_: None, lambda _: None)
        captured = strict_json((self.directory / "dispatch-000001/capture.json").read_bytes())
        self.assertFalse(captured["complete"])
        self.assertIsNone(captured["transport"]["response_headers"])
        self.assertIsNone(captured["transport"]["read_completed_mono"])
        self.assertFalse(captured["transport"]["eof_observed"])
        self.assertEqual(self.ledger.receipt()["halted"], "incomplete")

    def test_late_empty_read_cannot_fabricate_a_timely_eof(self):
        self.begin()
        original, request = self.request()
        response = FakeResponse(b"")
        def late_eof(_size):
            self.wall += 121
            self.mono += 121
            return b""
        response.read1 = late_eof
        connection = FakeConnection(response)
        forwarder = Forwarder(self.ledger, lambda: None, lambda _: connection, "synthetic-only",
                              lambda identifier, req, raw: Capture(self.directory, identifier, req, raw,
                                                                   binding=self.ledger.binding(req.scope)))
        with patch("transport.time.monotonic", side_effect=lambda: self.mono), \
                self.assertRaisesRegex(Denied, "dispatch_deadline"):
            forwarder.dispatch(request, original, lambda *_: None, lambda _: None)
        captured = strict_json((self.directory / "dispatch-000001/capture.json").read_bytes())
        self.assertFalse(captured["complete"])
        self.assertFalse(captured["transport"]["eof_observed"])
        self.assertIsNone(captured["transport"]["read_completed_mono"])
        self.assertEqual(len(connection.sent), 1)

    def test_duplicate_content_type_values_are_retained_for_independent_refusal(self):
        self.begin()
        self.forward_http(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Type: application/json\r\n"
                          b"Content-Type: text/plain\r\n\r\n{}")
        captured = strict_json((self.directory / "dispatch-000001/capture.json").read_bytes())
        self.assertEqual(captured["transport"]["response_headers"]["content_type"],
                         ["application/json", "text/plain"])

    def original_export(self):
        manifest = {"backend": "elephant-db", "provider_phase": "preparation"}
        self.caller = digest(encode(manifest))
        begin = self.begin()
        baseline = self.ledger.db.execute("SELECT baseline FROM cohort WHERE id=1").fetchone()[0].encode()
        self.forward()
        envelope = self.ledger.close_with_evidence("elephant-db", "preparation", self.caller, "c" * 64)
        index = strict_json(channel.read_owned(export_root(self.ledger), envelope["export"]["path"], channel.MAX_RECORD))
        return manifest, begin, baseline, envelope, index

    def test_original_baseline_and_close_are_exact_and_lookup_is_lazy(self):
        manifest, begin, baseline, envelope, index = self.original_export()
        self.assertEqual(envelope["close"], self.close())
        self.assertEqual(digest(baseline), begin["baseline_sha256"])
        with patch.object(channel, "MOUNT", str(export_root(self.ledger))), \
                patch.object(channel, "observe_channel", return_value=(
                    b"original-sidecar", b"original-mountinfo", self.directory_observation())):
            exported = channel.open_export(manifest, begin, envelope["close"], envelope["export"])
        self.assertEqual(set(exported["channel"]), {"sidecar", "mountinfo", "index", "directory"})
        self.assertEqual(exported["channel"]["directory"], self.directory_observation())
        raw = exported["raw_evidence"]
        bundle = strict_json(raw[exported["reference"]["sha256"]])
        self.assertEqual(raw[bundle["baseline"]["sha256"]], baseline)
        self.assertEqual(raw[bundle["close"]["sha256"]], encode(envelope["close"]))
        self.assertEqual(bundle["begin"]["kind"], "provider-cohort-begin")
        clock = strict_json(raw[bundle["phase_clock"]["sha256"]])
        self.assertEqual(clock["cohort_id"], 1)
        self.assertEqual(clock["phase_start"], {"wall": self.wall, "mono": self.mono})
        self.assertEqual(clock["deadline_mono"], self.mono + 3600)
        self.assertIsNone(clock["closure_timestamp"])
        dispatches = channel.decode(raw[bundle["dispatch_records"]["sha256"]])
        self.assertEqual(len(dispatches), 1)
        self.assertEqual(dispatches[0]["send_intent"], 1)
        self.assertEqual(dispatches[0]["ledger"]["id"], 1)
        self.assertEqual(dispatches[0]["binding"], self.ledger.binding(self.scope))
        self.assertLess(raw.cache_bytes, channel.MAX_CACHE)
        self.assertNotEqual(set(raw.cache), set(raw), "construction must not eagerly read the body inventory")
        self.assertEqual(len(self.ledger.receipt()["dispatches"]), 1)
        self.assertEqual(index["caller_sha256"], digest(encode(manifest)))

    def test_later_cohort_cannot_replace_original_baseline_or_after(self):
        _, _, baseline, envelope, index = self.original_export()
        self.begin("measurement", caller="d" * 64)
        self.forward()
        self.assertEqual(len(self.ledger.receipt()["dispatches"]), 2)
        repeated = closed_export(self.ledger, "elephant-db", "preparation", self.caller, envelope["close"])
        self.assertEqual(repeated, envelope["export"])
        raw = channel.RawEvidence(export_root(self.ledger), index["files"], cache_limit=0)
        bundle = strict_json(raw[index["reference"]["sha256"]])
        self.assertEqual(raw[bundle["baseline"]["sha256"]], baseline)
        sealed = strict_json(raw[bundle["sealed"]["sha256"]])
        self.assertEqual(len(sealed["after"]["dispatches"]), 1)

    def test_two_original_verification_boundaries_fit_exact_finite_read_budget(self):
        _, _, _, _, index = self.original_export()
        raw = channel.RawEvidence(export_root(self.ledger), index["files"], cache_limit=0)
        by_kind = {entry["ref"]["kind"]: entry["ref"] for entry in index["files"]}
        def read_kind(kind):
            return raw[by_kind[kind]["sha256"]]
        # Actual G open and S export prelude, with no retained body cache.
        read_kind("provider-accounting")
        read_kind("provider-accounting")
        read_kind("provider-cohort-begin")
        read_kind("provider-cohort-close")
        for _ in range(2):
            # L reads begin, the channel's bundle/begin/close and its capture
            # inventory, then L and F each verify the complete indexed set.
            for kind in ("provider-cohort-begin", "provider-accounting",
                         "provider-cohort-begin", "provider-cohort-close",
                         "provider-capture-inventory"):
                read_kind(kind)
            for _ in range(2):
                for entry in index["files"]:
                    raw[entry["ref"]["sha256"]]
        self.assertGreater(raw.read_bytes, raw.original_bytes)
        self.assertEqual(raw.read_bytes, raw.read_limit)
        self.assertEqual(raw.cache_bytes, 0)
        self.assertEqual(raw.original_bytes, sum(entry["ref"]["bytes"] for entry in index["files"]))
        with self.assertRaisesRegex(channel.AccountingUnavailable, "read budget exhausted"):
            read_kind("provider-accounting")

    def test_metadata_allowance_cannot_be_multiplied_by_duplicate_inventory_rows(self):
        _, _, _, _, index = self.original_export()
        original = next(entry for entry in index["files"]
                        if entry["ref"]["kind"] == "provider-cohort-begin")
        for forged in ({"path": "duplicate-begin", "ref": original["ref"]},
                       {"path": original["path"], "ref": {**original["ref"], "bytes": 65537}}):
            inventory = (index["files"] + [forged] if forged["path"] == "duplicate-begin"
                         else [forged])
            with self.assertRaisesRegex(channel.AccountingUnavailable, "export metadata"):
                channel.RawEvidence(export_root(self.ledger), inventory)

    def test_original_and_forwarded_share_one_kind_and_no_payload_copy(self):
        _, _, _, _, index = self.original_export()
        files = {row["path"]: row["ref"] for row in index["files"]}
        original = files["cohort-01/dispatch-000001/request.original.json"]
        forwarded = files["cohort-01/dispatch-000001/request.forwarded.json"]
        self.assertEqual(original, forwarded)
        self.assertEqual(original["kind"], "provider-request")
        lazy = channel.RawEvidence(export_root(self.ledger), index["files"])
        self.assertTrue(all(item["ref"]["sha256"] in lazy for item in index["files"]))
        self.assertEqual((lazy.read_bytes, lazy.accesses, lazy.cache_bytes), (0, 0, 0))
        source = (self.directory / "dispatch-000001/response.body").stat()
        alias = (export_root(self.ledger) / "cohort-01/dispatch-000001/response.body").stat()
        self.assertEqual((source.st_dev, source.st_ino), (alias.st_dev, alias.st_ino))
        self.assertEqual(source.st_nlink, 2)
        names = {Path(path).name for path in files}
        self.assertFalse(names & {"ledger.sqlite", "gateway.sock", "manifest.json", "launch.json"})

    def test_changed_stored_baseline_and_foreign_file_alias_refuse(self):
        _, _, baseline, envelope, _ = self.original_export()
        self.ledger.db.execute("UPDATE cohort SET baseline='{}' WHERE id=1")
        with self.assertRaisesRegex(Denied, "original_baseline"):
            closed_export(self.ledger, "elephant-db", "preparation", self.caller, envelope["close"])
        self.ledger.db.execute("UPDATE cohort SET baseline=? WHERE id=1", (baseline.decode(),))
        os.link(self.directory / "dispatch-000001/response.body", self.directory / "unreviewed-alias")
        with self.assertRaisesRegex(Denied, "unreviewed_file_alias"):
            closed_export(self.ledger, "elephant-db", "preparation", self.caller, envelope["close"])

    def test_unclosed_or_unsettled_cohort_never_exports_live_body(self):
        self.begin()
        with self.assertRaisesRegex(Denied, "not_closed"):
            closed_export(self.ledger, "elephant-db", "preparation", self.caller, {"verdict": "passed"})
        _, request = self.request()
        self.ledger.reserve(request, lambda: None)
        result = self.ledger.close_with_evidence("elephant-db", "preparation", self.caller, "c" * 64)
        self.assertEqual(result["close"]["verdict"], "unmet")
        self.assertIsNone(result["export"])
        self.assertFalse((self.directory / "accounting-export").exists())

    def test_changed_body_cannot_satisfy_original_hash(self):
        _, _, _, _, index = self.original_export()
        raw = channel.RawEvidence(export_root(self.ledger), index["files"], cache_limit=0)
        row = next(item for item in index["files"] if item["path"].endswith("response.body"))
        path = export_root(self.ledger) / row["path"]
        body = path.read_bytes()
        path.write_bytes(b"X" + body[1:])
        with self.assertRaisesRegex(channel.AccountingUnavailable, "record changed"):
            raw[row["ref"]["sha256"]]

    def test_foreign_cohort_and_symlink_cannot_replace_original_capture(self):
        manifest, begin, _, envelope, index = self.original_export()
        with patch.object(channel, "MOUNT", str(export_root(self.ledger))), \
                patch.object(channel, "observe_channel", return_value=(
                    b"sidecar", b"mountinfo", self.directory_observation())):
            with self.assertRaisesRegex(channel.AccountingUnavailable, "substituted original cohort"):
                channel.open_export({**manifest, "backend": "surreal-db"}, begin, envelope["close"], envelope["export"])
        row = next(item for item in index["files"] if item["path"].endswith("response.body"))
        path = export_root(self.ledger) / row["path"]
        path.unlink()
        path.symlink_to(self.directory / "dispatch-000001/response.body")
        raw = channel.RawEvidence(export_root(self.ledger), index["files"], cache_limit=0)
        with self.assertRaises(OSError):
            raw[row["ref"]["sha256"]]

    def test_directory_change_after_original_observation_cannot_be_adopted(self):
        manifest, begin, _, envelope, _ = self.original_export()
        changed = channel.decode(self.directory_observation())
        changed["inode"] += 1
        with patch.object(channel, "MOUNT", str(export_root(self.ledger))), \
                patch.object(channel, "observe_channel", return_value=(
                    b"sidecar", b"mountinfo", channel.encode(changed))), \
                self.assertRaisesRegex(channel.AccountingUnavailable, "original observed accounting directory changed"):
            channel.open_export(manifest, begin, envelope["close"], envelope["export"])

    def test_path_and_total_inventory_bounds_apply_before_reading(self):
        root = export_root(self.ledger)
        ref = channel.reference(b"x", "provider-request")
        for path in (".", "../private-ledger", "/outside", "a//b"):
            with self.subTest(path=path), self.assertRaises(channel.AccountingUnavailable):
                channel.RawEvidence(root, [{"path": path, "ref": ref}])
        oversized = [{"path": f"record-{index}", "ref": {**ref, "bytes": channel.MAX_RECORD}}
                     for index in range(channel.MAX_TOTAL // channel.MAX_RECORD + 1)]
        with self.assertRaisesRegex(channel.AccountingUnavailable, "capture cap"):
            channel.RawEvidence(root, oversized)
        with self.assertRaisesRegex(channel.AccountingUnavailable, "cache bound"):
            channel.RawEvidence(root, [{"path": "one", "ref": ref}], cache_limit=channel.MAX_CACHE + 1)
        with self.assertRaisesRegex(channel.AccountingUnavailable, "conflicting accounting digest or kind"):
            channel.RawEvidence(root, [{"path": "one", "ref": ref},
                                      {"path": "two", "ref": {**ref, "kind": "provider-capture"}}])

    def test_only_actual_readonly_mount_flags_and_explicit_launch_extension_pass(self):
        ro = b"45 24 8:1 /accounting /gateway-accounting ro,nosuid - ext4 /dev/root rw\n"
        channel.verify_mount(ro)
        for changed in (ro.replace(b"ro,nosuid", b"rw,nosuid"), ro + ro, ro.replace(b"/gateway-accounting", b"/elsewhere")):
            with self.assertRaises(channel.AccountingUnavailable):
                channel.verify_mount(changed)
        with patch("launcher.shutil.which", return_value="/reviewed/bwrap"), \
                patch("launcher.os.readlink", return_value="net:[original]"):
            args = launch_command(Path("/runtime"), Path("/work"), Path("/socket"), Path("/source/bridge.py"),
                                  ["/original/command"], {}, Path("/anchor"), accounting=Path("/owned/export"),
                                  accounting_sidecar=Path("/owned/channel.json"))
        position = args.index("/owned/export")
        self.assertEqual(args[position - 1:position + 2], ["--ro-bind", "/owned/export", "/gateway-accounting"])
        self.assertNotIn("--bind", args[position - 1:position + 2])


if __name__ == "__main__":
    unittest.main()
