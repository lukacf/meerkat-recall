"""Mocked dispatch-harness controls. Never launch listeners or a candidate."""
from email.message import Message
import copy
import datetime as dt
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("installed_dispatch", ROOT / "scripts/qualification/installed_linux_dispatch.py")
dispatch = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dispatch)


class InstalledDispatchControls(unittest.TestCase):
    def owner(self):
        owner = Mock()
        owner.cases = dispatch.plan()["expectations"]
        owner.saved = []
        owner.save = lambda name, raw: owner.saved.append((name, json.loads(raw)))
        return owner

    def request(self, space, generation):
        corpus = dispatch.core.fixtures.corpus()
        query = next(row["query"] for row in corpus["queries"] if row["space"] == space)
        dims = next(row["dimensions"] for row in corpus["spaces"] if row["id"] == space)
        headers = Message()
        headers["Authorization"] = "Bearer " + dispatch.synthetic_credential(space, generation)
        raw = dispatch.core.encoded({"model": space + "-spy-model", "input": [query], "dimensions": dims})
        return headers, raw

    def test_plan_is_inert_and_binds_core_source_and_exact_three_calls(self):
        with patch.object(dispatch.http.server, "HTTPServer", side_effect=AssertionError("no listener")), \
             patch.object(dispatch.core.subprocess, "Popen", side_effect=AssertionError("no child")):
            value = dispatch.plan()
        self.assertFalse(value["product_acceptance"])
        self.assertEqual(value["core_driver_sha256"], dispatch.core.file_digest(Path(dispatch.core.__file__)))
        self.assertEqual(value["expectations"]["limits"]["physical_embedding_calls"], 3)
        self.assertEqual([row["credential_generation"] for row in value["expectations"]["expected_outbound"]], [1, 1, 2])

    def test_actual_configuration_replace_uses_exact_named_bootstrap_role(self):
        value = dispatch.DispatchDriver.__new__(dispatch.DispatchDriver)
        value.cases = dispatch.plan()["expectations"]
        space = dispatch.core.fixtures.corpus()["spaces"][0]
        role = "bootstrap-" + space["id"]
        value.tokens = {"operator": "global-management", role: "named-bootstrap"}
        value.http = Mock(return_value={"status": 200, "body": {}})
        value.stored_config = Mock()
        current = dispatch.processing_config(space, "http://127.0.0.1:41001/v1")
        updated = value.replace(space, current, lambda row: row["bindings"]["query_embedding"].update(revoked=True))
        value.http.assert_called_once_with("PUT", f"/v1/spaces/{space['id']}", role, space["id"], {"processing": updated})
        self.assertEqual(updated["generation"], current["generation"] + 1)
        self.assertFalse(current["bindings"]["query_embedding"]["revoked"])
        self.assertTrue(updated["bindings"]["query_embedding"]["revoked"])
        del value.tokens[role]
        value.http.reset_mock()
        with self.assertRaisesRegex(ValueError, "bootstrap credential is absent"):
            value.replace(space, current, lambda row: None)
        value.http.assert_not_called()

    def test_three_declared_requests_preserve_original_query_and_rotation(self):
        owner = self.owner()
        spy = dispatch.TransportSpy(owner)
        for row in owner.cases["expected_outbound"]:
            headers, raw = self.request(row["space"], row["credential_generation"])
            response = spy.observe(row["space"], "POST", "/v1/embeddings", headers, raw)
            self.assertEqual(response["data"][0]["embedding"][0], 1.0)
            self.assertTrue(all(value == 0 for value in response["data"][0]["embedding"][1:]))
        self.assertEqual(spy.count(), 3)
        self.assertEqual([name for name, _ in owner.saved], ["spy-001.json", "spy-002.json", "spy-003.json"])
        self.assertTrue(all(value["valid"] for _, value in owner.saved))
        self.assertNotEqual(owner.saved[0][1]["authorization"], owner.saved[2][1]["authorization"])
        self.assertEqual(owner.saved[0][1]["body_sha256"], owner.saved[2][1]["body_sha256"])

    def test_extra_physical_attempt_fails_and_is_retained(self):
        owner = self.owner()
        spy = dispatch.TransportSpy(owner)
        for row in owner.cases["expected_outbound"]:
            headers, raw = self.request(row["space"], row["credential_generation"])
            spy.observe(row["space"], "POST", "/v1/embeddings", headers, raw)
        headers, raw = self.request(owner.cases["spaces"][0], 2)
        with self.assertRaisesRegex(ValueError, "extra physical"):
            spy.observe(owner.cases["spaces"][0], "POST", "/v1/embeddings", headers, raw)
        self.assertEqual(len(owner.saved), 4)
        self.assertFalse(owner.saved[-1][1]["valid"])
        with self.assertRaises(ValueError):
            spy.count()

    def test_wrong_space_credential_request_shape_or_framing_cannot_pass(self):
        home, business = dispatch.plan()["expectations"]["spaces"]
        good_headers, good_raw = self.request(home, 1)
        for mutation in ("space", "credential", "unknown-credential", "duplicate-auth", "query", "dimension", "model", "extra-field", "path", "framing"):
            with self.subTest(mutation=mutation):
                owner = self.owner()
                spy = dispatch.TransportSpy(owner)
                headers = copy.deepcopy(good_headers)
                body = json.loads(good_raw)
                space, path, complete = home, "/v1/embeddings", True
                if mutation == "space":
                    space = business
                elif mutation == "credential":
                    headers.replace_header("Authorization", "Bearer " + dispatch.synthetic_credential(business, 1))
                elif mutation == "unknown-credential":
                    headers.replace_header("Authorization", "Bearer never-copy-this")
                elif mutation == "duplicate-auth":
                    headers["Authorization"] = headers["Authorization"]
                elif mutation == "query":
                    body["input"] = ["rewritten question"]
                elif mutation == "dimension":
                    body["dimensions"] = 768
                elif mutation == "model":
                    body["model"] = "wrong-model"
                elif mutation == "extra-field":
                    body["user"] = "unexpected"
                elif mutation == "path":
                    path = "/v1/responses"
                else:
                    complete = False
                with self.assertRaises(ValueError):
                    spy.observe(space, "POST", path, headers, dispatch.core.encoded(body), complete)
                self.assertFalse(owner.saved[0][1]["valid"])
                self.assertNotIn("never-copy-this", json.dumps(owner.saved))

    def test_profile_pairs_are_distinct_and_credentials_are_not_profile_identity(self):
        spaces = dispatch.core.fixtures.corpus()["spaces"]
        home = dispatch.processing_config(spaces[0], "http://127.0.0.1:41001/v1")
        business = dispatch.processing_config(spaces[1], "http://127.0.0.1:41002/v1")
        self.assertNotEqual(home["profiles"], business["profiles"])
        self.assertNotEqual(home["connections"]["connection"]["account_id"], business["connections"]["connection"]["account_id"])
        self.assertNotEqual(home["connections"]["connection"]["endpoint"], business["connections"]["connection"]["endpoint"])
        rotated = copy.deepcopy(home)
        rotated["connections"]["connection"].update(credential_generation=2, credential_ref=dispatch.credential_reference(spaces[0]["id"], 2))
        self.assertEqual(rotated["profiles"], home["profiles"])
        self.assertEqual(rotated["bindings"], home["bindings"])
        self.assertNotEqual(rotated["connections"], home["connections"])

    def test_listener_cleanup_interrupts_owned_connections_before_shutdown(self):
        spy = dispatch.TransportSpy(self.owner())
        connection, listener, thread = Mock(), Mock(), Mock()
        thread.is_alive.return_value = False
        spy.connections.add(connection)
        spy.listeners = [listener]
        spy.threads = [thread]
        trace = []
        connection.shutdown.side_effect = lambda *_args: trace.append("connection")
        listener.shutdown.side_effect = lambda: trace.append("listener")
        spy.close()
        self.assertEqual(trace, ["connection", "listener"])
        connection.close.assert_called_once()
        listener.server_close.assert_called_once()

    def test_cleanup_failure_is_inside_core_failed_observation_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            value = dispatch.DispatchDriver.__new__(dispatch.DispatchDriver)
            value.root = Path(directory)
            value.started = dispatch.time.monotonic()
            value.requests = value.serial = 0
            value.report = {"product_acceptance": False}
            value.execute_cases = Mock()
            value.stop = Mock()
            value.spy = Mock()
            value.spy.requests = []
            value.spy.failed = True
            value.spy.close.side_effect = ValueError("owned listener remains")
            self.assertEqual(value.run(), 1)
            observed = dispatch.core.read_json(value.root / "observation.json")
            self.assertEqual(observed["subset_observation"], "failed")

    def test_review_binds_both_drivers_and_never_accepts_core_only_review(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            current = dispatch.plan()
            keys = ("expectations_sha256", "corpus_sha256", "driver_sha256", "core_driver_sha256", "collector_sha256", "installer_sha256")
            review = {key: current[key] for key in keys}
            review.update(format="recall-installed-linux-dispatch-review/v1", approved=True, reviewer="qualification")
            review_path = root / "review.json"
            manifest = {"format": "recall-installed-linux-dispatch-run/v1", "execution_authorized": True,
                "backend": "surreal-db", "source_commit": "a" * 40, "archive_sha256": "b" * 64, "binary_sha256": "c" * 64,
                "archive": str(root / "absent.tar.gz"), "run_directory": str(root / "new"),
                "expectation_review": str(review_path), "expires_at": "2026-10-07T23:00:00Z"}
            path = root / "run.json"
            now = dt.datetime(2026, 10, 7, tzinfo=dt.timezone.utc)
            for key in (None, *keys, "format"):
                with self.subTest(field=key):
                    changed = dict(review)
                    if key:
                        changed[key] = "recall-installed-linux-review/v1" if key == "format" else "wrong"
                    review_path.write_bytes(dispatch.core.encoded(changed))
                    manifest["expectation_review_sha256"] = dispatch.core.file_digest(review_path)
                    path.write_bytes(dispatch.core.encoded(manifest))
                    if key:
                        with self.assertRaises(ValueError):
                            dispatch.validate_manifest(path, now)
                    else:
                        self.assertEqual(dispatch.validate_manifest(path, now)[0], manifest)


if __name__ == "__main__":
    unittest.main()
