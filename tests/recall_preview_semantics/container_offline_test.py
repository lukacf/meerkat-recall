"""Source-only boundary tests. No subprocess, Docker, service or product verdict."""
import contextlib
import copy
import datetime as dt
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import Mock, patch
import uuid

MODULE_PATH = Path(__file__).resolve().parents[2] / "scripts/qualification/container_offline.py"
SPEC = importlib.util.spec_from_file_location("container_offline_under_test", MODULE_PATH)
driver = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(driver)


class FakeDocker:
    def __init__(self):
        self.rows = {}
        self.removed = []
        self.calls = []
        self.labels = {}

    def run(self, arguments, **kwargs):
        self.calls.append(arguments)
        if arguments[1] == "inspect":
            return (0 if (arguments[0], arguments[2]) in self.rows else 1), b"", b""
        if arguments[1] == "rm":
            self.removed.append((arguments[0], arguments[-1]))
            return 0, b"", b""
        if arguments[:2] == ["volume", "create"]:
            labels = dict(value.split("=", 1) for key, value in zip(arguments[2::2], arguments[3::2]) if key == "--label")
            name = arguments[-1]
            self.rows[("volume", name)] = {"Name": name, "Labels": labels}
            return 0, name.encode(), b""
        raise AssertionError("unexpected fake command")

    def json(self, arguments, **kwargs):
        self.calls.append(arguments)
        return [copy.deepcopy(self.rows[(arguments[0], arguments[2])])]


class ContainerOfflineTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.now = dt.datetime.now(dt.timezone.utc)
        self.inventory = driver.plan()
        utility_hashes = {}
        for relative in driver.DEPENDENCIES:
            path = self.root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("# inert source fixture\n")
            utility_hashes[relative] = driver.digest(path.read_bytes())
        self.review = self.root / "review.json"
        self.review.write_bytes(driver.encoded({"format": "recall-container-review/v1", "reviewer": "qualification",
            "approved": True, "driver_sha256": self.inventory["driver_sha256"],
            "expectations_sha256": self.inventory["expectations_sha256"], "utility_sha256": utility_hashes}))
        self.allocation = {"format": "recall-container-run/v1", "execution_authorized": True,
            "backend": "elephant-db", "run_id": str(uuid.uuid4()), "source_commit": "a" * 40,
            "source_tree": "b" * 40, "source_tar_sha256": "c" * 64,
            "recall_image_id": "sha256:" + "d" * 64, "paired_image_id": "sha256:" + "e" * 64,
            "expires_at": (self.now + dt.timedelta(minutes=30)).isoformat(),
            "run_directory": str(self.root / "new-run"), "docker_socket": str(self.root / "docker.sock"),
            "review": str(self.review), "review_sha256": driver.digest(self.review.read_bytes()),
            "utility_sha256": utility_hashes}
        for name in ("recall_binary_sha256", "paired_binary_sha256", "recall_provenance_sha256",
                     "paired_source_provenance_sha256", "paired_build_provenance_sha256"):
            self.allocation[name] = "f" * 64
        self.allocation_file = self.root / "allocation.json"
        self.no_process = patch.object(driver.subprocess, "Popen", side_effect=AssertionError("mock suite launched a process"))
        self.no_process.start()
        self.addCleanup(self.no_process.stop)
        self.addCleanup(self.temporary.cleanup)

    def validate(self, allocation=None):
        self.allocation_file.write_bytes(driver.encoded(allocation or self.allocation))
        with patch.object(driver, "ROOT", self.root):
            return driver.validate_allocation(self.allocation_file, self.now)

    def test_plan_is_inert_and_enumerates_both_backend_obligations(self):
        with patch.object(driver, "DockerCommands", side_effect=AssertionError("Docker initialized")):
            self.assertFalse(driver.plan()["product_acceptance"])
            self.assertEqual(len(driver.plan()["expectations"]["required_cases"]), 8)
            self.assertEqual(driver.plan()["expectations"]["backends"], ["elephant-db", "surreal-db"])

    def test_exact_allocation_accepts_without_contacting_docker(self):
        allocation, inventory = self.validate()
        self.assertEqual(allocation["run_id"], self.allocation["run_id"])
        self.assertFalse(inventory["product_acceptance"])

    def test_mutable_image_tag_is_refused(self):
        value = {**self.allocation, "recall_image_id": "meerkat-recall:latest"}
        with self.assertRaisesRegex(ValueError, "exact supplied"):
            self.validate(value)

    def test_unallocated_and_expired_execution_are_refused(self):
        for changed in ({"execution_authorized": False}, {"expires_at": self.now.isoformat()},
                        {"expires_at": (self.now + dt.timedelta(hours=3)).isoformat()}):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                self.validate({**self.allocation, **changed})

    def test_existing_run_directory_is_never_reused(self):
        Path(self.allocation["run_directory"]).mkdir()
        with self.assertRaisesRegex(ValueError, "must be new"):
            self.validate()

    def test_runtime_parent_and_symlink_are_refused(self):
        (self.root / ".elephant").mkdir()
        with self.assertRaisesRegex(ValueError, "outside existing"):
            self.validate()
        (self.root / ".elephant").rmdir()
        target = self.root / "actual"
        target.mkdir()
        (self.root / "alias").symlink_to(target)
        with self.assertRaisesRegex(ValueError, "canonical"):
            self.validate({**self.allocation, "run_directory": str(self.root / "alias/new")})

    def test_independent_review_binds_driver_and_cases(self):
        review = driver.strict_json(self.review.read_bytes())
        review["driver_sha256"] = "0" * 64
        self.review.write_bytes(driver.encoded(review))
        value = {**self.allocation, "review_sha256": driver.digest(self.review.read_bytes())}
        with self.assertRaisesRegex(ValueError, "different driver"):
            self.validate(value)

    def test_transitive_utility_source_change_is_refused(self):
        (self.root / driver.DEPENDENCIES[-1]).write_text("changed parser\n")
        with self.assertRaisesRegex(ValueError, "utility source changed"):
            self.validate()

    def test_custody_json_rejects_duplicate_and_nonfinite_fields(self):
        for raw in (b'{"approved":false,"approved":true}', b'{"duration":NaN}', b'{"duration":Infinity}'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                driver.strict_json(raw)

    def test_private_evidence_is_exclusive_and_mode_600(self):
        path = self.root / "receipt"
        driver.private_new(path, b"original")
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        with self.assertRaises(FileExistsError):
            driver.private_new(path, b"replacement")
        self.assertEqual(path.read_bytes(), b"original")

    def test_cleanup_checks_nonce_and_never_removes_tampered_volume(self):
        fake = FakeDocker()
        owned = driver.OwnedResources(fake, self.allocation["run_id"])
        volume = owned.create("volume", "data")
        fake.rows[("volume", volume)]["Labels"][driver.LABEL + ".nonce"] = "foreign"
        errors = owned.cleanup()
        self.assertEqual(len(errors), 1)
        self.assertEqual(fake.removed, [])

    def test_cleanup_removes_only_exact_owned_volume(self):
        fake = FakeDocker()
        owned = driver.OwnedResources(fake, self.allocation["run_id"])
        volume = owned.create("volume", "data")
        self.assertEqual(owned.cleanup(), [])
        self.assertEqual(fake.removed, [("volume", volume)])

    def test_existing_resource_name_is_not_adopted(self):
        fake = FakeDocker()
        owned = driver.OwnedResources(fake, self.allocation["run_id"])
        name = owned.prefix + "-data"
        fake.rows[("volume", name)] = {"Name": name, "Labels": {}}
        with self.assertRaisesRegex(ValueError, "existing resource"):
            owned.create("volume", "data")
        self.assertEqual(owned.resources, [])

    def test_command_interface_refuses_build_pull_and_prune_before_subprocess(self):
        directory = self.root / "commands"
        directory.mkdir()
        commands = driver.DockerCommands(self.allocation, directory)
        for args in (["build", "."], ["image", "pull", "anything"], ["volume", "prune"]):
            with self.subTest(args=args), self.assertRaises(ValueError):
                commands.run(args)

    def test_command_environment_drops_ambient_credentials_context_and_keys(self):
        directory = self.root / "commands"
        directory.mkdir()
        with patch.dict(driver.os.environ, {"DOCKER_HOST": "tcp://foreign", "OPENAI_API_KEY": "ambient-secret", "JWT_SECRET": "foreign"}):
            commands = driver.DockerCommands(self.allocation, directory)
        self.assertEqual(set(commands.environment), {"PATH", "LANG", "LC_ALL", "DOCKER_CONFIG", "HOME"})
        self.assertEqual(commands.prefix[2], "unix://" + self.allocation["docker_socket"])

    def test_command_redaction_includes_secret_inside_environment_argument(self):
        directory = self.root / "commands"
        directory.mkdir()
        commands = driver.DockerCommands(self.allocation, directory)
        commands.secrets.append("synthetic-private")
        self.assertEqual(commands.redact(b"SURREALDB_PASS=synthetic-private"), b"SURREALDB_PASS=[REDACTED]")

    @staticmethod
    def archive(entries):
        output = io.BytesIO()
        with tarfile.open(fileobj=output, mode="w") as archive:
            for name, payload, kind in entries:
                member = tarfile.TarInfo(name)
                member.type = kind
                member.size = len(payload) if kind == tarfile.REGTYPE else 0
                member.linkname = "/foreign" if kind == tarfile.SYMTYPE else ""
                archive.addfile(member, io.BytesIO(payload) if member.size else None)
        return output.getvalue()

    def test_stopped_copy_is_single_regular_file_without_extraction(self):
        raw = self.archive([("binary", b"immutable", tarfile.REGTYPE)])
        self.assertEqual(driver.one_tar_file(raw, 20), b"immutable")
        for entries in ([('binary', b'', tarfile.SYMTYPE)],
                        [('a', b'a', tarfile.REGTYPE), ('b', b'b', tarfile.REGTYPE)],
                        [('large', b'x' * 30, tarfile.REGTYPE)]):
            with self.subTest(entries=entries), self.assertRaises(ValueError):
                driver.one_tar_file(self.archive(entries), 20)

    @staticmethod
    def receipts():
        accepted = {"operation": {"operation_id": "original-op", "source_id": "Source:original",
                                  "source_generation": "original-generation", "desired_revision": "v1"}}
        complete = {"operation_id": "original-op", "internal_source_id": "Source:original",
                    "generation": "original-generation", "desired_revision": "v1", "published_revision": "v1",
                    "state": "complete", "lifecycle": "active", "cleanup_state": "not_required", "error_code": None}
        return accepted, complete

    def test_completed_receipt_requires_original_captured_operation(self):
        accepted, complete = self.receipts()
        driver.operation_matches(accepted, complete, complete=True)
        for changed in ({"operation_id": "latest"}, {"generation": "latest"},
                        {"internal_source_id": "Source:foreign"}, {"desired_revision": "v2"},
                        {"published_revision": None}, {"state": "running"}, {"error_code": "failed"}):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                driver.operation_matches(accepted, {**complete, **changed}, complete=True)

    def test_missing_journal_negative_cannot_pass_on_unrelated_startup_failure(self):
        for backend, reason in (("elephant-db", b"live recovery identity requires its original erasure journal"),
                                ("surreal-db", b"store requires its original independently retained erasure journal")):
            with self.subTest(backend=backend):
                driver.require_missing_journal_reason(backend, b"context: " + reason)
                with self.assertRaises(ValueError):
                    driver.require_missing_journal_reason(backend, b"connection refused or permission denied")

    def test_pending_restart_cannot_accept_completed_subset_or_queue_status(self):
        accepted, complete = self.receipts()
        with self.assertRaises(ValueError):
            driver.operation_matches(accepted, complete)
        pending = {**complete, "state": "pending", "published_revision": None}
        driver.operation_matches(accepted, pending)
        with self.assertRaises(ValueError):
            driver.operation_matches(accepted, {"queue_empty": True})

    def mock_driver(self):
        directory = Path(self.allocation["run_directory"])
        directory.mkdir()
        commands = Mock()
        commands.started = driver.time.monotonic()
        commands.commands = 0
        commands.remaining.return_value = 100
        utilities = (Mock(), Mock(), lambda _: contextlib.nullcontext())
        instance = driver.ContainerDriver(self.allocation, self.inventory, commands=commands, utilities=utilities)
        instance.port = 12345
        return instance

    def test_failure_leaves_every_other_case_explicitly_not_executed(self):
        instance = self.mock_driver()
        def fail():
            raise ValueError("synthetic failure")
        with self.assertRaises(ValueError):
            instance.case("immutable_images", fail)
        instance.owned.cleanup = lambda: []
        report = instance.finish("ValueError")
        self.assertFalse(report["product_acceptance"])
        self.assertEqual(report["cases"]["immutable_images"]["verdict"], "fail")
        self.assertEqual(sum(row["verdict"] == "not_executed" for row in report["cases"].values()), 7)

    def test_cleanup_uncertainty_cannot_be_product_acceptance(self):
        instance = self.mock_driver()
        for row in instance.report["cases"].values():
            row["verdict"] = "pass"  # Mock verdict fixture, not a product observation.
        instance.owned.cleanup = lambda: [{"identity": "unproven"}]
        self.assertFalse(instance.finish()["product_acceptance"])

    def test_actual_bootstrap_cli_separates_global_and_named_token_authority(self):
        instance = self.mock_driver()
        instance.owned.create = Mock(side_effect=lambda kind, purpose: purpose)
        instance.once = Mock(return_value="owned-auth-container")
        instance.copy = Mock(return_value=b"header.payload.signature")
        instance.volumes_and_auth()
        issued = [call.args[1] for call in instance.once.call_args_list if call.args[0] == "auth-issue"]
        self.assertEqual(len(issued), 3)
        by_subject = {args[args.index("--subject") + 1]: args for args in issued}
        def values(args, option):
            return [args[index + 1] for index, value in enumerate(args) if value == option]
        operator = by_subject["container-operator"]
        self.assertIn("--global-admin", operator)
        for forbidden in ("--clearances-file", "--space", "--security-level", "--subject-allowlist-file", "--purpose"):
            self.assertNotIn(forbidden, operator)
        self.assertEqual(set(values(operator, "--scope")), {
            "manage:grant", "manage:provider", "read:space_config", "write:space_config",
            "read:status", "read:system", "write:system"})
        for role, spaces in (("service", instance.cases["spaces"]), ("one-space", instance.cases["spaces"][:1])):
            args = by_subject["container-" + role]
            self.assertNotIn("--global-admin", args)
            self.assertEqual(values(args, "--space"), spaces)
            self.assertEqual(values(args, "--clearances-file"), [driver.AUTH_ROOT + "/clearances.json"])
            self.assertEqual(set(values(args, "--scope")), {"read:source", "write:source", "read:status", "read:space_config"})

    def test_fresh_space_bootstrap_uses_global_operator_record_clearance(self):
        instance = self.mock_driver()
        first, second = [instance.intended_space(space) for space in instance.cases["spaces"]]
        for config in (first, second):
            self.assertEqual(config["security"], {"policy_id": "default", "level": 0, "labels": [], "handling": []})
        for field in ("name", "embedding_model", "embedding_dims", "purpose", "skill"):
            self.assertNotEqual(first[field], second[field])

    def test_global_operator_data_selection_refused_before_any_connection(self):
        instance = self.mock_driver()
        with patch.object(driver.http.client, "HTTPConnection", side_effect=AssertionError("connection created")):
            with self.assertRaisesRegex(ValueError, "ordinary data space"):
                instance.http("POST", "/v1/sources/ingest", "operator", instance.cases["spaces"][0], {})
        self.assertEqual(instance.http_calls, 0)

    def configuration_fixture(self):
        instance = self.mock_driver()
        expected = instance.intended_space(instance.cases["spaces"][0])
        observed = {**copy.deepcopy(expected), "space_id": expected["name"], "id": "SpaceConfig:original",
                    "created_at": "2026-10-07T00:00:00Z", "updated_at": "2026-10-07T00:00:00Z",
                    "created_by": "container-operator", "deleted_at": None, "subjects": [],
                    "subjects_state": "complete", "provenance": {"sources": []}}
        observed.pop("name")
        return expected, observed

    def test_persisted_configuration_ignores_only_storage_update_and_content_epochs(self):
        expected, observed = self.configuration_fixture()
        baseline = driver.stable_space_configuration(observed, expected)
        later = copy.deepcopy(observed)
        later["updated_at"] = "2026-10-07T00:10:00Z"
        later["processing"]["index_generation"] = 9
        self.assertEqual(driver.stable_space_configuration(later, expected), baseline)
        self.assertIn("updated_at", observed)  # The captured response was not mutated.
        later["created_by"] = "another-owner"
        self.assertNotEqual(driver.stable_space_configuration(later, expected), baseline)

    def test_config_security_and_provider_binding_changes_cannot_be_omitted(self):
        expected, observed = self.configuration_fixture()
        variants = []
        for field, value in (("security", {"policy_id": "foreign", "level": 0}),
                             ("embedding_model", "substituted"), ("embedding_dims", 999),
                             ("purpose", "changed"), ("skill", "changed")):
            changed = copy.deepcopy(observed)
            changed[field] = value
            variants.append(changed)
        for field, value in (("generation", 2), ("bindings", {"hidden": "provider"}),
                             ("profiles", {"unexpected": {}}), ("policy_id", "foreign")):
            changed = copy.deepcopy(observed)
            changed["processing"][field] = value
            variants.append(changed)
        for changed in variants:
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                driver.stable_space_configuration(changed, expected)

    def test_configuration_requires_full_persisted_observation(self):
        expected, observed = self.configuration_fixture()
        for key in ("id", "space_id", "updated_at", "processing"):
            changed = copy.deepcopy(observed)
            changed.pop(key)
            with self.subTest(key=key), self.assertRaises(ValueError):
                driver.stable_space_configuration(changed, expected)

    def test_backup_payload_bytes_backend_and_journal_match_actual_manifest(self):
        payload = b"actual bounded copied backup fixture"
        manifest = {"bytes": len(payload), "sha256": driver.digest(payload),
                    "backend": "elephant-db", "journal_id": "original"}
        driver.verify_backup_payload(manifest, payload, "elephant-db", "original")
        for changed in ({"sha256": "0" * 64}, {"bytes": len(payload) + 1},
                        {"backend": "surreal-db"}, {"journal_id": "replacement"}):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                driver.verify_backup_payload({**manifest, **changed}, payload, "elephant-db", "original")
        with self.assertRaises(ValueError):
            driver.verify_backup_payload(manifest, payload + b"tampered", "elephant-db", "original")

    def response(self, data, headers, status=200):
        response = Mock()
        response.status = status
        response.headers.get_all.side_effect = lambda name, default: headers.get(name, default)
        chunks = iter([data, b""])
        response.read1.side_effect = lambda _: next(chunks)
        return response

    def test_http_incomplete_length_records_prefix_but_never_parses_as_success(self):
        instance = self.mock_driver()
        connection = Mock()
        connection.getresponse.return_value = self.response(b'{"ready":true}', {"Content-Length": ["99"]})
        with patch.object(driver.http.client, "HTTPConnection", return_value=connection), self.assertRaises(ValueError):
            instance.http("GET", "/v1/readiness")
        record = driver.strict_json((instance.root / "http-001.json").read_bytes())
        self.assertFalse(record["raw_body_complete"])
        self.assertEqual(record["body_sha256"], driver.digest(b'{"ready":true}'))
        self.assertEqual(record["transport_error_type"], "ValueError")

    def test_http_ambiguous_framing_is_refused(self):
        instance = self.mock_driver()
        connection = Mock()
        connection.getresponse.return_value = self.response(b'{}', {"Content-Length": ["2"], "Transfer-Encoding": ["chunked"]})
        with patch.object(driver.http.client, "HTTPConnection", return_value=connection), self.assertRaises(ValueError):
            instance.http("GET", "/v1/readiness")

    def test_complete_http_captures_exact_raw_body_and_deadline(self):
        instance = self.mock_driver()
        connection = Mock()
        connection.getresponse.return_value = self.response(b'{"ready":false}', {"Content-Length": ["15"]}, 503)
        with patch.object(driver.http.client, "HTTPConnection", return_value=connection):
            result = instance.http("GET", "/v1/readiness")
        self.assertEqual(result, {"status": 503, "body": {"ready": False}})
        record = driver.strict_json((instance.root / "http-001.json").read_bytes())
        self.assertTrue(record["raw_body_complete"])
        self.assertGreater(record["deadline_monotonic"], record["ended_monotonic"])
        self.assertNotIn("Authorization", record)


if __name__ == "__main__":
    unittest.main()
