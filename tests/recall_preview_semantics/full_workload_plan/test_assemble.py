"""Closed-world assembly controls. No product, database, process or provider."""
import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import assemble as module


def synthetic_load():
    return {"format": "recall-full-workload-lexical-load/v1", "requests": [
        {"id": f"synthetic-{caller}-{ordinal}", "caller": caller, "ordinal": ordinal,
         "space": module.SPACES[caller // 70],
         "body": {"query": "unit fixture only", "ranking_mode": "lexical", "limit": 20,
                  "source_revision_constraints": {"unit/key": "unit-r1"}},
         "expected_coverage": {"unit/key": {"requested_revision": "unit-r1", "published_revision": "unit-r1",
                                           "desired_revision": "unit-r1", "state": "complete"}}}
        for caller in range(140) for ordinal in (1, 2)]}


class AssemblyControls(unittest.TestCase):
    def setUp(self):
        for name in ("socket.socket", "subprocess.Popen", "asyncio.open_connection", "asyncio.start_server"):
            guard = patch(name, side_effect=AssertionError("assembly controls cannot execute runtime"))
            guard.start()
            self.addCleanup(guard.stop)
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name)

    def descriptor(self, raw=b'{"original":1}', name="input.json"):
        (self.root / name).write_bytes(raw)
        return {"root": "unit", "path": name, "sha256": module.sha(raw), "bytes": len(raw)}

    def test_exact_file_and_canonical_hash_retained_without_rewriting(self):
        raw = b'{ "original": 1, "text": "retained\\n" }\n'
        descriptor = self.descriptor(raw)
        descriptor["canonical_sha256"] = module.sha(module.encode(module.decode(raw)))
        inputs = module.Inputs({"unit": self.root})
        self.assertEqual(inputs.read(descriptor), {"original": 1, "text": "retained\n"})
        self.assertEqual((self.root / "input.json").read_bytes(), raw)
        self.assertEqual(inputs.bytes, len(raw))
        self.assertEqual(inputs.observed[0]["sha256"], descriptor["sha256"])

    def test_changed_file_or_canonical_digest_cannot_supply_expectations(self):
        descriptor = self.descriptor()
        (self.root / "input.json").write_bytes(b'{"original":2}')
        with self.assertRaises(module.PlanError):
            module.Inputs({"unit": self.root}).read(descriptor)
        descriptor = self.descriptor()
        descriptor["canonical_sha256"] = "0" * 64
        with self.assertRaises(module.PlanError):
            module.Inputs({"unit": self.root}).read(descriptor)

    def test_duplicate_members_nonfinite_and_invalid_utf8_refuse(self):
        for raw in (b'{"key":1,"key":2}', b'{"value":NaN}', b'{"value":Infinity}', b'"\xff"'):
            descriptor = self.descriptor(raw)
            with self.subTest(raw=raw), self.assertRaises(module.PlanError):
                module.Inputs({"unit": self.root}).read(descriptor)

    def test_path_traversal_absolute_path_and_symlink_escape_refuse(self):
        descriptor = self.descriptor()
        for name in ("../input.json", str(self.root / "input.json")):
            with self.subTest(path=name), self.assertRaises(module.PlanError):
                module.Inputs({"unit": self.root}).read({**descriptor, "path": name})
        child = self.root / "child"
        child.mkdir()
        (child / "escape.json").symlink_to(self.root / "input.json")
        with self.assertRaises(module.PlanError):
            module.Inputs({"unit": child}).read({**descriptor, "path": "escape.json"})

    def test_file_and_aggregate_caps_apply_before_parsing(self):
        descriptor = self.descriptor(b" " * 17)
        with patch.object(module, "FILE_LIMIT", 16), self.assertRaises(module.PlanError):
            module.Inputs({"unit": self.root}).read(descriptor)
        descriptor = self.descriptor()
        inputs = module.Inputs({"unit": self.root})
        with patch.object(module, "PACKAGE_LIMIT", descriptor["bytes"]):
            inputs.read(descriptor)
            with self.assertRaises(module.PlanError):
                inputs.read(descriptor)

    def test_exact_140_by_two_schedule_is_preserved(self):
        original = synthetic_load()
        before = module.encode(original)
        module.validate_load(original)
        self.assertEqual(module.encode(original), before)
        self.assertEqual(len({row["caller"] for row in original["requests"]}), 140)

    def test_load_identity_scope_coverage_and_mode_mutations_refuse(self):
        original = synthetic_load()
        mutations = [lambda p: p["requests"].pop(),
                     lambda p: p["requests"][1].update(id=p["requests"][0]["id"]),
                     lambda p: p["requests"][1].update(ordinal=1),
                     lambda p: p["requests"][0].update(caller=True),
                     lambda p: p["requests"][0].update(space="default"),
                     lambda p: p["requests"][0]["body"].update(ranking_mode="semantic"),
                     lambda p: p["requests"][0]["expected_coverage"]["unit/key"].update(published_revision="new")]
        for index, mutate in enumerate(mutations):
            changed = copy.deepcopy(original)
            mutate(changed)
            with self.subTest(index=index), self.assertRaises(module.PlanError):
                module.validate_load(changed)

    def test_source_package_cannot_claim_execution_authority(self):
        for field in ("execution_authorized", "provider_dispatch_authorized", "product_acceptance"):
            bundle = {"format": "recall-full-workload-plan-package/v1", "execution_authorized": False,
                      "provider_dispatch_authorized": False, "product_acceptance": False}
            bundle[field] = True
            with self.subTest(field=field), self.assertRaises(module.PlanError):
                module.assemble(bundle, {}, {})

    def test_worker_expectations_retain_explicit_values_and_detach_from_input(self):
        proposed = module.decode(Path(__file__).with_name("candidate-expectations.json").read_bytes())
        # An independently reviewed different literal is retained, never replaced
        # by this producer's preferred retry configuration or runtime defaults.
        proposed["worker_runtime_expectations"]["work_queue"]["transient_retry_max_attempts"] = 7
        before = module.encode(proposed["worker_runtime_expectations"])
        retained = module.worker_runtime_expectations(proposed)
        self.assertEqual(module.encode(retained), before)
        retained["work_queue"]["max_concurrent"] = 2
        self.assertEqual(module.encode(proposed["worker_runtime_expectations"]), before)

    def test_worker_expectations_cannot_omit_or_infer_runtime_fields(self):
        proposed = module.decode(Path(__file__).with_name("candidate-expectations.json").read_bytes())
        original = proposed["worker_runtime_expectations"]
        mutations = [lambda p: p.pop("work_queue"),
                     lambda p: p.update(format="observed-worker-recipe"),
                     lambda p: p["work_queue"].pop("transient_retry_max_attempts"),
                     lambda p: p["work_queue"].update(max_concurrent=True),
                     lambda p: p["work_queue"].update(disabled_kinds=["extract", "extract"]),
                     lambda p: p["work_queue"]["doc_tree"].pop("create_summaries"),
                     lambda p: p["embed_worker"].pop("provider"),
                     lambda p: p["embed_worker"].update(enabled="true"),
                     lambda p: p["embedding_execution"].pop(module.SPACES[1]),
                     lambda p: p["embedding_execution"].update(default=p["embedding_execution"][module.SPACES[0]]),
                     lambda p: p["embedding_execution"][module.SPACES[0]].pop("query_embedding"),
                     lambda p: p["embedding_execution"][module.SPACES[1]]["document_embedding"].pop("initial_segment_bytes"),
                     lambda p: p.update(observed_instance="candidate-observation-cannot-fill-expectations")]
        with self.assertRaises(module.PlanError):
            module.worker_runtime_expectations({})
        for index, mutate in enumerate(mutations):
            changed = copy.deepcopy(original)
            mutate(changed)
            with self.subTest(index=index), self.assertRaises(module.PlanError):
                module.worker_runtime_expectations({"worker_runtime_expectations": changed})

    def test_runtime_principal_templates_retain_reviewed_subjects_scopes_without_inference(self):
        proposed = module.decode(Path(__file__).with_name("candidate-expectations.json").read_bytes())
        original = proposed["runtime_principal_templates"]
        before = module.encode(original)
        retained = module.runtime_principal_expectations(proposed)
        self.assertEqual(module.encode(retained), before)
        retained[module.SPACES[0]]["manage"]["subject"] = "changed-management-subject"
        retained[module.SPACES[1]]["revocable"]["scopes"].append("write:source")
        self.assertEqual(module.encode(original), before)
        self.assertNotEqual(module.sha(module.encode(retained)), module.sha(before))
        mutations = [lambda p: p.pop(module.SPACES[0]),
                     lambda p: p[module.SPACES[0]].pop("manage"),
                     lambda p: p[module.SPACES[1]]["revocable"].pop("scopes"),
                     lambda p: p[module.SPACES[0]]["manage"].update(subject=True),
                     lambda p: p[module.SPACES[1]]["revocable"].update(scopes=["read:source", "read:source"])]
        with self.assertRaises(module.PlanError):
            module.runtime_principal_expectations({})
        for index, mutate in enumerate(mutations):
            changed = copy.deepcopy(original)
            mutate(changed)
            with self.subTest(index=index), self.assertRaises(module.PlanError):
                module.runtime_principal_expectations({"runtime_principal_templates": changed})

    def test_every_original_load_intent_requires_its_own_literal_evidence(self):
        plan = synthetic_load()
        literal = {"source_key": "unit/key", "revision": "unit-r1", "quote": "independent unit text",
                   "quote_hash": module.sha(b"independent unit text")}
        requirements = {row["id"]: {"required_evidence": [copy.deepcopy(literal)], "required_claim_evidence": []}
                        for row in plan["requests"]}
        original_sha256 = module.sha(module.encode(requirements))
        module.validate_load_evidence(plan, requirements, original_sha256)
        mutations = [lambda value: value.pop(plan["requests"][0]["id"]),
                     lambda value: value[plan["requests"][0]["id"]].update(required_evidence=[]),
                     lambda value: value[plan["requests"][0]["id"]]["required_evidence"][0].update(source_key="foreign"),
                     lambda value: value[plan["requests"][0]["id"]]["required_evidence"][0].update(revision="new"),
                     lambda value: value[plan["requests"][0]["id"]]["required_evidence"][0].update(quote="candidate output"),
                     lambda value: value[plan["requests"][0]["id"]].update(required_claim_evidence=[{**literal, "quote": "other"}])]
        for index, mutate in enumerate(mutations):
            changed = copy.deepcopy(requirements)
            mutate(changed)
            with self.subTest(index=index), self.assertRaises(module.PlanError):
                module.validate_load_evidence(plan, changed, module.sha(module.encode(changed)))

    def test_replacement_evidence_cannot_keep_original_independent_map_hash(self):
        plan = synthetic_load()
        original = {"source_key": "unit/key", "revision": "unit-r1", "quote": "original literal",
                    "quote_hash": module.sha(b"original literal")}
        requirements = {row["id"]: {"required_evidence": [copy.deepcopy(original)], "required_claim_evidence": []}
                        for row in plan["requests"]}
        original_sha256 = module.sha(module.encode(requirements))
        for malformed in (None, True, "", "0" * 63, original_sha256.upper(), "f" * 64):
            with self.subTest(digest=malformed), self.assertRaises(module.PlanError):
                module.validate_load_evidence(plan, requirements, malformed)
        changed = copy.deepcopy(requirements)
        changed[plan["requests"][0]["id"]]["required_evidence"][0].update(
            quote="substituted literal", quote_hash=module.sha(b"substituted literal"))
        with self.assertRaises(module.PlanError):
            module.validate_load_evidence(plan, changed, original_sha256)
        # Its valid inner quote digest cannot make it the original reviewed map.
        # A different separately reviewed map would need a new external pin.
        module.validate_load_evidence(plan, changed, module.sha(module.encode(changed)))

    def test_configuration_binding_changes_only_original_endpoint_and_preserves_template(self):
        value = {"space_request_templates": {space: {"name": space, "processing": {
            "connections": {"owned-spy": {"id": "owned-spy", "revoked": False}}}} for space in module.SPACES}}
        before = module.encode(value)
        endpoints = {space: "http://127.0.0.1:43131/v1" for space in module.SPACES}
        requests = module.configuration_requests(value, endpoints)
        self.assertEqual(module.encode(value), before)
        for space in module.SPACES:
            self.assertEqual(requests[space]["processing"]["connections"]["owned-spy"],
                             {"id": "owned-spy", "revoked": False, "endpoint": endpoints[space]})
        for invalid in ("https://127.0.0.1:43131", "http://example.invalid:43131", "http://127.0.0.1:43131/v1?key=x",
                        "http://credential@127.0.0.1:43131", "http://127.0.0.1:43131/other", "http://[::1]:43131"):
            with self.subTest(endpoint=invalid), self.assertRaises(module.PlanError):
                module.configuration_requests(value, {space: invalid for space in module.SPACES})

    def runtime_document_inputs(self):
        expected = module.decode(Path(__file__).with_name("candidate-expectations.json").read_bytes())
        runtime = {"backend": "elephant-db", "run_nonce": "unit-original-allocation",
                   "endpoint": "http://127.0.0.1:43130", "spy_endpoint": "http://127.0.0.1:43131/v1",
                   "run_directory": str(self.root / "uncreated-runtime"), "archive": str(self.root / "unopened.tar.gz"),
                   "artifact_sha256": "a" * 64, "binary_sha256": "b" * 64, "source_commit": "c" * 40,
                   "target": "x86_64-unknown-linux-gnu", "paired": None,
                   "spy_credential": expected["controlled_spy_credential"]["value"]}
        # Input-binding-only fixture: these rows do not describe a runnable spy.
        spy = {"rules": [{"credential_sha256": expected["controlled_spy_credential"]["sha256"]}]}
        return expected, runtime, spy

    def scoped_runtime_document_inputs(self):
        expected, runtime, _ = self.runtime_document_inputs()
        credentials = {space: {"credential_ref": "FULL_WORKLOAD_SPY_KEY_" + space.upper().replace("-", "_"),
                               "value": "synthetic-controlled-provider-key-" + space} for space in module.SPACES}
        expected["controlled_spy_credentials"] = {
            space: {**row, "sha256": module.sha(row["value"].encode())} for space, row in credentials.items()}
        runtime["spy_credentials"] = copy.deepcopy(credentials)
        rules = []
        for space, row in credentials.items():
            for configuration in (expected["space_request_templates"][space],
                                  expected["configurations"][space]["configuration"]):
                configuration["processing"]["connections"]["owned-spy"]["credential_ref"] = row["credential_ref"]
            rules.append({"space": space, "path": "/v1/embeddings", "model": "same-model",
                          "credential_sha256": module.sha(row["value"].encode())})
        return expected, runtime, {"rules": rules}

    def test_prestart_scoped_map_binds_both_spaces_without_changing_semantic_inputs(self):
        expected, runtime, spy = self.scoped_runtime_document_inputs()
        before = module.encode([expected, runtime, spy])
        document, digest = module.runtime_plan_document(expected, runtime, spy)
        self.assertEqual(digest, module.sha(module.encode(document)))
        self.assertEqual(document["spy_credentials"], runtime["spy_credentials"])
        for request in document["spaces"]:
            self.assertEqual(request["processing"]["connections"]["owned-spy"]["credential_ref"],
                             runtime["spy_credentials"][request["name"]]["credential_ref"])
        self.assertEqual(module.encode([expected, runtime, spy]), before)
        self.assertEqual(list(self.root.iterdir()), [])

    def test_prestart_scoped_map_rejects_missing_shared_foreign_and_ambiguous_bindings(self):
        household, business = module.SPACES
        changes = [lambda e, r, s: r.pop("spy_credentials"),
                   lambda e, r, s: e.pop("controlled_spy_credentials"),
                   lambda e, r, s: r["spy_credentials"].pop(household),
                   lambda e, r, s: r["spy_credentials"][household].update(value=r["spy_credentials"][business]["value"]),
                   lambda e, r, s: e["controlled_spy_credentials"][household].update(sha256="0" * 64),
                   lambda e, r, s: s["rules"][0].update(credential_sha256="0" * 64),
                   lambda e, r, s: s["rules"][0].update(space="foreign"),
                   lambda e, r, s: s["rules"].append(copy.deepcopy(s["rules"][0])),
                   lambda e, r, s: e["space_request_templates"][household]["processing"]["connections"]["owned-spy"].update(credential_ref="FULL_WORKLOAD_SPY_KEY")]
        for change in changes:
            expected, runtime, spy = self.scoped_runtime_document_inputs()
            change(expected, runtime, spy)
            with self.subTest(change=change), self.assertRaises(module.PlanError):
                module.runtime_plan_document(expected, runtime, spy)
        expected, runtime, spy = self.scoped_runtime_document_inputs()
        runtime.pop("spy_credentials")
        expected.pop("controlled_spy_credentials")
        for rule in spy["rules"]:
            rule.update(path="/v1/messages", credential_sha256=expected["controlled_spy_credential"]["sha256"])
        with self.assertRaisesRegex(module.PlanError, "ambiguous controlled provider space authority"):
            module.runtime_plan_document(expected, runtime, spy)

    def test_prestart_runtime_document_binds_original_spaces_principals_and_worker_without_output(self):
        expected, runtime, spy = self.runtime_document_inputs()
        before = module.encode([expected, runtime, spy])
        document, digest = module.runtime_plan_document(expected, runtime, spy)
        self.assertEqual(digest, module.sha(module.encode(document)))
        self.assertEqual(document["format"], "recall-full-workload-runtime-plan/v1")
        self.assertEqual([request["name"] for request in document["spaces"]], list(module.SPACES))
        for request in document["spaces"]:
            space = request["name"]
            self.assertEqual(request["processing"]["connections"]["owned-spy"]["endpoint"], runtime["spy_endpoint"])
            self.assertEqual(request["security"], expected["configurations"][space]["configuration"]["security"])
            self.assertEqual(document["principals"][space]["data"], {
                "subject": expected["configurations"][space]["subject"],
                "scopes": expected["configurations"][space]["grant"]["scopes"]})
            self.assertEqual(set(document["principals"][space]), {"data", "manage", "revocable"})
            original_case = next(case for case in expected["cases"]["FW10"]["spaces"] if case["space"] == space)
            self.assertEqual(document["principals"][space]["revocable"]["subject"], original_case["subject"])
        self.assertEqual(document["worker_runtime_expectations"], expected["worker_runtime_expectations"])
        document["principals"][module.SPACES[0]]["data"]["scopes"].append("unit-detached-change")
        self.assertEqual(module.encode([expected, runtime, spy]), before)
        self.assertEqual(list(self.root.iterdir()), [], "binding may not create future startup evidence or paths")

    def test_prestart_runtime_document_refuses_missing_principal_credential_or_invented_receipt(self):
        mutations = [lambda e, r, s: r.pop("source_commit"),
                     lambda e, r, s: r.update(startup_receipt={"status": "bootstrapped"}),
                     lambda e, r, s: r.update(spy_credential="full-workload-spy-replacement"),
                     lambda e, r, s: s["rules"][0].update(credential_sha256="0" * 64),
                     lambda e, r, s: r.update(backend="surreal-db"),
                     lambda e, r, s: r.update(run_directory="../foreign-runtime"),
                     lambda e, r, s: e["runtime_principal_templates"][module.SPACES[0]].pop("revocable"),
                     lambda e, r, s: e["runtime_principal_templates"][module.SPACES[0]]["revocable"].update(subject="replacement-subject"),
                     lambda e, r, s: e["configurations"][module.SPACES[0]]["grant"].update(active=False),
                     lambda e, r, s: e["runtime_principal_templates"][module.SPACES[0]]["manage"].update(subject=e["configurations"][module.SPACES[0]]["subject"])]
        for index, mutate in enumerate(mutations):
            expected, runtime, spy = self.runtime_document_inputs()
            mutate(expected, runtime, spy)
            with self.subTest(index=index), self.assertRaises(module.PlanError):
                module.runtime_plan_document(expected, runtime, spy)
        expected, runtime, spy = self.runtime_document_inputs()
        runtime.update(backend="surreal-db", paired={"binary": str(self.root / "paired-surreal"),
                       "binary_sha256": "d" * 64, "endpoint": "http://127.0.0.1:43132"})
        document, _ = module.runtime_plan_document(expected, runtime, spy)
        self.assertEqual(document["paired"], runtime["paired"])

    def test_cli_document_resolves_to_exact_original_plan_without_writing(self):
        recipe = {"original": "independent literal"}
        raw = b'{ "original": "independent literal" }\n'
        descriptor = self.descriptor(raw, "recipe.json")
        plan = {"graph_recipes": {"unit": recipe}, "other": {"retained": [1, 2]}}
        digest = module.sha(module.encode(plan))
        provenance = {"plan_sha256": digest, "graph_recipe_files": {"unit": {
            "path": str(self.root / "recipe.json"), "sha256": descriptor["sha256"],
            "bytes": len(raw), "canonical_sha256": module.sha(module.encode(recipe))}}}
        document, identity = module.driver_input_document(plan, digest, provenance)
        self.assertEqual(document["graph_recipes"], {"unit": {"file": {
            "path": str(self.root / "recipe.json"), "sha256": descriptor["sha256"]}}})
        self.assertEqual(identity["document_sha256"], module.sha(module.encode(document)))
        resolved = copy.deepcopy(document)
        resolved["graph_recipes"]["unit"] = module.Inputs({"unit": self.root}).read(descriptor)
        self.assertEqual(module.encode(resolved), module.encode(plan))
        self.assertEqual(module.sha(module.encode(resolved)), identity["resolved_plan_sha256"])
        self.assertEqual((self.root / "recipe.json").read_bytes(), raw)
        document["other"]["retained"].append(3)
        self.assertEqual(plan["other"]["retained"], [1, 2])
        changed = copy.deepcopy(plan)
        changed["graph_recipes"]["unit"]["original"] = "substituted output"
        with self.assertRaises(module.PlanError):
            module.driver_input_document(changed, digest, provenance)
        changed_sha = module.sha(module.encode(changed))
        with self.assertRaises(module.PlanError):
            module.driver_input_document(changed, changed_sha, {**provenance, "plan_sha256": changed_sha})
        with patch.object(module, "FILE_LIMIT", 64), self.assertRaises(module.PlanError):
            module.driver_input_document(plan, digest, provenance)
        (self.root / "recipe.json").write_bytes(b'{"original":"changed after export"}')
        with self.assertRaises(module.PlanError):
            module.Inputs({"unit": self.root}).read(descriptor)


if __name__ == "__main__":
    unittest.main()
