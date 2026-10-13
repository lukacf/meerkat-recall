"""Synthetic closed-profile evidence controls; no gateway/provider execution."""
from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import types
import unittest
from unittest import mock

import semantic140_expectations as expected
import dynamic_component_validator as dynamic
import provider_evidence_validator as provider
from dynamic_component_validator_test import static_fixture, runtime_fixture, retain, original
from provider_evidence_validator_test import OriginalFixture, raw

ROOT = Path(__file__).resolve().parents[2]
FILES = {"corpus":"corpus", "workload_contract":"contract", "preparation_contract":"preparation-contract",
         "rendered_map":"rendered-map", "provider_allocation":"allocation-proposal", "typed_delta":"allocation-typed-delta"}


def reviewed_fixture():
    reviewed, raws, plan = static_fixture()
    review = reviewed["review"]
    review["profile_id"] = expected.PROFILE_ID
    review["helper_sha256"]["tests/recall_preview_semantics/semantic140_expectations.py"] = "dbb7b64d9e30033574942f8c43fd3301f223088cc6bd8213cc006fc15d37721f"
    anchor = review["controller_policy"]["expected_anchor"]
    anchor.update(format="recall-gateway-launch-anchor/v2", profile_id=expected.PROFILE_ID,
                  allocation_sha256=expected.FROZEN_INPUTS["provider_allocation"],
                  corpus_sha256=expected.FROZEN_INPUTS["corpus"], caps=expected.caps())
    review["controller_policy"]["maximum_backend_seconds"] = 7800
    review["limits"]["runtime_raw_bytes"] = expected.RUNTIME_RAW_BYTES
    for name, filename in FILES.items():
        data = (ROOT / "tests/recall_preview" / ("semantic140-" + filename + ".json")).read_bytes()
        review["input_refs"][name] = retain(data, "original-" + name, raws)
    reviewed["raw_bytes"] = original(review)
    reviewed["raw_sha256"] = hashlib.sha256(reviewed["raw_bytes"]).hexdigest()
    return reviewed, raws, plan


def provider_fixture(backend="elephant-db", *, prefix=None):
    fixture = OriginalFixture(backend)
    fixture.identity.update({key + "_sha256": value for key, value in expected.FROZEN_INPUTS.items()})
    fixture.anchor.update(format="recall-gateway-launch-anchor/v2", profile_id=expected.PROFILE_ID,
                          allocation_sha256=expected.FROZEN_INPUTS["provider_allocation"],
                          corpus_sha256=expected.FROZEN_INPUTS["corpus"], caps=expected.caps())
    for target, name in (("allocation_sha256", "provider_allocation"), ("contract_sha256", "workload_contract"),
                         ("corpus_sha256", "corpus"), ("preparation_contract_sha256", "preparation_contract"),
                         ("typed_delta_sha256", "typed_delta")):
        fixture.authority[target] = expected.FROZEN_INPUTS[name]
    for call in fixture.calls:
        call["binding"]["identity"] = copy.deepcopy(fixture.authority)
        call["intent"]["binding"] = copy.deepcopy(call["binding"])
    if prefix is not None:
        fixture.baseline_entries = []
        for index in range(prefix):
            row = copy.deepcopy(fixture.calls[index % 2]["row"])
            row.update(id=index+1, backend="elephant-db", started=10001, finished=10002)
            fixture.baseline_entries.append(row)
        for index, call in enumerate(fixture.calls, prefix+1):
            call["row"]["id"] = index; call["intent"]["dispatch_id"] = index
    return fixture


def repeated_ledger(fixture, count):
    rows = []
    for index in range(count):
        row = copy.deepcopy(fixture.calls[index % 2]["row"]); row["id"] = index + 1; rows.append(row)
    return fixture.ledger(rows)


class Semantic140EvidenceControls(unittest.TestCase):
    def test_literal_caps_and_original_per_export_ceiling_are_separate(self):
        self.assertEqual(expected.caps(), {"physical_dispatches":4260, "maximum_nano_usd":5_000_000_000})
        self.assertEqual(expected.GLOBAL_CAPTURE_BYTES, 40*1024**3)
        self.assertEqual(expected.PROVIDER_EXPORT_BYTES, 20*1024**3)
        self.assertEqual(expected.PROVIDER_EXPORT_FILES, 10532)
        self.assertEqual(provider.MAX_PROVIDER_BYTES, 20*1024**3)

    def test_original_static_review_retains_original_shape_and_limits(self):
        reviewed, raws, plan = static_fixture()
        self.assertEqual(dynamic.validate_static(reviewed, raws, "elephant-db"), plan)
        self.assertEqual(dynamic.STATIC_LIMITS["runtime_raw_bytes"], 352*1024**2)

    def test_original_profile_never_imports_or_reads_successor_helper(self):
        poison = types.ModuleType("semantic140_expectations")
        poison.__getattr__ = lambda name: self.fail("legacy consulted successor: " + name)
        with mock.patch.dict(sys.modules, semantic140_expectations=poison), \
                mock.patch.object(Path, "read_bytes", side_effect=AssertionError("legacy helper read")):
            for name in ("dynamic_component_validator", "provider_evidence_validator"):
                path = Path(__file__).with_name(name + ".py")
                spec = importlib.util.spec_from_file_location("legacy_custody_" + name, path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                if name == "dynamic_component_validator":
                    reviewed, raws, plan = static_fixture()
                    self.assertEqual(module.validate_static(reviewed, raws, "elephant-db"), plan)
                else:
                    for backend in ("elephant-db", "surreal-db"):
                        self.assertEqual(module.validate_provider_accounting(*OriginalFixture(backend).materialize())
                                         ["begin"]["backend"], backend)

    def test_new_profile_ignores_poisoned_cached_expectations(self):
        poison = types.ModuleType("semantic140_expectations")
        poison.__getattr__ = lambda name: self.fail("successor consulted cached module: " + name)
        with mock.patch.dict(sys.modules, semantic140_expectations=poison):
            for module in (dynamic, provider):
                self.assertEqual(module.semantic140().FROZEN_INPUTS, expected.FROZEN_INPUTS)
                self.assertIs(sys.modules["semantic140_expectations"], poison)

    def test_substituted_expectations_source_refuses_before_execution(self):
        with mock.patch.object(Path, "read_bytes", return_value=b"raise AssertionError('unverified execution')"):
            for module in (dynamic, provider):
                with self.assertRaisesRegex(ValueError, "expectations_source"):
                    module.semantic140()

    def test_new_static_review_requires_exact_expectations_inventory_pin(self):
        path = "tests/recall_preview_semantics/semantic140_expectations.py"
        for bad in (None, "0"*64):
            reviewed, raws, _ = reviewed_fixture()
            if bad is None: reviewed["review"]["helper_sha256"].pop(path)
            else: reviewed["review"]["helper_sha256"][path] = bad
            reviewed["raw_bytes"] = original(reviewed["review"])
            reviewed["raw_sha256"] = hashlib.sha256(reviewed["raw_bytes"]).hexdigest()
            with self.assertRaisesRegex(ValueError, "semantic140_helper"):
                dynamic.validate_static(reviewed, raws, "elephant-db")

    def test_closed_new_static_review_binds_all_six_original_files(self):
        reviewed, raws, plan = reviewed_fixture()
        self.assertEqual(dynamic.validate_static(reviewed, raws, "elephant-db"), plan)
        self.assertEqual(dynamic.reviewed_runtime_bound(reviewed), 2*1024**3)

    def test_new_review_missing_profile_or_mixed_old_allocation_is_rejected(self):
        for change in (lambda review: review.pop("profile_id"),
                       lambda review: review.update(profile_id="d28-v1"),
                       lambda review: review["controller_policy"]["expected_anchor"].update(allocation_sha256="a"*64),
                       lambda review: review["controller_policy"]["expected_anchor"].update(corpus_sha256=expected.ORIGINAL_CORPUS_SHA256)):
            reviewed, raws, _ = reviewed_fixture(); change(reviewed["review"])
            reviewed["raw_bytes"] = original(reviewed["review"]); reviewed["raw_sha256"] = hashlib.sha256(reviewed["raw_bytes"]).hexdigest()
            with self.assertRaises(ValueError): dynamic.validate_static(reviewed, raws, "elephant-db")

    def test_new_review_cannot_substitute_a_valid_but_different_input(self):
        for name in FILES:
            reviewed, raws, _ = reviewed_fixture()
            reviewed["review"]["input_refs"][name] = retain(b'{"unreviewed":true}', "original-" + name, raws)
            reviewed["raw_bytes"] = original(reviewed["review"]); reviewed["raw_sha256"] = hashlib.sha256(reviewed["raw_bytes"]).hexdigest()
            with self.assertRaises(ValueError): dynamic.validate_static(reviewed, raws, "elephant-db")

    def test_new_limits_cannot_widen_provider_channel_or_legacy_capture(self):
        for key, bad in (("provider_capture_bytes", 40*1024**3), ("runtime_raw_bytes", 2*1024**3+1),
                         ("provider_cache_bytes", 512*1024**2), ("runtime_capture_bytes", 149*1024**2)):
            reviewed, raws, _ = reviewed_fixture(); reviewed["review"]["limits"][key] = bad
            reviewed["raw_bytes"] = original(reviewed["review"]); reviewed["raw_sha256"] = hashlib.sha256(reviewed["raw_bytes"]).hexdigest()
            with self.assertRaises(ValueError): dynamic.validate_static(reviewed, raws, "elephant-db")

    def test_closed_new_provider_preparation_retains_both_space_and_backend_proofs(self):
        for backend in ("elephant-db", "surreal-db"):
            result = provider.validate_provider_accounting(*provider_fixture(backend).materialize())
            self.assertEqual(result["begin"]["backend"], backend)
            self.assertEqual(result["close"]["physical_dispatch_reservations"], 2)
            self.assertEqual(result["close"]["per_request_association"], "unmet")

    def test_all4260_reservations_count_and4261_refuses(self):
        fixture = provider_fixture(); context = {"anchor":fixture.anchor}
        self.assertEqual(len(provider.provider_ledger(repeated_ledger(fixture, 4260), context)), 4260)
        with self.assertRaises(ValueError): provider.provider_ledger(repeated_ledger(fixture, 4261), context)
        old = OriginalFixture()
        with self.assertRaises(ValueError): provider.provider_ledger(repeated_ledger(old, 2101), {"anchor":old.anchor})

    def test_full_first_backend2130_prefix_is_allowed_but2131_refuses(self):
        result = provider.validate_provider_accounting(*provider_fixture("surreal-db", prefix=2130).materialize())
        self.assertEqual(result["close"]["physical_dispatch_reservations"], 2)
        with self.assertRaises(ValueError): provider.validate_provider_accounting(*provider_fixture("surreal-db", prefix=2131).materialize())

    def test_prior_backend_per_space1065_is_not_a_global_prefix_only_bound(self):
        fixture = provider_fixture("surreal-db", prefix=2130)
        fixture.baseline_entries[1]["space"] = fixture.baseline_entries[0]["space"]
        with self.assertRaises(ValueError): provider.validate_provider_accounting(*fixture.materialize())

    def test_reservations_never_refund_and_money_caps_are_exact(self):
        fixture = provider_fixture(); ledger = repeated_ledger(fixture, 2); ledger["reserved_nano_usd"] -= 1
        with self.assertRaises(ValueError): provider.provider_ledger(ledger, {"anchor":fixture.anchor})
        fixture.anchor["caps"]["maximum_nano_usd"] = 5_000_000_001
        with self.assertRaises(ValueError): provider.validate_provider_accounting(*fixture.materialize())

    def test_failed_or_unsettled_original_dispatch_cannot_qualify(self):
        for outcome, status in (("upstream_error", 503), ("uncertain", None)):
            fixture = provider_fixture(); fixture.calls[0]["row"].update(outcome=outcome, status=status)
            with self.assertRaises(ValueError): provider.validate_provider_accounting(*fixture.materialize())

    def test_missing_or_mixed_new_anchor_and_input_hashes_refuse(self):
        for change in (lambda f: f.anchor.pop("profile_id"), lambda f: f.anchor.update(format="recall-gateway-launch-anchor/v1"),
                       lambda f: f.anchor.update(corpus_sha256=expected.ORIGINAL_CORPUS_SHA256),
                       lambda f: f.identity.update(rendered_map_sha256="0"*64)):
            fixture = provider_fixture(); change(fixture)
            with self.assertRaises(ValueError): provider.validate_provider_accounting(*fixture.materialize())

    def test_original_whole_clock_cannot_restart_for_second_backend(self):
        fixture = provider_fixture("surreal-db", prefix=2130)
        fixture.clock["deadline_mono"] += 1
        with self.assertRaises(ValueError): provider.validate_provider_accounting(*fixture.materialize())

    def test_outer_gateway_manifest_requires_exact_new_policy_profile_and_pins(self):
        reviewed, observed, raws, wrapper = runtime_fixture(prepared=True)
        outer = json.loads(raws[observed["outer_launch"]["sha256"]])
        manifest = json.loads(raws[outer["manifest"]["sha256"]])
        anchor = copy.deepcopy(wrapper["context"]["anchor"])
        manifest.update(format="recall-semantic140-embedding-manifest/v1", profile_id=expected.PROFILE_ID,
                        accounting_policy="separate-exclusive-semantic140-embedding-reservation/v1",
                        allocation_sha256=expected.FROZEN_INPUTS["provider_allocation"],
                        contract_sha256=expected.FROZEN_INPUTS["workload_contract"],
                        **{key+"_sha256":expected.FROZEN_INPUTS[key] for key in ("corpus", "preparation_contract", "typed_delta")})
        anchor.update(format="recall-gateway-launch-anchor/v2", profile_id=expected.PROFILE_ID,
                      allocation_sha256=manifest["allocation_sha256"], proposal_sha256=manifest["allocation_sha256"],
                      corpus_sha256=manifest["corpus_sha256"], caps=expected.caps())
        self.assertEqual(dynamic.gateway_manifest(manifest, anchor, observed["identity"]), manifest)
        for key, bad in (("profile_id", "d28-v1"), ("format", "recall-exclusive-embedding-manifest/v1"),
                         ("accounting_policy", "separate-exclusive-workload-embedding-reservation/v1"),
                         ("corpus_sha256", expected.ORIGINAL_CORPUS_SHA256)):
            changed = copy.deepcopy(manifest); changed[key] = bad
            with self.assertRaises(ValueError): dynamic.gateway_manifest(changed, anchor, observed["identity"])

    def test_new_preparation_sources_bind_original50_classified_sources(self):
        inputs = {name:(ROOT/"tests/recall_preview"/("semantic140-"+filename+".json")).read_bytes() for name,filename in FILES.items()}
        identity = {key+"_sha256":value for key,value in expected.FROZEN_INPUTS.items()}
        sources, spaces, typed, rendered = dynamic.preparation_sources(inputs, identity)
        self.assertEqual((len(sources), len(spaces), len(rendered)), (50, 2, 50))
        inputs["corpus"] = (ROOT/"tests/recall_preview/workload-corpus.json").read_bytes()
        with self.assertRaises(ValueError): dynamic.preparation_sources(inputs, identity)


if __name__ == "__main__":
    unittest.main()
