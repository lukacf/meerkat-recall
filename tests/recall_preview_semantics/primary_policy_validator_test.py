"""Independent bootstrap policy controls; no runtime, credential or provider I/O."""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import unittest

import dynamic_component_validator as validator
from dynamic_component_validator_test import static_fixture, original, retain

ROOT = Path(__file__).resolve().parents[2]
CORPUS_SHA256 = "a866e3569dbec863c9214c5f4cba477333dd6629ace8a4816318e40afea90da7"


def original_primary_plan():
    raw = (ROOT / "tests/recall_preview_semantics/corpus.json").read_bytes()
    if hashlib.sha256(raw).hexdigest() != CORPUS_SHA256:
        raise ValueError("independent original primary corpus changed")
    corpus = json.loads(raw)
    _, _, plan = static_fixture()
    for target, source in zip(plan["spaces"], corpus["spaces"], strict=True):
        target.update(name=source["id"], purpose=source["guidance"],
                      security={"policy_id": source["policy"], "level": source["level"],
                                "labels": [], "handling": []})
    return plan


def primary_review(backend):
    reviewed, raws, _ = static_fixture()
    plan = original_primary_plan()
    plan["backend"] = backend
    plan["run_directory"] = "/work/bootstrap-" + plan["run_nonce"] + "-" + backend
    review = reviewed["review"]
    identity = review["identities"].pop("elephant-db")
    identity["backend"] = backend
    review["identities"] = {backend: identity}
    review["bootstrap_plans"] = {backend: retain(original(plan), "bootstrap-plan", raws)}
    anchor = review["controller_policy"]["expected_anchor"]
    anchor.update(allocation_sha256=None, caps={"physical_dispatches": 432, "maximum_nano_usd": 75_000_000_000})
    review["controller_policy"]["maximum_backend_seconds"] = 2700
    reviewed["raw_bytes"] = original(review)
    reviewed["raw_sha256"] = hashlib.sha256(reviewed["raw_bytes"]).hexdigest()
    return reviewed, raws, plan


class PrimaryPolicyValidatorControls(unittest.TestCase):
    def rejected(self, plan):
        with self.assertRaises((ValueError, TypeError, KeyError)):
            validator.static_plan(plan)

    def test_original_primary_corpus_pair_passes_actual_static_review_both_backends(self):
        for backend in ("surreal-db", "elephant-db"):
            reviewed, raws, plan = primary_review(backend)
            self.assertEqual(validator.validate_static(reviewed, raws, backend), plan)
            self.assertEqual({(s["name"], s["security"]["policy_id"], s["security"]["level"])
                              for s in plan["spaces"]},
                             {("semantics-household", "synthetic-household", 3),
                              ("semantics-business", "synthetic-business", 7)})

    def test_primary_order_does_not_change_named_policy_binding(self):
        plan = original_primary_plan()
        plan["spaces"].reverse()
        validator.static_plan(plan)

    def test_original_generic_and_actual_d28_names_remain_accepted(self):
        for names in (("household", "business"), ("qualification-household", "qualification-business")):
            _, _, plan = static_fixture()
            for row, name in zip(plan["spaces"], names, strict=True):
                row["name"] = name
            validator.static_plan(plan)

    def test_complete_policy_sets_cannot_be_mixed_between_families(self):
        for index in (0, 1):
            plan = original_primary_plan()
            _, _, old = static_fixture()
            plan["spaces"][index] = copy.deepcopy(old["spaces"][index])
            self.rejected(plan)

    def test_primary_names_cannot_carry_old_workload_policies(self):
        for index in (0, 1):
            plan = original_primary_plan()
            _, _, old = static_fixture()
            plan["spaces"][index]["security"] = old["spaces"][index]["security"]
            self.rejected(plan)

    def test_primary_policies_cannot_be_moved_to_other_names(self):
        for index in (0, 1):
            for name in ("qualification-household", "qualification-business", "new-space"):
                plan = original_primary_plan()
                plan["spaces"][index]["name"] = name
                self.rejected(plan)

    def test_primary_named_policy_and_level_swaps_refuse(self):
        for field in ("name", "security"):
            plan = original_primary_plan()
            left, right = plan["spaces"]
            left[field], right[field] = right[field], left[field]
            self.rejected(plan)
        plan = original_primary_plan()
        plan["spaces"][0]["security"]["level"] = 7
        self.rejected(plan)

    def test_duplicate_missing_or_extra_primary_space_refuses(self):
        for change in (lambda rows: rows.pop(), lambda rows: rows.append(copy.deepcopy(rows[0])),
                       lambda rows: rows.__setitem__(1, copy.deepcopy(rows[0]))):
            plan = original_primary_plan()
            change(plan["spaces"])
            self.rejected(plan)

    def test_unknown_policy_or_boolean_level_does_not_expand_closed_family(self):
        for value in ("synthetic-household-other", "household-policy"):
            plan = original_primary_plan()
            plan["spaces"][0]["security"]["policy_id"] = value
            self.rejected(plan)
        plan = original_primary_plan()
        plan["spaces"][0]["security"]["level"] = True
        self.rejected(plan)

    def test_primary_keeps_labels_handling_and_generation_bounds(self):
        for key in ("labels", "handling"):
            plan = original_primary_plan()
            plan["spaces"][0]["security"][key] = ["unreviewed"]
            self.rejected(plan)
        for key, value in (("generation", 2), ("index_generation", 1), ("pending_reindex", {})):
            plan = original_primary_plan()
            plan["spaces"][0]["processing"][key] = value
            self.rejected(plan)

    def test_scoped_wiki_exact_primary_marker_and_explicit_grants_both_backends(self):
        for backend in ("elephant-db", "surreal-db"):
            reviewed, raws, plan = primary_review(backend)
            plan["scoped_wiki"] = True
            plan["service_scopes"] += ["write:entity", "manage:wiki"]
            reviewed["review"]["bootstrap_plans"][backend] = retain(original(plan), "bootstrap-plan", raws)
            reviewed["raw_bytes"] = original(reviewed["review"])
            reviewed["raw_sha256"] = hashlib.sha256(reviewed["raw_bytes"]).hexdigest()
            self.assertEqual(validator.validate_static(reviewed, raws, backend), plan)
            self.assertNotIn("write:entity", validator.CORE_SCOPES)
            self.assertNotIn("write:entity", validator.CONDITIONAL_SCOPES)

    def test_scoped_wiki_marker_is_true_only_and_primary_only(self):
        for marker in (False, 0, 1, "true", None, {}):
            plan = original_primary_plan()
            plan.update(scoped_wiki=marker)
            plan["service_scopes"] += ["write:entity", "manage:wiki"]
            self.rejected(plan)
        for names in (("household", "business"), ("qualification-household", "qualification-business")):
            _, _, plan = static_fixture()
            for row, name in zip(plan["spaces"], names, strict=True): row["name"] = name
            plan.update(scoped_wiki=True)
            plan["service_scopes"] += ["write:entity", "manage:wiki"]
            self.rejected(plan)

    def test_scoped_entity_write_requires_marker_and_both_exact_grants(self):
        for absent in ("marker", "write:entity", "manage:wiki"):
            plan = original_primary_plan()
            plan.update(scoped_wiki=True)
            plan["service_scopes"] += ["write:entity", "manage:wiki"]
            if absent == "marker": plan.pop("scoped_wiki")
            else: plan["service_scopes"].remove(absent)
            self.rejected(plan)

    def test_scoped_wiki_never_grants_extra_authority_or_skips_original_scopes(self):
        for bad in ("*", "write:*", "manage:provider", "write:relationship", "write:entity"):
            plan = original_primary_plan()
            plan.update(scoped_wiki=True)
            plan["service_scopes"] += ["write:entity", "manage:wiki", bad]
            self.rejected(plan)
        plan = original_primary_plan()
        plan.update(scoped_wiki=True)
        plan["service_scopes"] += ["write:entity", "manage:wiki"]
        plan["service_scopes"].remove("read:source")
        self.rejected(plan)

    def test_scoped_primary_names_cannot_grant_entity_write_in_workload_allocations(self):
        from semantic140_evidence_test import reviewed_fixture

        for fixture in (static_fixture, reviewed_fixture):
            reviewed, raws, _ = fixture()
            plan = original_primary_plan()

            def bind_plan():
                reviewed["review"]["bootstrap_plans"]["elephant-db"] = retain(original(plan), "bootstrap-plan", raws)
                reviewed["raw_bytes"] = original(reviewed["review"])
                reviewed["raw_sha256"] = hashlib.sha256(reviewed["raw_bytes"]).hexdigest()

            bind_plan()
            # The exact workload allocation is otherwise valid with these names.
            self.assertEqual(validator.validate_static(reviewed, raws, "elephant-db"), plan)
            self.assertIsNotNone(reviewed["review"]["controller_policy"]["expected_anchor"]["allocation_sha256"])
            plan["scoped_wiki"] = True
            plan["service_scopes"] += ["write:entity", "manage:wiki"]
            bind_plan()
            with self.assertRaisesRegex(ValueError, "dynamic_static_scoped_wiki_primary_allocation"):
                validator.validate_static(reviewed, raws, "elephant-db")

    def test_primary_keeps_exact_original_allocation_and_backend_clock(self):
        for change in (lambda review: review["controller_policy"].update(maximum_backend_seconds=2701),
                       lambda review: review["controller_policy"]["expected_anchor"]["caps"].update(physical_dispatches=433),
                       lambda review: review["controller_policy"]["expected_anchor"]["caps"].update(maximum_nano_usd=75_000_000_001)):
            reviewed, raws, _ = primary_review("elephant-db")
            change(reviewed["review"])
            reviewed["raw_bytes"] = original(reviewed["review"])
            reviewed["raw_sha256"] = hashlib.sha256(reviewed["raw_bytes"]).hexdigest()
            with self.assertRaises(ValueError):
                validator.validate_static(reviewed, raws, "elephant-db")


if __name__ == "__main__":
    unittest.main()
