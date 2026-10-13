"""Original primary and workload policy families; no processes or provider calls."""
import copy
import hashlib
import json
from pathlib import Path
import unittest
from unittest import mock

import runtime_package_bootstrap_contract as contract
import runtime_package_bootstrap_test as fixtures
import runtime_package_bootstrap as driver


def primary_plan():
    raw = (Path(__file__).resolve().parents[2] / "tests/recall_preview_semantics/corpus.json").read_bytes()
    if hashlib.sha256(raw).hexdigest() != "a866e3569dbec863c9214c5f4cba477333dd6629ace8a4816318e40afea90da7":
        raise ValueError("original primary corpus bytes changed")
    corpus = json.loads(raw)
    value = fixtures.plan()
    value["spaces"] = []
    for original in corpus["spaces"]:
        request = fixtures.space(original["id"], original["policy"], original["level"])
        request["purpose"] = original["guidance"]
        value["spaces"].append(request)
    return value


def phase_sources(*, marked, scoped_case):
    value = primary_plan()
    if marked:
        value["scoped_wiki"] = True
        value["service_scopes"] += ["manage:wiki", "write:entity"]
    conditional = {"format": "recall-primary-conditional-cases/v1",
                   "reviewer": "qualification (independent D lane)", "status": "approved-before-execution",
                   "cases": [{"category": "wiki", "tool": "compile_wiki_article"}] if scoped_case else []}
    values = {"/inputs/plan.json": contract.encode(value),
              "/inputs/conditional_cases.json": contract.encode(conditional)}
    names = ("corpus", "expectations", "proposal", "configurations", "rendered_map", "runtime_review", "runtime_original_inputs")
    for name in names:
        values["/inputs/" + name + ".json"] = contract.encode({"offline_control": name})
    refs = {name: {"path": "/inputs/" + name + ".json", "sha256": contract.sha(values["/inputs/" + name + ".json"])}
            for name in (*names, "conditional_cases")}
    refs["bootstrap_plan"] = {"path": "/inputs/plan.json", "sha256": contract.sha(values["/inputs/plan.json"])}
    template = {"format": "recall-primary-provider-run/v2", "execution_authorized": True,
                "provider_dispatch_authorized": True, "authentication_enabled": True,
                **{key: value[key] for key in contract.IDENTITIES if key not in ("configuration_sha256", "run_nonce")},
                "nonce": value["run_nonce"], "expires_at": value["expires_at"],
                "fixture_timestamp": "2026-10-07T01:00:00Z", "inputs": refs}
    values["/inputs/template.json"] = contract.encode(template)
    values["/inputs/review.json"] = contract.encode({
        "format": "recall-primary-driver-review/v2", "approved": True,
        "reviewer": "qualification (independent D lane)",
        "primary_template_sha256": contract.sha(values["/inputs/template.json"]),
        "inputs": {name: ref["sha256"] for name, ref in refs.items()}, "dependencies": value["dependencies"],
        "nonce": value["run_nonce"], "backend": value["backend"], "endpoint": value["endpoint"]})
    return values


class OriginalPolicyFamilyControls(unittest.TestCase):
    def setUp(self):
        self.addCleanup(mock.patch.stopall)
        mock.patch("subprocess.Popen", side_effect=AssertionError("process forbidden")).start()
        mock.patch("socket.create_connection", side_effect=AssertionError("TCP forbidden")).start()

    def test_actual_primary_corpus_envelopes_pass_both_backends_in_either_order(self):
        original = primary_plan()
        self.assertEqual({(s["name"], s["security"]["policy_id"], s["security"]["level"])
                          for s in original["spaces"]},
                         {("semantics-household", "synthetic-household", 3),
                          ("semantics-business", "synthetic-business", 7)})
        for backend in ("elephant-db", "surreal-db"):
            for reverse in (False, True):
                value = copy.deepcopy(original)
                value["backend"] = backend
                value["run_directory"] = "/work/bootstrap-" + value["run_nonce"] + "-" + backend
                if reverse:
                    value["spaces"].reverse()
                before = copy.deepcopy(value)
                with self.subTest(backend=backend, reverse=reverse):
                    self.assertIs(contract.validate_plan(value), value)
                    self.assertEqual(value, before)

    def test_original_workload_names_policies_and_generation_remain_unchanged(self):
        for prefix in ("", "qualification-"):
            for backend in ("elephant-db", "surreal-db"):
                value = fixtures.plan()
                value["backend"] = backend
                value["run_directory"] = "/work/bootstrap-" + value["run_nonce"] + "-" + backend
                for row in value["spaces"]:
                    row["name"] = prefix + row["name"]
                before = copy.deepcopy(value)
                self.assertIs(contract.validate_plan(value), value)
                self.assertEqual(value, before)
                for row in value["spaces"]:
                    for key, changed in (("generation", 2), ("index_generation", 1), ("pending_reindex", {})):
                        wrong = copy.deepcopy(row)
                        wrong["processing"][key] = changed
                        with self.subTest(prefix=prefix, backend=backend, field=key), self.assertRaises(ValueError):
                            contract.create_request(wrong)

    def test_cross_family_and_duplicate_spaces_never_form_a_valid_plan(self):
        primary = primary_plan()["spaces"]
        workload = fixtures.plan()["spaces"]
        for first in primary:
            for second in workload:
                value = fixtures.plan()
                value["spaces"] = [copy.deepcopy(first), copy.deepcopy(second)]
                # Each is independently valid; combining them is the refusal.
                for row in value["spaces"]:
                    contract.create_request(row)
                with self.subTest(first=first["name"], second=second["name"]), self.assertRaises(ValueError):
                    contract.validate_plan(value)
        for row in primary + workload:
            value = fixtures.plan()
            value["spaces"] = [copy.deepcopy(row), copy.deepcopy(row)]
            with self.subTest(duplicate=row["name"]), self.assertRaises(ValueError):
                contract.validate_plan(value)

    def test_primary_names_cannot_be_reassigned_to_other_original_policy_pairs(self):
        primary = primary_plan()["spaces"]
        wrongs = []
        for row in primary:
            for name in ("foreign-" + row["name"], "qualification-household", "qualification-business"):
                wrong = copy.deepcopy(row)
                wrong["name"] = name
                wrongs.append(wrong)
            for policy, level in (("household-policy", 3), ("business-policy", 7),
                                  ("synthetic-business", 7) if row["name"] == "semantics-household"
                                  else ("synthetic-household", 3)):
                wrong = copy.deepcopy(row)
                wrong["security"].update(policy_id=policy, level=level)
                wrongs.append(wrong)
        for value in wrongs:
            with self.subTest(name=value["name"], security=value["security"]), self.assertRaises(ValueError):
                contract.create_request(value)

    def test_primary_addition_keeps_exact_level_labels_and_handling(self):
        for row in primary_plan()["spaces"]:
            for key, changed in (("level", True), ("level", float(row["security"]["level"])),
                                 ("level", 0), ("policy_id", []), ("labels", ["widened"]),
                                 ("handling", ["widened"])):
                wrong = copy.deepcopy(row)
                wrong["security"][key] = changed
                with self.subTest(name=row["name"], field=key, value=changed), self.assertRaises(ValueError):
                    contract.create_request(wrong)

    def test_scoped_primary_explicitly_grants_only_the_two_required_wiki_scopes(self):
        self.assertNotIn("write:entity", contract.CORE_SCOPES | contract.CONDITIONAL_SCOPES)
        for backend in ("elephant-db", "surreal-db"):
            value = primary_plan()
            value.update(backend=backend, scoped_wiki=True)
            value["run_directory"] = "/work/bootstrap-" + value["run_nonce"] + "-" + backend
            value["service_scopes"] += ["manage:wiki", "write:entity"]
            before = copy.deepcopy(value)
            self.assertIs(contract.validate_plan(value), value)
            self.assertEqual(value, before)

    def test_entity_write_never_enters_an_unmarked_or_workload_plan(self):
        for value in (primary_plan(), fixtures.plan()):
            value["service_scopes"] += ["manage:wiki", "write:entity"]
            with self.subTest(marked=False, names=[s["name"] for s in value["spaces"]]), self.assertRaises(ValueError):
                contract.validate_plan(value)
        value = fixtures.plan()
        value["scoped_wiki"] = True
        value["service_scopes"] += ["manage:wiki", "write:entity"]
        with self.assertRaises(ValueError):
            contract.validate_plan(value)

    def test_scoped_primary_marker_and_both_scopes_are_required_without_wildcards(self):
        original = primary_plan()
        original["scoped_wiki"] = True
        original["service_scopes"] += ["manage:wiki", "write:entity"]
        for marker in (False, 0, 1, "true", None):
            value = copy.deepcopy(original)
            value["scoped_wiki"] = marker
            with self.subTest(marker=marker), self.assertRaises(ValueError):
                contract.validate_plan(value)
        for removed in ("manage:wiki", "write:entity"):
            value = copy.deepcopy(original)
            value["service_scopes"].remove(removed)
            with self.subTest(missing=removed), self.assertRaises(ValueError):
                contract.validate_plan(value)
        for extra in ("*", "write:assertion", "manage:provider"):
            value = copy.deepcopy(original)
            value["service_scopes"].append(extra)
            with self.subTest(extra=extra), self.assertRaises(ValueError):
                contract.validate_plan(value)

    def test_actual_phase_binds_scoped_grant_to_the_same_reviewed_case_bytes(self):
        for scoped in (False, True):
            values = phase_sources(marked=scoped, scoped_case=scoped)
            with mock.patch.object(driver, "static_input", side_effect=lambda path: values[str(path)]) as read, \
                 mock.patch.object(driver, "load_original_inputs", return_value={"offline": True}) as original_inputs:
                phase = driver.phase_inputs(Path("/inputs/plan.json"), Path("/inputs/review.json"), Path("/inputs/template.json"))
            self.assertEqual(phase["plan"].get("scoped_wiki") is True, scoped)
            self.assertEqual(sum(str(call.args[0]) == "/inputs/conditional_cases.json" for call in read.call_args_list), 1)
            original_inputs.assert_called_once()

    def test_actual_phase_refuses_scope_case_mismatch_before_runtime_inputs_or_startup(self):
        for marked, scoped in ((True, False), (False, True)):
            values = phase_sources(marked=marked, scoped_case=scoped)
            with mock.patch.object(driver, "static_input", side_effect=lambda path: values[str(path)]), \
                 mock.patch.object(driver, "load_original_inputs") as originals, \
                 mock.patch.object(driver, "Bootstrap") as startup, \
                 self.assertRaisesRegex(ValueError, "plan and reviewed cases differ"):
                driver.phase_inputs(Path("/inputs/plan.json"), Path("/inputs/review.json"), Path("/inputs/template.json"))
            originals.assert_not_called()
            startup.assert_not_called()

    def test_conditional_scope_cannot_come_from_replaced_or_unreviewed_input(self):
        values = phase_sources(marked=True, scoped_case=True)
        values["/inputs/conditional_cases.json"] = contract.encode({"cases": []})
        with mock.patch.object(driver, "static_input", side_effect=lambda path: values[str(path)]), \
             mock.patch.object(driver, "load_original_inputs") as originals, \
             self.assertRaisesRegex(ValueError, "immutable reviewed input differs"):
            driver.phase_inputs(Path("/inputs/plan.json"), Path("/inputs/review.json"), Path("/inputs/template.json"))
        originals.assert_not_called()


if __name__ == "__main__":
    unittest.main()
