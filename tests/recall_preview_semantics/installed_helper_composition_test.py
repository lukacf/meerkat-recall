"""Offline composition controls: actual source readers and full installed predicate.

The original literal transcript is a controlled wire fixture, not an installed
binary observation or product acceptance. No predicate, source reader, service,
provider or process lifecycle is mocked. Nothing spawns a child or listener.
"""
import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


o = load("installed_composition_orchestration", HERE / "workload_orchestration.py")
t = load("installed_composition_transcripts", HERE / "installed_evidence_validator_test.py")
# Independently spell out the original producer's four source roles. A shared
# production constant cannot make an incorrect mapping pass this control.
PATHS = {
    "driver": "scripts/qualification/installed_linux_offline.py",
    "process_capture": "scripts/qualification/installed_process_capture.py",
    "collector": "scripts/qualification/recall_preview.py",
    "installer": "scripts/release/artifact.py",
}


def source_pins(profile):
    paths = {o.SELF, o.VALIDATOR, o.STARTUP, *o.PREDICATES.values(), *PATHS.values()}
    if profile == o.profiles.SEMANTIC140_ID:
        paths |= {o.PROFILE_SOURCE, o.EXPECTATIONS_SOURCE}
    return {path: o.digest((ROOT / path).read_bytes()) for path in paths}


def fixture(backend, plan_override=None):
    transcript = t.Transcript(backend)
    pins = {role + "_sha256": o.digest((ROOT / path).read_bytes()) for role, path in PATHS.items()}
    if plan_override:
        pins.update(plan_override)
    transcript.pins.update(pins)
    transcript.plan.update(pins)
    transcript.offline_review.update(pins)
    transcript.run["expectation_review_sha256"] = t.sha(t.wire(transcript.offline_review))
    return transcript.populate()


def bind_review(transcript, profile=o.profiles.LEGACY_ID):
    reviewed, carrier, raw = transcript.seal()
    review = reviewed["review"]
    review["helper_sha256"] = source_pins(profile)
    if profile == o.profiles.SEMANTIC140_ID:
        review["profile_id"] = profile
    original = t.wire(review)
    return {"review": review, "raw_bytes": original, "raw_sha256": t.sha(original)}, carrier, raw


def composed(reviewed, carrier, raw):
    o.verify_helpers(reviewed["review"])
    with o.reviewed_predicates(reviewed["review"]):
        predicate = sys.modules["installed_evidence_validator"]
        return predicate.validate_installed(reviewed, carrier, o.EvidenceUnion(raw))


class InstalledHelperCompositionControls(unittest.TestCase):
    def test_canonical_source_map_is_the_original_four_producer_roles(self):
        self.assertEqual(t.ORACLE.INSTALLED_HELPER_PATHS, PATHS)
        self.assertEqual(len(set(PATHS.values())), 4)

    def test_both_profiles_and_backends_use_real_source_readers_and_full_installed_predicate(self):
        for profile in (o.profiles.LEGACY_ID, o.profiles.SEMANTIC140_ID):
            for backend in ("elephant-db", "surreal-db"):
                with self.subTest(profile=profile, backend=backend):
                    original = fixture(backend)
                    result = composed(*bind_review(original, profile))
                    self.assertEqual(result, {"identity": original.identity,
                                              "required_cases": t.CASE["required_cases"]})
                    self.assertNotIn("verdict", result)
                    self.assertNotIn("product_acceptance", result)

    def test_each_role_pin_must_match_even_when_original_plan_and_review_agree(self):
        for role in PATHS:
            with self.subTest(role=role):
                original = fixture("elephant-db", {role + "_sha256": "0" * 64})
                with self.assertRaisesRegex(ValueError, "dynamic_installed_original_helper_pin"):
                    composed(*bind_review(original))

    def test_missing_canonical_pin_cannot_be_replaced_by_legacy_alias(self):
        for role, path in PATHS.items():
            with self.subTest(role=role):
                reviewed, carrier, raw = bind_review(fixture("surreal-db"))
                review = reviewed["review"]
                review["helper_sha256"]["installed_" + role] = review["helper_sha256"].pop(path)
                original = t.wire(review)
                reviewed = {"review": review, "raw_bytes": original, "raw_sha256": t.sha(original)}
                # The original predicate itself refuses a logical alias; there
                # is no compatibility fallback that skips the canonical pin.
                with self.assertRaisesRegex(ValueError, "dynamic_installed_invalid_evidence"):
                    t.ORACLE.validate_installed(reviewed, carrier, o.EvidenceUnion(raw))

    def test_changed_wire_denial_still_fails_after_valid_source_and_pin_handoff(self):
        original = fixture("elephant-db")
        denied = next(row for row in original.http if row["status"] == 403 and row["raw_body_complete"])
        denied["status"] = 200
        with self.assertRaisesRegex(ValueError, "^dynamic_installed_"):
            composed(*bind_review(original))

    def test_changed_canonical_source_bytes_fail_before_predicate_handoff(self):
        review = bind_review(fixture("elephant-db"))[0]["review"]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for path in review["helper_sha256"]:
                target = root / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes((ROOT / path).read_bytes())
            o.verify_helpers(review, root)
            for role, path in PATHS.items():
                with self.subTest(role=role):
                    target = root / path
                    original = target.read_bytes()
                    target.write_bytes(original + b"\n# changed after review\n")
                    with self.assertRaisesRegex(ValueError, "orchestration_helper_changed"):
                        o.verify_helpers(review, root)
                    target.write_bytes(original)


if __name__ == "__main__":
    unittest.main()
