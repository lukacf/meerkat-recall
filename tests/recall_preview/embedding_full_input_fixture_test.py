"""Independent literal arithmetic controls; never import the product provider."""
import copy
import hashlib
import json
import math
from pathlib import Path
import struct
import unittest
from unittest.mock import patch


FIXTURE = Path(__file__).with_name("embedding-full-input-fixtures.json")


def sha(value):
    return hashlib.sha256(value).hexdigest()


def original(parts):
    return "".join(part["text"] * part["repeat"] for part in parts).encode()


def verify_literal_cover(raw, segments, maximum):
    cursor = 0
    for row in segments:
        start, end = row["start"], row["end"]
        if start != cursor or not start < end <= len(raw) or end - start > maximum:
            raise ValueError("original segment cover differs")
        piece = raw[start:end]
        piece.decode("utf-8")
        if len(piece) != row["utf8_bytes"] or sha(piece) != row["sha256"]:
            raise ValueError("original segment bytes differ")
        cursor = end
    if cursor != len(raw):
        raise ValueError("original suffix missing")


class IndependentEmbeddingFixtures(unittest.TestCase):
    def setUp(self):
        for name in ("subprocess.Popen", "socket.socket", "asyncio.open_connection", "asyncio.start_server"):
            guard = patch(name, side_effect=AssertionError("fixture arithmetic cannot start runtime"))
            guard.start()
            self.addCleanup(guard.stop)
        self.data = json.loads(FIXTURE.read_bytes())

    def test_explicit_version_and_unexecuted_scope(self):
        self.assertEqual(self.data["preprocessing"], "text-full-v2")
        self.assertEqual(self.data["old_preprocessing"], "text-v1")
        self.assertEqual(self.data["algorithm"]["initial_max_utf8_bytes"], 2048)
        for flag in ("execution_authorized", "provider_dispatch_authorized", "product_acceptance"):
            self.assertIs(self.data[flag], False)
        self.assertEqual(self.data["review_status"], "pending_qualification_owner")

    def test_seven_literal_covers_retain_every_original_byte(self):
        self.assertEqual(len(self.data["segmentation_cases"]), 7)
        for case in self.data["segmentation_cases"]:
            with self.subTest(case=case["id"]):
                raw = original(case["input_parts"])
                self.assertEqual(len(raw), case["input_utf8_bytes"])
                self.assertEqual(sha(raw), case["input_sha256"])
                verify_literal_cover(raw, case["expected_segments"], 2048)
                # A nonfinal interval is maximal: adding its next WHOLE scalar
                # would violate the fixed byte bound. No candidate splitter runs.
                for row in case["expected_segments"][:-1]:
                    next_character = raw[row["end"]:].decode()[0].encode()
                    self.assertGreater(row["utf8_bytes"] + len(next_character), 2048)

    def test_original_two_million_byte_suffix_and_977_segments(self):
        case = self.data["original_full_workload_capacity"]
        raw = original(case["input_parts"])
        self.assertEqual(len(raw), 2_000_000)
        self.assertEqual(sha(raw), "fdae2b405d6f3123836e3dc6b6aef46de8cf41342349265ff06a403d1f1af613")
        self.assertEqual(case["input_sha256"], sha(raw))
        self.assertEqual(case["expected_regular_segments"], {"first_start": 0, "bytes_each": 2048, "count": 976})
        tail = case["expected_final_segment"]
        self.assertEqual((tail["start"], tail["end"], tail["utf8_bytes"]), (1998848, 2000000, 1152))
        self.assertEqual(sha(raw[1998848:]), tail["sha256"])
        self.assertEqual(case["expected_segment_count"], 977)
        self.assertEqual(976 * 2048 + 1152, len(raw))
        self.assertTrue(raw.endswith(case["required_final_original_suffix"].encode()))
        self.assertEqual(case["sdk_regular_segment_vector"], [1.0, 0.0])
        self.assertEqual(case["sdk_final_segment_vector"], [0.0, 1.0])
        self.assertEqual(case["sdk_expected_weighted_sum"], [1998848.0, 1152.0])
        self.assertEqual(1998848 ** 2 + 1152 ** 2, 3995394654208)
        for numerator, expected in zip(case["sdk_expected_weighted_sum"],
                                       case["sdk_expected_unit_vector"], strict=True):
            self.assertAlmostEqual(numerator / math.sqrt(3995394654208), expected, places=14)

    def test_fixed_weighted_sums_and_closed_form_normalized_answers(self):
        for case in self.data["aggregation_cases"]:
            with self.subTest(case=case["id"]):
                raw = original(case["input_parts"])
                self.assertEqual(sha(raw), case["input_sha256"])
                weights, vectors = case["segment_utf8_bytes"], case["vectors"]
                self.assertEqual(sum(weights), len(raw))
                expected_sum = case["weighted_sum"]
                self.assertEqual([sum(w * v[column] for w, v in zip(weights, vectors, strict=True))
                                  for column in range(2)], expected_sum)
                norm = math.sqrt(sum(value * value for value in expected_sum))
                for numerator, expected in zip(expected_sum, case["expected_unit_vector"], strict=True):
                    self.assertAlmostEqual(numerator / norm, expected, places=14)
                    converted = struct.unpack("!f", struct.pack("!f", numerator / norm))[0]
                    self.assertLessEqual(abs(converted - expected), 1e-7)

    def test_suffix_changes_result_and_input_order_never_pools(self):
        by_id = {case["id"]: case for case in self.data["aggregation_cases"]}
        pair = self.data["suffix_pair"]
        first, changed = (by_id[pair[key]] for key in ("original_case", "changed_case"))
        self.assertEqual(original(first["input_parts"])[:2048], original(changed["input_parts"])[:2048])
        self.assertNotEqual(original(first["input_parts"])[2048:], original(changed["input_parts"])[2048:])
        self.assertEqual(first["vectors"][0], changed["vectors"][0])
        self.assertNotEqual(first["expected_unit_vector"], changed["expected_unit_vector"])
        multi = self.data["multi_input_case"]
        self.assertEqual(multi["expected_ordered_vectors"], [by_id[key]["expected_unit_vector"] for key in multi["ids"]])
        self.assertFalse(multi["allow_shared_aggregate"])

    def test_identical_original_segments_have_one_declared_provider_vector(self):
        declared = {}
        for case in self.data["aggregation_cases"]:
            raw = original(case["input_parts"])
            start = 0
            for length, vector in zip(case["segment_utf8_bytes"], case["vectors"], strict=True):
                segment = raw[start:start + length]
                segment.decode("utf-8")
                self.assertEqual(declared.setdefault(segment, vector), vector)
                start += length
            self.assertEqual(start, len(raw))

    def test_both_retry_halves_preserve_suffix_and_weight(self):
        case = self.data["retry_case"]
        raw = original(case["input_parts"])
        self.assertEqual(case["expected_retry_segments"], [[2047, 2051], [2051, 2052]])
        pieces = [raw[start:end] for start, end in case["successful_final_segments"]]
        self.assertEqual(pieces, [b"a" * 2047, "🦊".encode(), b"Z"])
        self.assertEqual(b"".join(pieces), raw)
        self.assertEqual([len(piece) for piece in pieces], case["segment_utf8_bytes"])
        self.assertFalse(case["rejected_response_contributes_to_mean"])
        self.assertFalse(case["full_result_before_all_segments_succeed"])
        self.assertEqual(case["expected_unit_vector"], self.data["aggregation_cases"][-1]["expected_unit_vector"])

    def test_refusal_expectations_do_not_claim_runtime_outcomes(self):
        cases = {case["id"]: case for case in self.data["refusal_cases"]}
        self.assertEqual(len(cases), 10)
        self.assertNotEqual(cases["duplicate_or_missing_provider_index"]["physical_response_indices"],
                            cases["duplicate_or_missing_provider_index"]["expected_indices"])
        for key in ("later_segment_transport_failure", "binding_revoked_before_later_dispatch"):
            self.assertEqual(cases[key]["expected_returned_aggregate_count"], 0)
        self.assertEqual(cases["binding_revoked_before_later_dispatch"]["additional_sends_after_revocation"], 0)
        legacy = cases["text_v1_oversize"]
        self.assertEqual(sha(original(legacy["input_parts"])), legacy["expected_first_physical_input_sha256"])
        self.assertEqual(legacy["expected_prefix_retry_count"], 0)

    def test_mutated_cover_cannot_hide_missing_or_duplicated_suffix(self):
        case = self.data["segmentation_cases"][1]
        raw = original(case["input_parts"])
        for kind in ("omit", "overlap", "wrong_hash"):
            rows = copy.deepcopy(case["expected_segments"])
            if kind == "omit":
                rows.pop()
            elif kind == "overlap":
                rows[-1]["start"] -= 1
            else:
                rows[-1]["sha256"] = "0" * 64
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                verify_literal_cover(raw, rows, 2048)


if __name__ == "__main__":
    unittest.main()
