"""Closed-world spy controls; no listeners, installed service, or paid models."""
import asyncio
import copy
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from full_workload_spy import (ControlledSpy, SEMANTIC_FIELDS, provider_response,
                               support_response, user_texts, validate_recipe)
from full_workload_test_transport import Writer, reader
from full_workload_transport import Capture, Unmet, decode, encode, sha


LIMITS = {"owned_spy_dispatches_per_backend": 4096, "owned_spy_request_bytes": 8388608,
          "owned_spy_response_bytes": 8388608, "owned_spy_concurrency": 160,
          "held_response_seconds": 120}
QUOTE = "Mira maintains the Helios shared notebook."
CREDENTIAL = "synthetic-controlled-provider-key"


def rule(path="/v1/responses", mode="structured"):
    output = {"dimensions": 1536} if mode == "embedding" else {"assertions": []}
    return {"id": "original-extraction", "space": "qualification-household", "model": "controlled-model",
            "path": path, "credential_sha256": sha(CREDENTIAL.encode()),
            "required_text": [] if mode == "embedding" else [QUOTE], "forbidden_text": [],
            "maximum_calls": 1, "hold_calls": [], "statuses": [200], "mode": mode, "output": output}


def recipe(value):
    return {"format": "recall-controlled-graph-spy/v1", "rules": [value]}


def body(path="/v1/responses"):
    if path == "/v1/embeddings":
        return {"model": "controlled-model", "input": ["first source", "second source"], "dimensions": 1536}
    values = [{"role": "system", "content": "Do not use system examples as source selectors"},
              {"role": "user", "content": QUOTE}]
    return {"model": "controlled-model", "stream": True,
            "messages" if path == "/v1/messages" else "input": values}


def frame(value, path="/v1/responses", space="qualification-household", credential=CREDENTIAL, extra=b""):
    raw = encode(value)
    authorization = f"x-api-key: {credential}" if path == "/v1/messages" else f"Authorization: Bearer {credential}"
    return (f"POST {path} HTTP/1.1\r\nHost: 127.0.0.1:43129\r\n"
            f"Content-Type: application/json\r\nContent-Length: {len(raw)}\r\n"
            f"X-Space-Id: {space}\r\n{authorization}\r\n").encode() + extra + b"\r\n" + raw


def headerless_frame(value, path, credential):
    raw = encode(value)
    authorization = f"x-api-key: {credential}" if path == "/v1/messages" else f"Authorization: Bearer {credential}"
    return (f"POST {path} HTTP/1.1\r\nHost: 127.0.0.1:43129\r\n"
            f"Content-Type: application/json\r\nContent-Length: {len(raw)}\r\n"
            f"{authorization}\r\n\r\n").encode() + raw


def expected_claim():
    semantic = {key: None for key in SEMANTIC_FIELDS}
    semantic.update(subject="Mira", predicate="maintains", object="Helios shared notebook",
                    subject_type="person", object_type="document", confidence=1.0, significance="medium")
    expected = {"quote": QUOTE, "semantic": semantic}
    candidate = {**semantic, "revision": "doc_revision:11111111-1111-4111-8111-111111111111",
                 "node": "doc_node:22222222-2222-4222-8222-222222222222",
                 "start": 0, "end": len(QUOTE.encode()), "quote_hash": sha(QUOTE.encode())}
    return expected, {"index": 7, "claim": candidate, "quote": QUOTE}


class SpyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        for name in ("asyncio.start_server", "asyncio.open_connection", "subprocess.Popen"):
            guard = patch(name, side_effect=AssertionError("offline controls cannot launch or connect"))
            guard.start()
            self.addCleanup(guard.stop)
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.capture = Capture(Path(self.temporary.name) / "capture", 64 * 1024 * 1024)

    def spy(self, value=None, **kwargs):
        specification = recipe(value or rule())
        return ControlledSpy(specification, sha(encode(specification)), self.capture,
                             kwargs.pop("limits", LIMITS), kwargs.pop("deadline", time.monotonic() + 30), **kwargs)

    async def test_held_original_request_has_durable_intent_before_release(self):
        value = rule()
        value["hold_calls"] = [1]
        spy, writer = self.spy(value), Writer()
        task = asyncio.create_task(spy.handle(reader(frame(body())), writer))
        identifier = await asyncio.wait_for(spy.wait_held(value["id"]), 1)
        self.assertEqual(identifier, 100001)
        self.assertEqual(writer.data, [])
        intent = decode((self.capture.directory / "100001.intent.json").read_bytes())
        self.assertEqual(intent["request_sha256"], sha(encode(body())))
        self.assertEqual(intent["rule_ordinal"], 1)
        self.assertEqual(spy.snapshot_calls()["held_ids"], [identifier])
        spy.release(identifier)
        with self.assertRaises(Unmet):
            spy.release(identifier)
        await task
        self.assertEqual(len(writer.data), 1)
        self.assertTrue(writer.closed)
        self.assertTrue(spy.calls[0]["response_complete"])
        self.assertEqual((self.capture.directory / "100001.request.json").read_bytes(), encode(body()))
        self.assertEqual(self.capture.held, 0)
        self.assertFalse(spy.snapshot_calls()["model_quality_qualified"])

    async def test_scheduled_503_is_one_physical_call_without_hidden_retry(self):
        value = rule()
        value.update(maximum_calls=2, statuses=[503, 200])
        spy = self.spy(value)
        first, second = Writer(), Writer()
        await spy.handle(reader(frame(body())), first)
        self.assertEqual(spy.count, 1)
        self.assertTrue(first.data[0].startswith(b"HTTP/1.1 503 "))
        await spy.handle(reader(frame(body())), second)
        self.assertEqual(spy.count, 2)
        self.assertTrue(second.data[0].startswith(b"HTTP/1.1 200 "))
        self.assertEqual([row["rule_ordinal"] for row in spy.calls], [1, 2])
        self.assertIsNone(spy.failure)

    async def test_wrong_space_or_credential_refuses_without_controlled_response(self):
        for changes in ({"space": "qualification-business"}, {"credential": "different-synthetic-key"}):
            spy, writer = self.spy(), Writer()
            # Each original run needs its own exclusive capture IDs.
            spy.capture = Capture(Path(self.temporary.name) / str(len(list(Path(self.temporary.name).iterdir()))),
                                  4 * 1024 * 1024)
            await spy.handle(reader(frame(body(), **changes)), writer)
            self.assertEqual(writer.data, [])
            self.assertFalse(spy.calls[0]["response_complete"])
            self.assertEqual(spy.failure, "Unmet")
            original_count = spy.count
            await spy.handle(reader(frame(body())), Writer())
            self.assertEqual(spy.count, original_count + 1)
            self.assertFalse(spy.calls[-1]["response_complete"])
            self.assertEqual(spy.rule_counts, {})

    async def test_original_deadline_cannot_be_reset_by_hold_release(self):
        value = rule()
        value["hold_calls"] = [1]
        clock = [100.0]
        spy, writer = self.spy(value, deadline=110.0, clock=lambda: clock[0]), Writer()
        task = asyncio.create_task(spy.handle(reader(frame(body())), writer))
        identifier = await asyncio.wait_for(spy.wait_held(value["id"]), 1)
        clock[0] = 111.0
        spy.release(identifier)
        await task
        self.assertEqual(writer.data, [])
        self.assertEqual(spy.count, 1)
        self.assertEqual(spy.failure, "Unmet")
        self.assertEqual(self.capture.held, 0)

    async def test_cancelled_held_call_is_captured_and_owned_task_removed(self):
        value = rule()
        value["hold_calls"] = [1]
        spy, writer = self.spy(value), Writer()
        task = asyncio.create_task(spy.handle(reader(frame(body())), writer))
        await asyncio.wait_for(spy.wait_held(value["id"]), 1)
        task.cancel()
        await task
        self.assertEqual(spy.calls[0]["error_type"], "CancelledError")
        self.assertEqual(spy.held, {})
        self.assertEqual(spy.tasks, set())
        self.assertEqual(self.capture.held, 0)
        self.assertTrue(writer.closed)

    async def test_duplicate_framing_and_truncated_request_are_terminal(self):
        for index, wire in enumerate((frame(body(), extra=b"Content-Length: 2\r\n"), frame(body())[:-3])):
            spy, writer = self.spy(), Writer()
            spy.capture = Capture(Path(self.temporary.name) / f"invalid-{index}", 4 * 1024 * 1024)
            await spy.handle(reader(wire), writer)
            self.assertEqual(writer.data, [])
            self.assertEqual(spy.count, 1)
            self.assertFalse(spy.calls[0]["request_complete"])
            self.assertEqual(spy.failure, "Unmet")
            self.assertEqual(spy.capture.held, 0)
        self.assertEqual((spy.capture.directory / "100001.request.json").read_bytes(), encode(body())[:-3])

    async def test_exhausted_capture_has_serializable_unmet_verdict(self):
        spy, writer = self.spy(), Writer()
        spy.capture.maximum = 1
        await spy.handle(reader(frame(body())), writer)
        self.assertEqual(writer.data, [])
        self.assertEqual(spy.snapshot_calls()["error_type"], "Unmet")
        self.assertEqual(spy.count, 1)

    async def test_owned_close_releases_only_existing_held_call(self):
        value = rule()
        value["hold_calls"] = [1]
        spy, writer = self.spy(value), Writer()
        task = asyncio.create_task(spy.handle(reader(frame(body())), writer))
        await asyncio.wait_for(spy.wait_held(value["id"]), 1)
        await spy.close()
        await task
        self.assertEqual(spy.count, 1)
        self.assertEqual(spy.active, 0)
        self.assertEqual(spy.tasks, set())

    def test_independent_support_rejects_semantic_and_coordinate_changes(self):
        expected, original = expected_claim()
        self.assertEqual(support_response([encode([original]).decode()], [expected]),
                         {"decisions": [{"index": 7, "supported": True}]})
        mutations = [("subject", "Someone else"), ("predicate", "owns"), ("confidence", True),
                     ("event_title", "Invented event"), ("quote_hash", "0" * 64), ("end", original["claim"]["end"] + 1),
                     ("revision", "doc_revision:unsupported")]
        for key, value in mutations:
            candidate = copy.deepcopy(original)
            candidate["claim"][key] = value
            with self.subTest(key=key):
                self.assertEqual(support_response([encode([candidate]).decode()], [expected]),
                                 {"decisions": [{"index": 7, "supported": False}]})
        with self.assertRaises(Unmet):
            support_response([encode([original, original]).decode()], [expected])

    def test_system_examples_cannot_select_a_source_or_support_response(self):
        value = {"input": [{"role": "system", "content": QUOTE}, {"role": "user", "content": "unrelated"}]}
        self.assertEqual(user_texts(value, "/v1/responses"), ["unrelated"])
        observed = {key: rule()[key] for key in ("space", "model", "path", "credential_sha256")}
        with self.assertRaises(Unmet):
            self.spy().select(observed, value)

    def test_actual_protocol_envelopes_keep_exact_independent_output(self):
        for path in ("/v1/responses", "/v1/messages"):
            value = rule(path)
            content_type, raw = provider_response(value, body(path))
            self.assertEqual(content_type, "text/event-stream")
            events = [decode(line[6:]) for line in raw.splitlines() if line.startswith(b"data: ")]
            if path == "/v1/responses":
                self.assertEqual(events[-1]["type"], "response.completed")
                text = events[0]["delta"]
            else:
                self.assertEqual(events[-1]["type"], "message_stop")
                text = events[2]["delta"]["text"]
            self.assertEqual(decode(text.encode()), value["output"])
        value = rule("/v1/embeddings", "embedding")
        content_type, raw = provider_response(value, body("/v1/embeddings"))
        self.assertEqual(content_type, "application/json")
        self.assertEqual(len(decode(raw)["data"]), 2)
        self.assertEqual(len(decode(raw)["data"][1]["embedding"]), 1536)

    def test_recipe_hash_caps_and_duplicate_rule_refuse(self):
        value = recipe(rule())
        with self.assertRaises(Unmet):
            validate_recipe(value, "0" * 64)
        value["rules"].append(copy.deepcopy(value["rules"][0]))
        with self.assertRaises(Unmet):
            validate_recipe(value, sha(encode(value)))
        changed = {**LIMITS, "owned_spy_dispatches_per_backend": 4097}
        with self.assertRaises(Unmet):
            self.spy(limits=changed)

    async def test_actual_embedding_batches_fit_original_response_and_capture_bounds(self):
        for count in (100, 128):
            value = rule("/v1/embeddings", "embedding")
            value["output"]["dimensions"] = 3072
            request = body("/v1/embeddings")
            request.update(input=[f"original complete source {i}" for i in range(count)], dimensions=3072)
            spy, writer = self.spy(value), Writer()
            spy.capture = Capture(Path(self.temporary.name) / f"embedding-{count}", 4 * 1024 * 1024)
            await spy.handle(reader(frame(request, "/v1/embeddings")), writer)
            self.assertIsNone(spy.failure)
            self.assertEqual(spy.count, 1)
            self.assertTrue(spy.calls[0]["response_complete"])
            raw = (spy.capture.directory / "100001.response.bin").read_bytes()
            self.assertLess(len(raw), LIMITS["owned_spy_response_bytes"])
            observed = decode(raw)
            self.assertEqual([row["index"] for row in observed["data"]], list(range(count)))
            self.assertTrue(all(len(row["embedding"]) == 3072 for row in observed["data"]))
            self.assertEqual((spy.capture.directory / "100001.request.json").read_bytes(), encode(request))
            self.assertFalse(spy.snapshot_calls()["model_quality_qualified"])

    def test_embedding_batch_above_actual_bound_or_empty_refuses(self):
        value = rule("/v1/embeddings", "embedding")
        for inputs in ([], ["exact input"] * 129, [""], "unbounded implicit batch"):
            request = body("/v1/embeddings")
            request["input"] = inputs
            with self.subTest(inputs_count=len(inputs)), self.assertRaises(Unmet):
                provider_response(value, request)

    async def test_actual_headerless_document_and_query_embeddings_bind_both_spaces_by_credentials(self):
        specification = {"format": "recall-controlled-graph-spy/v1", "rules": []}
        for space in ("qualification-household", "qualification-business"):
            value = rule("/v1/embeddings", "embedding")
            value.update(id=space + "-embedding", space=space, maximum_calls=2, statuses=[200, 200],
                         credential_sha256=sha(("synthetic-controlled-provider-key-" + space).encode()))
            specification["rules"].append(value)
        spy = ControlledSpy(specification, sha(encode(specification)), self.capture, LIMITS, time.monotonic() + 30)
        for space in ("qualification-household", "qualification-business"):
            for text in ("Same document in either space", "Same query in either space"):
                request, writer = body("/v1/embeddings"), Writer()
                request["input"] = [text]
                await spy.handle(reader(headerless_frame(request, "/v1/embeddings",
                                 "synthetic-controlled-provider-key-" + space)), writer)
                observed = spy.calls[-1]
                self.assertIsNone(spy.failure)
                self.assertTrue(observed["response_complete"])
                self.assertEqual(observed["space"], space)
                self.assertIsNone(observed["wire_space"])
                self.assertIn("credential hash, model and path", observed["space_authority"])
                intent = decode((self.capture.directory / f'{observed["id"]:06}.intent.json').read_bytes())
                self.assertIsNone(intent["wire_space"])
                self.assertEqual(intent["space"], space)
                self.assertEqual((self.capture.directory / f'{observed["id"]:06}.request.json').read_bytes(), encode(request))
        self.assertEqual(spy.count, 4)
        self.assertEqual(set(spy.rule_counts.values()), {2})
        self.assertEqual(self.capture.held, 0)

    async def test_embedding_conflicting_header_foreign_credential_and_model_refuse_before_response(self):
        value = rule("/v1/embeddings", "embedding")
        request = body("/v1/embeddings")
        wrong_model = {**request, "model": "foreign-model"}
        wires = [frame(request, "/v1/embeddings", space="qualification-business"),
                 headerless_frame(request, "/v1/embeddings", "foreign-key"),
                 headerless_frame(wrong_model, "/v1/embeddings", CREDENTIAL)]
        for index, wire in enumerate(wires):
            spy, writer = self.spy(value), Writer()
            spy.capture = Capture(Path(self.temporary.name) / f"wrong-authority-{index}", 4 * 1024 * 1024)
            await spy.handle(reader(wire), writer)
            self.assertEqual(writer.data, [])
            self.assertEqual(spy.failure, "Unmet")
            self.assertEqual(spy.rule_counts, {})
            self.assertEqual(spy.count, 1)
            self.assertTrue(spy.calls[0]["request_complete"])
            self.assertFalse(spy.calls[0]["response_complete"])
            self.assertEqual(spy.capture.held, 0)

    async def test_ambiguous_embedding_recipe_refuses_before_listener_and_cannot_dispatch_after_mutation(self):
        first = rule("/v1/embeddings", "embedding")
        second = {**copy.deepcopy(first), "id": "other-space", "space": "qualification-business"}
        specification = {"format": "recall-controlled-graph-spy/v1", "rules": [first, second]}
        with self.assertRaisesRegex(Unmet, "ambiguous controlled embedding rule authority"):
            ControlledSpy(specification, sha(encode(specification)), self.capture, LIMITS, time.monotonic() + 30)
        spy, writer = self.spy(first), Writer()
        spy.recipe["rules"].append(second)
        await spy.handle(reader(headerless_frame(body("/v1/embeddings"), "/v1/embeddings", CREDENTIAL)), writer)
        self.assertEqual(writer.data, [])
        self.assertEqual(spy.rule_counts, {})
        self.assertEqual(spy.failure, "Unmet")

    async def test_actual_headerless_generation_binds_space_before_selecting_source_rules(self):
        for protocol, path in enumerate(("/v1/responses", "/v1/messages")):
            specification = {"format": "recall-controlled-graph-spy/v1", "rules": []}
            for space in ("qualification-household", "qualification-business"):
                for index, text in enumerate((QUOTE, "Other independently frozen source")):
                    value = rule(path)
                    value.update(id=f"{space}-{index}", space=space, required_text=[text],
                                 credential_sha256=sha(("synthetic-controlled-provider-key-" + space).encode()))
                    specification["rules"].append(value)
            capture = Capture(Path(self.temporary.name) / f"generation-{protocol}", 4 * 1024 * 1024)
            spy = ControlledSpy(specification, sha(encode(specification)), capture, LIMITS, time.monotonic() + 30)
            for space in ("qualification-household", "qualification-business"):
                for index, text in enumerate((QUOTE, "Other independently frozen source")):
                    request, writer = body(path), Writer()
                    request["messages" if path == "/v1/messages" else "input"][-1]["content"] = text
                    await spy.handle(reader(headerless_frame(request, path,
                                     "synthetic-controlled-provider-key-" + space)), writer)
                    observed = spy.calls[-1]
                    self.assertIsNone(spy.failure)
                    self.assertTrue(observed["response_complete"])
                    self.assertEqual(observed["space"], space)
                    self.assertIsNone(observed["wire_space"])
                    self.assertEqual(observed["rule_id"], f"{space}-{index}")
                    intent = decode((capture.directory / f'{observed["id"]:06}.intent.json').read_bytes())
                    self.assertIsNone(intent["wire_space"])
                    self.assertEqual(intent["space"], space)
            self.assertEqual(spy.count, 4)
            self.assertEqual(set(spy.rule_counts.values()), {1})

    async def test_generation_conflicting_header_foreign_key_and_model_refuse_on_both_protocols(self):
        for protocol, path in enumerate(("/v1/responses", "/v1/messages")):
            request = body(path)
            wires = [frame(request, path, space="qualification-business"),
                     headerless_frame(request, path, "foreign-key"),
                     headerless_frame({**request, "model": "foreign-model"}, path, CREDENTIAL)]
            for index, wire in enumerate(wires):
                spy, writer = self.spy(rule(path)), Writer()
                spy.capture = Capture(Path(self.temporary.name) / f"generation-denied-{protocol}-{index}", 4 * 1024 * 1024)
                await spy.handle(reader(wire), writer)
                self.assertEqual(writer.data, [])
                self.assertEqual(spy.failure, "Unmet")
                self.assertEqual(spy.rule_counts, {})
                self.assertEqual(spy.count, 1)
                self.assertTrue(spy.calls[0]["request_complete"])
                self.assertFalse(spy.calls[0]["response_complete"])

    async def test_generation_space_ambiguity_cannot_be_resolved_by_source_text_or_header(self):
        for protocol, path in enumerate(("/v1/responses", "/v1/messages")):
            first = rule(path)
            second = {**copy.deepcopy(first), "id": "other-space", "space": "qualification-business",
                      "required_text": ["An unrelated source absent from this request"]}
            specification = {"format": "recall-controlled-graph-spy/v1", "rules": [first, second]}
            with self.assertRaisesRegex(Unmet, "ambiguous controlled provider space authority"):
                ControlledSpy(specification, sha(encode(specification)), self.capture, LIMITS, time.monotonic() + 30)
            for named in (False, True):
                spy, writer = self.spy(first), Writer()
                spy.capture = Capture(Path(self.temporary.name) / f"generation-ambiguous-{protocol}-{named}", 4 * 1024 * 1024)
                spy.recipe["rules"].append(second)
                wire = frame(body(path), path) if named else headerless_frame(body(path), path, CREDENTIAL)
                await spy.handle(reader(wire), writer)
                self.assertEqual(writer.data, [])
                self.assertEqual(spy.rule_counts, {})
                self.assertEqual(spy.failure, "Unmet")


if __name__ == "__main__":
    unittest.main()
