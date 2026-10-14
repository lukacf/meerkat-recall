"""Synthetic SDK-shape spies through actual wire/Handler; no provider acceptance."""
from dataclasses import replace
from email.message import Message
import copy
import hashlib
import io
import json
from types import SimpleNamespace
import unittest

import server
from wire import Denied, Scope, encode, validate


SCOPE = Scope("surreal-db", "semantics-household", "extraction", "gpt-6.1-sol",
              binding_id="binding-a", connection_id="connection-a",
              profile_id="profile-a", credential_generation=1)


# Frozen known answer from the documented full-key tuple, computed without importing wire.
BOUNDED_CACHE_KEY = '1bbf41cec50f7a9f8f1a7c27e5df82ff209dc65810be759e77ba7095f0e5a76b'

def body():
    # Exact named cache defaults are copied from pinned SDK source. This is
    # a controlled fixture, not a claimed captured provider request.
    return {"model": "gpt-6.1-sol", "input": [{"type": "message", "role": "user",
             "content": [{"type": "input_text", "text": "Original synthetic household instruction."}]}],
            "max_output_tokens": 128000, "stream": True, "store": False,
            "include": ["reasoning.encrypted_content"],
            "reasoning": {"effort": "medium", "summary": "auto"},
            "prompt_cache_key": "meerkat:profile:openai:gpt-6.1-sol",
            "prompt_cache_options": {"mode": "implicit", "ttl": "30m"}}


class ForwarderSpy:
    def __init__(self):
        self.calls = []
        self.ledger = SimpleNamespace()
    def dispatch(self, request, raw, start, write):
        self.calls.append((request, raw))
        start(200, "application/json")
        write(b"{}")


def invoke(value, scope=SCOPE):
    raw = encode(value)
    spy = ForwarderSpy()
    policy = SimpleNamespace(authenticate=lambda _capability: scope,
                             output_cap_mode="preserve", context_policy="provider-full-context-v1")
    handler = server.Handler.__new__(server.Handler)
    handler.server = SimpleNamespace(policy=policy, forwarder=spy, capture_denial=lambda *args: None)
    handler.request_version = "HTTP/1.1"
    handler.path = "/v1/responses"
    handler.headers = Message()
    for key, value in {"Host": "127.0.0.1:43129", "Authorization": "Bearer SYNTHETIC_CAPABILITY",
                       "Content-Type": "application/json", "Content-Length": str(len(raw))}.items():
        handler.headers[key] = value
    handler.rfile, handler.wfile = io.BytesIO(raw), io.BytesIO()
    statuses = []
    handler.send_response = statuses.append
    handler.send_header = lambda *args: None
    handler.end_headers = lambda: None
    handler.do_POST()
    return statuses, spy.calls


class CacheMetadataControls(unittest.TestCase):
    def test_reported_85_character_scoped_key_reaches_forwarder_as_full_64_character_digest(self):
        # Root's actual upstream error reported 85 > 64. This fixture reproduces
        # that gateway-generated length through the real Handler with an explicit
        # ForwarderSpy; it is not a replayed or successful provider response.
        value = body()
        value.update(service_tier="default", truncation="disabled")
        value["input"][0]["content"][0]["text"] = "Keep the complete café instruction and its scope."
        previous_scoped = "recall-qualification:" + SCOPE.context_key
        self.assertEqual(len(previous_scoped), 85)
        statuses, calls = invoke(value)
        self.assertEqual(statuses, [200])
        self.assertEqual(len(calls), 1)
        request, original = calls[0]
        forwarded = json.loads(request.body)
        key = forwarded["prompt_cache_key"]
        self.assertEqual(key, BOUNDED_CACHE_KEY)
        self.assertRegex(key, r"^[0-9a-f]{64}$")
        self.assertNotEqual(key, previous_scoped[:64])
        self.assertEqual(original, encode(value))
        self.assertEqual({k: v for k, v in forwarded.items() if k != "prompt_cache_key"},
                         {k: v for k, v in value.items() if k != "prompt_cache_key"})
        self.assertEqual(request.original_hash, hashlib.sha256(original).hexdigest())
        self.assertEqual(request.forwarded_hash, hashlib.sha256(request.body).hexdigest())
        self.assertNotEqual(request.original_hash, request.forwarded_hash)
        self.assertIn("scoped_prompt_cache_key:sha256:v2", request.transforms)
        self.assertNotIn("scoped_prompt_cache_key:v1", request.transforms)
        self.assertEqual(request.output_cap, 128000)
        self.assertEqual(request.reservation, 1050000 * 5000 + 128000 * 15000)
        self.assertEqual(json.loads(invoke(value)[1][0][0].body)["prompt_cache_key"], key)
        keys = [key]
        for name, changed in (("backend", "elephant-db"), ("space", "semantics-business"),
                              ("operation", "support"), ("binding_id", "binding-b"),
                              ("connection_id", "connection-b"), ("profile_id", "profile-b"),
                              ("credential_generation", 2)):
            with self.subTest(axis=name):
                statuses, calls = invoke(value, replace(SCOPE, **{name: changed}))
                self.assertEqual(statuses, [200])
                self.assertEqual(len(calls), 1)
                changed_key = json.loads(calls[0][0].body)["prompt_cache_key"]
                self.assertRegex(changed_key, r"^[0-9a-f]{64}$")
                keys.append(changed_key)
        self.assertEqual(len(keys), len(set(keys)))
        for forbidden in (previous_scoped, key):
            rejected = copy.deepcopy(value)
            rejected["prompt_cache_key"] = forbidden
            self.assertEqual(invoke(rejected), ([403], []))
        self.assertEqual(invoke(value, replace(SCOPE, model="unapproved-model")), ([403], []))

    def test_exact_sdk_default_and_turn_markers_reach_actual_forwarder_spy(self):
        for anchored in (False, True):
            value = body()
            if anchored:
                value["input"][0]["content"][0]["prompt_cache_breakpoint"] = {"mode": "explicit"}
                value["input"].append({"type": "function_call_output", "call_id": "synthetic-call",
                    "output": [{"type": "input_text", "text": "Synthetic tool result.",
                                "prompt_cache_breakpoint": {"mode": "explicit"}}]})
            statuses, calls = invoke(value)
            self.assertEqual(statuses, [200])
            self.assertEqual(len(calls), 1)
            request, original = calls[0]
            forwarded = json.loads(request.body)
            self.assertEqual(original, encode(value))
            self.assertEqual(forwarded["input"], value["input"])
            self.assertEqual(forwarded["prompt_cache_options"], value["prompt_cache_options"])
            self.assertEqual(forwarded["prompt_cache_key"], BOUNDED_CACHE_KEY)
            self.assertIn("scoped_prompt_cache_key:sha256:v2", request.transforms)
            self.assertNotEqual(request.original_hash, request.forwarded_hash)
            self.assertEqual(request.output_cap, 128000)
            self.assertEqual(request.reservation, 1050000 * 5000 + 128000 * 15000)

    def test_malformed_unknown_and_unselected_cache_shapes_have_zero_forward_calls(self):
        variants = []
        for key in ("prompt_cache_key", "prompt_cache_options"):
            value = body();del value[key];variants.append(value)
        for key in ("foreign-space-cache", "", None, 7):
            value = body();value["prompt_cache_key"] = key;variants.append(value)
        for options in (None, [], {}, {"mode": "implicit"}, {"mode": "explicit", "ttl": "30m"},
                        {"mode": "implicit", "ttl": "24h"},
                        {"mode": "implicit", "ttl": "30m", "prewarm": False},
                        {"mode": "implicit", "ttl": "30m", "unknown": True}):
            value = body();value["prompt_cache_options"] = options;variants.append(value)
        for marker in (None, {}, {"mode": "implicit"}, {"mode": "explicit", "unknown": 1}):
            value = body();value["input"][0]["content"][0]["prompt_cache_breakpoint"] = marker;variants.append(value)
        value = body();value["input"][0]["content"][0].update(type="output_text", prompt_cache_breakpoint={"mode":"explicit"});variants.append(value)
        value = body();value["input"][0]["content"][0]["prompt_cache_breakpoint"]={"mode":"explicit"}
        del value["prompt_cache_key"];del value["prompt_cache_options"];variants.append(value)
        for extra in ({"prompt_cache_retention":"24h"}, {"unknown":True},
                      {"previous_response_id":"synthetic-state"}, {"conversation":"synthetic-state"}):
            value=body();value.update(extra);variants.append(value)
        for index, value in enumerate(variants):
            with self.subTest(index=index):
                statuses, calls=invoke(value)
                self.assertEqual(statuses,[403])
                self.assertEqual(calls,[])

    def test_cache_accounting_namespace_separates_every_approved_context_axis(self):
        scopes=[SCOPE]
        for name,value in (("backend","elephant-db"),("space","semantics-business"),
                           ("operation","support"),("binding_id","binding-b"),
                           ("connection_id","connection-b"),("profile_id","profile-b"),
                           ("credential_generation",2)):
            scopes.append(replace(SCOPE,**{name:value}))
        keys=[]
        for scope in scopes:
            request=validate(encode(body()),"/v1/responses",scope,
                             output_cap_mode="preserve",context_policy="provider-full-context-v1")
            keys.append(json.loads(request.body)["prompt_cache_key"])
        self.assertEqual(len(keys),len(set(keys)))
        self.assertEqual(keys[0], BOUNDED_CACHE_KEY)
        value=body();value["prompt_cache_key"]=keys[1]
        self.assertEqual(invoke(value)[1],[])

    def test_cacheless_contract_and_gross_reservation_stay_unchanged(self):
        cached=body();plain=copy.deepcopy(cached)
        del plain["prompt_cache_key"];del plain["prompt_cache_options"]
        options={"output_cap_mode":"preserve","context_policy":"provider-full-context-v1"}
        current=validate(encode(cached),"/v1/responses",SCOPE,**options)
        baseline=validate(encode(plain),"/v1/responses",SCOPE,**options)
        self.assertEqual((current.input_bound,current.output_cap,current.reservation),
                         (baseline.input_bound,baseline.output_cap,baseline.reservation))
        self.assertNotIn("scoped_prompt_cache_key:sha256:v2",baseline.transforms)
        normalized=json.loads(current.body)
        del normalized["prompt_cache_key"];del normalized["prompt_cache_options"]
        self.assertEqual(normalized,json.loads(baseline.body))
        with self.assertRaises(Denied):
            validate(encode(cached),"/v1/responses",SCOPE)

if __name__ == "__main__":
    unittest.main()
