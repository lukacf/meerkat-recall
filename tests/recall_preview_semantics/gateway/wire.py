"""Strict, text-only qualification wire contract; no network or credentials.

The byte-BPE/protocol bound is an explicitly reviewed assumption, not a model
quality result or a claim that provider internals are publicly specified.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math

BOUND_VERSION = "utf8-wire-plus-protocol-v1"
PROTOCOL_BASE = 4096
PROTOCOL_ITEM = 64
PROTOCOL_TOOL = 64
MAX_BODY_BYTES = 131072
GENERATION_INPUT = 32768
GENERATION_OUTPUT = 16384
EMBEDDING_INPUT = 8192
GENERATION_RESERVATION_NANO_USD = 245760000
EMBEDDING_RESERVATION_NANO_USD = 1064960


class Denied(Exception):
    """Safe machine-readable reason. Never contains input or secret values."""


def require(condition: bool, code: str) -> None:
    if not condition:
        raise Denied(code)


def integer(value: object, maximum: int, code: str, minimum: int = 1) -> int:
    require(type(value) is int and minimum <= value <= maximum, code)
    return value


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def encode(value: object) -> bytes:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True,
                          separators=(",", ":"), allow_nan=False).encode("utf-8", "strict")
    except (ValueError, TypeError, UnicodeError, RecursionError) as error:
        raise Denied("invalid_json_value") from error


def strict_json(raw: bytes) -> dict:
    require(len(raw) <= MAX_BODY_BYTES, "request_body_limit")
    def unique(pairs: list[tuple[str, object]]) -> dict:
        result = {}
        for key, value in pairs:
            require(key not in result, "duplicate_json_key")
            result[key] = value
        return result
    try:
        value = json.loads(raw.decode("utf-8", "strict"), object_pairs_hook=unique,
                           parse_constant=lambda _: (_ for _ in ()).throw(Denied("nonfinite_json")))
    except (ValueError, UnicodeError, RecursionError) as error:
        raise Denied("invalid_json") from error
    require(type(value) is dict, "request_object_required")
    encode(value)  # Reject unpaired escaped surrogates as well as invalid UTF-8.
    return value


def fields(value: object, allowed: set[str], required: set[str] = frozenset()) -> dict:
    require(type(value) is dict and required <= value.keys()
            and value.keys() <= allowed, "unsupported_fields")
    return value


def text(value: object) -> str:
    require(type(value) is str, "text_required")
    value.encode("utf-8", "strict")
    return value


def schema(value: object, depth: int = 0) -> None:
    require(depth < 64, "schema_depth_limit")
    if isinstance(value, dict):
        for key, child in value.items():
            if key in ("$ref", "$dynamicRef"):
                require(isinstance(child, str) and child.startswith("#"), "remote_schema_reference")
            schema(child, depth + 1)
    elif isinstance(value, list):
        for child in value:
            schema(child, depth + 1)


def content(value: object) -> None:
    if isinstance(value, str):
        text(value)
        return
    require(type(value) is list and len(value) <= 128, "text_content_required")
    for part in value:
        part = fields(part, {"type", "text", "annotations"}, {"type", "text"})
        require(part["type"] in ("input_text", "output_text", "summary_text"), "nontext_content")
        require(part.get("annotations", []) == [], "remote_annotations")
        text(part["text"])


@dataclass(frozen=True)
class Scope:
    backend: str
    space: str
    operation: str
    model: str
    dimensions: int | None = None
    binding_id: str = ""
    connection_id: str = ""
    profile_id: str = ""
    credential_generation: int = 0

    @property
    def context_key(self) -> str:
        identity = [self.backend, self.space, self.operation, self.model]
        if self.binding_id or self.connection_id or self.profile_id or self.credential_generation:
            identity += [self.binding_id, self.connection_id, self.profile_id, self.credential_generation]
        return digest(encode(identity))


@dataclass(frozen=True)
class Request:
    path: str
    body: bytes
    original_hash: str
    forwarded_hash: str
    input_bound: int
    output_cap: int
    reservation: int
    scope: Scope
    streaming: bool
    transforms: tuple[str, ...]
    requested_output_cap: int = 0


def validate(raw: bytes, path: str, scope: Scope, *, output_cap_mode: str = "reject",
             context_policy: str = BOUND_VERSION, authorize_reasoning=None) -> Request:
    full_context = context_policy == "provider-full-context-v1"
    require(context_policy in (BOUND_VERSION, "provider-full-context-v1"), "unapproved_context_policy")
    require(output_cap_mode in (("preserve", "cap_at_16384") if full_context else
                               ("reject", "cap_at_16384")), "unapproved_transform_mode")
    require(path in ("/v1/responses", "/v1/embeddings"), "unsupported_path")
    body = strict_json(raw)
    require(body.get("model") == scope.model, "unapproved_model")
    transforms = []
    if path == "/v1/embeddings":
        fields(body, {"model", "input", "dimensions", "encoding_format"}, {"model", "input"})
        require(scope.operation == "embedding", "operation_path_mismatch")
        require(scope.model in ("text-embedding-3-small", "text-embedding-3-large"), "unapproved_model")
        expected = 1536 if scope.model == "text-embedding-3-small" else 3072
        require(scope.dimensions == expected and body.get("dimensions", expected) == expected,
                "unapproved_dimensions")
        require(body.get("encoding_format", "float") == "float", "unsupported_encoding")
        values = [body["input"]] if isinstance(body["input"], str) else body["input"]
        require(type(values) is list and 1 <= len(values) <= 64, "embedding_batch_limit")
        for value in values:
            require(bool(text(value)), "empty_embedding_input")
        # Whole serialized JSON upper-bounds all text bytes, including the batch.
        bound = len(encode(body)) + 64 * len(values)
        require(bound <= EMBEDDING_INPUT, "embedding_input_limit")
        output = 0
        requested_output = 0
        reservation = EMBEDDING_RESERVATION_NANO_USD
        streaming = False
    else:
        fields(body, {"model", "input", "instructions", "max_output_tokens", "stream", "store",
                      "service_tier", "reasoning", "include", "tools", "tool_choice", "parallel_tool_calls",
                      "text", "temperature", "top_p", "previous_response_id"} | ({"truncation"} if full_context else set()),
               {"model", "input", "max_output_tokens", "reasoning"})
        require(scope.operation != "embedding", "operation_path_mismatch")
        require(scope.model == "gpt-6.1-sol", "unapproved_model")
        # This optional qualification transform is explicitly approved/pinned
        # separately. The ordinary/default path still refuses oversized caps.
        requested_output = integer(body["max_output_tokens"], 128000, "output_token_limit")
        output = requested_output
        if requested_output > GENERATION_OUTPUT and output_cap_mode != "preserve":
            require(output_cap_mode == "cap_at_16384", "output_token_limit")
            output = GENERATION_OUTPUT
            body["max_output_tokens"] = output
            transforms.append(f"approved_output_cap:{requested_output}->{output}")
        require(body.get("store", False) is False, "server_storage_forbidden")
        require(body.get("service_tier", "default") == "default", "unapproved_service_tier")
        for key, value in (("service_tier", "default"), ("store", False)):
            if key not in body:
                body[key] = value
                transforms.append(f"explicit_{key}")
        reason = fields(body["reasoning"], {"effort", "summary"}, {"effort"})
        require(reason["effort"] == "medium" and reason.get("summary", "auto") in
                ("auto", "concise", "detailed"), "unapproved_reasoning")
        require(body.get("include", []) in ([], ["reasoning.encrypted_content"]), "unsupported_include")
        streaming = body.get("stream", False)
        require(type(streaming) is bool, "invalid_stream")
        if "instructions" in body:
            text(body["instructions"])
        if "parallel_tool_calls" in body:
            require(type(body["parallel_tool_calls"]) is bool, "invalid_tool_parallelism")
        for sampling in ("temperature", "top_p"):
            if sampling in body:
                require(type(body[sampling]) in (int, float) and math.isfinite(body[sampling])
                        and 0 <= body[sampling] <= (2 if sampling == "temperature" else 1), "invalid_sampling")
        require("previous_response_id" not in body, "state_reference_forbidden")
        if full_context:
            require(body.get("truncation", "disabled") == "disabled", "truncation_forbidden")
            if "truncation" not in body:
                body["truncation"] = "disabled"
                transforms.append("explicit_disabled_truncation")
        inputs = body["input"]
        if isinstance(inputs, str):
            text(inputs)
            item_count = 1
        else:
            require(type(inputs) is list and 1 <= len(inputs) <= 128, "input_item_limit")
            item_count = len(inputs)
            reasoning_ids = set()
            for item in inputs:
                require(type(item) is dict, "input_item_object_required")
                kind = item.get("type", "message")
                if kind == "message":
                    fields(item, {"type", "role", "content", "id", "status", "phase"}, {"role", "content"})
                    require(item["role"] in ("user", "assistant", "developer", "system"), "unsupported_role")
                    require(item.get("status", "completed") == "completed", "incomplete_history_item")
                    require(item.get("phase", "final_answer") in ("commentary", "final_answer"), "unsupported_phase")
                    content(item["content"])
                    require("id" not in item, "state_reference_forbidden")
                elif kind == "function_call":
                    fields(item, {"type", "call_id", "name", "arguments"}, {"type", "call_id", "name", "arguments"})
                    for key in ("call_id", "name", "arguments"):
                        text(item[key])
                elif kind == "function_call_output":
                    fields(item, {"type", "call_id", "output"}, {"type", "call_id", "output"})
                    text(item["call_id"])
                    content(item["output"])
                elif kind == "reasoning":
                    require(full_context and callable(authorize_reasoning), "opaque_reasoning_forbidden")
                    fields(item, {"type", "id", "summary", "encrypted_content"},
                           {"type", "id", "summary", "encrypted_content"})
                    identifier = text(item["id"])
                    require(identifier and identifier not in reasoning_ids, "duplicate_reasoning_item")
                    reasoning_ids.add(identifier)
                    authorize_reasoning(item, scope)
                else:
                    raise Denied("unsupported_input_type")
        tools = body.get("tools", [])
        require(type(tools) is list and len(tools) <= 64, "tool_limit")
        names = set()
        for tool in tools:
            fields(tool, {"type", "name", "description", "parameters", "strict"}, {"type", "name", "parameters"})
            require(tool["type"] == "function", "hosted_tool_forbidden")
            name = text(tool["name"])
            require(bool(name) and name not in names, "duplicate_tool")
            names.add(name)
            if "description" in tool:
                text(tool["description"])
            require(type(tool.get("strict", False)) is bool and type(tool["parameters"]) is dict, "invalid_tool_schema")
            schema(tool["parameters"])
        choice = body.get("tool_choice", "auto")
        if isinstance(choice, str):
            require(choice in ("auto", "none", "required"), "unsupported_tool_choice")
        else:
            fields(choice, {"type", "name"}, {"type", "name"})
            require(choice["type"] == "function" and choice["name"] in names, "unsupported_tool_choice")
        if "text" in body:
            config = fields(body["text"], {"format", "verbosity"})
            require(config.get("verbosity", "medium") in ("low", "medium", "high"), "unsupported_verbosity")
            if "format" in config:
                fmt = fields(config["format"], {"type", "name", "schema", "strict", "description"}, {"type"})
                require(fmt["type"] in ("text", "json_object", "json_schema"), "unsupported_output_format")
                if fmt["type"] == "json_schema":
                    require(type(fmt.get("schema")) is dict, "output_schema_required")
                    schema(fmt["schema"])
                if "strict" in fmt:
                    require(type(fmt["strict"]) is bool, "invalid_output_schema")
                for key in ("name", "description"):
                    if key in fmt:
                        text(fmt[key])
        if full_context:
            # Provider's full context maximum includes opaque reasoning and
            # protocol context. Ciphertext length is never a token estimate.
            from terminal_usage import FULL_CONTEXT, INPUT_RATE, OUTPUT_RATE
            bound = FULL_CONTEXT
            reservation = FULL_CONTEXT * INPUT_RATE + output * OUTPUT_RATE
        else:
            bound = len(encode(body)) + PROTOCOL_BASE + PROTOCOL_ITEM * item_count + PROTOCOL_TOOL * len(tools)
            require(bound <= GENERATION_INPUT, "generation_input_limit")
            reservation = GENERATION_RESERVATION_NANO_USD
    forwarded = encode(body)
    require(len(forwarded) <= MAX_BODY_BYTES, "request_body_limit")
    return Request(path, forwarded, digest(raw), digest(forwarded), bound, output,
                   reservation, scope, streaming, tuple(transforms), requested_output)
