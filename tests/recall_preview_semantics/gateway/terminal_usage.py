"""Strict completed Responses accounting. No transport, credentials or discounts."""
from __future__ import annotations

from dataclasses import dataclass
import json

from ledger import RESPONSE_CAP
from wire import Denied, encode, fields, integer, require, text

POLICY = "full-context-terminal-v2"
BOUND_VERSION = "provider-full-context-v1"
RATE_VERSION = "sol-standard-long-max-2026-10-06"
FULL_CONTEXT = 1_050_000
MAX_OUTPUT = 128_000
INPUT_RATE = 5000
OUTPUT_RATE = 15000
SDK_PROJECTION_VERSION = "sdk-0.8.51-reasoning-projection-v1"
SDK_SOURCE_SHA256 = "105229842f1a221277513653d5396c8e36a116384dde406182f2e173cdd0f8c3"


def strict_value(raw: bytes) -> dict:
    require(0 < len(raw) <= RESPONSE_CAP, "settlement_body_limit")
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, "settlement_duplicate_key")
            result[key] = value
        return result
    try:
        value = json.loads(raw.decode("utf-8", "strict"), object_pairs_hook=unique,
                           parse_constant=lambda _: (_ for _ in ()).throw(Denied("settlement_nonfinite")))
        require(type(value) is dict, "settlement_object_required")
        encode(value)
        return value
    except (ValueError, UnicodeError, RecursionError) as error:
        raise Denied("settlement_invalid_json") from error


def reasoning_projection(item: dict) -> bytes:
    """Exactly the vendored 0.8.51 summary replay projection, never ciphertext size."""
    fields(item, {"id", "type", "summary", "encrypted_content", "status", "content"},
           {"id", "type", "summary", "encrypted_content"})
    # The native SDK omits the provider's empty content array from replay.
    # Nonempty/unknown reasoning content is outside this reviewed subset.
    require("content" not in item or (type(item["content"]) is list and not item["content"]),
            "reasoning_content_unsupported")
    require(item["type"] == "reasoning" and bool(text(item["id"]))
            and bool(text(item["encrypted_content"])), "reasoning_identity_required")
    require(item.get("status", "completed") == "completed", "reasoning_incomplete")
    require(type(item["summary"]) is list and len(item["summary"]) <= 128, "reasoning_summary_required")
    combined = ""
    for part in item["summary"]:
        fields(part, {"type", "text"}, {"type", "text"})
        require(part["type"] == "summary_text", "reasoning_summary_type")
        if combined:
            combined += "\n"
        combined += text(part["text"])
    return encode({"type": "reasoning", "id": item["id"],
                   "encrypted_content": item["encrypted_content"],
                   "summary": [{"type": "summary_text", "text": combined}] if combined else []})


def terminal_item_covers(observed: bytes, final: bytes | None) -> bool:
    """Only terminal reasoning ciphertext may replace its provisional encoding.

    SDK completed coverage correlates by type/id and lowers final output. Keep
    this accounting subset stricter: every other field must remain identical,
    and neither provisional ciphertext nor unreviewed content grants replay.
    """
    if observed == final:
        return True
    if final is None:
        return False
    earlier, terminal = strict_value(observed), strict_value(final)
    if earlier.get("type") != "reasoning" or terminal.get("type") != "reasoning":
        return False
    reasoning_projection(earlier)
    reasoning_projection(terminal)
    return {key: value for key, value in earlier.items() if key != "encrypted_content"} == {
        key: value for key, value in terminal.items() if key != "encrypted_content"}


@dataclass(frozen=True)
class TerminalUsage:
    response_id: str
    input_tokens: int
    output_tokens: int
    cached_tokens: int
    cache_write_tokens: int
    reasoning_tokens: int
    charge_nano_usd: int
    replay_items: tuple[bytes, ...]


# This is an explicit text/function streaming subset. Unknown event semantics
# cannot authorize spending release. Actual SDK compatibility is separately tested.
EVENTS = frozenset({
    "response.created", "response.in_progress", "response.completed",
    "response.output_item.added", "response.output_item.done",
    "response.content_part.added", "response.content_part.done",
    "response.output_text.delta", "response.output_text.done",
    "response.function_call_arguments.delta", "response.function_call_arguments.done",
    "response.reasoning_summary_part.added", "response.reasoning_summary_part.done",
    "response.reasoning_summary_text.delta", "response.reasoning_summary_text.done",
    "response.reasoning_text.delta", "response.reasoning_text.done",
    "response.reasoning_summary.done", "response.reasoning.done",
})


def terminal_response(body: bytes, streaming: bool) -> dict:
    if not streaming:
        return strict_value(body)
    require(0 < len(body) <= RESPONSE_CAP, "settlement_body_limit")
    normalized = body.replace(b"\r\n", b"\n")
    require(b"\r" not in normalized and normalized.endswith(b"\n\n"), "settlement_sse_framing")
    terminal = None
    response_id = None
    done = False
    last_sequence = -1
    completed_items = {}
    for frame in normalized.split(b"\n\n"):
        if not frame:
            continue
        data, event_name = [], None
        for line in frame.split(b"\n"):
            if not line or line.startswith(b":"):
                continue
            require(b":" in line, "settlement_sse_field")
            name, value = line.split(b":", 1)
            value = value[1:] if value.startswith(b" ") else value
            if name == b"data":
                data.append(value)
            elif name == b"event":
                require(event_name is None, "settlement_sse_duplicate_event")
                event_name = value
            else:
                raise Denied("settlement_sse_field")
        if not data:
            require(event_name is None, "settlement_sse_missing_data")
            continue
        raw = b"\n".join(data)
        if raw == b"[DONE]":
            require(terminal is not None and not done and event_name is None, "settlement_sse_done")
            done = True
            continue
        require(terminal is None and not done, "settlement_event_after_terminal")
        event = strict_value(raw)
        kind = event.get("type")
        require(type(kind) is str and kind in EVENTS, "settlement_unsupported_event")
        require(event_name is None or event_name == kind.encode(), "settlement_event_mismatch")
        sequence = integer(event.get("sequence_number"), 2**63 - 1, "settlement_sequence", 0)
        require(sequence > last_sequence, "settlement_sequence")
        last_sequence = sequence
        if "response_id" in event:
            candidate = text(event["response_id"])
            require(bool(candidate) and (response_id is None or response_id == candidate), "settlement_response_identity")
            response_id = candidate
        if "response" in event:
            response = event["response"]
            require(type(response) is dict and bool(text(response.get("id"))), "settlement_response_identity")
            require(response_id is None or response_id == response["id"], "settlement_response_identity")
            response_id = response["id"]
            expected = "completed" if kind == "response.completed" else "in_progress"
            require(response.get("status") == expected, "settlement_response_status")
        if kind in ("response.output_item.done", "response.reasoning.done", "response.reasoning_summary.done"):
            item = event.get("item")
            require(type(item) is dict and type(item.get("id")) is str and bool(item["id"]),
                    "settlement_stream_item_identity")
            item_bytes = encode(item)
            require(item["id"] not in completed_items or completed_items[item["id"]] == item_bytes,
                    "settlement_stream_item_changed")
            completed_items[item["id"]] = item_bytes
        if kind == "response.completed":
            require("response" in event, "settlement_missing_terminal")
            terminal = event["response"]
    require(terminal is not None, "settlement_missing_terminal")
    output = terminal.get("output")
    require(type(output) is list, "settlement_output_required")
    final_items = {}
    for item in output:
        require(type(item) is dict, "settlement_output_type")
        if "id" in item:
            identifier = text(item["id"])
            require(identifier not in final_items, "settlement_duplicate_output_item")
            final_items[identifier] = encode(item)
    require(all(terminal_item_covers(value, final_items.get(identifier))
                for identifier, value in completed_items.items()),
            "settlement_stream_terminal_item_mismatch")
    return terminal


def validate_terminal(body: bytes, *, streaming: bool, model: str, output_cap: int) -> TerminalUsage:
    integer(output_cap, MAX_OUTPUT, "settlement_output_cap")
    response = terminal_response(body, streaming)
    require(response.get("object") == "response" and response.get("status") == "completed",
            "settlement_response_status")
    response_id = text(response.get("id"))
    require(bool(response_id), "settlement_response_identity")
    require(model == "gpt-6.1-sol" and response.get("model") == model, "settlement_model")
    require(type(response.get("max_output_tokens")) is int
            and response["max_output_tokens"] == output_cap, "settlement_output_cap")
    require(response.get("service_tier") == "default", "settlement_tier")
    require("error" in response and response["error"] is None
            and "incomplete_details" in response and response["incomplete_details"] is None,
            "settlement_error_or_incomplete")
    require(response.get("background", False) is False, "settlement_background")
    tools = response.get("tools", [])
    require(type(tools) is list, "settlement_tools_shape")
    for tool in tools:
        require(type(tool) is dict and tool.get("type") == "function", "settlement_hosted_tool")
    usage = fields(response.get("usage"), {"input_tokens", "output_tokens", "total_tokens",
                   "input_tokens_details", "output_tokens_details"},
                   {"input_tokens", "output_tokens", "total_tokens", "input_tokens_details", "output_tokens_details"})
    inputs = integer(usage["input_tokens"], FULL_CONTEXT, "settlement_input_usage", 0)
    outputs = integer(usage["output_tokens"], output_cap, "settlement_output_usage", 0)
    total = integer(usage["total_tokens"], FULL_CONTEXT + MAX_OUTPUT, "settlement_total_usage", 0)
    require(total == inputs + outputs, "settlement_usage_sum")
    detail = fields(usage["input_tokens_details"], {"cached_tokens", "cache_write_tokens"},
                    {"cached_tokens", "cache_write_tokens"})
    cached = integer(detail["cached_tokens"], inputs, "settlement_cache_usage", 0)
    written = integer(detail["cache_write_tokens"], inputs, "settlement_cache_usage", 0)
    require(cached + written <= inputs, "settlement_cache_sum")
    detail = fields(usage["output_tokens_details"], {"reasoning_tokens"}, {"reasoning_tokens"})
    reasoning = integer(detail["reasoning_tokens"], outputs, "settlement_reasoning_usage", 0)
    output = response.get("output")
    require(type(output) is list, "settlement_output_required")
    replay, ids = [], set()
    for item in output:
        require(type(item) is dict and item.get("type") in ("message", "function_call", "reasoning"),
                "settlement_output_type")
        require(item.get("status", "completed") == "completed", "settlement_output_incomplete")
        if item["type"] == "message":
            parts = item.get("content")
            require(type(parts) is list and all(type(part) is dict and part.get("type") == "output_text"
                    for part in parts), "settlement_refusal_or_nontext")
        if item["type"] == "reasoning":
            item_id = text(item.get("id"))
            require(item_id not in ids, "settlement_duplicate_reasoning")
            ids.add(item_id)
            # A completed response can settle without replayable reasoning. Such
            # an item never authorizes continuation using an opaque reference.
            if "encrypted_content" in item:
                replay.append(reasoning_projection(item))
    return TerminalUsage(response_id, inputs, outputs, cached, written, reasoning,
                         inputs * INPUT_RATE + outputs * OUTPUT_RATE, tuple(replay))
