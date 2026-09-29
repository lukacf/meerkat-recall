import json
import os
import time
import urllib.request

from phase13_helpers import auth_headers, make_claims


def build_headers():
    token = os.getenv("MCP_BEARER_TOKEN")
    if token:
        return {"Authorization": f"Bearer {token}"}
    claims = make_claims("mcp-check", os.getenv("MCP_SPACE", "default"))
    return auth_headers(claims)


def sse_iter_events(response):
    event_lines = []
    for raw_line in response:
        line = raw_line.decode("utf-8", errors="replace").strip("\r\n")
        if line == "":
            if event_lines:
                yield event_lines
                event_lines = []
            continue
        event_lines.append(line)
    if event_lines:
        yield event_lines


def parse_event(lines):
    data_lines = []
    for line in lines:
        if line.startswith("data:"):
            data_lines.append(line[len("data:") :].lstrip())
    if not data_lines:
        return None
    payload = "\n".join(data_lines)
    if payload == "":
        return None
    return json.loads(payload)


def run():
    url = os.getenv("MCP_URL", "http://127.0.0.1:3000/mcp")
    headers = build_headers()
    headers["Accept"] = "application/json, text/event-stream"
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}).encode("utf-8")
    request = urllib.request.Request(url, data=body, method="POST")
    request.add_header("Content-Type", "application/json")
    for key, value in headers.items():
        request.add_header(key, value)

    deadline = time.time() + 10
    with urllib.request.urlopen(request, timeout=10) as response:
        for event in sse_iter_events(response):
            message = parse_event(event)
            if not message:
                if time.time() > deadline:
                    raise RuntimeError("Timed out waiting for MCP response")
                continue
            result = message.get("result")
            if not result:
                continue
            tools = result.get("tools") or []
            print(f"tool_count={len(tools)}")
            for tool in tools:
                name = tool.get("name")
                if name:
                    print(name)
            return
    raise RuntimeError("No MCP response received")


if __name__ == "__main__":
    run()
