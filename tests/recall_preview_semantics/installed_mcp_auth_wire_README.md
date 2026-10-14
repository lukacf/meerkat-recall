# Installed MCP qualification wire correction

This external qualification input candidate targets the unchanged product commit
`fefe773fac2616a2cce39753000f434f7a562ae5` and its existing installed Linux
artifact. It changes no product source, authorization, route, provider binding,
canonical source, semantic corpus, expected evidence, acceptance threshold or
resource limit. Candidate input bytes are external overrides, not committed
fefe product bytes. Root owns execution and independent review; S authored this
candidate and has not executed its tests or a product probe.

The four original installed fefe runs remain failed. The old basic helper
mistook anonymous `tools/list` status-only discovery for admitted data access;
the old dispatch helper mistook HTTP 200 carrying a typed tool denial for an
admitted call. The observed foreign-space requests returned `isError: true` and
`forbidden_space`, with only the two earlier approved spy calls recorded. These
facts explain the helper mismatch; they do not retroactively pass any original
run or establish later cases that never executed.

The pre-existing source contract is explicit in
`bin/elephant-api/src/mcp.rs`: `runtime_tools_for` always starts with the status
descriptor, `list_tools_for_profile` adds authorized dispatcher tools only for
an authorized caller, and `call_tool_for_profile` returns `public_tool_error`
before any dispatch on authorization failure. `public_tool_error` uses
`CallToolResult::error` with one serialized public error. The existing consumer
contract `docs/internal/recall-preview-contract/homecore-operations.md:134`
describes MCP `isError: true` with shared error classification;
`homecore-followup.md:82` documents composed, caller-dependent tool discovery.
The original HTTP-only 401/403 assertions remain unchanged.

The basic `mcp_auth_denied` case now requires two actual unauthenticated MCP
requests: status-only tool discovery, followed by `search_scoped` using the
original household query and a strict `unauthorized` denial. A discovery-only
response cannot pass the case. Unexpected advertised tools, result fields,
schemas, private source text or tokens fail. The later foreign accepted-source
call keeps its household token, business space, original business query and
revision constraint and requires the same strict MCP `forbidden_space` denial.

The dispatch case keeps its exact household token, business space, original
business query, `search_entities`, vector flag and limit. It still requires
exactly two physical calls before and after the foreign-space request. Missing
and revoked approval use the same strict checker for `provider_unapproved`.
Their zero-extra-call checks remain unchanged. Rotation still requires exactly
three total physical calls, with the same space/profile/credential expectations.

The shared checker requires HTTP 200, the exact JSON-RPC version and integer
request ID, only a result envelope, `isError` exactly true, and exactly one text
block containing the strict three-field typed denial. Success, mismatched IDs,
RPC errors, extra data/content, wrong classifications, retryable denials,
malformed/duplicate JSON and private canaries fail. Original raw captures and
the strict HTTP/SSE parser remain unchanged. No fallback status or permissive
tool parsing is used for a denial.

The 12 focused pure tests load the actual candidate HTTP/checker/auth methods
and dispatch query method with only controlled in-memory HTTP responses. They
exercise complete captured SSE, exact caller/request preservation, false denial
mutations, anonymous discovery plus denied data access, and the provider error
classification. They perform no socket, driver initialization, product import,
child process, model or provider call. Passing these tests qualifies only the
qualification checks, not actual authorization, zero outbound calls, semantic
relevance, native inference or the preview gates. Existing successful pure and
Rust controls do not need replay.

Root's selected focused command, after independent source review:

```text
python3 -I -B <candidate>/tests/recall_preview_semantics/installed_mcp_auth_wire_test.py --source-root /mnt/recall-release/sources/fastembed-resource-8e5c-1 --driver <candidate>/scripts/qualification/installed_linux_offline.py --dispatch <candidate>/scripts/qualification/installed_linux_dispatch.py -v
```

The 19 basic and 8 dispatch case inventories, both backends, two spaces, original
corpus and all deadlines/count ceilings remain byte-for-byte unchanged. Basic
adds one HTTP call within the existing 128-call ceiling; dispatch adds none.
Native's separate source-head path correction belongs to G. B must join exact
reviewed overrides into a fresh external input snapshot while retaining the
fefe product/archive identity and every previous failed original/clock/receipt.
