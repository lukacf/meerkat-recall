# Managed bootstrap route activation

The installed ElephantDB basic and controlled-dispatch originals reached a
successful `initialize_runtime` response, then captured an empty HTTP 404 from
the correct `POST /v1/spaces` route. The previous server constructed data routes
only when storage already existed before the listener was bound. Updating the
runtime manager did not update that already-served router.

The managed server now retains the runtime, MCP, readiness and health routes and
dispatches other requests against the runtime manager's current storage. Before
initialization those requests retain their existing 404 behavior. After
initialization the existing data route graph and the original authentication,
runtime admission, current grant and handler policy checks serve the request in
the same process. There is no helper restart or retry. Route state is constructed
from current storage for each request; it does not cache an independent storage
authority. Extension and storage startup validation still runs before binding
an already-configured server, or before first data admission following bootstrap.
Route construction cost has not been benchmarked by these regressions.

Two Rust regressions use the same production router instance before and after
the real RuntimeManager transition used by MCP. They check uninitialized refusal,
unauthenticated and wrong-space denial, classified household and business
creation/readback, cross-space denial, and runtime drain refusal. They initialize
without workers, use unapproved embedding configuration and make no provider
calls. They do not replace the installed originals or establish MCP transport,
provider dispatch, native inference or semantic relevance acceptance.

The qualification helper separately retains strict response parsing and raw
captures while adding HTTP status, request method/path, body byte count and
capture filename to a malformed-response error. Its seven pure controls use the
actual method and strict parser with a controlled in-memory HTTP connection;
they cannot prove an installed route works. Empty successes, malformed denials,
duplicate keys, invalid UTF-8, non-finite numbers, invalid MCP identity/events and
incomplete framing still fail. Valid payloads remain exact. No expected status,
schema, case inventory, input, outbound count, deadline or threshold changes.

At the source checkpoint these tests are authored but unexecuted. Root owns
subsequent validation and exact artifact qualification. The prior failed
installed observations remain failed originals.
