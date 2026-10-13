# Recall approved-endpoint transport patch

Upstream: `meerkat-llm-core` 0.8.51, MIT OR Apache-2.0, registry package checksum
`3e79b75220cf9c0cd79bd229bfe8044f73776bfe885fd20dc7abf6d36714a944`.
Registry VCS metadata identifies commit `1d5d844c8a75a70cbe90e62673dca5cd3621744e`,
path `crates/meerkat-llm-core`. Source and normalized/original manifests are copied
from that published package. License texts are copied from that exact upstream
commit. No version or API changes are introduced.

Recall approves a specific per-space processing endpoint. Reqwest's default
redirect policy could forward a model request body beyond that endpoint. The
native shared `build_http_client_for_base_url` now sets `Policy::none()`. Providers
return the redirect response as a request failure; the operator must approve the
actual intended endpoint explicitly. The browser transport is unchanged because
this native service patch does not establish a browser egress contract.

The initial transport-policy change is in `src/http.rs`. Its deterministic local
307 regression ensures a private fixture body reaches only the approved listener.
This patch covers the shared helper used by Meerkat's Anthropic, OpenAI and Gemini
clients. Recall's own embedding client separately disables redirects.

Tests are authored; no execution or live-provider qualification is implied by
vendoring this source. Retain this notice and both licenses in source/artifact
provenance. Remove the patch when an upstream release supplies equivalent
approved-endpoint behavior and is independently qualified.

Follow-up: `src/error.rs` now maps all HTTP 3xx responses to terminal
`LlmError::InvalidConfig`, retaining the fact that the first request reached the
configured endpoint. This is an exact backport of upstream
`a34b9fc7bbadd95e9284bcf1182d6d10bcc072be`, including its error-classification
regression. OpenAI/OpenAI-compatible and Gemini clients are separately pinned
at released 0.8.51 with that commit's fail-closed client-construction hunks; no
default redirect-following client is substituted after a builder failure.
Anthropic already propagates construction failure and needs no extra vendor pin.
Three additional client-stream regressions use a real invalid reqwest builder
and assert that its stored failure propagates before any loopback connection.
These tests remain unexecuted.
