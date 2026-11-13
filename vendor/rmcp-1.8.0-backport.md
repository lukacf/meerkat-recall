# RMCP 1.8.0 bounded HTTP responses

Recall and Meerkat 0.9.10 share RMCP 1.8.0. This local crate backports the
non-streaming HTTP response limit from the upstream
[response-body repair](https://github.com/modelcontextprotocol/rust-sdk/commit/3e0156c1eaf1573a1b878e2a8998d2fa2d0b1439)
for [RUSTSEC-2026-0462](https://rustsec.org/advisories/RUSTSEC-2026-0462).
It preserves the shared 1.8 API instead of requiring an unrelated SDK migration.

## Provenance

- Registry package: `rmcp` 1.8.0.
- Archive SHA-256: `1d1f571c72940a19d9532fe52dbea8bc9912bf1d766c2970bb824056b86f3f59`.
- Original upstream revision: `25220361d5540715294c501c289d79de4bec2bfc`.
- Upstream repair: `3e0156c1eaf1573a1b878e2a8998d2fa2d0b1439`.
- All 138 original archive files are retained. [Original file checksums](rmcp-1.8.0-upstream-sha256.json)
  and the [implementation delta](rmcp-1.8.0-backport.patch) permit reconstruction.
- The registry archive omits a license file. The [supplemental upstream notice](rmcp-1.8.0/LICENSE)
  is copied verbatim from the repository at the original revision above:
  [source](https://raw.githubusercontent.com/modelcontextprotocol/rust-sdk/25220361d5540715294c501c289d79de4bec2bfc/LICENSE),
  SHA-256 `0382b0057770ca05e9c350a50aa3b1c1fea84da0bc81d723bf00b9aa841be58a`. It includes upstream's license-transition notice and
  Apache-2.0/MIT texts. The package's published `license = "MIT"` metadata is
  preserved; this supplemental file is not part of the original archive manifest.
- The test manifest adds `rstest` and two test targets. The dependency's own
  lockfile records this standalone test resolution; Recall uses its root lockfile.

## Behavior

The built-in reqwest and Unix-socket clients reject successful JSON bodies
larger than 16 MiB and non-success bodies larger than 64 KiB. A declared
oversize is rejected before body polling; streamed bytes are counted before
retention, including when a body remains open. The bound applies after any
reqwest decompression. It does not trust a smaller `Content-Length` as the
actual size, parse a truncated prefix, or turn an oversized notification reply
into an accepted result.

`StreamableHttpError::ResponseBodyTooLarge` identifies rejection. The existing
error enum is non-exhaustive. Error text retains reqwest's feature-dependent
charset and BOM handling: only the bounded in-memory body and its content type
are passed to its decoder. Network streams and content encodings are not copied
into the decoding response.

Existing authentication challenges, attached-session 404 handling, body-free
202/204 responses, JSON-RPC error replies and the 1.8 malformed-JSON fallback
are preserved. Ordinary reqwest body-read errors retain the existing accepted
fallback; Unix-socket body-read errors retain their existing error result.
The newer upstream status-metadata and discovery protocols are not introduced.

SSE handling is unchanged. These limits cover non-streaming POST bodies, not
timeouts for a server that stops sending bytes below the bound. Custom HTTP
clients must enforce their own body bounds. This repair does not claim to fix
every transport or memory-safety issue in this or other dependencies.

## Validation

The helper tests cover exact limits, dishonest size hints, early rejection,
stopping on an oversized chunk and body-read errors. Loopback HTTP tests cover
declared and streamed oversizes with held-open connections, valid JSON prefixes,
request and notification replies, exact-limit JSON/errors, authentication and
session handling, and charset/BOM decoding. Unix-socket tests cover declared
and streamed rejection before EOF. Existing JSON, error-body, empty-response and
Unix-socket handshake regressions also pass.

Vendor crates are excluded from the workspace gate. Run their tests explicitly:

```sh
./scripts/repo-cargo test --manifest-path vendor/rmcp-1.8.0/Cargo.toml --locked \
  --features client,transport-streamable-http-client-reqwest,transport-streamable-http-client-unix-socket,reqwest/charset \
  --lib --test test_recall_http_response_limits --test test_unix_socket_response_limits
```

Also run the response-limit tests without `reqwest/charset`; a consumer can
enable that dependency feature independently. The focused tests use synthetic
loopback responses and do not contact an external MCP server.
