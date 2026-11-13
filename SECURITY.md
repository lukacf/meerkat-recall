# Security Policy

Elephant handles provenance, authorization, and tenant isolation data, so please
report vulnerabilities privately first.

## Supported Versions

Security fixes are prepared against `main` and the latest tagged release. During
the pre-1.0 period, older releases may receive fixes when the patch is small and
the affected surface is still active.

## Reporting A Vulnerability

Use [GitHub private vulnerability reporting](https://github.com/lukacf/meerkat-recall/security/advisories/new)
when the repository has it enabled. Before public launch, maintainers must enable
that channel and verify that an external reporter can reach it. While it is
unavailable, request a private contact route without including vulnerability
details in a public issue.

Please include:

- affected commit or release tag
- the vulnerable endpoint, MCP tool, crate, or workflow
- reproduction steps
- expected impact
- any suggested mitigation

We aim to acknowledge reports within seven days and will coordinate disclosure
timing with the reporter when a fix is needed.

## Sensitive Data

Do not include real API keys, bearer tokens, database credentials, customer data,
or private documents in issues, pull requests, fixtures, logs, screenshots, or
test artifacts.

## Deployment defaults

Authenticated servers require `JWT_SECRET` with at least 32 random bytes and
`JWT_ALGORITHM=HS256`. Startup rejects missing or short secrets. Setting
`SKIP_AUTH=true` explicitly bypasses authentication and ABAC for local development.
The Compose examples bind published ports to loopback and use development
database credentials; they require a separate production configuration.

## Repository checks

CI scans reachable Git history with the built-in Gitleaks rules. Narrow
allowlists bind reviewed synthetic fixtures and public integrity digests to exact
paths and literal matches. Use a manual privacy
review as well: names, household data, incident narratives and private project
paths are often not credentials and are not detected by a secret scanner.
Source archive exclusions do not hide files from Git history or GitHub PRs.

## Upstream parser maintenance

The pinned Meerkat 0.9.10 SDK uses `serde-saphyr` for skill frontmatter. The
current lockfile no longer includes `serde_yaml` or `unsafe-libyaml`, and the
previous `RUSTSEC-2024-0449` maintenance exception has been removed. Published
Recall 0.9.1 archives retain their original dependency graph and audit scope.

## Wasm timer maintenance

`RUSTSEC-2026-0441` reports that `pin-utils` is unmaintained, without a specific
vulnerability or patched version. The dependency is retained by SurrealDB's
Wasm-only `wasmtimer` 0.2.1 integration, where `Interval` and `Timeout` use its
pin-projection macros. Native builds use Tokio timers. Even `wasmtimer` 0.4.3
retains this dependency, so upgrading it does not remove the maintenance issue.

The audit exception is limited to this advisory. Revisit it when the upstream
timer removes `pin-utils` or another advisory changes the security assessment.
Replacing the pin-projection code requires separate Wasm validation.

## Dependency repair scope

The root lockfile selects `rustix` 1.1.6 for the zero-length extended-attribute
buffer repair in `RUSTSEC-2026-0486`. The shared `rusqlite` 0.37 API is retained
with a local dependency update to `libsqlite3-sys` 0.39.1, which bundles SQLite
3.54.1 and includes the WAL page-size fix for `RUSTSEC-2026-0459`. System-linked
SQLite and SQLCipher need their own version assessment. See the
[SQLite provenance and validation](vendor/rusqlite-0.37.0-backport.md).

The local RMCP 1.8 backport caps built-in non-streaming HTTP response bodies
for `RUSTSEC-2026-0462` while preserving the shared Meerkat SDK API. Its
[repair record](vendor/rmcp-1.8.0-backport.md) describes limits and tests.

The [reblessive repairs](vendor/reblessive-0.4.3-backport.md) cover cancellation
cleanup and ordinary async-driver ownership, but not all of the dependency's
memory-safety defects. Unsound erased-future Send/Sync behavior reported under
`RUSTSEC-2026-0454` and explicitly forgotten drivers remain outside their scope. A successful dependency audit is not proof that a
vendored crate is generally memory-safe; retain the source review and explicit
regression tests. No new advisory exception is added for these repairs.
