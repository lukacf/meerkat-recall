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

The default `JWT_ALGORITHM=HS256` verifier requires `JWT_SECRET` with at least
32 random bytes. Startup rejects missing or short secrets. Operators may instead
select `JWT_ALGORITHM=RS256` with a public-only `JWT_JWKS_FILE` and fixed
`JWT_ISSUER` and `JWT_AUDIENCE`; Recall does not receive the signing key. See
[public-key authentication](docs/operations/deployment.mdx#asymmetric-token-verification)
for the accepted key and claim profile. Key rotation requires a restart; this
does not provide OIDC discovery or individual-token revocation. Setting
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

The pinned Meerkat 0.9.18 SDK uses `serde-saphyr` for skill frontmatter. The
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

The lockfile selects `url` 2.5.9 for the nested `blob:` origin stack overflow
in [RUSTSEC-2026-0592](https://rustsec.org/advisories/RUSTSEC-2026-0592.html)
and `zip` 9.0.5 for
[RUSTSEC-2026-0548](https://rustsec.org/advisories/RUSTSEC-2026-0548.html).
Recall's DOCX reader reads `word/document.xml` directly through `ZipArchive`;
it does not inspect with one ZIP reader and extract with `ZipStreamReader`.
The upstream header-consistency check is opt-in. Any future streaming extraction
of untrusted archives must enable it; the version bump alone does not do so.

The root lockfile selects `rustix` 1.1.6 for the zero-length extended-attribute
buffer repair in `RUSTSEC-2026-0486`. Recall's `rusqlite` 0.37 API is retained
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

## Time dependency maintenance

The 2026-11-25 dependency review adds narrowly scoped maintenance exceptions
for [chrono (RUSTSEC-2026-0543)](https://rustsec.org/advisories/RUSTSEC-2026-0543.html),
[chrono-tz (RUSTSEC-2026-0555)](https://rustsec.org/advisories/RUSTSEC-2026-0555.html)
and [humantime (RUSTSEC-2026-0542)](https://rustsec.org/advisories/RUSTSEC-2026-0542.html).
These advisories report unmaintained packages, with no specific vulnerability or
patched version. All three were already present before the Meerkat 0.9.18 upgrade.

Recall's public and persisted timestamp types use `chrono`, which SurrealDB also
depends on. Meerkat's schedule implementation uses `chrono-tz`; its configuration
uses `humantime` through `humantime-serde`. Removing these requires coordinated
API, serialization and time-zone compatibility work. Reassess the exceptions at
the next Meerkat or SurrealDB upgrade, or by 2027-01-01, and immediately if a
vulnerability advisory appears. Other advisories remain enforced.
