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

## Tracked upstream maintenance exception

The pinned Meerkat 0.9.0 SDK uses `meerkat-skills` → `serde_yaml` →
`unsafe-libyaml` 0.2.11 to parse and serialize skill frontmatter.
[RUSTSEC-2024-0449](https://rustsec.org/advisories/RUSTSEC-2024-0449.html)
classifies `unsafe-libyaml` as unmaintained and lists no patched version. The
security audit permits this one advisory while that SDK remains pinned;
this does not establish memory safety or suppress other vulnerability advisories.

The Meerkat owner confirms that its 0.9 SDK also retains this dependency chain;
the version upgrade alone will not resolve this maintenance advisory. The Recall
release owner must re-evaluate this exception on SDK upgrades and remove it when
Meerkat adopts a maintained safe Rust parser. Replacing the parser inside the
pinned SDK requires a separate compatibility change; a lockfile upgrade cannot
remove it.
