# ADR-001 authentication context prerequisite

This change implements an authentication prerequisite on top of `feae4da`. It
does not implement governed Elephant resource execution or complete ADR-001.
Elephant remains the final authority for its resources.

## Implemented boundary

`elephant-auth` is the token-verification owner used by HTTP and MCP. Its opaque
`VerifiedCallerContext` retains the verified issuer, audience, subject, expiry
and other legacy claims. HTTP request extensions and MCP `ToolContext` retain
that context alongside the existing ABAC projection. Skip-auth and trusted
embedding constructors produce no verified context. The context cannot be
deserialized or constructed from a public ABAC principal.

Trusted composition can install exact issuer/audience-to-domain mappings. A
mapped legacy identity requires independently configured issuer and audience,
an exact singleton token audience, and a valid subject. No mapping is inferred
from a token, deployment name or default domain. Existing unmapped legacy
authentication remains available with its existing local policy semantics,
including explicitly configured HS256, HS384 and HS512 algorithms.

The separate exchange syntax validates a configured HS256 key and key ID,
distinct token type, exact issuer/audience/domain, required qualified identities,
declared time bounds, and equality with a separately observed operation. It
rejects unknown fields and profiles. Clock uncertainty shrinks the accepted
interval: its earliest possible time must reach activation and the claimed
observation, while its latest possible time must precede both expiry bounds.
The operation includes transport, action,
resource authority, space, resources, destination, audience, purpose, identity
and an exact argument-byte digest. The qualified principal wire subset is tested
against a pinned generated Meerkat schema fixture.

`AuthenticatedExchange` proves only these checks at verification time. Its
delegation references and revocation observation are authenticated claims,
not live authority. It has no dispatcher consumer and is not a serialized
permit. Current authenticated HTTP/MCP resource surfaces refuse its token type.
Explicit trusted and skip-auth modes retain their existing local semantics and
create no verified context or governed authority.

New protected exchange contracts and identity fields redact `Debug` output.
The legacy public `Claims` diagnostic representation is preserved solely for
compatibility; applications must continue treating it as protected metadata.

## Denial and compatibility evidence

- Auth unit cases cover wrong issuer, audience, signature and mapping; malformed
  qualified identities; absent or extra fields; unsupported versions/profiles;
  every operation-binding field; and expiry/observation bounds. Signed hybrid
  tokens cannot shed exchange fields through a legacy JWT type or absent type,
  even when a reserved field is null. Ordinary legacy extensions still work.
- The HTTP middleware test counts handler entries and checks a private response
  canary. Only the valid mapped legacy token enters the handler. A public
  principal or skip-auth configuration cannot manufacture verified context.
- The MCP transport test uses a real embedded repository, a loopback HTTP
  server and the production dispatcher on all four MCP profiles. A valid
  legacy token reads the stored canary; invalid identity/expiry and a validly
  signed exchange token return no canary. The exchange fixture first passes its
  own verifier, so refusal does not depend on an invalid signature or payload.
- A separate production dispatcher construction test retains two distinct
  caller contexts, including issuer, audience, expiry and domain.

The HTTP canary is a test handler behind production middleware. The MCP canary
is a production `get_entity` operation against embedded storage. These tests do
not establish SurrealDB policy-generation or transaction semantics.

## Commands and results

All Cargo commands use the repository wrapper. The separate `auth-contract`
lane isolates small contract checks from the cold API/MCP build.

| Command | Result |
| --- | --- |
| `RUST_LANE_ID=auth-contract ./scripts/repo-cargo test -p elephant-auth --lib` | 15 passed |
| `RUST_LANE_ID=auth-contract ./scripts/repo-cargo clippy -p elephant-auth --all-targets -- -D warnings` | Passed |
| `./scripts/repo-cargo test -p elephant-auth -p elephant-mcp -p elephant-api --lib --no-fail-fast` | Passed: API 267, auth 15, MCP 209 |
| `./scripts/repo-cargo clippy -p elephant-auth -p elephant-mcp -p elephant-api --all-targets -- -D warnings` | Passed |
| `./scripts/repo-cargo fmt --all -- --check` | Passed |
| `make docs-check` | Passed, 56 files, no errors |
| `bash scripts/verify_active_surface.sh` | Passed |
| `bash scripts/verify_legacy_terms.sh` | Passed |
| `PATH="/opt/homebrew/bin:$PATH" bash scripts/verify-version-parity.sh` | Passed |
| `git diff --check` | Passed |

Final package logs are `/tmp/elephant-federation-tests-verified.log` and
`/tmp/elephant-federation-clippy-final.log`. Independent bounded source review is
recorded in `/tmp/adr-001-elephant-auth-slice-review.md`; the final package run
also passes its hybrid-token HTTP/MCP regression cases.

Four test-only annotations join existing process-environment serial groups:
three managed-pipeline runtime tests and the capability-response shape test.
Their former interference was observed in full package runs. New auth tests do
not mutate process environment. No production runtime behavior changes with
these annotations.

## Remaining owners and gates

No production trust mapping, credential or endpoint was installed. This slice
does not import issuer claims as Elephant clearances, invent a security epoch,
default missing authority to allow, or advertise a governed deployment profile.

Before governed dispatch is available, the composition still needs canonical
resource/policy generations with all-writer coverage and antirollback behavior;
current delegation and trust validation; same-domain participating fences or
explicitly negotiated independent-domain freshness; exact request observation
from a trusted adapter; replay and entry ownership; mandatory durable evidence;
and protected result/dependency envelopes through final disclosure. The
exchange's signed references cannot substitute for those owners. Audit is not
execution or replay authority.

The prerequisite's full workspace gate exposed a retention retry/archive race
that also reproduces with a freshly compiled clean baseline `1aefaad` storage
binary. It is an existing storage defect requiring a separate repair, not a
passing gate or an assumed flake. Exact logs, executable hashes, commands and
the separate API test-interference diagnosis are preserved in
`/tmp/adr-001-elephant-gate-failures.md`. This authentication slice does not change
that retention owner or establish future governing transaction semantics.

Separate authorized relabel/declassification and other mutation owners remain
open, as recorded in the prerequisite report. Repository-wide required gates,
independent review, integrated adversarial acceptance and CI remain separate
from these package results. No deployment, push or PR was performed here.
