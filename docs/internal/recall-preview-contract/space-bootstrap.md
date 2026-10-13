# Creating a classified named space

The CLI's explicit `service-token issue --bootstrap-space NAME` profile resolves
initial space creation before a live service grant can exist. The operator must
already possess the deployment's signing secret. It authorizes one named space's
HTTP administration for at most one hour; it is not a data token or an MCP token.
A global administration token remains management-only and has no custom policy
clearances. Normal `--space` tokens continue to require a current active grant.

Use the existing local provisioning commands with private key and token files.
`service-token bootstrap --secret-file PATH` creates a new signing key and refuses
to overwrite an existing one. An existing deployment must use its existing key.
Do not log keys or bearer strings. Namespace-isolated qualification uses only the
bridge-generated synthetic signing key; it does not import deployment secrets.

Write an explicit clearance array to a local file, for example:

```json
[{"policy_id":"household-policy","max_level":3,"labels":[],"handling_allow":[]}]
```

Issue the bootstrap token without ordinary `--space`, `--global-admin`, or
`--read-only` flags:

```sh
meerkat-recall service-token issue \
  --secret-file /private/recall/signing-key \
  --bootstrap-space household --subject local-bootstrap \
  --scope read:space_config --scope write:space_config --scope manage:grant \
  --clearances-file /private/recall/household-clearances.json \
  --expires-in 900 --output /private/recall/household-bootstrap.jwt
```

To declare or change explicit processing configuration, also supply the
`manage:provider` scope. It does not admit reindex/job-producing routes for this
bootstrap profile. Provider approvals, credentials and generation checks remain
separate requirements. No provider work is approved by issuing a token.

The signed claims include `bootstrap_space:"household"`,
`space_id:"household"`, and `spaces:["household"]`. All three must agree exactly.
The original `exp - iat` must be 1 through 3600 seconds and each verification
requires `iat <= now < exp`. The scope ceiling is exactly
`read:space_config`, `write:space_config`, `manage:grant`, `manage:provider`.
Only explicitly supplied scopes are granted. Wildcards, data scopes, implicit
security-level/label clearance, subjects and purposes are refused. Clearances
must be nonempty and explicit. An explicitly null/malformed bootstrap field is
rejected; absence preserves the existing ordinary/legacy token contract.
Ordinary and global issuance functions reject bootstrap claims.

With this bearer and `X-Space-Id: household`, call the existing routes in order:

1. `POST /v1/spaces` with the exact name and classification. Example offline
   configuration (the fake provider is solely an offline fixture):
   `{"name":"household","security":{"policy_id":"household-policy","level":3,"labels":[],"handling":[]},"embedding_provider":"fake","embedding_model":"fixture","embedding_dims":8}`.
   Live qualification supplies its separately reviewed actual provider settings.
   Require HTTP 201 and read back the exact space and security envelope.
2. `PUT /v1/spaces/household/grants/homecore` with
   `{"active":true,"scopes":["read:source","write:source","read:doc","read:entity","read:assertion","read:space_config"],"expected_generation":null}`.
   Require HTTP 200 and retain the returned nonempty grant generation. Existing
   grants require their exact generation for replacement or revocation.
3. Issue the ordinary data token using `--space household --subject homecore`,
   explicit data scopes and the matching explicit clearances. It contains no
   bootstrap claim. The intersection of the signed scopes and current grant
   applies to every ordinary dispatch. Issuing a token does not create a grant.
   Supply only the scopes needed by the selected public operations; additional
   operations can require additional explicit scopes.
4. Retire the bootstrap token after setup. Its expiry is independent of the
   service token. Repeat the named bootstrap flow for `business` with its own
   explicit `business-policy` level 7 clearance and configuration; do not lower
   the policy or infer it from a space name.

The complete bootstrap HTTP admission set is:

| Method | Matched route | Target authority |
| --- | --- | --- |
| GET, POST | `/v1/spaces` | Exact selected space; POST body name must also match |
| GET, PUT | `/v1/spaces/{space_id}` | Exact signed, path and optional header space |
| GET, PUT | `/v1/spaces/{space}/grants/{subject}` | Exact signed, path and optional header space |

GET collection requests return only the selected space. Unknown routes, other
methods, data routes, operation polling, runtime management, processing/reindex
and all MCP ingress reject this profile. Existing runtime readiness, recovery
barriers and read-only restrictions still apply. Every admitted handler retains
its operation scope and proposed/current record ABAC checks. A bootstrap token
with insufficient clearance cannot create, read or update a classified config,
and cannot manage its grants. Existing space-security relabeling remains absent
from the update DTO. The bootstrap profile supplies no `bypass_abac` behavior.

Source regressions: `crates/auth/src/bootstrap.rs` verifies separate issuance,
external signed malformed profiles and exact original lifetime; HTTP
`bin/elephant-api/src/bootstrap_tests.rs` exercises classified creation, current
and proposed ABAC, body/header disagreement, actual data reads with ordinary
service grants, revocation and collection isolation; the MCP test
`explicit_space_bootstrap_is_not_mcp_authority` rejects the profile; CLI
`classified_space_bootstrap_is_explicit_short_lived_and_not_an_ordinary_token`
checks actual argument exclusivity, private output and shared verification.
These are authored source tests until covered by a central validation receipt.
