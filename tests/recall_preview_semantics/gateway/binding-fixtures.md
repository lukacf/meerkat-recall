# Qualification binding fixture

This is an unapproved configuration draft, not an installed configuration or
permission to execute models. binding-fixtures.json uses the existing
SpaceProcessingConfig shape for two named spaces on both exposed backends.
The account identity and operator deployment version are explicit placeholders.

Each gateway role has its own connection, profile and exact credential reference
RECALL_GATEWAY_<gateway binding id>. The generative model remains the model in
D's proposal. Document and query embedding share one active profile/partition and
one combined embedding capability. Promotion/identity and wiki operations use
their explicitly grouped quota; this does not enforce separate subrole limits.
Triage, media and reindex receive no new approval in this qualification subset.

The physical endpoint is the isolated gateway at 127.0.0.1:43129/v1, not the
upstream provider. Consequently its vector partition differs from a direct
upstream configuration; no direct-provider parity is claimed. Only the gateway
may hold the real provider credential. The worker resolver requests the explicit
capability reference and has no conventional OpenAI-key fallback.

Connected Rust regression source lives in
crates/pipeline/src/provider_dispatch_gateway_tests.rs, included in the active
model dispatch test module. It persists the existing typed configuration through
the real repository and checks each operation's resolution, current endpoint,
exact capability selection, repeated physical HTTP authorization identities,
denial before credential attachment for foreign endpoints, current revocation,
named-space isolation and shared query/document partition. The embedding guard is
the one called again inside the physical retry loop. Tests authorize requests
without sending HTTP or reading process credentials.

Those tests are authored, not locally executed. This is not proof of installed
SDK wire bodies, the isolated namespace, tokenizer bounds, reasoning settings,
model output or quality. In particular, the current extraction request's 64K
output limit exceeds the qualification proposal's 16K limit; the fixture does not
silently change that request. The reviewed proposal delta now explicitly permits G's qualification-only 16K
output-cap transform; product defaults remain unchanged. The fixture pins D's
updated delta from commit 7788e499. Installed wire verification and SDK opaque
continuations still require qualification before execution authorization.

Before use, D/C must review the exact mapping and gateway proposal delta; approved
account/version values must replace placeholders; the runner must persist and
read back each named configuration before accepting source operations. The final
manifest must bind the complete reviewed proposal, configuration, artifact and
gateway hashes. No settings, secrets or provider requests are installed by this
fixture.
