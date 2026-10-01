# Shared security contract vectors

`v1/negotiation.json` is an exact, unmodified copy from Meerkat commit
`0955991d5`, path `crates/meerkat-authorization/conformance/v1/negotiation.json`.
Its SHA-256 is
`02777b7a941e70f226997922f5195eaf7beae30d01943f0c9cb577e301005849`.
The security contract version is independent of either repository's release.

`elephant-auth::contract` independently implements these compatibility rules.
The integration test evaluates all 14 vectors through that implementation and
checks the corpus digest. No Meerkat crate or evaluator is linked by this test.
The expected outcomes are literal shared fixtures, not evaluator-generated data.

`v1/information-flow.json` is also byte-for-byte from the same Meerkat commit,
path `crates/meerkat-authorization/conformance/v1/information-flow.json`, SHA-256
`8498e08af750fa545a2a801369345ec99c35eeccc89ffd1ea76105413133548c`.
Its original evidence-status field describes that upstream candidate; it is
preserved verbatim and is not an execution receipt for this consumer.

`elephant-auth::restrictions` and `elephant-auth::information` implement the pure
contract independently. Six tests cover all 26 normalized cases: nine exact
set intersections, three lifetime intersections, five delegation depths, two
complete attenuation examples, three dependency unions and four derivation
sequences. The sequences cover all eight ordinary derivation categories.
Additional checks exercise strict raw decoding, source-policy coverage,
protected Debug output and finite intersection laws. The consumer uses literal
expected values from the shared file and does not invoke Meerkat code.

The source/processor/audience principal carriers use the already qualified-only
Elephant exchange claim. This is the governed claim subset, not a promise to
accept Meerkat's legacy unqualified identities. Restriction and provenance
construction, decoding and pure composition establish no authority.

This is normalized pure-contract evidence. No active endpoint advertises
governed support. It does not establish issuer trust, resource-policy
equivalence, current grants, truthful dependency inventory, enforcement at real
source/provider/recipient boundaries, mandatory receipts or a protected sink.
Elephant resource-domain differential vectors and negotiated dispatch refusal
before actual sinks remain required; ADR-001 A003 remains open.

Update both repositories' corpus copies together for an agreed protocol change,
retain the exact upstream revision and digest here, and rerun both independent
consumers. Do not weaken an expected refusal to make an implementation pass.
