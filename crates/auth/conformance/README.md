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

This proves only normalized version/profile/semantic/obligation compatibility.
No active endpoint advertises governed support. It does not establish issuer
trust, resource-policy equivalence, current grants, source dependency handling,
mandatory receipts or a protected sink. The separate information-flow corpus
and Elephant resource-domain differential vectors still need a consumer and
executed acceptance; ADR-001 A003 remains open.

Update both repositories' corpus copies together for an agreed protocol change,
retain the exact upstream revision and digest here, and rerun both independent
consumers. Do not weaken an expected refusal to make an implementation pass.
