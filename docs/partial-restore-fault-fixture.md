# Authenticated partial restore failure fixture

The SurrealDB partial-import regression uses one immutable authenticated backup bundle across both attempts. The backup contains one seeded observable row and its exact native origin proof. After signing the bundle, the test deletes the live row. The unchanged SQL imports the original export and ends with a conditional failure controlled by an owned loopback test server. The server returns false once, then true. No temporary row or other unauthenticated value is injected into native history.

The test uses the existing private native constructor, which enables only its bound ephemeral loopback port. The server accepts exactly two requests under one sixty-second deadline and bounds request headers. Production constructors and restore policy are unchanged.

The test requires the injected import error, the restored original row as evidence of committed mutation, the exact original digest in the durable restore marker, and ordinary-query refusal. Resumption must use unchanged bundle bytes, restore exactly the original values, clear the original marker and restore readiness. It never changes the digest, bypasses authenticated native post-floor coverage, or clears the journal to permit retry.

This is regression source only, not evidence of successful execution. Production authentication, destructive admission, erasure replay, readiness and retention rules are unchanged.
