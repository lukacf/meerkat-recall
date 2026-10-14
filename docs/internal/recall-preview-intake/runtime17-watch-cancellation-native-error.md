# Original watch cancellation result diagnostics

Runtime17 at `4979bb883adde3d8fa884d6e820a6a1a1a76dfc4` reports the native source-watch cancellation test failing its exact `Conflict(stale_attempt)` assertion after a real original claim was cancelled. The assertion did not include the actual returned error. This checkpoint retains the exact error predicate and every original zero-effect/head/receipt/work assertion, and includes the original staging and observation results in their failure diagnostics.

Read-only source tracing identifies a likely type-mapping mismatch: both native staging and observation transactions append the original attempt fence, whose native guard throws unprefixed `stale_attempt`. Their shared `publication_error` mapper only converts a `publication:` prefix to a typed conflict. The embedded attempt fence directly returns `Conflict(stale_attempt)`. This inference is not a measured native error result; the original run omitted that value. The shared publication/attempt owners have received the exact source trace for a bounded repair. Intake does not edit their shared mapper or authority without custody agreement.

Direct Rust formatting and Git whitespace checks only. No Cargo/build/test/clippy or runtime/provider execution. The original backend tests remain the central validation oracles, with preserved strict cancellation behavior.
