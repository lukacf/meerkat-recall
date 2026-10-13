# Original85 document fixture expectations

The shared API document fixture submits single-line raw text without trailing
whitespace. The active `elephant.renderer.text` version `1.0.0` appends a final
LF to this input. Comparing a published Doc directly with the submitted raw
text therefore rejects correct rendering before the endpoint assertions run.

The fixture now checks the original accepted source revision against the exact
submitted raw text, and checks that this entire immutable revision is unchanged
after the actual dispatcher and finalizer complete. Its rendered expectation is
the original fixture text plus a literal LF. Explicit input assertions keep this
bounded expectation restricted to the single-line fixtures it describes.
The GET and restored-publication assertions use independent literal rendered
expectations. The immediate restore response still expects the original raw
template before rendering.

The pending-read refusal, actual governed embedding transport, original receipt
completion, generation/revision identity, security and subjects checks remain.
No production renderer, publication guard, endpoint behavior or test selection
changes. This source checkpoint addresses the six shared-helper failures in
Original85 early01/early02 at frozen source
`a7e46c17fad0be72b2d483afafe8d1675356d65e`.

Validation: direct rustfmt and `git diff --check` only. No Cargo, Rust tests,
services, runtime or provider execution was performed by this worker; the
central validation owner must execute these original tests.
