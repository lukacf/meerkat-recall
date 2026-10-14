# Original Watch fixture authority

The cancellation fixture now transitions its real Running Work with the lease
token retained from the original claim. It then exercises the existing
acceptance-to-document gap and requires both document staging and Watch-check
persistence to reject that cancelled attempt. The head and original source
receipt remain unchanged.

The missing-delegation fixture now creates an original Runnable Work without a
delegation, claims it through the attempt repository and records its matching
WorkRun and attempt fence. It no longer edits the payload of an already Running
item. Capturing a source producer must return the standing-authority refusal,
despite the current Watch having a valid registration. The claimed Work and
WorkRun remain unchanged, and the proposed source revision remains absent.

The native multiple-document-readset fixture now gives its two independent
sources different external identities before acceptance. The common source
request helper otherwise assigns both the same external identity, so the second
`Absent` request correctly conflicts. Each actual accepted source ID is checked;
the two original document readsets and all mutation refusals remain intact.

These repairs change fixture setup, not production guards. The four separately
reported native standing-authority failures depend on the configuration policy
comparison owned by recovery; this checkpoint does not claim those pass.
Direct Rust formatting and diff checks were run. No Cargo or Rust tests were
executed locally; the original both-backend tests remain the central regression
oracles.
