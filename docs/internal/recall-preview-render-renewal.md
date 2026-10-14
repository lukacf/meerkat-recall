# Render execution after lease renewal

The 509 outer worker renewed its execution lease and discarded the returned work
item. It then dispatched the pre-renewal snapshot. Render input custody compares
that original snapshot against the persisted work row, including the lease expiry;
it correctly refused the mismatch before calling the built-in text renderer.
The Mac retrieval report observed intake and metadata progress followed by three
render failures, zero chunks and `failed_retryable`. This source diagnosis fits
that report; it is not an executed confirmation of the repaired path.

The worker now dispatches the exact row returned by `renew_execution_work`.
That helper retains the original identity, attempt, input and security envelope,
allowing only the fields the existing queue renewal actually changes: update time,
lease expiry, and clearing retry scheduling/error bookkeeping. Any other difference
is a lease conflict. Render custody and source publication guards are unchanged.
The returned row is used consistently for the work run, provider usage context and
execution, with the original attempt fence preserved.

Two authored regression cases use real embedded and native in-memory storage,
the same live renewal helper and the actual deterministic text renderer. They
retain the negative control that a pre-renewal snapshot produces no revision,
check the exact rendered text from the renewed snapshot, and refuse actual changed
input and lost ownership. These cases isolate the render custody seam with a
retained direct source. They do not prove accepted-source publication, installed
service success, Graph extraction, provider relevance, latency or memory bounds.

On 509, `render_doc failed` is the outer anyhow context. Both the warning's `%err`
and persisted WorkRun `err.to_string()` omit its cause; changing log filters does
not restore information absent from those fields. The worker now records a static `failure_context_code` in failed WorkRun metrics
and a `failure_context` warning field. Only an exact recognized custody error maps
to `original_work_authority_changed`; other nested errors remain unclassified.
A negative control verifies that suffixes containing private input or credentials
are not surfaced. Raw cause chains and public operation error codes are unchanged.

After the actual cause is repaired, an authorized operator can retry the existing
failed render work via `POST /v1/work-items/{id}/retry`, or `/mcp/full` tool
`retry_work_item` with `{"id":"work_item:..."}`. Current `read:work_item`,
`write:work_item` and record authorization apply. The original triage id is not the
failed render id. This retains attempt history and the original source operation;
retry acknowledgement does not mean publication completed. Same-key intake remains
an idempotent lookup, while a fresh revision/key plus `expected_generation` is a
replacement acceptance. No retry, model call or new authorization follows merely
from this documentation.
