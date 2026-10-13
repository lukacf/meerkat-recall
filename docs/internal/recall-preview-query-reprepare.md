# Scoped search preparation under canonical maintenance

The completed source108 provider100 original contains one semantic search HTTP
503 (`01071.response.json`) with the public embedding-authority-change message.
The request overlapped a maintenance sweep. Both epoch and binding changes used
that same public message, so timing alone does not establish which branch fired.
The original query remains failed; this repair is not evidence of qualification.

The vector preparation path now distinguishes an epoch conflict privately from
terminal provider, configuration, authorization, readiness, and storage failures.
Only the epoch conflict may cause one internal reprepare of the whole search.
The second preparation uses the same request, ranking mode, limits, filters and
source revision constraints. It creates a new assembly, candidates, ranks, vectors
and coverage. A second conflict returns the existing public failure. There is no
public error-schema change or message-string retry classifier.

The first selected binding remains pinned, including its configuration identity,
profile, connection, account, credential generation and embedding policy. Both
preparations reauthorize against that exact binding, and the final response
rechecks it. Every physical model call still uses the governed provider and its
original dispatch observer; an abandoned old-epoch fill is not cached. A cache
hit stores only a vector of the caller's exact query and confers no read authority.

One existing storage activity lease spans both preparations, including direct
in-process callers. Nested HTTP/MCP activity reuses the lease. Restore and
physical erasure cannot complete while it is held; explicit readiness checks
reject pending exclusive maintenance even though nested storage reads are allowed
to drain. Source heads and authorization snapshots from the first preparation
remain negative fences before and after a retry, and the shared read fence retains
its source envelope and definition observations. A changed source is terminal,
not permission to switch revisions during the request.

No new timeout, spawned search task, budget increase or detached retry is created.
The caller's original future, cancellation and deadline encompass both attempts.
Regression sources cover governed physical requests, semantic and hybrid whole
preparation, repeated churn, binding revocation, credential rotation, profile
replacement, wrong space, cancellation and queued restore/physical erasure.
The two-backend indexed scenarios also retain exact original source coordinates
and prove distinct dispatch admission, measured usage and completion for both
physical calls. Withdrawal of the observed source terminates without a second
call. Existing low-level epoch rejection and failed-fill cache tests remain unchanged.
