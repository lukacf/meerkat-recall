# Explicit intent when reusing an immutable revision

This is the source contract for the A → B → A repair in integrity `6ba0bcd45945b6ead77b9ebd76702017b561c474`, finalization `b04892f6a8fd1e748f85d1e8425ee4c29dd32fac`, and search coverage `cc04dbce601e9e83bd46bd56ddbe14c7e955e910`, with recovery's existing accepted-fence document identity. It supersedes the *current-gap* paragraph in [HomeCore C6](homecore-followup.md) once that dependency group is integrated. It is not a passing runtime or provider qualification receipt.

Immutable revision identifies semantic input; operation/generation identifies an acceptance intent. Reusing identical content under an old revision is supported without inventing a new content hash. To deliberately return to A, send the identical A semantic input with a **fresh explicit `Idempotency-Key` and the exact current `expected_generation`**. The operation and generation are new; the source's physical incarnation stays the same. The immutable original revision payload and its first-acceptance metadata stay unchanged. Work and document mappings are bound to the new acceptance fence, so an old A worker cannot publish into the new A operation.

| Request | Result and head effect |
| --- | --- |
| Same previously accepted key and same semantic input, even with stale/omitted generation | HTTP 202 with that key's historical operation; no new acceptance and no head change |
| New key, known identical revision, exact current generation | HTTP 202 with new operation/generation; new processing is pending until its own completion |
| New key, known revision, stale or omitted generation on an existing source | HTTP 409 `conflict_generation`; no head change |
| New key, known revision, changed semantic input | HTTP 409 `conflict_generation`; immutable payload cannot be replaced |
| Same key, conflicting semantic input | HTTP 409 `conflict_generation` |
| Retry of the newly accepted intent key | HTTP 202 with the same new operation; no duplicate acceptance |

These rows retain source-envelope authorization and lifecycle checks. They do not grant automatic revival of retracted/erased content or bypass explicit lifecycle flags. A changed observation time alone is not changed semantic input.

A consumer may derive its explicit intent key from the tuple `(space, external_source_id, revision, expected_generation-or-create-marker)`, for example using a hash over an unambiguous length-delimited or structured encoding. **Persist the exact key and expected generation before the request, then retry them unchanged after an uncertain response.** Do not recompute a fresh key from a newer head during an ordinary retry: that would express another new acceptance. The key must meet the existing 512-byte bound.

The server's omitted-key fallback remains stable over `(space, external_source_id, revision)`. It therefore expresses the original revision retry, not a deliberate reacceptance. Explicit new intent is required for the revert. An old generation with an already accepted key still replays by design; only an old generation with an unseen key conflicts.

Poll the returned operation and current head. Historical operation completion is not completion of the new acceptance. For a requested revision equal to the **current desired revision**, coverage follows that current operation, including when an older published A has the same revision string. Until the new A operation completes, coverage is pending/running/failed as appropriate and `partial:true`. Unconstrained coverage for that current desired revision follows the same rule. Earlier published A evidence remains readable throughout; partial coverage does not hide it or claim that the new operation completed.

An explicitly requested older published revision **different from the current desired revision** may have complete historical coverage while its replacement is pending. A head whose desired or published revision equals A therefore does not alone establish new-intent completion. Consumers requiring the new acceptance to finish must inspect its operation and constrained coverage. These rules preserve separate observations of source acceptance, current-instance completion, and readable evidence.
