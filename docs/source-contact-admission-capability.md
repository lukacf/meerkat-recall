# Original contact admission and prepared-plan availability

`StoredOperation.contact_digest` is the existing original admission authority. Only source acceptance with a validated server-prepared contact seal writes a nonempty digest, in the same transaction as the source, immutable revision, head, operation and prepared contact attachment. Generic acceptance writes explicit null and cannot replay a sealed key as plain. Callers cannot supply the digest; lifecycle operations do not create contact capability. The exact original operation binds its space, physical source, generation, revision, operation ID and input fingerprints.

`accepted_contact_capability_for_publication(space, original_fence)` returns a nonserializable `AcceptedContactCapability` descriptor after checking the exact current fence and original operation. Its digest/publication/space getters contain no historical graph payload. A missing legacy digest field is unknown; explicit null means verified plain admission. This descriptor says nothing about the availability or validity of a historical prepared plan.

Erasure of another source can legitimately remove a prepared plan containing that source's old before-images. `SourceAcceptanceRetry::ExistingContactPlanUnavailable { operation, capability }` distinguishes this from both new intent and original plain admission. Strict prepared-plan getters refuse `accepted_contact_plan_unavailable`. `matching_source_receipt(request)` provides an authorized adapter a read-only semantic retry receipt without rebuilding that plan, changing its status, staging content or enqueueing work. Adapters must suppress processing when the historical plan is unavailable; a fresh operation needs new explicit CAS and authorization.

Fresh reprocess and restore helpers use the original descriptor to authorize preparation from surviving immutable source input under current scopes and policy. They never reconstruct the original plan or infer capability from editable metadata. Retraction predecessor verification therefore checks the original accepted operation without requiring an old payload-bearing contact attachment.

Both-backend source tests fault-remove a real accepted attachment, retain exact original descriptor/receipt, refuse original plan replay, permit freshly authorized new admission and reject a missing legacy digest marker. Full shared-source physical erasure and survivor maintenance are separate F/E qualification oracles. No local Cargo or Rust tests have run.

For a fresh intent reusing the same immutable revision, `acceptance_source`
returns that revision's exact retained Source after matching semantic input. The
caller prepares its contact seal from this preview. Observation/creation/runtime
bookkeeping may be ignored by revision idempotency, but it cannot cause a new
seal to bind a different Source than the one retained for finalization and
inverse projection. Contact acceptance checks both the proposed Source and the
retained revision before writing. Direct callers supplying an inconsistent seal
receive a conflict. Two real admission tests cover changed observation/creator
bookkeeping, immutable snapshot reuse, direct mismatch refusal and semantic
metadata conflicts; they do not fabricate completed publication.
