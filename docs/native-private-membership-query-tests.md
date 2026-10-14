# Native private-membership query controls

Two `elephant-storage` unit-module controls execute the exact five-statement
`QUERY` embedded in the qualification helper from commit
`e9451c37444890e941b5054fa67a5d2b7819d4c3`. The tests include the helper source and
verify both its full-file SHA-256 and the extracted literal's SHA-256. They do not
copy or rewrite the SQL, import Python, or create another Cargo test target.

Both controls use the native SDK Mem engine and the complete production
`039_private_reference_fence.surql` migration. The populated control publishes
membership through the existing original-journal projection writer and reference
normalizer. It covers two named spaces, Unicode in a space, a durable union, and
idempotent re-publication. No test-only private schema or replacement writer is
used.

The controls inspect all five actual native results: database/table definitions,
integer counts before and after, complete rows, native `Thing` IDs with string
inner IDs, native string fields, boolean type witnesses and complete key lists.
A repeated identical inspection checks that this fixed query is read-only.

Exact selectors:

- `private_membership_native_tests::native_private_membership_fixed_query_empty_production_schema_has_complete_native_shape`
- `private_membership_native_tests::native_private_membership_fixed_query_populated_original_projection_preserves_native_witnesses`

These are authored source awaiting central execution. Mem does not establish the
paired HTTP root-authentication gate, installed capture completeness, or physical
source deletion. Those remain the original qualification phases.
