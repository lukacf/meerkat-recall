
# SurrealDB Core

This crate is the internal core library of SurrealDB.
It contains most of the database functionality on top of which the surreal binary is
implemented. This crate should not be used outside of SurrealDB itself. 
For a stable interface to the SurrealDB library see [the Rust SDK](https://crates.io/crates/surrealdb)

## Recall filtered vector result backport

Recall filters every record associated with an identical-vector HNSW element
before applying the KNN result limit. Upstream 2.6.5 admitted an element when
any associated row matched, but counted all associated rows before final
filtering. A foreign-space or deleted twin could consume the only publication
readiness slot and return an empty result despite an indexed eligible row.

The patch retains graph traversal, distances, query predicates, and the original
source publication's exact vector/envelope checks. It also applies to ordinary
filtered searches. `vendor/verify_recall_surrealdb.py` records the two added
upstream deltas (`idx/planner/checker.rs` and `idx/trees/hnsw/index.rs`). The paired
server copies this same core through its existing byte-exact vendor overlay;
external unpatched servers do not acquire this fix from a client upgrade.

Regressions live in Recall storage tests:
`native_publication_index_probe_ignores_deleted_and_foreign_vector_twins` and
`filtered_hnsw_twins_do_not_consume_any_requested_result_slots`.
