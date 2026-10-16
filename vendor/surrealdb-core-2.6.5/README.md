
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

The same bounded HNSW backport persists duplicate-vector document membership
when `Ids64` uses an in-place bitmap. Both insertion above nine equal-vector
records and deletion while the bitmap stays above eight must reach storage;
otherwise live records disappear from search or deleted record IDs consume
result slots. The filtered-twins storage regression also verifies all 27 live
rows and the three survivors after physically deleting the 24 foreign rows.
Deletion also checks membership before changing the set: an old damaged index
may lack the deleted row, and removing that absent ID must not discard a live
member of a small set. Existing missing memberships require an explicit index
rebuild or a supported fresh intake; upgrading does not synthesize old entries.


Recall's `recall::hnsw_coverage` is a bounded read-only physical-membership check
for historical coverage completion in the caller's transaction. It requires an
ordinary, ready, nondeferred F32 cosine index on `vector`, verifies native
HI/HD identity roundtrips, HV duplicate membership, the exact HE vector,
HS entry/state, and HN entry/per-element layer-zero membership. It does not use
an approximate query or a cached graph. Legacy HL-only graphs must first pass
through the normal explicit reindex writer; certification does not migrate them.
The storage caller separately compares every complete vector row, input record,
publication authority and configuration in that same completion transaction.
Calls are bounded to 100,000 rows and 16,777,216 components; a larger partition
is checked in bounded chunks within the same transaction. These are physical
membership checks, not a recall-quality guarantee for arbitrary graph corruption.
Engine controls exercise twelve identical vectors, normal deletion, missing
HI/HD/HV/HE/HS/HN keys, wrong reverse identity, missing index, duplicate inputs,
wrong vectors and nonzero f64 input that becomes zero in the actual F32 index.
