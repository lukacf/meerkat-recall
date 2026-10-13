//! Execute the exact vendored filesystem helper and its crash-cut controls in
//! the normal workspace test lane. Dependency crate unit tests are otherwise
//! absent from workspace nextest; this includes the original source, not a copy.
//! Engine admission and native-state preservation remain separate adapter tests.

#[cfg(unix)]
#[rustfmt::skip]
#[path = "../../../vendor/surrealdb-core-2.6.5/src/kvs/physical_generation.rs"]
pub(crate) mod physical_generation;
