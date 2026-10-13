//! Reusable pipeline worker entry points.

// Preserve the concrete Send future types in the nested worker tests.
#![cfg_attr(test, recursion_limit = "256")]
#![allow(missing_docs)]

#[allow(dead_code)]
#[path = "main.rs"]
pub mod runtime;

pub use runtime::config;
pub use runtime::run_pipeline_with_client;
pub use runtime::run_pipeline_with_shutdown;
