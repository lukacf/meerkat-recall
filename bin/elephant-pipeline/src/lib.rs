//! Reusable pipeline worker entry points.

#![allow(missing_docs)]

#[allow(dead_code)]
#[path = "main.rs"]
pub mod runtime;

pub use runtime::config;
pub use runtime::run_pipeline_with_client;
pub use runtime::run_pipeline_with_shutdown;
