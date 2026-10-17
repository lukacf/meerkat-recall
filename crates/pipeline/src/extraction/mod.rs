//! Extraction framework for document assertion extraction.
//!
//! Per spec: Extractors transform document nodes into raw assertions
//! that are later resolved to entities.
//!
//! The extraction architecture follows the plugin model:
//! - Extractors declare manifests specifying capabilities and constraints
//! - Core provides the pipeline stage and storage
//! - Extraction logic is implemented by extractor plugins

mod action_disposition;
pub use action_disposition::*;
mod allowed_tools;
mod extractor;
mod plan;
mod support;
mod validator;

#[cfg(feature = "meerkat-extraction")]
mod agentic_extractor;
#[cfg(feature = "meerkat-extraction")]
mod failure;

pub use allowed_tools::*;
pub(crate) use extractor::ExtractionRepairContext;
pub use extractor::*;
pub use plan::*;
pub use support::{
    verify_assertion_support, AssertionSupportVerifier, SupportCandidate, SupportVerdict,
    SupportVerification, SUPPORT_VERIFIER_VERSION,
};
pub use validator::*;

#[cfg(feature = "meerkat-extraction")]
pub use agentic_extractor::*;
