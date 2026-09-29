//! Event outbox and streaming for Elephant.
//!
//! This crate provides the durable event stream:
//! - `system_event` writer
//! - Cursor-based streaming
//! - Sequence number allocation

pub mod error;
pub mod producer;
pub mod stream;
pub mod writer;

pub use error::{OutboxError, OutboxResult};
pub use producer::{EventOperation, OutboxPayload, RecordRef};
pub use stream::StreamCursor;
pub use writer::OutboxWriter;

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::mixed_attributes_style,
        clippy::similar_names,
        clippy::type_complexity,
        clippy::float_cmp,
        clippy::const_is_empty,
        clippy::expect_used,
        clippy::panic,
        unused_imports,
        dead_code,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )]
    use super::*;

    #[test]
    fn test_outbox_error_display() {
        let err = OutboxError::SequenceError("test".to_string());
        assert!(err.to_string().contains("test"));
    }
}
