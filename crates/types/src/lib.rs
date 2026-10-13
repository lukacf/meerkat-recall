//! Core types and data models for the Elephant knowledge system.
//!
//! This crate provides the canonical Rust types for all records in Elephant,
//! including entities, relationships, events, documents, and more. It implements
//! a clear separation between DTOs (for API) and Record structs (for DB).

pub mod base_fields;
pub mod error;
pub mod ids;
pub mod mcp_simple;
pub mod pipeline_status;
pub mod provider_binding;
pub mod public_error;
pub mod publication;
pub mod publication_wire;
pub mod records;
pub mod rendered_text;
pub mod space_grant;
pub mod validation;

pub use base_fields::BaseFieldsBuilder;
pub use error::TypeError;
pub use ids::{Link, RecordId, TableName};

/// Re-export common types for convenience.
pub mod prelude {
    pub use crate::base_fields::BaseFieldsBuilder;
    pub use crate::error::TypeError;
    pub use crate::ids::{Link, RecordId, TableName};
    pub use crate::records::base::{BaseFields, Provenance, Security, SubjectsState};
    pub use crate::records::*;
}

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
    fn test_record_id_table_parsing() {
        // Verify RecordId correctly parses and extracts table name
        let id = RecordId::new(TableName::Entity, uuid::Uuid::new_v4());
        assert_eq!(id.table(), TableName::Entity);
    }

    #[test]
    fn test_link_id_extraction() {
        // Verify Link correctly stores and retrieves RecordId
        let id = RecordId::new(TableName::Rel, uuid::Uuid::new_v4());
        let link: Link<()> = Link::new(id.clone());
        assert_eq!(*link.id(), id);
    }
}
