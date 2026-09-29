//! Cursor-based event streaming.

use crate::error::{OutboxError, OutboxResult};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Serialize};

/// Cursor for event streaming.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamCursor {
    /// Space ID.
    pub space_id: String,
    /// Last seen sequence number.
    pub seq: i64,
    /// Cursor format version.
    pub version: u32,
}

impl StreamCursor {
    /// Current cursor version.
    pub const CURRENT_VERSION: u32 = 1;

    /// Creates a new cursor.
    #[must_use]
    pub const fn new(space_id: String, seq: i64) -> Self {
        Self {
            space_id,
            seq,
            version: Self::CURRENT_VERSION,
        }
    }

    /// Encodes the cursor to a base64 string.
    ///
    /// # Errors
    ///
    /// Returns an error if JSON serialization fails.
    pub fn encode(&self) -> OutboxResult<String> {
        let json =
            serde_json::to_string(self).map_err(|e| OutboxError::InvalidCursor(e.to_string()))?;
        Ok(STANDARD.encode(json.as_bytes()))
    }

    /// Decodes a cursor from a base64 string.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The base64 decoding fails
    /// - The JSON deserialization fails
    /// - The cursor version is unsupported
    pub fn decode(s: &str) -> OutboxResult<Self> {
        let bytes = STANDARD
            .decode(s)
            .map_err(|e| OutboxError::InvalidCursor(e.to_string()))?;
        let json =
            String::from_utf8(bytes).map_err(|e| OutboxError::InvalidCursor(e.to_string()))?;
        let cursor: Self =
            serde_json::from_str(&json).map_err(|e| OutboxError::InvalidCursor(e.to_string()))?;

        // Validate version
        if cursor.version > Self::CURRENT_VERSION {
            return Err(OutboxError::InvalidCursor(format!(
                "cursor version {} not supported",
                cursor.version
            )));
        }

        Ok(cursor)
    }

    /// Advances the cursor to a new sequence number.
    pub const fn advance(&mut self, seq: i64) {
        self.seq = seq;
    }
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
    fn test_cursor_roundtrip() {
        let cursor = StreamCursor::new("space_1".to_string(), 42);
        let encoded = cursor.encode().unwrap();
        let decoded = StreamCursor::decode(&encoded).unwrap();

        assert_eq!(decoded.space_id, "space_1");
        assert_eq!(decoded.seq, 42);
    }

    #[test]
    fn test_cursor_advance() {
        let mut cursor = StreamCursor::new("space_1".to_string(), 0);
        cursor.advance(100);
        assert_eq!(cursor.seq, 100);
    }

    #[test]
    fn test_cursor_version() {
        let cursor = StreamCursor::new("test".to_string(), 0);
        assert_eq!(cursor.version, StreamCursor::CURRENT_VERSION);
    }

    #[test]
    fn test_cursor_decode_invalid_base64() {
        let result = StreamCursor::decode("not valid base64!!!");
        assert!(result.is_err());
    }

    #[test]
    fn test_cursor_decode_invalid_json() {
        // Valid base64 but invalid JSON
        let encoded = STANDARD.encode(b"not json");
        let result = StreamCursor::decode(&encoded);
        assert!(result.is_err());
    }

    #[test]
    fn test_cursor_decode_future_version() {
        // Create a cursor with future version
        let json = r#"{"space_id":"test","seq":0,"version":999}"#;
        let encoded = STANDARD.encode(json.as_bytes());
        let result = StreamCursor::decode(&encoded);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not supported"));
    }

    #[test]
    fn test_cursor_negative_seq() {
        let cursor = StreamCursor::new("space_1".to_string(), -1);
        let encoded = cursor.encode().unwrap();
        let decoded = StreamCursor::decode(&encoded).unwrap();

        assert_eq!(decoded.seq, -1);
    }

    #[test]
    fn test_cursor_zero_seq() {
        let cursor = StreamCursor::new("space_1".to_string(), 0);
        let encoded = cursor.encode().unwrap();
        let decoded = StreamCursor::decode(&encoded).unwrap();

        assert_eq!(decoded.seq, 0);
    }

    #[test]
    fn test_cursor_large_seq() {
        let cursor = StreamCursor::new("space_1".to_string(), i64::MAX);
        let encoded = cursor.encode().unwrap();
        let decoded = StreamCursor::decode(&encoded).unwrap();

        assert_eq!(decoded.seq, i64::MAX);
    }

    #[test]
    fn test_cursor_unicode_space_id() {
        let cursor = StreamCursor::new("空间_αβγ_🎉".to_string(), 42);
        let encoded = cursor.encode().unwrap();
        let decoded = StreamCursor::decode(&encoded).unwrap();

        assert_eq!(decoded.space_id, "空间_αβγ_🎉");
    }

    #[test]
    fn test_cursor_empty_space_id() {
        let cursor = StreamCursor::new(String::new(), 0);
        let encoded = cursor.encode().unwrap();
        let decoded = StreamCursor::decode(&encoded).unwrap();

        assert_eq!(decoded.space_id, "");
    }

    #[test]
    fn test_cursor_advance_multiple_times() {
        let mut cursor = StreamCursor::new("space_1".to_string(), 0);
        cursor.advance(10);
        cursor.advance(20);
        cursor.advance(15); // Can go backwards (seek)
        assert_eq!(cursor.seq, 15);
    }

    #[test]
    fn test_cursor_serde_json_roundtrip() {
        let cursor = StreamCursor::new("test_space".to_string(), 12345);

        let json = serde_json::to_string(&cursor).unwrap();
        let decoded: StreamCursor = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.space_id, "test_space");
        assert_eq!(decoded.seq, 12345);
        assert_eq!(decoded.version, StreamCursor::CURRENT_VERSION);
    }
}
