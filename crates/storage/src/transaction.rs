//! Transaction helpers for `SurrealDB` operations.
//!
//! This module provides utilities for managing database transactions
//! with automatic rollback on error and commit on success.
//!
//! Note: `SurrealDB` transactions are atomic - if any statement fails,
//! all changes are automatically rolled back.

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};

/// A builder for constructing transactional operations.
///
/// Provides a fluent API for building and executing database
/// transactions with proper error handling.
#[derive(Debug)]
pub struct TransactionBuilder<'a> {
    client: &'a StorageClient,
    statements: Vec<String>,
}

impl<'a> TransactionBuilder<'a> {
    /// Creates a new transaction builder.
    #[must_use]
    pub const fn new(client: &'a StorageClient) -> Self {
        Self {
            client,
            statements: Vec::new(),
        }
    }

    /// Adds a `SurrealQL` statement to the transaction.
    #[must_use]
    pub fn add_statement(mut self, sql: impl Into<String>) -> Self {
        self.statements.push(sql.into());
        self
    }

    /// Adds multiple `SurrealQL` statements to the transaction.
    #[must_use]
    pub fn add_statements<I, S>(mut self, statements: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        for stmt in statements {
            self.statements.push(stmt.into());
        }
        self
    }

    /// Executes the transaction.
    ///
    /// Wraps all statements in `BEGIN`/`COMMIT` and handles rollback on error.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction is empty or if any statement fails.
    pub async fn execute(self) -> StorageResult<surrealdb::Response> {
        if self.statements.is_empty() {
            return Err(StorageError::TransactionError(
                "No statements to execute".to_string(),
            ));
        }

        // Build the transaction SQL
        let mut sql = String::from("BEGIN TRANSACTION;\n");

        for stmt in &self.statements {
            sql.push_str(stmt);
            if !stmt.trim().ends_with(';') {
                sql.push(';');
            }
            sql.push('\n');
        }

        sql.push_str("COMMIT TRANSACTION;");

        // Execute the transaction through the checked/retrying path: the
        // explicit BEGIN..COMMIT batch is atomic, so a retryable conflict
        // means nothing was committed and replaying the whole transaction is
        // safe.
        let response = self
            .client
            .query_checked(&sql)
            .await
            .map_err(|e| StorageError::TransactionError(format!("Transaction failed: {e}")))?;

        // query_checked already surfaced statement errors; this second check
        // is harmless and keeps the historical error shape as a backstop.
        let response = response.check().map_err(|e| {
            StorageError::TransactionError(format!("Transaction statement failed: {e}"))
        })?;

        Ok(response)
    }

    /// Returns the number of statements in this transaction.
    #[must_use]
    pub const fn statement_count(&self) -> usize {
        self.statements.len()
    }
}

/// Extension trait for `StorageClient` to provide transaction helpers.
pub trait TransactionExt {
    /// Starts a new transaction builder.
    fn transaction(&self) -> TransactionBuilder<'_>;

    /// Executes a single statement within a transaction.
    fn transact(&self, sql: impl Into<String>) -> TransactionBuilder<'_>;
}

impl TransactionExt for StorageClient {
    fn transaction(&self) -> TransactionBuilder<'_> {
        TransactionBuilder::new(self)
    }

    fn transact(&self, sql: impl Into<String>) -> TransactionBuilder<'_> {
        TransactionBuilder::new(self).add_statement(sql)
    }
}

/// Executes multiple statements atomically.
///
/// This is a convenience function for simple transaction cases.
///
/// # Errors
///
/// Returns an error if any statement fails.
pub async fn execute_atomic(
    client: &StorageClient,
    statements: Vec<String>,
) -> StorageResult<surrealdb::Response> {
    TransactionBuilder::new(client)
        .add_statements(statements)
        .execute()
        .await
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
    use crate::client::StorageConfig;

    #[tokio::test]
    async fn test_transaction_builder_empty() -> Result<(), Box<dyn std::error::Error>> {
        let client = StorageClient::new(StorageConfig::default()).await?;

        let result = TransactionBuilder::new(&client).execute().await;

        match result {
            Err(StorageError::TransactionError(msg)) => {
                assert!(msg.contains("No statements"));
                Ok(())
            }
            Err(err) => Err(err.into()),
            Ok(_) => Err(StorageError::QueryError("Expected TransactionError".to_string()).into()),
        }
    }

    #[tokio::test]
    async fn test_transaction_builder_single_statement() -> Result<(), Box<dyn std::error::Error>> {
        let client = StorageClient::new(StorageConfig::default()).await?;

        let builder = client.transaction().add_statement("RETURN 1");

        assert_eq!(builder.statement_count(), 1);

        builder.execute().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_transaction_builder_multiple_statements() -> Result<(), Box<dyn std::error::Error>>
    {
        let client = StorageClient::new(StorageConfig::default()).await?;

        client
            .transaction()
            .add_statement("LET $x = 1")
            .add_statement("LET $y = 2")
            .add_statement("RETURN $x + $y")
            .execute()
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_transact_shorthand() -> Result<(), Box<dyn std::error::Error>> {
        let client = StorageClient::new(StorageConfig::default()).await?;

        client.transact("RETURN 42").execute().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_execute_atomic() -> Result<(), Box<dyn std::error::Error>> {
        let client = StorageClient::new(StorageConfig::default()).await?;

        let statements = vec!["LET $a = 1".to_string(), "RETURN $a".to_string()];

        execute_atomic(&client, statements).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_transaction_add_statements_iterator() -> Result<(), Box<dyn std::error::Error>> {
        let client = StorageClient::new(StorageConfig::default()).await?;

        let stmts = vec!["LET $x = 1", "LET $y = 2"];
        let builder = client.transaction().add_statements(stmts);

        assert_eq!(builder.statement_count(), 2);
        Ok(())
    }
}
