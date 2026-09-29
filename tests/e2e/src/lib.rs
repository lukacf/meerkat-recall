pub mod api;
pub mod pipeline_driver;
pub mod scenarios;

use elephant_migrations::apply_schema;
use elephant_storage::{StorageClient, StorageConfig};
use surrealdb::engine::any::Any;
use surrealdb::Surreal;

/// Shared test harness for E2E scenarios.
pub struct Harness {
    pub client: StorageClient,
    pub db: Surreal<Any>,
    pub ns: String,
    pub db_name: String,
}

impl Harness {
    /// Creates a new harness with a unique in-memory database.
    pub async fn new() -> anyhow::Result<Self> {
        let id = uuid::Uuid::new_v4().to_string();
        let ns = format!("ns_{}", id.replace('-', ""));
        let db_name = format!("db_{}", id.replace('-', ""));

        // Use kv-mem for high-speed local testing (or connect to real one if env var set)
        let config = StorageConfig {
            url: "mem://".to_string(),
            namespace: ns.clone(),
            database: db_name.clone(),
            ..StorageConfig::default()
        };

        let client = StorageClient::new(config).await?;

        // Apply schema
        apply_schema(&client)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        Ok(Self {
            client: client.clone(),
            db: client.db().expect("surreal test harness db").clone(),
            ns,
            db_name,
        })
    }
}
