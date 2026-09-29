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

#[cfg(test)]
mod embedded_elephant_db;
#[cfg(test)]
mod fast_scenarios;

#[cfg(test)]
#[path = "../../e2e/src/pipeline_driver.rs"]
pub mod pipeline_driver;

#[cfg(test)]
#[path = "../../e2e/src/scenarios/mod.rs"]
pub mod scenario;

#[cfg(test)]
/// Shared test harness for E2E scenarios.
pub struct Harness {
    pub client: StorageClient,
    pub db: Surreal<Any>,
    pub ns: String,
    pub db_name: String,
}

#[cfg(test)]
impl Harness {
    /// Creates a new harness with a unique in-memory database.
    pub async fn new() -> anyhow::Result<Self> {
        let id = uuid::Uuid::new_v4().to_string();
        let ns = format!("ns_{}", id.replace('-', ""));
        let db_name = format!("db_{}", id.replace('-', ""));

        // Use kv-mem for high-speed local testing
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

#[cfg(test)]
use elephant_migrations::apply_schema;
#[cfg(test)]
use elephant_storage::{StorageClient, StorageConfig};
#[cfg(test)]
use surrealdb::engine::any::Any;
#[cfg(test)]
use surrealdb::Surreal;
