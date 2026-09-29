//! API server configuration.

use elephant_storage::StorageBackendKind;
use serde::{Deserialize, Serialize};
use std::env;
use std::path::{Path, PathBuf};

/// API server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    /// Server host.
    #[serde(default = "default_host")]
    pub host: String,
    /// Server port.
    #[serde(default = "default_port")]
    pub port: u16,
    /// Database URL.
    #[serde(default = "default_db_url")]
    pub db_url: String,
    /// Database namespace.
    #[serde(default = "default_db_namespace")]
    pub db_namespace: String,
    /// Database name.
    #[serde(default = "default_db_database")]
    pub db_database: String,
    /// Database username.
    #[serde(default = "default_db_user")]
    pub db_user: String,
    /// Database password.
    #[serde(default = "default_db_pass")]
    pub db_pass: String,
    /// Storage backend selector.
    #[serde(default = "default_db_backend")]
    pub db_backend: StorageBackendKind,
    /// Optional snapshot file to load for embedded mode.
    #[serde(default)]
    pub db_snapshot_load: Option<PathBuf>,
    /// Policy registry path.
    #[serde(default)]
    pub policy_registry: Option<String>,
    /// Enable strict mode.
    #[serde(default)]
    pub strict_mode: bool,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            db_url: default_db_url(),
            db_namespace: default_db_namespace(),
            db_database: default_db_database(),
            db_user: default_db_user(),
            db_pass: default_db_pass(),
            db_backend: default_db_backend(),
            db_snapshot_load: None,
            policy_registry: None,
            strict_mode: false,
        }
    }
}

#[allow(dead_code)]
impl ApiConfig {
    /// Creates a new API configuration from environment variables.
    ///
    /// # Errors
    ///
    /// Returns an error when `ELEPHANT_DB` is set to an invalid backend name.
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            host: env::var("ELEPHANT_HOST")
                .ok()
                .or_else(runtime_config_host_hint)
                .unwrap_or_else(default_host),
            port: env::var("ELEPHANT_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .or_else(runtime_config_port_hint)
                .unwrap_or_else(default_port),
            db_url: env::var("SURREALDB_URL").unwrap_or_else(|_| default_db_url()),
            db_namespace: env::var("SURREALDB_NAMESPACE")
                .unwrap_or_else(|_| default_db_namespace()),
            db_database: env::var("SURREALDB_DATABASE").unwrap_or_else(|_| default_db_database()),
            db_user: env::var("SURREALDB_USER").unwrap_or_else(|_| default_db_user()),
            db_pass: env::var("SURREALDB_PASS").unwrap_or_else(|_| default_db_pass()),
            db_backend: backend_from_env()?,
            db_snapshot_load: env::var("ELEPHANT_DB_SNAPSHOT_LOAD")
                .ok()
                .map(PathBuf::from),
            policy_registry: env::var("ELEPHANT_POLICY_REGISTRY").ok(),
            strict_mode: env::var("ELEPHANT_STRICT_MODE")
                .map(|s| s == "true" || s == "1")
                .unwrap_or(false),
        })
    }

    /// Creates a new API configuration with default values.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the host.
    #[must_use]
    pub fn with_host(mut self, host: impl Into<String>) -> Self {
        self.host = host.into();
        self
    }

    /// Sets the port.
    #[must_use]
    pub const fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Sets the database URL.
    #[must_use]
    pub fn with_db_url(mut self, db_url: impl Into<String>) -> Self {
        self.db_url = db_url.into();
        self
    }

    /// Enables strict mode.
    #[must_use]
    pub const fn with_strict_mode(mut self, strict: bool) -> Self {
        self.strict_mode = strict;
        self
    }

    /// Sets the policy registry path.
    #[must_use]
    pub fn with_policy_registry(mut self, path: impl Into<String>) -> Self {
        self.policy_registry = Some(path.into());
        self
    }

    /// Returns the configured socket address for binding.
    ///
    /// # Errors
    ///
    /// Returns an error when `host` is not an IPv4 or IPv6 address. Refusing
    /// an invalid bind address prevents a private deployment from silently
    /// falling back to all interfaces.
    pub fn socket_addr(&self) -> anyhow::Result<std::net::SocketAddr> {
        use std::net::{IpAddr, SocketAddr};

        let host = self.host.trim();
        let host = host
            .strip_prefix('[')
            .and_then(|value| value.strip_suffix(']'))
            .unwrap_or(host);
        let ip = host
            .parse::<IpAddr>()
            .map_err(|err| anyhow::anyhow!("invalid Elephant bind host `{host}`: {err}"))?;
        Ok(SocketAddr::new(ip, self.port))
    }
}

fn backend_from_env() -> anyhow::Result<StorageBackendKind> {
    env::var("ELEPHANT_DB").map_or_else(
        |_| Ok(runtime_config_backend_hint().unwrap_or_else(default_db_backend)),
        |value| {
            value
                .parse()
                .map_err(|err| anyhow::anyhow!("Invalid ELEPHANT_DB value `{value}`: {err}"))
        },
    )
}

fn runtime_config_backend_hint() -> Option<StorageBackendKind> {
    let value = runtime_config_value()?;
    value
        .get("storage")
        .and_then(|storage| storage.get("backend"))
        .and_then(toml::Value::as_str)
        .and_then(|backend| backend.parse().ok())
}

fn runtime_config_host_hint() -> Option<String> {
    let value = runtime_config_value()?;
    value
        .get("server")
        .and_then(|server| server.get("host"))
        .and_then(toml::Value::as_str)
        .map(str::to_owned)
}

fn runtime_config_port_hint() -> Option<u16> {
    let value = runtime_config_value()?;
    value
        .get("server")
        .and_then(|server| server.get("port"))
        .and_then(toml::Value::as_integer)
        .and_then(|port| u16::try_from(port).ok())
}

fn runtime_config_value() -> Option<toml::Value> {
    let config_path = find_project_root(env::current_dir().ok()?)
        .join(".elephant")
        .join("elephant_config.toml");
    let content = std::fs::read_to_string(config_path).ok()?;
    toml::from_str(&content).ok()
}

fn find_project_root(start: PathBuf) -> PathBuf {
    let mut current = start;
    loop {
        if current.join(".git").exists() {
            return current;
        }
        if !current.pop() {
            return env::current_dir().unwrap_or_else(|_| Path::new(".").to_path_buf());
        }
    }
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

const fn default_port() -> u16 {
    3000
}

fn default_db_url() -> String {
    "ws://localhost:8000".to_string()
}

fn default_db_namespace() -> String {
    "elephant".to_string()
}

fn default_db_database() -> String {
    "test".to_string()
}

fn default_db_user() -> String {
    "root".to_string()
}

fn default_db_pass() -> String {
    "root".to_string()
}

const fn default_db_backend() -> StorageBackendKind {
    StorageBackendKind::SurrealDb
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
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn test_default_config() {
        let config = ApiConfig::default();
        assert_eq!(config.port, 3000);
        assert!(!config.strict_mode);
    }

    #[test]
    fn test_config_new() {
        let config = ApiConfig::new();
        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, 3000);
        assert_eq!(config.db_url, "ws://localhost:8000");
    }

    #[test]
    fn test_config_builder() {
        let config = ApiConfig::new()
            .with_host("127.0.0.1")
            .with_port(8080)
            .with_db_url("ws://db.example.com:8000")
            .with_strict_mode(true);

        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 8080);
        assert_eq!(config.db_url, "ws://db.example.com:8000");
        assert!(config.strict_mode);
    }

    #[test]
    fn test_config_serde_roundtrip() {
        let config = ApiConfig::new().with_port(9000).with_strict_mode(true);

        let json = serde_json::to_string(&config).expect("serialize");
        let decoded: ApiConfig = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(decoded.port, 9000);
        assert!(decoded.strict_mode);
    }

    #[test]
    fn test_socket_addr() {
        let config = ApiConfig::new().with_port(8080);
        let addr = config.socket_addr().expect("valid default bind address");
        assert_eq!(addr.port(), 8080);
    }

    #[serial_test::serial(cwd_env)]
    #[test]
    fn test_backend_from_runtime_config_when_env_missing() {
        let _guard = env_lock().lock().expect("env lock");
        let prior_cwd = env::current_dir().expect("cwd");
        let prior_backend = env::var("ELEPHANT_DB").ok();
        let prior_host = env::var("ELEPHANT_HOST").ok();
        let prior_port = env::var("ELEPHANT_PORT").ok();
        let root =
            std::env::temp_dir().join(format!("elephant-config-backend-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join(".git")).expect("git dir");
        std::fs::create_dir_all(root.join(".elephant")).expect("config dir");
        std::fs::write(
            root.join(".elephant").join("elephant_config.toml"),
            r#"
[runtime]
mode = "local_memory"
ingest_enabled = true

[storage]
backend = "elephant-db"

[storage.elephant_db]
snapshot_path = ".elephant/state.snapshot"

[storage.surreal_db]
url = "ws://localhost:8000"
namespace = "elephant"
database = "test"
username = "root"
password = "root"

[extractor]
provider = "meerkat-agentic"
required = true
mcp_url = "http://127.0.0.1:3000/mcp/agent"
model = "gpt-5.5"
timeout_secs = 30

[server]
host = "127.0.0.1"
port = 4312
"#,
        )
        .expect("write config");
        std::env::set_current_dir(&root).expect("set cwd");
        std::env::remove_var("ELEPHANT_DB");
        std::env::remove_var("ELEPHANT_HOST");
        std::env::remove_var("ELEPHANT_PORT");

        let backend = backend_from_env().expect("backend");
        let config = ApiConfig::from_env().expect("config");
        assert_eq!(backend, StorageBackendKind::ElephantDb);
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 4312);

        match prior_backend {
            Some(value) => std::env::set_var("ELEPHANT_DB", value),
            None => std::env::remove_var("ELEPHANT_DB"),
        }
        match prior_host {
            Some(value) => std::env::set_var("ELEPHANT_HOST", value),
            None => std::env::remove_var("ELEPHANT_HOST"),
        }
        match prior_port {
            Some(value) => std::env::set_var("ELEPHANT_PORT", value),
            None => std::env::remove_var("ELEPHANT_PORT"),
        }
        std::env::set_current_dir(prior_cwd).expect("restore cwd");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_with_policy_registry() {
        let config = ApiConfig::new().with_policy_registry("/etc/elephant/policies");

        assert_eq!(
            config.policy_registry,
            Some("/etc/elephant/policies".to_string())
        );
    }

    #[test]
    fn test_serde_roundtrip_all_fields() {
        let config = ApiConfig::new()
            .with_host("api.example.com")
            .with_port(9000)
            .with_db_url("ws://db.example.com:8000")
            .with_strict_mode(true)
            .with_policy_registry("/etc/policies");

        let json = serde_json::to_string(&config).expect("serialize");
        let decoded: ApiConfig = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(decoded.host, "api.example.com");
        assert_eq!(decoded.port, 9000);
        assert_eq!(decoded.db_url, "ws://db.example.com:8000");
        assert!(decoded.strict_mode);
        assert_eq!(decoded.policy_registry, Some("/etc/policies".to_string()));
    }

    #[test]
    fn test_serde_deserialization_with_defaults() {
        // Test that partial JSON uses default values
        let json = r#"{"port": 8080}"#;
        let config: ApiConfig = serde_json::from_str(json).expect("deserialize");

        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, 8080);
        assert_eq!(config.db_url, "ws://localhost:8000");
        assert!(!config.strict_mode);
        assert!(config.policy_registry.is_none());
    }

    #[test]
    fn test_socket_addr_honors_configured_loopback_host() {
        let config = ApiConfig::new().with_host("127.0.0.1").with_port(3000);

        let addr = config.socket_addr().expect("valid loopback bind address");
        assert_eq!(addr.ip().to_string(), "127.0.0.1");
        assert_eq!(addr.port(), 3000);
    }

    #[test]
    fn test_socket_addr_refuses_invalid_host() {
        let config = ApiConfig::new().with_host("not an ip address");

        let error = config.socket_addr().expect_err("invalid host must fail");
        assert!(error.to_string().contains("invalid Elephant bind host"));
    }

    #[serial_test::serial(cwd_env)]
    #[test]
    fn test_backend_from_env_invalid_value_errors() {
        let _guard = env_lock().lock().expect("env lock");
        let prior = env::var("ELEPHANT_DB").ok();
        env::set_var("ELEPHANT_DB", "definitely-not-a-backend");
        let result = backend_from_env();
        match prior {
            Some(value) => env::set_var("ELEPHANT_DB", value),
            None => env::remove_var("ELEPHANT_DB"),
        }
        let error = result.expect_err("invalid backend should error");
        assert!(error
            .to_string()
            .contains("Invalid ELEPHANT_DB value `definitely-not-a-backend`"));
    }
}
