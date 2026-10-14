//! Fixed operator guidance; never project private errors, tokens or grant contents.
use elephant_storage::{space_grant_repository::SpaceGrantRepository, StorageClient};
use elephant_types::space_grant::intersect_scopes;
use serde::{Deserialize, Serialize};

use crate::auth::{AuthConfig, Principal};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetupDiagnostic {
    pub code: String,
    pub action: String,
}

fn diagnostic(code: &str, action: &str) -> SetupDiagnostic {
    SetupDiagnostic {
        code: code.into(),
        action: action.into(),
    }
}

pub(crate) fn extractor_unavailable(url: &str) -> SetupDiagnostic {
    let valid = url.parse::<axum::http::Uri>().ok().is_some_and(|uri| {
        matches!(uri.scheme_str(), Some("http" | "https")) && uri.host().is_some()
    });
    if !valid {
        return diagnostic("extractor_endpoint_invalid", "Set extractor.mcp_url in the selected TOML (or PIPELINE_AGENTIC_MCP_URL for env-only startup) to the reachable absolute http(s) agent MCP endpoint, including its listener port and /mcp/agent path.");
    }
    diagnostic("extractor_tools_unavailable", "Verify the agent MCP endpoint and MCP_BEARER_TOKEN. Discovery must expose document, source, entity and predicate read tools. For a multi-space token, set extractor.readiness_space_id or PIPELINE_AGENTIC_MCP_SPACE_ID to one granted space; each extraction still selects its own space and current grant.")
}

pub(crate) fn is_local_agent_endpoint(url: &str, bound: std::net::SocketAddr) -> bool {
    let Ok(uri) = url.parse::<axum::http::Uri>() else {
        return false;
    };
    let ip = uri.host().and_then(|host| {
        host.trim_matches(['[', ']'])
            .parse::<std::net::IpAddr>()
            .ok()
    });
    uri.scheme_str() == Some("http")
        && uri.path() == "/mcp/agent"
        && uri.query().is_none()
        && uri.port_u16().unwrap_or(80) == bound.port()
        && ip.is_some_and(|ip| {
            ip.is_loopback()
                && ip.is_ipv4() == bound.is_ipv4()
                && (bound.ip().is_unspecified() || bound.ip() == ip)
        })
}

pub(crate) fn remote_authority_unchecked() -> SetupDiagnostic {
    diagnostic("extractor_remote_authority_unchecked", "The extractor endpoint is not identified as this listener's local agent route. Its remote signing key and current space grants cannot be checked locally; verify them at that endpoint. Global discovery confirms only the selected discovery space's tools.")
}

// These scopes correspond to the seven minimum discovery tools checked by the
// extractor. They do not certify ABAC access to future document records.
const WORKER_SCOPES: &[&str] = &[
    "read:doc:metadata",
    "read:doc:raw",
    "read:entity",
    "read:predicate",
    "read:source",
];

fn has_worker_scopes(scopes: &[String]) -> bool {
    let scopes = elephant_auth::projection::operation_scopes(scopes);
    scopes.contains("*") || WORKER_SCOPES.iter().all(|scope| scopes.contains(*scope))
}

/// Observe the same signature, expiry, signed selection and current raw grant
/// intersection as MCP. This is diagnostic evidence, never an execution permit.
pub(crate) async fn worker_authority(
    auth: &AuthConfig,
    storage: &StorageClient,
    space: &str,
    token: Option<&str>,
) -> Option<SetupDiagnostic> {
    if auth.skip_auth {
        return None;
    }
    let Some(token) = token.filter(|token| !token.trim().is_empty()) else {
        return Some(diagnostic("extractor_token_missing", "Set MCP_BEARER_TOKEN to the worker bearer token value before starting the service; do not use a filename or an operator token."));
    };
    let verified = match auth.verify_token(token, None) {
        Ok(verified) => verified,
        Err(error) => {
            return Some(if error.error == "expired_token" {
                diagnostic("extractor_token_expired", "Issue a new bounded worker token for the intended named spaces and update MCP_BEARER_TOKEN; keep current space grants in place.")
            } else {
                diagnostic("extractor_token_invalid", "Use a worker token signed for this service's configured JWT key, issuer and audience; set MCP_BEARER_TOKEN to its value.")
            })
        }
    };
    let claims = verified.claims();
    if claims.bootstrap_space.is_some() || (claims.spaces.is_none() && claims.space_id.is_none()) {
        return Some(diagnostic(
            "extractor_token_not_data_authority",
            "Use a named-space worker token, not bootstrap or global management authority.",
        ));
    }
    let mut worker = Principal::from(claims.clone());
    if worker.bind_space(Some(space)).is_err() {
        return Some(diagnostic("extractor_space_not_granted", "Include the selected space in the signed worker token ceiling; a discovery space selection cannot extend that ceiling."));
    }
    if !has_worker_scopes(&worker.scopes) {
        return Some(diagnostic("extractor_token_scopes_missing", "Issue the worker token with read:doc:metadata, read:doc:raw, read:entity, read:predicate and read:source. Read-only extraction does not require write or management scopes."));
    }
    if claims.spaces.is_some() {
        if !storage.recovery_status().ready {
            return Some(diagnostic(
                "extractor_grant_unavailable",
                "Wait for storage recovery before checking current worker grants.",
            ));
        }
        let grant = match SpaceGrantRepository::new(storage.clone()).get(space, &worker.subject).await {
            Ok(Some(grant)) if grant.active => grant,
            Ok(_) => return Some(diagnostic("extractor_current_grant_missing", "Use an authorized grant administrator to create or reactivate this worker's current grant in the selected space, within its signed scope ceiling.")),
            Err(_) => return Some(diagnostic("extractor_grant_unavailable", "Resolve the storage failure before checking current worker grants.")),
        };
        worker.scopes = intersect_scopes(&worker.scopes, &grant.scopes);
        if !has_worker_scopes(&worker.scopes) {
            return Some(diagnostic("extractor_current_grant_scopes_missing", "Update this worker's current selected-space grant to include the required document, source, entity and read:predicate scopes already present in its signed token; do not widen unrelated grants."));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::auth::Claims;
    use elephant_storage::{Repository, SpaceConfigRepository};
    use elephant_types::{records::SpaceConfig, space_grant::SpaceGrant, RecordId, TableName};
    use jsonwebtoken::{encode, EncodingKey, Header};
    use serde_json::json;

    fn auth() -> AuthConfig {
        AuthConfig {
            secret: "synthetic-worker-diagnostic-test-key-only".into(),
            algorithm: jsonwebtoken::Algorithm::HS256,
            audience: None,
            issuer: None,
            skip_auth: false,
        }
    }
    fn claims() -> Claims {
        serde_json::from_value(json!({"sub":"private-worker-subject", "iat":chrono::Utc::now().timestamp(), "exp":chrono::Utc::now().timestamp()+600, "space_id":"alpha", "spaces":["alpha"], "scopes":WORKER_SCOPES, "security_level":0, "security_labels":[], "clearances":[]})).unwrap()
    }
    fn token(auth: &AuthConfig, claims: &Claims) -> String {
        encode(
            &Header::default(),
            claims,
            &EncodingKey::from_secret(auth.secret.as_bytes()),
        )
        .unwrap()
    }
    async fn check(
        auth: &AuthConfig,
        storage: &StorageClient,
        space: &str,
        claims: &Claims,
    ) -> Option<SetupDiagnostic> {
        worker_authority(auth, storage, space, Some(&token(auth, claims))).await
    }

    #[test]
    fn local_authority_requires_exact_listener_and_literal_loopback_route() {
        let bound = "0.0.0.0:3000".parse().unwrap();
        assert!(is_local_agent_endpoint(
            "http://127.0.0.1:3000/mcp/agent",
            bound
        ));
        for url in [
            "http://127.0.0.1:3001/mcp/agent",
            "https://127.0.0.1:3000/mcp/agent",
            "http://remote.example:3000/mcp/agent",
            "http://localhost:3000/mcp/agent",
            "http://127.0.0.1:3000/mcp/full",
            "http://127.0.0.1:3000/mcp/agent?remote=true",
        ] {
            assert!(!is_local_agent_endpoint(url, bound));
        }
        assert!(!is_local_agent_endpoint(
            "http://127.0.0.1:3000/mcp/agent",
            "127.0.0.2:3000".parse().unwrap()
        ));
    }

    #[test]
    fn endpoint_diagnosis_never_echoes_credentials_or_private_url() {
        for value in [
            "private-invalid-endpoint",
            "https://private-user:secret@internal.example/mcp/agent",
        ] {
            let diagnostic = extractor_unavailable(value);
            let text = serde_json::to_string(&diagnostic).unwrap();
            assert!(!text.contains("secret"));
            assert!(!text.contains("internal.example"));
            assert!(!text.contains("private-invalid"));
        }
        assert_eq!(
            extractor_unavailable("relative/path").code,
            "extractor_endpoint_invalid"
        );
        assert_eq!(
            extractor_unavailable("https://localhost:3000/mcp/agent").code,
            "extractor_tools_unavailable"
        );
    }

    #[tokio::test]
    async fn worker_diagnostics_preserve_signed_space_expiry_and_legacy_contract(
    ) -> anyhow::Result<()> {
        let auth = auth();
        let storage = StorageClient::new_embedded().await?;
        assert_eq!(
            worker_authority(&auth, &storage, "alpha", None)
                .await
                .unwrap()
                .code,
            "extractor_token_missing"
        );
        let mut claims = claims();
        assert_eq!(
            check(&auth, &storage, "beta", &claims).await.unwrap().code,
            "extractor_space_not_granted"
        );
        claims.scopes.retain(|scope| scope != "read:predicate");
        assert_eq!(
            check(&auth, &storage, "alpha", &claims).await.unwrap().code,
            "extractor_token_scopes_missing"
        );
        claims.scopes.push("read:predicate".into());
        claims.spaces = None;
        assert!(check(&auth, &storage, "alpha", &claims).await.is_none());
        assert_eq!(
            check(&auth, &storage, "beta", &claims).await.unwrap().code,
            "extractor_space_not_granted"
        );
        claims.exp = 1;
        assert_eq!(
            check(&auth, &storage, "alpha", &claims).await.unwrap().code,
            "extractor_token_expired"
        );
        let mut wrong = auth.clone();
        wrong.secret = "different-synthetic-key-with-enough-bytes".into();
        assert_eq!(
            worker_authority(
                &auth,
                &storage,
                "alpha",
                Some(&token(&wrong, &self::claims()))
            )
            .await
            .unwrap()
            .code,
            "extractor_token_invalid"
        );
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain fixture storage and grant repository owners through the complete current grant authority assertions"
    )]
    async fn worker_diagnostics_observe_current_grant_revocation_and_scope_intersection(
    ) -> anyhow::Result<()> {
        let auth = auth();
        let storage = StorageClient::new_embedded().await?;
        let config: SpaceConfig = serde_json::from_value(
            json!({"id":RecordId::new_v4(TableName::SpaceConfig),"space_id":"alpha","created_at":chrono::Utc::now(),"updated_at":chrono::Utc::now(),"created_by":"operator","security":{"policy_id":"default","level":0,"labels":[],"handling":[]},"provenance":{},"subjects":[],"subjects_state":"complete","version":1,"embedding_provider":"openai","embedding_model":"fixture","embedding_dims":3,"purpose":null,"skill":null}),
        )?;
        SpaceConfigRepository::new(storage.clone())
            .create(config)
            .await?;
        let claims = claims();
        assert_eq!(
            check(&auth, &storage, "alpha", &claims).await.unwrap().code,
            "extractor_current_grant_missing"
        );
        let repository = SpaceGrantRepository::new(storage.clone());
        let mut grant = repository
            .replace(
                SpaceGrant {
                    space_id: "alpha".into(),
                    subject: claims.sub.clone(),
                    generation: String::new(),
                    active: true,
                    scopes: claims.scopes.iter().cloned().collect(),
                    updated_by: "operator".into(),
                    updated_at: chrono::Utc::now(),
                },
                None,
            )
            .await?;
        assert!(check(&auth, &storage, "alpha", &claims).await.is_none());
        grant.scopes.remove("read:predicate");
        let generation = grant.generation.clone();
        grant = repository.replace(grant, Some(&generation)).await?;
        let diagnostic = check(&auth, &storage, "alpha", &claims).await.unwrap();
        assert_eq!(diagnostic.code, "extractor_current_grant_scopes_missing");
        assert!(!serde_json::to_string(&diagnostic)?.contains(&claims.sub));
        grant.active = false;
        let generation = grant.generation.clone();
        repository.replace(grant, Some(&generation)).await?;
        assert_eq!(
            check(&auth, &storage, "alpha", &claims).await.unwrap().code,
            "extractor_current_grant_missing"
        );
        Ok(())
    }
}
