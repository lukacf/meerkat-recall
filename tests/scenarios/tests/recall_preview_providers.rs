//! Independent D provider conformance through actual production clients and DBs.
//! Loopback transports are deterministic SPIES, not provider relevance acceptance.
//! Depends on provider owner with_credentials/index_generation successor contract.

#[path = "../../recall_preview_semantics/provider_spy.rs"]
mod spy;

#[path = "../../recall_preview_semantics/provider_vector_controls.rs"]
mod vector_controls;

#[path = "../../recall_preview_semantics/provider_adapters.rs"]
mod adapters;

#[path = "../../recall_preview_semantics/provider_direct.rs"]
mod direct;

#[path = "../../recall_preview_semantics/provider_media.rs"]
mod media;

#[path = "../../recall_preview_semantics/provider_reindex.rs"]
mod reindex;

#[path = "../../recall_preview_semantics/native_reindex.rs"]
mod native_reindex;

#[path = "../../recall_preview_semantics/provider_cache.rs"]
mod cache;

#[path = "../../recall_preview_semantics/provider_extraction.rs"]
mod extraction;

// D-owned publication helpers are consumed without changing their expectations.
#[path = "../../recall_preview_semantics/index_fixture.rs"]
mod index_fixture;
#[path = "../../recall_preview/publication_support.rs"]
mod publication_support;
#[path = "../../recall_preview_semantics/render_coordinates_http.rs"]
mod render_coordinates_http;

#[path = "../../recall_preview_semantics/render_coordinates.rs"]
mod render_coordinates;

#[path = "../../recall_preview_semantics/scoped_input_hash.rs"]
mod scoped_input_hash;

#[path = "../../recall_preview_semantics/event_sequence.rs"]
mod event_sequence;
#[path = "../../recall_preview_semantics/scoped_index.rs"]
mod scoped_index;
#[path = "../../recall_preview_semantics/scoped_reprepare.rs"]
mod scoped_reprepare;
#[path = "../../recall_preview_semantics/source_projection.rs"]
mod source_projection;
#[path = "../../recall_preview/support.rs"]
mod support;

use anyhow::Context;
use elephant_embeddings::governed::policy_for_binding;
use elephant_storage::SpaceConfigRepository;
use elephant_types::provider_binding::ModelOperation;
use spy::{BACKENDS, EMBEDDING, GENERATION, SPACES};

#[tokio::test]
async fn refusal_oracle_distinguishes_sdk_terminal_error_success_and_empty_stream(
) -> anyhow::Result<()> {
    use meerkat::{LlmDoneOutcome, LlmError, LlmEvent, StopReason};
    let refusal = || LlmError::InvalidRequest {
        message: "controlled refusal".into(),
    };
    for events in [
        vec![Err(refusal())],
        vec![Ok(LlmEvent::Done {
            outcome: LlmDoneOutcome::Error { error: refusal() },
        })],
    ] {
        spy::require_model_refusal(futures::stream::iter(events)).await?;
    }
    for events in [
        Vec::new(),
        vec![Ok(LlmEvent::Done {
            outcome: LlmDoneOutcome::Success {
                stop_reason: StopReason::EndTurn,
            },
        })],
        vec![
            Ok(LlmEvent::TextDelta {
                delta: "invented completion".into(),
                meta: None,
            }),
            Err(refusal()),
        ],
    ] {
        anyhow::ensure!(spy::require_model_refusal(futures::stream::iter(events))
            .await
            .is_err());
    }
    Ok(())
}

#[tokio::test]
async fn malformed_or_misdirected_http_cannot_count_as_zero_outbound() -> anyhow::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for (method, path, credential, body) in [
        ("POST", "/v1/v1/messages", "x-api-key: spy-only\r\n", "{}"),
        ("GET", "/v1/messages", "x-api-key: spy-only\r\n", "{}"),
        ("POST", "/v1/messages", "", "{}"),
        (
            "POST",
            "/v1/messages",
            "x-api-key: spy-only\r\n",
            "{malformed",
        ),
    ] {
        let endpoint = spy::Spy::start().await?;
        let address = endpoint
            .endpoint
            .strip_prefix("http://")
            .and_then(|value| value.strip_suffix("/v1"))
            .context("spy address")?;
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            let mut connection = tokio::net::TcpStream::connect(address).await?;
            let request = format!("{method} {path} HTTP/1.1\r\nHost: {address}\r\n{credential}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            connection.write_all(request.as_bytes()).await?;
            let mut status = [0_u8; 12];
            connection.read_exact(&mut status).await?;
            anyhow::ensure!(&status == b"HTTP/1.1 400");
            anyhow::ensure!(endpoint.count().is_err(),
                "unexpected physical request must invalidate the spy instead of returning zero");
            Ok::<_, anyhow::Error>(())
        }).await??;
    }
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::cloned_ref_to_slice_refs,
    reason = "retain the owned input clone across the provider await in the transport oracle"
)]
async fn every_governed_operation_uses_its_current_space_transport() -> anyhow::Result<()> {
    for backend in BACKENDS {
        let repo = SpaceConfigRepository::new(spy::storage(backend).await?);
        let home = spy::Spy::start().await?;
        let business = spy::Spy::start().await?;
        spy::configure(&repo, SPACES[0], &home.endpoint, 8).await?;
        spy::configure(&repo, SPACES[1], &business.endpoint, 12).await?;
        // Interleave spaces at each operation; stale process-default caches fail.
        for operation in EMBEDDING {
            for (space, endpoint, dimensions) in [(SPACES[0], &home, 8), (SPACES[1], &business, 12)]
            {
                let before = endpoint.count()?;
                let (policy, provider, provenance) =
                    spy::context(&repo, space).embedding(operation).await?;
                let marker = format!("private synthetic input for {space}/{operation:?}");
                let vectors = provider.embed(&[marker.clone()]).await?;
                anyhow::ensure!(vectors.len() == 1 && vectors[0].len() == dimensions);
                anyhow::ensure!(policy.dims == dimensions && provenance.config.space_id == space);
                let requests = endpoint.requests()?;
                anyhow::ensure!(
                    requests.len() == before + 1,
                    "{backend:?}: wrong outbound count"
                );
                let request = requests.last().context("missing outbound request")?;
                anyhow::ensure!(request.path == "/v1/embeddings");
                anyhow::ensure!(request.body["input"][0] == marker);
                anyhow::ensure!(
                    request.body["model"] == format!("text-embedding-spy-{dimensions}")
                );
                anyhow::ensure!(
                    request.credential
                        == format!("Bearer spy-only-SEMANTICS_SPY_{dimensions}_EMBED")
                );
            }
        }
        for operation in GENERATION {
            for (space, endpoint, dimensions) in [(SPACES[0], &home, 8), (SPACES[1], &business, 12)]
            {
                let before = endpoint.count()?;
                let context = spy::context(&repo, space);
                let (model, client, provenance) = context.model_client(operation).await?;
                anyhow::ensure!(
                    provenance.operation == operation && provenance.config.space_id == space
                );
                let marker = format!("{space}/{operation:?}");
                spy::model_call(client, &model, &marker).await?;
                let requests = endpoint.requests()?;
                anyhow::ensure!(
                    requests.len() == before + 1,
                    "{backend:?}/{operation:?}: transport not exercised"
                );
                let request = requests.last().context("missing outbound request")?;
                anyhow::ensure!(request.body["model"] == model);
                anyhow::ensure!(request.body.to_string().contains(&marker));
                anyhow::ensure!(request
                    .credential
                    .ends_with(&format!("spy-only-SEMANTICS_SPY_{dimensions}_GENERATE")));
                anyhow::ensure!(
                    request.path
                        == if space == SPACES[0] {
                            "/v1/messages"
                        } else {
                            "/v1/responses"
                        }
                );
                let safe = serde_json::to_string(&provenance)?;
                anyhow::ensure!(
                    !safe.contains("spy-only")
                        && !safe.contains("SEMANTICS_SPY")
                        && !safe.contains("127.0.0.1")
                );
                // Agent builds must install the same gate and erase caller model
                // overrides/fallbacks before any multi-turn agent is constructed.
                let mut build = meerkat::AgentBuildConfig::new("unapproved-job-model".to_string());
                context.govern_agent_build(operation, &mut build).await?;
                anyhow::ensure!(build.model == model && build.agent_llm_client_override.is_none());
                let governed = build
                    .llm_client_override
                    .context("agent build missing governed client")?;
                spy::model_call(governed, &model, &marker).await?;
                anyhow::ensure!(endpoint.count()? == before + 2);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn missing_and_wrong_space_approvals_emit_zero_outbound_requests() -> anyhow::Result<()> {
    for backend in BACKENDS {
        let repo = SpaceConfigRepository::new(spy::storage(backend).await?);
        let endpoint = spy::Spy::start().await?;
        spy::configure(&repo, SPACES[0], &endpoint.endpoint, 8).await?;
        spy::configure(&repo, SPACES[1], &endpoint.endpoint, 12).await?;
        // Space A approval cannot authorize an operation in selected space B.
        spy::edit(&repo, SPACES[1], |config| {
            config.bindings.clear();
            Ok(())
        })
        .await?;
        for space in [SPACES[1], "semantics-unregistered"] {
            let context = spy::context(&repo, space);
            for operation in EMBEDDING {
                anyhow::ensure!(context.embedding(operation).await.is_err());
            }
            for operation in GENERATION {
                anyhow::ensure!(context.model_client(operation).await.is_err());
                let mut build = meerkat::AgentBuildConfig::new("unapproved-job-model".to_string());
                anyhow::ensure!(context
                    .govern_agent_build(operation, &mut build)
                    .await
                    .is_err());
            }
        }
        anyhow::ensure!(
            endpoint.count()? == 0,
            "{backend:?}: unauthorized call reached transport"
        );
    }
    Ok(())
}

#[tokio::test]
async fn revoked_pinned_clients_and_agent_continuations_never_dispatch() -> anyhow::Result<()> {
    for backend in BACKENDS {
        let repo = SpaceConfigRepository::new(spy::storage(backend).await?);
        let endpoint = spy::Spy::start().await?;
        for (space, dimensions) in [(SPACES[0], 8), (SPACES[1], 12)] {
            spy::configure(&repo, space, &endpoint.endpoint, dimensions).await?;
            for operation in GENERATION {
                let context = spy::context(&repo, space);
                let (model, cached, _) = context.model_client(operation).await?;
                let mut build = meerkat::AgentBuildConfig::new("unapproved-job-model".to_string());
                context.govern_agent_build(operation, &mut build).await?;
                let agent_client = build
                    .llm_client_override
                    .context("missing governed agent client")?;
                spy::edit(&repo, space, |config| {
                    config
                        .bindings
                        .get_mut(&operation)
                        .context("binding missing")?
                        .revoked = true;
                    Ok(())
                })
                .await?;
                let before = endpoint.count()?;
                spy::model_call(cached, &model, "pinned retry after revocation").await?;
                spy::model_call(agent_client, &model, "agent continuation after revocation")
                    .await?;
                anyhow::ensure!(
                    endpoint.count()? == before,
                    "{backend:?}/{operation:?}: revoked cached client dispatched"
                );
            }
            for operation in EMBEDDING {
                let (_, cached, _) = spy::context(&repo, space).embedding(operation).await?;
                spy::edit(&repo, space, |config| {
                    config
                        .bindings
                        .get_mut(&operation)
                        .context("binding missing")?
                        .revoked = true;
                    Ok(())
                })
                .await?;
                let before = endpoint.count()?;
                anyhow::ensure!(cached.embed(&["revoked embedding".into()]).await.is_err());
                anyhow::ensure!(endpoint.count()? == before);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn actual_embedding_http_retry_rechecks_committed_revocation() -> anyhow::Result<()> {
    for backend in BACKENDS {
        for operation in EMBEDDING {
            let repo = SpaceConfigRepository::new(spy::storage(backend).await?);
            let endpoint = spy::Spy::start().await?;
            for (space, dims) in [(SPACES[0], 8), (SPACES[1], 12)] {
                spy::configure(&repo, space, &endpoint.endpoint, dims).await?;
                let (_, cached, _) = spy::context(&repo, space).embedding(operation).await?;
                endpoint.before_retry(spy::BeforeRetry::Revoke(
                    repo.clone(),
                    space.into(),
                    operation,
                ))?;
                let before = endpoint.count()?;
                let result = tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    cached.embed(&["retry-bound-input".into()]),
                )
                .await?;
                anyhow::ensure!(result.is_err());
                anyhow::ensure!(
                    endpoint.count()? == before + 1,
                    "{backend:?}: post-429 retry escaped current approval"
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn credential_rotation_changes_transport_without_changing_vector_identity(
) -> anyhow::Result<()> {
    for backend in BACKENDS {
        let repo = SpaceConfigRepository::new(spy::storage(backend).await?);
        let endpoint = spy::Spy::start().await?;
        for (space, dims) in [(SPACES[0], 8), (SPACES[1], 12)] {
            spy::configure(&repo, space, &endpoint.endpoint, dims).await?;
            let operation = ModelOperation::QueryEmbedding;
            let before = repo
                .authorize_model_operation(space, operation, None)
                .await?;
            let (policy, cached, _) = spy::context(&repo, space).embedding(operation).await?;
            endpoint.before_retry(spy::BeforeRetry::Rotate(
                repo.clone(),
                space.into(),
                format!("SEMANTICS_SPY_{dims}_ROTATED"),
            ))?;
            let offset = endpoint.count()?;
            let vectors = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                cached.embed(&["rotation-bound-input".into()]),
            )
            .await??;
            anyhow::ensure!(vectors.len() == 1);
            let requests = endpoint.requests()?;
            anyhow::ensure!(requests.len() == offset + 2);
            anyhow::ensure!(requests[offset].credential != requests[offset + 1].credential);
            anyhow::ensure!(requests[offset + 1]
                .credential
                .ends_with(&format!("SEMANTICS_SPY_{dims}_ROTATED")));
            let after = repo
                .authorize_model_operation(space, operation, None)
                .await?;
            anyhow::ensure!(before.vector_partition()? == after.vector_partition()?);
            anyhow::ensure!(policy.model == policy_for_binding(&after)?.model);
            anyhow::ensure!(
                after.connection.credential_generation
                    == before.connection.credential_generation + 1
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn immutable_partition_changes_are_refused_while_rotation_is_distinct() -> anyhow::Result<()>
{
    for backend in BACKENDS {
        let repo = SpaceConfigRepository::new(spy::storage(backend).await?);
        let endpoint = spy::Spy::start().await?;
        spy::configure(&repo, SPACES[0], &endpoint.endpoint, 8).await?;
        spy::configure(&repo, SPACES[1], &endpoint.endpoint, 12).await?;
        let home = repo
            .authorize_model_operation(SPACES[0], ModelOperation::QueryEmbedding, None)
            .await?;
        let business = repo
            .authorize_model_operation(SPACES[1], ModelOperation::QueryEmbedding, None)
            .await?;
        anyhow::ensure!(home.vector_partition()? != business.vector_partition()?);
        anyhow::ensure!(policy_for_binding(&home)?.model != policy_for_binding(&business)?.model);
        for field in [
            "model",
            "model_version",
            "dimensions",
            "preprocessing",
            "account",
            "endpoint",
            "provider",
        ] {
            let result = spy::edit(&repo, SPACES[0], |config| {
                let profile = config
                    .profiles
                    .get_mut(&home.profile.id)
                    .context("profile missing")?;
                match field {
                    "model" => profile.model = "different-model".into(),
                    "model_version" => profile.model_version = "v2".into(),
                    "dimensions" => profile.dimensions = Some(99),
                    "preprocessing" => profile.preprocessing = "unapproved-normalization-v2".into(),
                    _ => {
                        let connection = config
                            .connections
                            .get_mut("embed")
                            .context("connection missing")?;
                        match field {
                            "account" => connection.account_id = "different-account".into(),
                            "endpoint" => connection.endpoint = "http://127.0.0.1:1/v1".into(),
                            _ => connection.provider = "fastembed".into(),
                        }
                    }
                }
                Ok(())
            })
            .await;
            anyhow::ensure!(
                result.is_err(),
                "{backend:?}: {field} silently replaced active identity"
            );
        }
        anyhow::ensure!(endpoint.count()? == 0);
    }
    Ok(())
}

#[tokio::test]
async fn cached_generation_rejects_model_override_and_revoked_connection() -> anyhow::Result<()> {
    for backend in BACKENDS {
        let repo = SpaceConfigRepository::new(spy::storage(backend).await?);
        for (space, dims) in [(SPACES[0], 8), (SPACES[1], 12)] {
            let endpoint = spy::Spy::start().await?;
            spy::configure(&repo, space, &endpoint.endpoint, dims).await?;
            let mut handles = Vec::new();
            for operation in GENERATION {
                let (model, client, _) = spy::context(&repo, space).model_client(operation).await?;
                spy::model_call(
                    client.clone(),
                    "unapproved-request-override",
                    "must not leave process",
                )
                .await?;
                handles.push((model, client));
            }
            anyhow::ensure!(
                endpoint.count()? == 0,
                "pinned generation client accepted caller model override"
            );
            spy::edit(&repo, space, |processing| {
                processing
                    .connections
                    .get_mut("generate")
                    .context("generation connection missing")?
                    .revoked = true;
                Ok(())
            })
            .await?;
            for (model, client) in handles {
                spy::model_call(client, &model, "account revocation before continuation").await?;
            }
            anyhow::ensure!(
                endpoint.count()? == 0,
                "{backend:?}: connection revocation failed to fence cached model clients"
            );
            let mut embeddings = Vec::new();
            for operation in EMBEDDING {
                let (_, provider, _) = spy::context(&repo, space).embedding(operation).await?;
                embeddings.push(provider);
            }
            spy::edit(&repo, space, |processing| {
                processing
                    .connections
                    .get_mut("embed")
                    .context("embedding connection missing")?
                    .revoked = true;
                Ok(())
            })
            .await?;
            for provider in embeddings {
                anyhow::ensure!(provider.embed(&["revoked account".into()]).await.is_err());
            }
            anyhow::ensure!(endpoint.count()? == 0);
        }
    }
    Ok(())
}
