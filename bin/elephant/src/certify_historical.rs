//! Local owner completion of the exact closed imported population. No provider,
//! activation, worker or replacement-head adoption is performed here.
use anyhow::{ensure, Context};
use elephant_storage::client::HistoricalCatchupSource;
use elephant_storage::historical_embedding_coverage::HistoricalEmbeddingCoverageRepository;
use elephant_storage::source_publication::SourcePublicationRepository;
use elephant_storage::source_publication_manifest::SourcePublicationManifestRepository;
use elephant_storage::{SpaceConfigRepository, StorageClient};
use elephant_types::publication::{PublicationFence, SourceVisibility};
use elephant_types::{RecordId, TableName};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};

#[derive(clap::Args, Debug, Clone)]
pub struct CertifyHistoricalArgs {
    #[command(flatten)]
    pub target: crate::data_migrate::TransferTarget,
    #[arg(long)]
    pub space: String,
    /// Closed initial or final catchup report; the exact hash is mandatory.
    #[arg(long)]
    pub baseline_report: PathBuf,
    #[arg(long)]
    pub baseline_report_sha256: String,
    /// Exact report-bound imported publication/head/receipt JSONL.
    #[arg(long)]
    pub bindings: PathBuf,
    #[arg(long)]
    pub configuration_id: String,
    #[arg(long)]
    pub reindex_id: String,
    /// Durable create-only per-source receipts. Reuse only for identical input.
    #[arg(long)]
    pub output: PathBuf,
    /// Stop admitting sources after this duration; an admitted transaction may finish.
    #[arg(long, value_parser=clap::value_parser!(u64).range(1..=3600))]
    pub max_elapsed_seconds: u64,
}
fn hash(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}
fn bounded(path: &Path, limit: u64) -> anyhow::Result<Vec<u8>> {
    ensure!(
        std::fs::symlink_metadata(path)?.file_type().is_file(),
        "regular immutable input required"
    );
    let f = std::fs::File::open(path)?;
    ensure!(
        f.metadata()?.len() <= limit,
        "certification input exceeds bound"
    );
    let mut bytes = Vec::new();
    f.take(limit + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= limit, "certification input grew");
    Ok(bytes)
}
fn strict(bytes: &[u8]) -> anyhow::Result<Value> {
    Ok(elephant_storage::transfer_batch::strict_json(
        std::str::from_utf8(bytes)?,
    )?)
}
fn new_record(path: &Path, value: &Value) -> anyhow::Result<()> {
    let mut bytes = serde_json::to_vec(value)?;
    bytes.push(b'\n');
    if path.try_exists()? {
        ensure!(
            bounded(path, 8 * 1024 * 1024)? == bytes,
            "existing certification receipt differs"
        );
        return Ok(());
    }
    let parent = path.parent().context("receipt parent absent")?;
    let partial = parent.join(format!(".writing-{}", uuid::Uuid::new_v4()));
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&partial)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    // New-name publication cannot replace any consumed successful receipt.
    std::fs::hard_link(&partial, path)?;
    std::fs::File::open(parent)?.sync_all()?;
    std::fs::remove_file(partial)?;
    Ok(())
}
fn input(
    args: &CertifyHistoricalArgs,
) -> anyhow::Result<(Value, BTreeMap<RecordId, HistoricalCatchupSource>)> {
    let bytes = bounded(&args.baseline_report, 1024 * 1024)?;
    ensure!(
        hash(&bytes) == args.baseline_report_sha256,
        "original report hash differs"
    );
    let report = strict(&bytes)?;
    ensure!(
        elephant_storage::historical_catchup_ledger::valid_baseline_report(&report)
            && report["target_space_id"] == args.space
            && report["catchup_baseline"]["format"] == "recall-initial-catchup-bindings-v1",
        "closed population report required"
    );
    ensure!(
        std::fs::symlink_metadata(&args.bindings)?
            .file_type()
            .is_file(),
        "regular bindings input required"
    );
    let file = std::fs::File::open(&args.bindings)?;
    let mut reader = BufReader::new(file);
    let mut digest = Sha256::new();
    let mut sources = BTreeMap::new();
    let mut total = 0usize;
    loop {
        let mut line = Vec::new();
        let n = reader
            .by_ref()
            .take(1024 * 1024 + 1)
            .read_until(b'\n', &mut line)?;
        if n == 0 {
            break;
        }
        ensure!(
            n <= 1024 * 1024 && line.last() == Some(&b'\n'),
            "bounded complete bindings line required"
        );
        total = total.checked_add(n).context("binding size overflow")?;
        ensure!(
            total <= 128 * 1024 * 1024 && sources.len() < 100_000,
            "population evidence bound exceeded"
        );
        digest.update(&line);
        let source: HistoricalCatchupSource = serde_json::from_value(strict(&line)?)?;
        ensure!(
            source.head.space_id == args.space
                && source.source_id == source.head.source_id
                && source.source_id.table() == TableName::Source
                && source.head.lifecycle == SourceVisibility::Active
                && source.head.desired_revision.is_some()
                && source.head.desired_revision == source.head.published_revision
                && sources.insert(source.source_id.clone(), source).is_none(),
            "population identity, duplicate or lifecycle differs"
        );
    }
    ensure!(
        report["catchup_baseline"]["bindings_sha256"] == hex::encode(digest.finalize())
            && report["catchup_baseline"]["bindings_count"].as_u64() == Some(sources.len() as u64),
        "population hash or count differs"
    );
    Ok((report, sources))
}
async fn population(
    client: &StorageClient,
    space: &str,
    expected: &BTreeMap<RecordId, HistoricalCatchupSource>,
) -> anyhow::Result<()> {
    let manifests = SourcePublicationManifestRepository::new(client.clone());
    let mut cursor = None;
    let mut found = BTreeMap::new();
    let mut count = 0usize;
    loop {
        let page = SourcePublicationRepository::new(client.clone())
            .list_heads_page(space, cursor.as_deref(), 100)
            .await?;
        count += page.heads.len();
        ensure!(count <= 100_000, "physical population bound exceeded");
        for head in page.heads {
            if head.lifecycle != SourceVisibility::Active {
                continue;
            }
            let Some(revision) = head.published_revision.clone() else {
                continue;
            };
            let fence = PublicationFence {
                source_id: head.source_id.clone(),
                generation: head.generation.clone(),
                revision_id: revision,
                operation_id: head.operation_id.clone(),
            };
            if manifests
                .get(space, &fence)
                .await?
                .is_some_and(|m| m.historical_adoption.is_some())
            {
                ensure!(
                    found
                        .insert(head.source_id.clone(), serde_json::to_value(head)?)
                        .is_none(),
                    "duplicate live imported head"
                );
            }
        }
        match page.next_key {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    ensure!(
        found.len() == expected.len(),
        "closed imported population count changed"
    );
    for (id, source) in expected {
        ensure!(
            found.get(id) == Some(&serde_json::to_value(&source.head)?),
            "closed imported publication changed"
        );
    }
    Ok(())
}
async fn activated(
    client: &StorageClient,
    args: &CertifyHistoricalArgs,
    id: &RecordId,
) -> anyhow::Result<elephant_types::records::SpaceConfig> {
    let config = SpaceConfigRepository::new(client.clone())
        .get_by_space_id(&args.space)
        .await?
        .context("configuration absent")?;
    let processing = config.processing.as_ref().context("processing absent")?;
    let proof = processing
        .completed_embedding_reindex
        .as_ref()
        .context("actual completed activation absent")?;
    ensure!(
        config.base.id == *id
            && proof.reindex_id == args.reindex_id
            && processing.pending_reindex.is_none()
            && proof.content_generation == processing.index_generation
            && proof.activated_config_generation == processing.generation,
        "selected activation changed"
    );
    let resolved = elephant_types::provider_binding::ResolvedSpaceConfig {
        token: elephant_types::provider_binding::SpaceConfigToken {
            space_id: args.space.clone(),
            config_id: id.to_string(),
            generation: processing.generation,
            policy_id: processing.policy_id.clone(),
        },
        processing: processing.clone(),
        purpose: config.purpose.clone(),
        skill: config.skill.clone(),
    };
    for operation in [
        elephant_types::provider_binding::ModelOperation::DocumentEmbedding,
        elephant_types::provider_binding::ModelOperation::QueryEmbedding,
    ] {
        ensure!(
            resolved
                .authorize(&args.space, operation, Some(&proof.partition.profile_id))?
                .vector_partition()?
                == proof.partition,
            "selected full activation partition changed"
        );
    }
    let grant = processing
        .bindings
        .get(&elephant_types::provider_binding::ModelOperation::ReindexEmbedding)
        .context("reindex grant absent")?;
    ensure!(
        !grant.revoked
            && grant.profile_id == proof.partition.profile_id
            && processing.active_embedding_profile_id.as_deref()
                == Some(proof.partition.profile_id.as_str()),
        "reindex approval or active profile changed"
    );
    Ok(config)
}
pub async fn run(args: CertifyHistoricalArgs) -> anyhow::Result<()> {
    run_inner(args, None).await
}
async fn run_inner(
    args: CertifyHistoricalArgs,
    opened: Option<StorageClient>,
) -> anyhow::Result<()> {
    let started = std::time::Instant::now();
    let id: RecordId = args.configuration_id.parse()?;
    ensure!(
        id.table() == TableName::SpaceConfig
            && !args.space.trim().is_empty()
            && !args.reindex_id.trim().is_empty(),
        "explicit certification identity required"
    );
    if opened.is_none() {
        let path = args
            .target
            .url
            .as_deref()
            .context("explicit local target URL required")?
            .strip_prefix("rocksdb://")
            .context("certification requires local native custody")?;
        ensure!(
            Path::new(path).is_absolute() && Path::new(path).is_dir(),
            "existing absolute native target required"
        );
    }
    let (report, sources) = input(&args)?;
    if !args.output.try_exists()? {
        let mut d = std::fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            d.mode(0o700);
        }
        d.create(&args.output)?;
    }
    ensure!(
        std::fs::symlink_metadata(&args.output)?
            .file_type()
            .is_dir(),
        "certification output must be directory"
    );
    let binding = serde_json::json!({"format":"recall-historical-certification-v1","space":args.space,"baseline_report_sha256":args.baseline_report_sha256,"configuration_id":id,"reindex_id":args.reindex_id,"sources":sources.len()});
    new_record(&args.output.join("binding.json"), &binding)?;
    let owns_connection = opened.is_none();
    let client = match opened {
        Some(c) => c,
        None => args.target.clone().connect().await?,
    };
    let result = async {
    client.verify_journal_identity().await?;
    ensure!(
        report["catchup_baseline"]["journal_identity"] == client.erasure_journal()?.identity(),
        "original journal identity changed"
    );
    let original_config = activated(&client, &args, &id).await?;
    population(&client, &args.space, &sources).await?;
    if sources.is_empty() {
        client
            .verify_completed_empty_historical_population(
                &args.baseline_report_sha256,
                &report,
                &args.space,
                &id,
            )
            .await?;
    }
    let attempt = uuid::Uuid::new_v4().to_string();
    let mut done = 0usize;
    for (source_id, source) in &sources {
        if started.elapsed().as_secs() >= args.max_elapsed_seconds {
            new_record(
                &args.output.join(format!("attempt-{attempt}.json")),
                &serde_json::json!({"state":"admission_deadline_reached","completed":done,"total":sources.len(),"seconds":started.elapsed().as_secs_f64()}),
            )?;
            anyhow::bail!("source admission deadline reached; exact population remains resumable");
        }
        let result = HistoricalEmbeddingCoverageRepository::new(client.clone())
            .certify_bound(&args.space, source, &id, &args.reindex_id)
            .await;
        let receipt = match result {
            Ok(receipt) => receipt,
            Err(error) => {
                new_record(
                    &args.output.join(format!("attempt-{attempt}.json")),
                    &serde_json::json!({"state":"partial_failure","completed":done,"total":sources.len(),"failed_source":source_id,"seconds":started.elapsed().as_secs_f64()}),
                )?;
                return Err(error.into());
            }
        };
        new_record(
            &args.output.join(format!(
                "source-{}.json",
                hash(source_id.to_string().as_bytes())
            )),
            &serde_json::to_value(receipt)?,
        )?;
        done += 1;
    }
    let canonical = elephant_storage::canonical_commit::CanonicalRepository::new(client.clone());
    let final_epoch = canonical.observe_epoch(&args.space).await?;
    let final_config = activated(&client, &args, &id).await?;
    ensure!(
        serde_json::to_value(&original_config)? == serde_json::to_value(&final_config)?,
        "activation configuration changed during certification"
    );
    population(&client, &args.space, &sources).await?;
    ensure!(
        canonical.observe_epoch(&args.space).await? == final_epoch,
        "population changed during final certification read"
    );
    drop(canonical);
    new_record(
        &args.output.join("complete.json"),
        &serde_json::json!({"state":"all_bound_current_imported_publications_certified","binding":binding,"completed":done,"empty_population":sources.is_empty(),"configuration_sha256":hash(&serde_json::to_vec(&final_config)?),"activation":final_config.processing.as_ref().and_then(|p|p.completed_embedding_reindex.as_ref()),"final_epoch":final_epoch,"custody":"exclusive local native writer; no managed workers or external client clones"}),
    )?;
    new_record(
        &args.output.join(format!("attempt-{attempt}.json")),
        &serde_json::json!({"state":"completed","completed":done,"seconds":started.elapsed().as_secs_f64()}),
    )?;
    Ok::<(), anyhow::Error>(())
    }.await;
    if owns_connection {
        crate::data_migrate::finish_owned_connection(client, result).await
    } else {
        result
    }
}

#[cfg(all(test, feature = "rocksdb"))]
#[path = "certify_historical_tests.rs"]
mod tests;
