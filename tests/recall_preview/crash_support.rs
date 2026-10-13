//! Process harness: each kill targets an owned child, never an external service.
//! The acknowledgment file is test IPC, not a database save or backup operation.

#![allow(dead_code)]

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use elephant_storage::{StorageBackendKind, StorageClient, StorageConfig};

pub const SPACE: &str = "crash-qualification";
pub const CHILD_MODE: &str = "RECALL_QUALIFICATION_CRASH_CHILD_MODE";
pub const CHILD_ROOT: &str = "RECALL_QUALIFICATION_CRASH_CHILD_ROOT";

pub struct CrashDirectory(pub PathBuf);

impl CrashDirectory {
    pub fn new() -> anyhow::Result<Self> {
        let path = std::env::temp_dir().join(format!("recall-crash-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path)?;
        Ok(Self(path))
    }
}

impl Drop for CrashDirectory {
    fn drop(&mut self) {
        let _cleanup = std::fs::remove_dir_all(&self.0);
    }
}

/// A reopened destination may have only a WAL and no base snapshot yet.
/// The production constructor must replay acknowledged mutations in that case.
pub async fn open(directory: &Path) -> anyhow::Result<StorageClient> {
    let destination = directory.join("live.snapshot");
    let config = StorageConfig {
        backend: StorageBackendKind::ElephantDb,
        namespace: "qualification_crash".into(),
        database: "acknowledged_mutations".into(),
        snapshot_load: destination.exists().then(|| destination.clone()),
        ..StorageConfig::default()
    };
    Ok(StorageClient::new_with_durability(
        config,
        Some(destination),
        Some(directory.join("independent-journal")),
    )
    .await?)
}

pub fn acknowledge(directory: &Path, observation: &serde_json::Value) -> anyhow::Result<()> {
    let temporary = directory.join("ack-writing.json");
    let mut file = File::create(&temporary)?;
    file.write_all(&serde_json::to_vec(observation)?)?;
    file.sync_all()?;
    std::fs::rename(temporary, directory.join("ack.json"))?;
    Ok(())
}

/// Inspect every live file in this isolated store, including checkpoint/WAL/temp
/// and the independent journal. No named backups are created by erase scenarios.
pub fn assert_no_live_payload(directory: &Path, needle: &[u8]) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        anyhow::ensure!(
            !file_type.is_symlink(),
            "unexpected symlink in owned crash fixture"
        );
        if file_type.is_dir() {
            assert_no_live_payload(&entry.path(), needle)?;
        } else if entry.file_name() != "ack.json" && entry.file_name() != "child.stderr" {
            let bytes = std::fs::read(entry.path())?;
            anyhow::ensure!(
                !bytes.windows(needle.len()).any(|window| window == needle),
                "erased source text remains in live file {}",
                entry.path().display()
            );
        }
    }
    Ok(())
}

struct OwnedChild(Child);

impl Drop for OwnedChild {
    fn drop(&mut self) {
        // Also reap a child when a bounded wait or an assertion fails.
        let _kill = self.0.kill();
        let _wait = self.0.wait();
    }
}

#[cfg(unix)]
pub fn acknowledge_then_sigkill(directory: &Path, mode: &str) -> anyhow::Result<serde_json::Value> {
    use std::os::unix::process::ExitStatusExt;

    let stderr = directory.join("child.stderr");
    let mut child = OwnedChild(
        Command::new(std::env::current_exe()?)
            .args(["--exact", "crash_fixture_child", "--nocapture"])
            .env(CHILD_MODE, mode)
            .env(CHILD_ROOT, directory)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(File::create(&stderr)?)
            .spawn()?,
    );
    let deadline = Instant::now() + Duration::from_secs(45);
    let acknowledgment = directory.join("ack.json");
    loop {
        if acknowledgment.exists() {
            break;
        }
        if let Some(status) = child.0.try_wait()? {
            anyhow::bail!(
                "{mode}: fixture exited before acknowledgment ({status}): {}",
                std::fs::read_to_string(&stderr)?
            );
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "{mode}: no acknowledged product checkpoint within 45 seconds"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let acknowledgment: serde_json::Value =
        serde_json::from_slice(&std::fs::read(acknowledgment)?)?;
    anyhow::ensure!(
        acknowledgment["mode"] == mode,
        "child checkpoint identity mismatch"
    );
    child.0.kill()?; // std::process::Child::kill sends SIGKILL on Unix.
    let status = child.0.wait()?;
    anyhow::ensure!(
        status.signal() == Some(9),
        "child did not die from SIGKILL: {status}"
    );
    Ok(acknowledgment)
}
