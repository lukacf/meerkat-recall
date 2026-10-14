//! Thin local provisioning CLI over the shared HTTP/MCP claims contract.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use elephant_auth::{service_token, Claims};
use elephant_policy::Clearance;
use elephant_types::RecordId;

#[derive(Subcommand, Debug)]
pub enum ServiceTokenCommand {
    /// Create a private local signing secret. Refuses to replace any file.
    Bootstrap {
        #[arg(long)]
        secret_file: PathBuf,
    },
    /// Issue a bounded, explicitly scoped bearer token without a running server.
    Issue(Box<IssueArgs>),
}

#[derive(Args, Debug)]
pub struct IssueArgs {
    /// Read signing secret from a private file (or `JWT_SECRET_FILE/JWT_SECRET`).
    #[arg(long)]
    secret_file: Option<PathBuf>,
    #[arg(long)]
    subject: String,
    /// Explicit named-space grant; repeat for each authorized space.
    #[arg(
        long = "space",
        required_unless_present_any = ["global_admin", "bootstrap_space"],
        conflicts_with_all = ["global_admin", "bootstrap_space"]
    )]
    spaces: Vec<String>,
    /// Bootstrap one named space's HTTP management with explicit clearances.
    /// Has no data/MCP authority and expires within one hour.
    #[arg(long, conflicts_with_all = ["spaces", "global_admin", "read_only"], requires = "clearances_file")]
    bootstrap_space: Option<String>,
    /// Explicit global bootstrap/snapshot management, no ordinary data grants.
    #[arg(long, conflicts_with = "read_only")]
    global_admin: bool,
    /// Explicit operation scope; repeat for multiple scopes.
    #[arg(long = "scope", required = true)]
    scopes: Vec<String>,
    /// Refuse all scopes outside the read: namespace.
    #[arg(long)]
    read_only: bool,
    /// Lifetime in seconds, from 1 through 2678400 (31 days).
    #[arg(long, default_value_t = 3600)]
    expires_in: u32,
    #[arg(long)]
    issuer: Option<String>,
    #[arg(long)]
    audience: Option<String>,
    #[arg(long, default_value_t = 0)]
    security_level: i32,
    #[arg(long = "security-label")]
    security_labels: Vec<String>,
    /// JSON array of explicit ABAC clearances; never inferred from the space.
    #[arg(long)]
    clearances_file: Option<PathBuf>,
    /// JSON array of subject record IDs. [] denies all subject-bearing records.
    #[arg(long)]
    subject_allowlist_file: Option<PathBuf>,
    #[arg(long = "purpose")]
    purposes: Vec<String>,
    /// Write only the bearer to a new private file instead of stdout.
    #[arg(long, conflicts_with = "json")]
    output: Option<PathBuf>,
    /// Emit JSON containing the bearer and expiry. Treat stdout as a secret.
    #[arg(long)]
    json: bool,
}

pub fn run(command: ServiceTokenCommand) -> Result<()> {
    match command {
        ServiceTokenCommand::Bootstrap { secret_file } => {
            // Three independently random UUIDs provide 366 random bits; the
            // encoded secret comfortably exceeds the shared verifier minimum.
            let secret = format!(
                "{}{}{}",
                uuid::Uuid::new_v4().simple(),
                uuid::Uuid::new_v4().simple(),
                uuid::Uuid::new_v4().simple()
            );
            write_private_new(&secret_file, format!("{secret}\n").as_bytes())?;
            println!(
                "{}",
                serde_json::json!({"secret_file": secret_file, "created": true})
            );
            Ok(())
        }
        ServiceTokenCommand::Issue(args) => issue(*args),
    }
}

fn signing_secret(path: Option<PathBuf>) -> Result<String> {
    let path = path.or_else(|| std::env::var_os("JWT_SECRET_FILE").map(PathBuf::from));
    let environment = std::env::var("JWT_SECRET")
        .ok()
        .filter(|value| !value.is_empty());
    anyhow::ensure!(
        path.is_none() || environment.is_none(),
        "provide one of JWT_SECRET or a signing-secret file, not both"
    );
    if let Some(path) = path {
        return service_token::read_secret_file(&path).map_err(Into::into);
    }
    environment.context("set JWT_SECRET_FILE, --secret-file, or JWT_SECRET")
}

fn issue(args: IssueArgs) -> Result<()> {
    let secret = signing_secret(args.secret_file)?;
    let algorithm = std::env::var("JWT_ALGORITHM").unwrap_or_else(|_| "HS256".into());
    anyhow::ensure!(algorithm == "HS256", "JWT_ALGORITHM must be HS256");
    let now = chrono::Utc::now().timestamp();
    let spaces = args
        .bootstrap_space
        .as_ref()
        .map_or_else(|| args.spaces.clone(), |space| vec![space.clone()]);
    let claims = Claims {
        sub: args.subject,
        exp: now
            .checked_add(i64::from(args.expires_in))
            .context("expiry overflow")?,
        iat: now,
        iss: args.issuer.or_else(|| std::env::var("JWT_ISSUER").ok()),
        aud: args
            .audience
            .or_else(|| std::env::var("JWT_AUDIENCE").ok())
            .map(|value| vec![value]),
        space_id: (spaces.len() == 1).then(|| spaces[0].clone()),
        spaces: (!args.global_admin).then_some(spaces),
        bootstrap_space: args.bootstrap_space.clone(),
        scopes: args.scopes,
        security_level: args.security_level,
        security_labels: args.security_labels,
        clearances: args
            .clearances_file
            .map(|path| read_json_file::<Vec<Clearance>>(&path))
            .transpose()?
            .unwrap_or_default(),
        subject_allowlist: args
            .subject_allowlist_file
            .map(|path| read_json_file::<Vec<RecordId>>(&path))
            .transpose()?,
        purpose: args.purposes,
    };
    let token = if args.bootstrap_space.is_some() {
        service_token::issue_space_bootstrap(&secret, &claims, now)?
    } else if args.global_admin {
        service_token::issue_global_admin(&secret, &claims, now)?
    } else {
        service_token::issue(&secret, &claims, args.read_only, now)?
    };
    if let Some(path) = args.output {
        write_private_new(&path, format!("{token}\n").as_bytes())?;
        println!(
            "{}",
            serde_json::json!({"token_file": path, "expires_at": claims.exp})
        );
    } else if args.json {
        println!(
            "{}",
            serde_json::json!({"token": token, "token_type": "Bearer", "expires_at": claims.exp, "space_id": claims.space_id, "spaces": claims.spaces, "bootstrap_space": claims.bootstrap_space, "scopes": claims.scopes})
        );
    } else {
        println!("{token}");
    }
    Ok(())
}

fn read_json_file<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    use std::io::Read;
    let mut data = Vec::new();
    std::fs::File::open(path)?
        .take(65_537)
        .read_to_end(&mut data)?;
    anyhow::ensure!(data.len() <= 65_536, "claim file exceeds 64 KiB");
    serde_json::from_slice(&data).context("invalid claim JSON file")
}

pub fn write_private_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .context("create new private output file")?;
    file.write_all(bytes).context("write private output file")?;
    file.sync_all().context("persist private output file")?;
    #[cfg(unix)]
    {
        let parent = path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}
