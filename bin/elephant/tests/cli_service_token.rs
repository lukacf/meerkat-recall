//! Installed provisioning commands use the same verifier as HTTP and MCP.
#![allow(clippy::unwrap_used)]
use elephant_auth::LegacyVerifier;
use tokio::process::Command;

fn cli() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_meerkat-recall"));
    for name in [
        "JWT_SECRET",
        "JWT_SECRET_FILE",
        "JWT_ISSUER",
        "JWT_AUDIENCE",
        "JWT_ALGORITHM",
    ] {
        command.env_remove(name);
    }
    command
}

#[tokio::test]
async fn bootstrap_refuses_overwrite_and_tokens_preserve_explicit_space() {
    let directory = tempfile::tempdir().unwrap();
    let secret = directory.path().join("signing-key");
    let output = cli()
        .args(["service-token", "bootstrap", "--secret-file"])
        .arg(&secret)
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value = std::fs::read_to_string(&secret).unwrap();
    assert!(!String::from_utf8_lossy(&output.stdout).contains(value.trim()));
    assert!(!cli()
        .args(["service-token", "bootstrap", "--secret-file"])
        .arg(&secret)
        .status()
        .await
        .unwrap()
        .success());
    assert_eq!(value, std::fs::read_to_string(&secret).unwrap());
    let token_file = directory.path().join("reader-token");
    let output = cli()
        .args(["service-token", "issue", "--secret-file"])
        .arg(&secret)
        .args([
            "--subject",
            "household-reader",
            "--space",
            "household",
            "--scope",
            "read:entity",
            "--read-only",
            "--expires-in",
            "3600",
            "--output",
        ])
        .arg(&token_file)
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let token = std::fs::read_to_string(&token_file).unwrap();
    assert!(!String::from_utf8_lossy(&output.stdout).contains(token.trim()));
    // Verify through AuthConfig's publicly shared token verifier configuration.
    let verified = LegacyVerifier {
        secret: value.trim(),
        algorithm: jsonwebtoken::Algorithm::HS256,
        issuer: None,
        audience: None,
        mappings: None,
    }
    .verify(token.trim())
    .unwrap();
    assert_eq!(verified.claims().space_id.as_deref(), Some("household"));
    assert_eq!(verified.claims().scopes, vec!["read:entity"]);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(token_file).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}

#[tokio::test]
async fn issuance_refuses_write_scope_in_read_only_mode_and_missing_space() {
    for args in [
        vec![
            "--subject",
            "reader",
            "--space",
            "household",
            "--scope",
            "write:entity",
            "--read-only",
        ],
        vec!["--subject", "reader", "--scope", "read:entity"],
    ] {
        let output = cli()
            .env(
                "JWT_SECRET",
                "fixture-secret-with-at-least-thirty-two-bytes",
            )
            .args(["service-token", "issue"])
            .args(args)
            .output()
            .await
            .unwrap();
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
    }
}

#[tokio::test]
async fn multi_space_requires_explicit_selection_and_global_admin_has_no_data_scopes() {
    let secret = "fixture-secret-with-at-least-thirty-two-bytes";
    let output = cli()
        .env("JWT_SECRET", secret)
        .args([
            "service-token",
            "issue",
            "--subject",
            "reader",
            "--space",
            "business-a",
            "--space",
            "business-b",
            "--scope",
            "read:entity",
            "--read-only",
        ])
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let token = String::from_utf8(output.stdout).unwrap();
    let verified = LegacyVerifier {
        secret,
        algorithm: jsonwebtoken::Algorithm::HS256,
        issuer: None,
        audience: None,
        mappings: None,
    }
    .verify(token.trim())
    .unwrap();
    assert_eq!(verified.claims().space_id, None);
    assert_eq!(
        verified.claims().spaces.as_ref().unwrap(),
        &vec!["business-a".to_string(), "business-b".to_string()]
    );
    for scope in ["manage:grant", "read:doc", "*"] {
        let output = cli()
            .env("JWT_SECRET", secret)
            .args([
                "service-token",
                "issue",
                "--global-admin",
                "--subject",
                "bootstrap",
                "--scope",
                scope,
                "--expires-in",
                "900",
            ])
            .output()
            .await
            .unwrap();
        assert_eq!(output.status.success(), scope == "manage:grant");
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "Keep bootstrap issuance and all negative authority controls in one scenario."
)]
async fn classified_space_bootstrap_is_explicit_short_lived_and_not_an_ordinary_token() {
    let directory = tempfile::tempdir().unwrap();
    let clearances = directory.path().join("clearances.json");
    std::fs::write(
        &clearances,
        br#"[{"policy_id":"household-policy","max_level":3,"labels":[],"handling_allow":[]}]"#,
    )
    .unwrap();
    let secret = "synthetic-only-cli-bootstrap-signing-secret";
    let token_file = directory.path().join("bootstrap-token");
    let output = cli()
        .env("JWT_SECRET", secret)
        .args([
            "service-token",
            "issue",
            "--bootstrap-space",
            "household",
            "--subject",
            "local-operator",
            "--scope",
            "read:space_config",
            "--scope",
            "write:space_config",
            "--scope",
            "manage:grant",
            "--expires-in",
            "900",
            "--clearances-file",
        ])
        .arg(&clearances)
        .arg("--output")
        .arg(&token_file)
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let token = std::fs::read_to_string(&token_file).unwrap();
    assert!(!String::from_utf8_lossy(&output.stdout).contains(token.trim()));
    let verified = LegacyVerifier {
        secret,
        algorithm: jsonwebtoken::Algorithm::HS256,
        issuer: None,
        audience: None,
        mappings: None,
    }
    .verify(token.trim())
    .unwrap();
    assert_eq!(
        verified.claims().bootstrap_space.as_deref(),
        Some("household")
    );
    assert_eq!(verified.claims().space_id.as_deref(), Some("household"));
    assert_eq!(
        verified.claims().spaces.as_ref().unwrap(),
        &vec!["household".to_string()]
    );
    assert_eq!(verified.claims().exp - verified.claims().iat, 900);
    assert_eq!(verified.claims().clearances[0].max_level, 3);
    for extra in [
        vec!["--space", "household"],
        vec!["--global-admin"],
        vec!["--read-only"],
        vec!["--scope", "read:doc"],
        vec!["--scope", "*"],
        vec!["--expires-in", "3601"],
    ] {
        let output = cli()
            .env("JWT_SECRET", secret)
            .args([
                "service-token",
                "issue",
                "--bootstrap-space",
                "household",
                "--subject",
                "local-operator",
                "--scope",
                "manage:grant",
                "--clearances-file",
            ])
            .arg(&clearances)
            .args(extra)
            .output()
            .await
            .unwrap();
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
    }
    let missing_clearance = cli()
        .env("JWT_SECRET", secret)
        .args([
            "service-token",
            "issue",
            "--bootstrap-space",
            "household",
            "--subject",
            "local-operator",
            "--scope",
            "manage:grant",
        ])
        .output()
        .await
        .unwrap();
    assert!(!missing_clearance.status.success());
    assert!(missing_clearance.stdout.is_empty());
}
