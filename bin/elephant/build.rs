//! Link the required native libraries for the Recall command-line binary.

use std::{env, error::Error, io, path::PathBuf, process::Command};

fn main() -> Result<(), Box<dyn Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    if env::var("TARGET").ok().as_deref() != Some("x86_64-unknown-linux-gnu")
        || env::var_os("CARGO_FEATURE_FASTEMBED").is_none()
    {
        return Ok(());
    }

    // The pinned Linux static ORT CPU initializer uses GCC's __cpu_features2.
    // Rust disables the compiler driver's default libraries, so libgcc_s alone
    // does not supply this implementation. Link the compiler's actual archive
    // into this final binary; do not manufacture a replacement symbol.
    let output = Command::new("cc").arg("-print-libgcc-file-name").output()?;
    if !output.status.success() {
        return Err(io::Error::other("native cc could not locate libgcc.a").into());
    }
    let archive = PathBuf::from(String::from_utf8(output.stdout)?.trim());
    if !archive.is_absolute()
        || archive.file_name().and_then(|name| name.to_str()) != Some("libgcc.a")
    {
        return Err(io::Error::other("native cc must return an absolute libgcc.a path").into());
    }
    let archive = archive.canonicalize()?;
    println!("cargo:rerun-if-changed={}", archive.display());
    println!("cargo:rustc-link-arg={}", archive.display());
    Ok(())
}
