use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=MONGRELDB_GIT_SHA");
    println!("cargo:rerun-if-env-changed=MONGRELDB_KIT_GIT_SHA");
    let engine = std::env::var("MONGRELDB_GIT_SHA").ok().or_else(git_sha);
    println!(
        "cargo:rustc-env=MONGRELDB_GIT_SHA={}",
        engine
            .as_deref()
            .filter(|sha| valid_sha(sha))
            .unwrap_or("unknown")
    );
    let kit = std::env::var("MONGRELDB_KIT_GIT_SHA").ok();
    println!(
        "cargo:rustc-env=MONGRELDB_KIT_GIT_SHA={}",
        kit.as_deref()
            .filter(|sha| valid_sha(sha))
            .unwrap_or("unknown")
    );
}

fn valid_sha(sha: &str) -> bool {
    sha.len() == 40 && sha.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn git_sha() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}
