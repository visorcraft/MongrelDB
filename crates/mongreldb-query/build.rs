use std::path::Path;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=MONGRELDB_GIT_SHA");
    let sha = std::env::var("MONGRELDB_GIT_SHA")
        .ok()
        .filter(|sha| valid_sha(sha))
        .or_else(git_sha)
        .or_else(package_sha)
        .unwrap_or_else(|| "unknown".into());
    println!("cargo:rustc-env=MONGRELDB_GIT_SHA={sha}");
}

fn valid_sha(sha: &str) -> bool {
    sha.len() == 40 && sha.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn git_sha() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(Path::new(&std::env::var("CARGO_MANIFEST_DIR").ok()?))
        .output()
        .ok()?;
    let sha = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    output
        .status
        .success()
        .then_some(sha)
        .filter(|sha| valid_sha(sha))
}

fn package_sha() -> Option<String> {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").ok()?;
    let text = std::fs::read_to_string(Path::new(&manifest).join(".cargo_vcs_info.json")).ok()?;
    let rest = text.split_once("\"sha1\"")?.1.split_once(':')?.1;
    let sha = rest
        .trim_start()
        .strip_prefix('"')?
        .split_once('"')?
        .0
        .to_owned();
    valid_sha(&sha).then_some(sha)
}
