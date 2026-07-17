#[test]
fn build_info_reports_exact_engine_identity() {
    let info = mongreldb_core::build_info();
    assert_eq!(info.artifact_version, env!("CARGO_PKG_VERSION"));
    assert_eq!(info.engine_version, env!("CARGO_PKG_VERSION"));
    assert_eq!(info.mongreldb_git_sha.len(), 40);
    assert!(info
        .mongreldb_git_sha
        .bytes()
        .all(|byte| byte.is_ascii_hexdigit()));
    assert!(!info.target_triple.is_empty());
    assert!(!info.build_profile.is_empty());
}
