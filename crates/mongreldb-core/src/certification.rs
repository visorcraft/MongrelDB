//! Machine-generated release certification manifests.
//!
//! A manifest is valid only when every recorded command passed, its evidence
//! artifact exists, and its source and artifact identities match what the
//! verifier expected. CI creates the manifest after executing the commands.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CertificationManifest {
    pub commit: String,
    pub artifact_sha256: String,
    pub implementation_status_sha256: String,
    pub rust_version: String,
    pub tests: Vec<CertificationTest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CertificationTest {
    pub id: String,
    pub command: String,
    pub status: CertificationStatus,
    pub duration_ms: u64,
    pub artifact: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CertificationStatus {
    Passed,
    Failed,
}

impl CertificationManifest {
    pub fn validate(
        &self,
        expected_commit: &str,
        expected_artifact_sha256: &str,
    ) -> Result<(), String> {
        if self.commit != expected_commit {
            return Err(format!(
                "certification commit mismatch: expected {expected_commit}, got {}",
                self.commit
            ));
        }
        if self.artifact_sha256 != expected_artifact_sha256 {
            return Err("certification artifact SHA-256 mismatch".into());
        }
        if self.implementation_status_sha256.len() != 64
            || !self
                .implementation_status_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err("certification implementation-status SHA-256 is invalid".into());
        }
        if self.tests.is_empty() {
            return Err("certification manifest has no tests".into());
        }
        for test in &self.tests {
            if test.id.is_empty() || test.command.is_empty() || test.artifact.is_empty() {
                return Err("certification test has an empty required field".into());
            }
            if test.duration_ms == 0 {
                return Err(format!(
                    "certification test {} has no measured duration",
                    test.id
                ));
            }
            if test.status != CertificationStatus::Passed {
                return Err(format!("certification test {} did not pass", test.id));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest(status: CertificationStatus) -> CertificationManifest {
        CertificationManifest {
            commit: "abc".into(),
            artifact_sha256: "def".into(),
            implementation_status_sha256: "0".repeat(64),
            rust_version: "rustc test".into(),
            tests: vec![CertificationTest {
                id: "workspace".into(),
                command: "cargo test --workspace".into(),
                status,
                duration_ms: 1,
                artifact: "cargo-test.log".into(),
            }],
        }
    }

    #[test]
    fn validation_rejects_failed_or_wrong_identity_evidence() {
        assert!(manifest(CertificationStatus::Passed)
            .validate("abc", "def")
            .is_ok());
        assert!(manifest(CertificationStatus::Failed)
            .validate("abc", "def")
            .is_err());
        assert!(manifest(CertificationStatus::Passed)
            .validate("other", "def")
            .is_err());
        assert!(manifest(CertificationStatus::Passed)
            .validate("abc", "other")
            .is_err());
        let mut invalid_status = manifest(CertificationStatus::Passed);
        invalid_status.implementation_status_sha256 = "not-a-hash".into();
        assert!(invalid_status.validate("abc", "def").is_err());
        let mut unmeasured = manifest(CertificationStatus::Passed);
        unmeasured.tests[0].duration_ms = 0;
        assert!(unmeasured.validate("abc", "def").is_err());
    }
}
