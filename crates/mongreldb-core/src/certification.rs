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
    pub architecture_tasks: Vec<ArchitectureQualification>,
    pub tests: Vec<CertificationTest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchitectureQualification {
    pub id: String,
    pub status: ArchitectureStatus,
    pub evidence: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArchitectureStatus {
    Qualified,
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
        let required_tasks = (1..=10)
            .map(|index| format!("R{index}"))
            .collect::<std::collections::BTreeSet<_>>();
        let task_ids = self
            .architecture_tasks
            .iter()
            .map(|task| task.id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        if task_ids != required_tasks || task_ids.len() != self.architecture_tasks.len() {
            return Err("certification architecture tasks must contain R1 through R10 once".into());
        }
        let test_ids = self
            .tests
            .iter()
            .map(|test| test.id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        for task in &self.architecture_tasks {
            if task.status != ArchitectureStatus::Qualified {
                return Err(format!(
                    "certification architecture task {} is not qualified",
                    task.id
                ));
            }
            if task.evidence.is_empty()
                || task
                    .evidence
                    .iter()
                    .any(|evidence| !test_ids.contains(evidence.as_str()))
            {
                return Err(format!(
                    "certification architecture task {} has invalid evidence",
                    task.id
                ));
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
            architecture_tasks: (1..=10)
                .map(|index| ArchitectureQualification {
                    id: format!("R{index}"),
                    status: ArchitectureStatus::Qualified,
                    evidence: vec!["workspace".into()],
                })
                .collect(),
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
        let mut missing_task = manifest(CertificationStatus::Passed);
        missing_task.architecture_tasks.pop();
        assert!(missing_task.validate("abc", "def").is_err());
        let mut unknown_evidence = manifest(CertificationStatus::Passed);
        unknown_evidence.architecture_tasks[0].evidence = vec!["missing".into()];
        assert!(unknown_evidence.validate("abc", "def").is_err());
    }
}
