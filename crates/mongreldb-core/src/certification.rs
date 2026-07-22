//! Machine-generated release certification manifests.
//!
//! A manifest is valid only when every recorded command passed, its evidence
//! artifact exists, and its source and artifact identities match what the
//! verifier expected. CI creates the manifest after executing the commands.
//!
//! Architecture task IDs follow the Stage 0–5 architecture specification
//! (FND-*, S1*, S2*, S3*, S4*, S5*). Residual R1–R10 IDs are optional aliases
//! for backward compatibility and never substitute for Stage IDs.
//!
//! Status rule (audit §2.4 / P0.9): Integrated ≠ Qualified. Validation accepts
//! Integrated rows with traceability evidence. Qualified requires non-empty
//! evidence that maps to recorded tests and at least one evidence class.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Mandatory Stage 0–5 architecture task IDs.
///
/// Must stay aligned with
/// `scripts/generate-certification-manifest.py::MANDATORY_ARCHITECTURE_TASKS`.
pub const MANDATORY_ARCHITECTURE_TASK_IDS: &[&str] = &[
    // Stage 0
    "FND-001", "FND-002", "FND-003", "FND-004", "FND-005", "FND-006", "FND-007",
    // Stage 1
    "S1A-001", "S1A-002", "S1A-003", "S1A-004", "S1B-001", "S1B-002", "S1B-003", "S1B-004",
    "S1B-005", "S1C-001", "S1C-002", "S1C-003", "S1C-004", "S1D-001", "S1D-002", "S1D-003",
    "S1D-004", "S1D-005", "S1D-006", "S1D-007", "S1E-001", "S1E-002", "S1E-003", "S1E-004",
    "S1F-001", "S1F-002", "S1F-003", "S1G", // Stage 2
    "S2A-001", "S2A-002", "S2B-001", "S2B-002", "S2B-003", "S2B-004", "S2C", "S2D", "S2E", "S2F",
    "S2G", "S2H", // Stage 3
    "S3A", "S3B", "S3C", "S3D", "S3E", "S3F", "S3G", "S3H", "S3I", "S3J", "S3K", "S3L",
    // Stage 4
    "S4A", "S4B", "S4C", "S4D", "S4E", "S4F", "S4G", // Stage 5
    "S5A", "S5B", "S5C", "S5D", "S5E", "S5F",
];

/// Optional residual R1–R10 aliases from the previous audit matrix.
pub const RESIDUAL_ALIAS_TASK_IDS: &[&str] =
    &["R1", "R2", "R3", "R4", "R5", "R6", "R7", "R8", "R9", "R10"];

/// Evidence classes that may justify a Qualified promotion (audit §12 / P0.9-T3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceClass {
    SourceIntegration,
    MultiProcess,
    FaultChaos,
    PackagedArtifact,
    PerformanceSlo,
    Security,
    UpgradeCompatibility,
    Documentation,
}

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
    /// Required and non-empty only when `status == Qualified`.
    #[serde(default)]
    pub evidence_classes: Vec<EvidenceClass>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArchitectureStatus {
    /// Production path exists; not release-Qualified without multi-class evidence.
    Integrated,
    /// Exact-SHA product evidence of the required classes has been recorded.
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

/// Stage groupings for Stage-level qualification checks (P0.9-T2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArchitectureStage {
    Stage0Foundations,
    Stage1SingleNode,
    Stage2ReplicatedHa,
    Stage3Sharded,
    Stage4AiNative,
    Stage5AdaptersOps,
}

impl ArchitectureStage {
    pub fn all() -> &'static [ArchitectureStage] {
        &[
            Self::Stage0Foundations,
            Self::Stage1SingleNode,
            Self::Stage2ReplicatedHa,
            Self::Stage3Sharded,
            Self::Stage4AiNative,
            Self::Stage5AdaptersOps,
        ]
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Stage0Foundations => "Stage 0",
            Self::Stage1SingleNode => "Stage 1",
            Self::Stage2ReplicatedHa => "Stage 2",
            Self::Stage3Sharded => "Stage 3",
            Self::Stage4AiNative => "Stage 4",
            Self::Stage5AdaptersOps => "Stage 5",
        }
    }

    pub fn task_ids(self) -> &'static [&'static str] {
        match self {
            Self::Stage0Foundations => &MANDATORY_ARCHITECTURE_TASK_IDS[0..7],
            Self::Stage1SingleNode => &MANDATORY_ARCHITECTURE_TASK_IDS[7..35],
            Self::Stage2ReplicatedHa => &MANDATORY_ARCHITECTURE_TASK_IDS[35..47],
            Self::Stage3Sharded => &MANDATORY_ARCHITECTURE_TASK_IDS[47..59],
            Self::Stage4AiNative => &MANDATORY_ARCHITECTURE_TASK_IDS[59..66],
            Self::Stage5AdaptersOps => &MANDATORY_ARCHITECTURE_TASK_IDS[66..72],
        }
    }
}

fn is_residual_alias(id: &str) -> bool {
    RESIDUAL_ALIAS_TASK_IDS.contains(&id)
}

fn mandatory_task_set() -> BTreeSet<&'static str> {
    MANDATORY_ARCHITECTURE_TASK_IDS.iter().copied().collect()
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

        let test_ids = self
            .tests
            .iter()
            .map(|test| test.id.as_str())
            .collect::<BTreeSet<_>>();
        if test_ids.len() != self.tests.len() {
            return Err("certification tests must have unique ids".into());
        }

        let mandatory = mandatory_task_set();
        let mut seen: BTreeSet<String> = BTreeSet::new();
        let mut present_mandatory: BTreeSet<String> = BTreeSet::new();

        for task in &self.architecture_tasks {
            if task.id.is_empty() {
                return Err("certification architecture task has an empty id".into());
            }
            if !seen.insert(task.id.clone()) {
                return Err(format!(
                    "certification architecture task {} is duplicated",
                    task.id
                ));
            }
            let is_mandatory = mandatory.contains(task.id.as_str());
            let is_residual = is_residual_alias(&task.id);
            if !is_mandatory && !is_residual {
                return Err(format!(
                    "certification architecture task {} is not a Stage 0–5 id or residual R1–R10 alias",
                    task.id
                ));
            }
            if is_mandatory {
                present_mandatory.insert(task.id.clone());
            }

            match task.status {
                ArchitectureStatus::Integrated => {
                    // Traceability evidence is optional for Integrated, but when
                    // present every entry must name a recorded test.
                    if task
                        .evidence
                        .iter()
                        .any(|evidence| !test_ids.contains(evidence.as_str()))
                    {
                        return Err(format!(
                            "certification architecture task {} has invalid evidence",
                            task.id
                        ));
                    }
                    // Integrated must never claim product evidence classes.
                    if !task.evidence_classes.is_empty() {
                        return Err(format!(
                            "certification architecture task {} is Integrated but lists evidence_classes (would imply Qualified)",
                            task.id
                        ));
                    }
                }
                ArchitectureStatus::Qualified => {
                    if task.evidence.is_empty() {
                        return Err(format!(
                            "certification architecture task {} is Qualified without evidence",
                            task.id
                        ));
                    }
                    if task
                        .evidence
                        .iter()
                        .any(|evidence| !test_ids.contains(evidence.as_str()))
                    {
                        return Err(format!(
                            "certification architecture task {} has invalid evidence",
                            task.id
                        ));
                    }
                    if task.evidence_classes.is_empty() {
                        return Err(format!(
                            "certification architecture task {} is Qualified without evidence_classes",
                            task.id
                        ));
                    }
                }
            }
        }

        if present_mandatory.len() != mandatory.len() {
            let missing = mandatory
                .iter()
                .filter(|id| !present_mandatory.contains(**id))
                .copied()
                .collect::<Vec<_>>();
            return Err(format!(
                "certification architecture tasks missing mandatory Stage 0–5 ids: {}",
                missing.join(", ")
            ));
        }

        Ok(())
    }

    /// Stage is Qualified only when every mandatory task in that stage is Qualified.
    pub fn stage_is_qualified(&self, stage: ArchitectureStage) -> bool {
        let by_id: BTreeMap<&str, &ArchitectureQualification> = self
            .architecture_tasks
            .iter()
            .map(|task| (task.id.as_str(), task))
            .collect();
        stage.task_ids().iter().all(|id| {
            by_id
                .get(id)
                .is_some_and(|task| task.status == ArchitectureStatus::Qualified)
        })
    }

    /// True when every mandatory Stage 0–5 task is Qualified for this manifest.
    pub fn architecture_is_qualified(&self) -> bool {
        ArchitectureStage::all()
            .iter()
            .all(|stage| self.stage_is_qualified(*stage))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn passed_tests() -> Vec<CertificationTest> {
        vec![
            CertificationTest {
                id: "workspace_tests".into(),
                command: "cargo test --workspace".into(),
                status: CertificationStatus::Passed,
                duration_ms: 1,
                artifact: "cargo-test.log".into(),
            },
            CertificationTest {
                id: "crash_matrix".into(),
                command: "cargo test -p mongreldb-core --test crash".into(),
                status: CertificationStatus::Passed,
                duration_ms: 2,
                artifact: "crash-matrix.log".into(),
            },
        ]
    }

    fn stage_manifest(status: ArchitectureStatus) -> CertificationManifest {
        let evidence = match status {
            ArchitectureStatus::Integrated => vec!["workspace_tests".into()],
            ArchitectureStatus::Qualified => vec!["workspace_tests".into()],
        };
        let evidence_classes = match status {
            ArchitectureStatus::Integrated => Vec::new(),
            ArchitectureStatus::Qualified => vec![EvidenceClass::SourceIntegration],
        };
        CertificationManifest {
            commit: "abc".into(),
            artifact_sha256: "def".into(),
            implementation_status_sha256: "0".repeat(64),
            rust_version: "rustc test".into(),
            architecture_tasks: MANDATORY_ARCHITECTURE_TASK_IDS
                .iter()
                .map(|id| ArchitectureQualification {
                    id: (*id).to_owned(),
                    status,
                    evidence: evidence.clone(),
                    evidence_classes: evidence_classes.clone(),
                })
                .collect(),
            tests: passed_tests(),
        }
    }

    #[test]
    fn mandatory_task_list_matches_stage_slices() {
        assert_eq!(MANDATORY_ARCHITECTURE_TASK_IDS.len(), 72);
        let mut covered = 0usize;
        for stage in ArchitectureStage::all() {
            covered += stage.task_ids().len();
        }
        assert_eq!(covered, MANDATORY_ARCHITECTURE_TASK_IDS.len());
        assert_eq!(
            ArchitectureStage::Stage0Foundations.task_ids()[0],
            "FND-001"
        );
        assert_eq!(
            ArchitectureStage::Stage0Foundations.task_ids().last(),
            Some(&"FND-007")
        );
        assert_eq!(ArchitectureStage::Stage1SingleNode.task_ids()[0], "S1A-001");
        assert_eq!(
            ArchitectureStage::Stage1SingleNode.task_ids().last(),
            Some(&"S1G")
        );
        assert_eq!(
            ArchitectureStage::Stage5AdaptersOps.task_ids().last(),
            Some(&"S5F")
        );
    }

    #[test]
    fn validation_accepts_integrated_stage_matrix() {
        let manifest = stage_manifest(ArchitectureStatus::Integrated);
        assert!(manifest.validate("abc", "def").is_ok());
        assert!(!manifest.architecture_is_qualified());
        assert!(!manifest.stage_is_qualified(ArchitectureStage::Stage0Foundations));
    }

    #[test]
    fn validation_accepts_optional_residual_aliases() {
        let mut manifest = stage_manifest(ArchitectureStatus::Integrated);
        for id in RESIDUAL_ALIAS_TASK_IDS {
            manifest.architecture_tasks.push(ArchitectureQualification {
                id: (*id).to_owned(),
                status: ArchitectureStatus::Integrated,
                evidence: vec!["workspace_tests".into()],
                evidence_classes: Vec::new(),
            });
        }
        assert!(manifest.validate("abc", "def").is_ok());
    }

    #[test]
    fn validation_rejects_r1_r10_only_matrix() {
        let manifest = CertificationManifest {
            commit: "abc".into(),
            artifact_sha256: "def".into(),
            implementation_status_sha256: "0".repeat(64),
            rust_version: "rustc test".into(),
            architecture_tasks: (1..=10)
                .map(|index| ArchitectureQualification {
                    id: format!("R{index}"),
                    status: ArchitectureStatus::Integrated,
                    evidence: vec!["workspace_tests".into()],
                    evidence_classes: Vec::new(),
                })
                .collect(),
            tests: passed_tests(),
        };
        let err = manifest.validate("abc", "def").unwrap_err();
        assert!(
            err.contains("missing mandatory") || err.contains("FND-001"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validation_rejects_qualified_without_evidence_or_classes() {
        let mut no_evidence = stage_manifest(ArchitectureStatus::Integrated);
        no_evidence.architecture_tasks[0].status = ArchitectureStatus::Qualified;
        no_evidence.architecture_tasks[0].evidence.clear();
        no_evidence.architecture_tasks[0].evidence_classes = vec![EvidenceClass::SourceIntegration];
        assert!(no_evidence
            .validate("abc", "def")
            .unwrap_err()
            .contains("without evidence"));

        let mut no_classes = stage_manifest(ArchitectureStatus::Integrated);
        no_classes.architecture_tasks[0].status = ArchitectureStatus::Qualified;
        no_classes.architecture_tasks[0].evidence = vec!["workspace_tests".into()];
        no_classes.architecture_tasks[0].evidence_classes.clear();
        assert!(no_classes
            .validate("abc", "def")
            .unwrap_err()
            .contains("without evidence_classes"));
    }

    #[test]
    fn validation_rejects_integrated_with_evidence_classes() {
        let mut manifest = stage_manifest(ArchitectureStatus::Integrated);
        manifest.architecture_tasks[0].evidence_classes = vec![EvidenceClass::SourceIntegration];
        assert!(manifest
            .validate("abc", "def")
            .unwrap_err()
            .contains("Integrated but lists evidence_classes"));
    }

    #[test]
    fn stage_is_qualified_requires_every_task_in_stage() {
        let mut manifest = stage_manifest(ArchitectureStatus::Integrated);
        for task in &mut manifest.architecture_tasks {
            if task.id.starts_with("FND-") {
                task.status = ArchitectureStatus::Qualified;
                task.evidence = vec!["workspace_tests".into()];
                task.evidence_classes = vec![EvidenceClass::SourceIntegration];
            }
        }
        assert!(manifest.validate("abc", "def").is_ok());
        assert!(manifest.stage_is_qualified(ArchitectureStage::Stage0Foundations));
        assert!(!manifest.stage_is_qualified(ArchitectureStage::Stage1SingleNode));
        assert!(!manifest.architecture_is_qualified());

        // Drop one Stage 0 task from Qualified → stage fails.
        manifest.architecture_tasks[0].status = ArchitectureStatus::Integrated;
        manifest.architecture_tasks[0].evidence_classes.clear();
        assert!(!manifest.stage_is_qualified(ArchitectureStage::Stage0Foundations));
    }

    #[test]
    fn validation_rejects_failed_or_wrong_identity_evidence() {
        assert!(stage_manifest(ArchitectureStatus::Integrated)
            .validate("abc", "def")
            .is_ok());
        let mut failed = stage_manifest(ArchitectureStatus::Integrated);
        failed.tests[0].status = CertificationStatus::Failed;
        assert!(failed.validate("abc", "def").is_err());
        assert!(stage_manifest(ArchitectureStatus::Integrated)
            .validate("other", "def")
            .is_err());
        assert!(stage_manifest(ArchitectureStatus::Integrated)
            .validate("abc", "other")
            .is_err());
        let mut invalid_status = stage_manifest(ArchitectureStatus::Integrated);
        invalid_status.implementation_status_sha256 = "not-a-hash".into();
        assert!(invalid_status.validate("abc", "def").is_err());
        let mut unmeasured = stage_manifest(ArchitectureStatus::Integrated);
        unmeasured.tests[0].duration_ms = 0;
        assert!(unmeasured.validate("abc", "def").is_err());
        let mut missing_task = stage_manifest(ArchitectureStatus::Integrated);
        missing_task.architecture_tasks.pop();
        assert!(missing_task.validate("abc", "def").is_err());
        let mut unknown_evidence = stage_manifest(ArchitectureStatus::Integrated);
        unknown_evidence.architecture_tasks[0].evidence = vec!["missing".into()];
        assert!(unknown_evidence.validate("abc", "def").is_err());
        let mut unknown_task = stage_manifest(ArchitectureStatus::Integrated);
        unknown_task
            .architecture_tasks
            .push(ArchitectureQualification {
                id: "NOT-A-STAGE".into(),
                status: ArchitectureStatus::Integrated,
                evidence: Vec::new(),
                evidence_classes: Vec::new(),
            });
        assert!(unknown_task
            .validate("abc", "def")
            .unwrap_err()
            .contains("not a Stage"));
    }

    #[test]
    fn fully_qualified_matrix_requires_evidence_classes() {
        let manifest = stage_manifest(ArchitectureStatus::Qualified);
        assert!(manifest.validate("abc", "def").is_ok());
        assert!(manifest.architecture_is_qualified());
        for stage in ArchitectureStage::all() {
            assert!(manifest.stage_is_qualified(*stage), "{:?}", stage);
        }
    }
}
