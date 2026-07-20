use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{
    Database, KeyManagementError, KeyManagementHealth, KeyManagementProvider, KeyRotationJournal,
    KeyRotationPhase, KmsWrappedKey, Value,
};
use mongreldb_fault::{Action, ScopedGuard};
use sha2::{Digest, Sha256};
use tempfile::tempdir;
use zeroize::Zeroizing;

struct TestKms {
    id: &'static str,
}

struct FailingRewrapKms<'a> {
    inner: &'a TestKms,
    fail: AtomicBool,
}

impl KeyManagementProvider for FailingRewrapKms<'_> {
    fn provider_id(&self) -> &str {
        self.inner.provider_id()
    }

    fn wrap_key(
        &self,
        key_id: &str,
        plaintext_key: &[u8],
    ) -> Result<KmsWrappedKey, KeyManagementError> {
        self.inner.wrap_key(key_id, plaintext_key)
    }

    fn unwrap_key(
        &self,
        wrapped: &KmsWrappedKey,
    ) -> Result<Zeroizing<Vec<u8>>, KeyManagementError> {
        self.inner.unwrap_key(wrapped)
    }

    fn rewrap_key(
        &self,
        wrapped: &KmsWrappedKey,
        new_key_id: &str,
    ) -> Result<KmsWrappedKey, KeyManagementError> {
        if self.fail.load(Ordering::Relaxed) {
            return Err(KeyManagementError::Unavailable("injected outage".into()));
        }
        self.inner.rewrap_key(wrapped, new_key_id)
    }

    fn provider_health(&self) -> KeyManagementHealth {
        if self.fail.load(Ordering::Relaxed) {
            KeyManagementHealth::Unavailable
        } else {
            KeyManagementHealth::Ready
        }
    }
}

impl TestKms {
    fn mask(key_id: &str) -> [u8; 32] {
        Sha256::digest(key_id.as_bytes()).into()
    }
}

impl KeyManagementProvider for TestKms {
    fn provider_id(&self) -> &str {
        self.id
    }

    fn wrap_key(
        &self,
        key_id: &str,
        plaintext_key: &[u8],
    ) -> Result<KmsWrappedKey, KeyManagementError> {
        let mask = Self::mask(key_id);
        Ok(KmsWrappedKey {
            kms_key_id: key_id.into(),
            key_version: format!("{key_id}:1"),
            wrapped_dek: plaintext_key
                .iter()
                .enumerate()
                .map(|(index, byte)| byte ^ mask[index % mask.len()])
                .collect(),
            algorithm: "test-xor".into(),
        })
    }

    fn unwrap_key(
        &self,
        wrapped: &KmsWrappedKey,
    ) -> Result<Zeroizing<Vec<u8>>, KeyManagementError> {
        let mask = Self::mask(&wrapped.kms_key_id);
        Ok(Zeroizing::new(
            wrapped
                .wrapped_dek
                .iter()
                .enumerate()
                .map(|(index, byte)| byte ^ mask[index % mask.len()])
                .collect(),
        ))
    }

    fn rewrap_key(
        &self,
        wrapped: &KmsWrappedKey,
        new_key_id: &str,
    ) -> Result<KmsWrappedKey, KeyManagementError> {
        self.wrap_key(new_key_id, self.unwrap_key(wrapped)?.as_ref())
    }

    fn provider_health(&self) -> KeyManagementHealth {
        KeyManagementHealth::Ready
    }
}

fn schema() -> Schema {
    Schema {
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 2,
                name: "value".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        ..Schema::default()
    }
}

#[test]
fn kms_envelope_opens_and_rotates_without_rewriting_data() {
    let directory = tempdir().unwrap();
    let provider = TestKms { id: "test-kms" };
    let database = Database::create_with_kms(directory.path(), &provider, "primary").unwrap();
    database.create_table("records", schema()).unwrap();

    let mut transaction = database.begin();
    transaction
        .put(
            "records",
            vec![(1, Value::Int64(1)), (2, Value::Bytes(b"durable".to_vec()))],
        )
        .unwrap();
    transaction.commit().unwrap();
    database.table("records").unwrap().lock().flush().unwrap();

    let rotation = database.rotate_kms_key(&provider, "secondary").unwrap();
    assert_eq!(rotation.phase, KeyRotationPhase::Succeeded);
    assert!(rotation.completed_unix_micros.is_some());
    assert_eq!(
        database
            .rows_for("records", None)
            .unwrap()
            .remove(0)
            .columns
            .get(&2),
        Some(&Value::Bytes(b"durable".to_vec()))
    );

    drop(database);
    let reopened = Database::open_with_kms(directory.path(), &provider).unwrap();
    assert_eq!(
        reopened
            .rows_for("records", None)
            .unwrap()
            .remove(0)
            .columns
            .get(&2),
        Some(&Value::Bytes(b"durable".to_vec()))
    );
    drop(reopened);

    let wrong_provider = TestKms { id: "other-kms" };
    assert!(Database::open_with_kms(directory.path(), &wrong_provider).is_err());

    for (hook, expected_phase) in [
        ("kms.rotation.phase.1", KeyRotationPhase::WrappingNewKey),
        ("kms.rotation.phase.2", KeyRotationPhase::DualRead),
        ("kms.rotation.phase.3", KeyRotationPhase::Reencrypting),
        ("kms.rotation.phase.4", KeyRotationPhase::Validating),
        ("kms.rotation.phase.5", KeyRotationPhase::Published),
        ("kms.rotation.phase.6", KeyRotationPhase::RetiringOldKey),
        ("kms.rotation.phase.7", KeyRotationPhase::Succeeded),
    ] {
        let crash_directory = tempdir().unwrap();
        let database =
            Database::create_with_kms(crash_directory.path(), &provider, "primary").unwrap();
        let guard = ScopedGuard::new(hook, Action::Fail);
        assert!(database.rotate_kms_key(&provider, "secondary").is_err());
        drop(guard);
        let journal = KeyRotationJournal::new(crash_directory.path().join("_meta"));
        assert_eq!(journal.load().unwrap().unwrap().phase, expected_phase);
        drop(database);

        let reopened = Database::open_with_kms(crash_directory.path(), &provider).unwrap();
        if expected_phase != KeyRotationPhase::Succeeded {
            assert_eq!(
                reopened
                    .rotate_kms_key(&provider, "secondary")
                    .unwrap()
                    .phase,
                KeyRotationPhase::Succeeded
            );
        }
        drop(reopened);
        Database::open_with_kms(crash_directory.path(), &provider).unwrap();
    }

    let outage_directory = tempdir().unwrap();
    let database =
        Database::create_with_kms(outage_directory.path(), &provider, "primary").unwrap();
    let flaky = FailingRewrapKms {
        inner: &provider,
        fail: AtomicBool::new(true),
    };
    assert!(database.rotate_kms_key(&flaky, "secondary").is_err());
    assert_eq!(
        KeyRotationJournal::new(outage_directory.path().join("_meta"))
            .load()
            .unwrap()
            .unwrap()
            .phase,
        KeyRotationPhase::Failed
    );
    flaky.fail.store(false, Ordering::Relaxed);
    assert_eq!(
        database.retry_kms_key_rotation(&flaky).unwrap().phase,
        KeyRotationPhase::Succeeded
    );
}

#[test]
fn kms_encryption_composes_with_database_credentials() {
    let directory = tempdir().unwrap();
    let provider = Arc::new(TestKms { id: "test-kms" });
    let database = Database::create_with_kms_and_credentials(
        directory.path(),
        provider.as_ref(),
        "primary",
        "admin",
        "admin-password",
    )
    .unwrap();
    drop(database);

    assert!(Database::open_with_kms_and_credentials(
        directory.path(),
        provider.as_ref(),
        "admin",
        "wrong-password",
    )
    .is_err());
    Database::open_with_kms_and_credentials(
        directory.path(),
        provider.as_ref(),
        "admin",
        "admin-password",
    )
    .unwrap();
}

#[test]
fn oversized_kms_envelope_fails_closed() {
    let directory = tempdir().unwrap();
    let provider = TestKms { id: "test-kms" };
    let database = Database::create_with_kms(directory.path(), &provider, "primary").unwrap();
    drop(database);

    std::fs::write(
        directory.path().join("_meta/kms_key.json"),
        vec![b'x'; 1024 * 1024 + 1],
    )
    .unwrap();
    let error = Database::open_with_kms(directory.path(), &provider).unwrap_err();
    assert!(error.to_string().contains("exceeds 1 MiB"), "{error}");
}
