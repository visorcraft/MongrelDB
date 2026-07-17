//! Stage 1E (spec §10.5, S1E-004) integration: the spill manager end to end
//! through its public API — query-ID-namespaced, checksummed, budgeted spill
//! files with guaranteed cleanup on every path, startup sweep, encryption
//! under the meta DEK, and the memory governor's spill grant driving a real
//! spill under pressure.

use std::path::PathBuf;

use mongreldb_core::durable_file::DurableRoot;
use mongreldb_core::memory::{GovernorConfig, MemoryClass, MemoryGovernor};
use mongreldb_core::resource::{ResourceGroup, WorkloadClass};
use mongreldb_core::spill::{SpillError, SpillManager};
use mongreldb_types::ids::QueryId;
use tempfile::tempdir;

fn manager(dir: &tempfile::TempDir, global_bytes: u64) -> SpillManager {
    let root = DurableRoot::open(dir.path()).unwrap();
    SpillManager::open(
        &root,
        mongreldb_core::spill::SpillConfig::new(global_bytes),
        None,
    )
    .unwrap()
}

fn query_dir(dir: &tempfile::TempDir, query_id: QueryId) -> PathBuf {
    dir.path()
        .join("temp")
        .join("spill")
        .join(format!("q-{}", query_id.to_hex()))
}

fn only_file(dir: &tempfile::TempDir, query_id: QueryId) -> PathBuf {
    let entries: Vec<_> = std::fs::read_dir(query_dir(dir, query_id))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(entries.len(), 1, "expected exactly one spill file");
    entries[0].clone()
}

#[test]
fn round_trip_frames_including_large_multi_chunk() {
    let dir = tempdir().unwrap();
    let manager = manager(&dir, 1 << 24);
    let group = ResourceGroup::for_class(WorkloadClass::Analytics);
    let query_id = QueryId::new_random();
    let session = manager.begin_query_in_group(query_id, &group).unwrap();
    assert_eq!(session.cap(), group.temporary_disk_bytes);

    // Sixteen frames across four chunk files, mixed sizes up to 1 MiB, with
    // deterministic content so corruption cannot pass silently.
    let mut expected = Vec::new();
    let mut handles = Vec::new();
    for chunk in 0..4u64 {
        let mut writer = session.new_writer().unwrap();
        let mut payloads = Vec::new();
        for frame in 0..4u64 {
            let len = match frame {
                0 => 1 << 20,
                1 => 7,
                2 => 0,
                _ => 65_537,
            };
            let payload: Vec<u8> = (0..len)
                .map(|i| (chunk as u8) ^ (frame as u8) ^ (i % 251) as u8)
                .collect();
            writer.append(&payload).unwrap();
            payloads.push(payload);
        }
        expected.push(payloads);
        handles.push(writer.finish().unwrap());
    }
    assert_eq!(manager.stats().files_live, 4);
    assert_eq!(manager.stats().global_used, session.used());
    assert_eq!(manager.stats().budget_remaining, (1 << 24) - session.used());

    // Frames stream back in order, byte-identical, across every chunk file.
    let mut total_read = 0u64;
    for (handle, payloads) in handles.iter().zip(expected.iter()) {
        assert_eq!(handle.query_id(), query_id);
        assert_eq!(handle.frames(), 4);
        let frames: Vec<Vec<u8>> = handle.reader().unwrap().collect::<Result<_, _>>().unwrap();
        assert_eq!(&frames, payloads);
        total_read += handle.bytes_on_disk();
    }
    assert_eq!(manager.stats().bytes_read, total_read);
    assert_eq!(manager.stats().bytes_written, session.used());
}

#[test]
fn checksum_corruption_is_detected() {
    let dir = tempdir().unwrap();
    let manager = manager(&dir, 1 << 20);
    let query_id = QueryId::new_random();
    let session = manager.begin_query(query_id, 1 << 20).unwrap();
    let mut writer = session.new_writer().unwrap();
    writer.append(&vec![0x42; 512]).unwrap();
    writer.append(b"tail").unwrap();
    let handle = writer.finish().unwrap();

    // Flip a byte midway through the file (inside the first frame's payload).
    let path = only_file(&dir, query_id);
    let mut raw = std::fs::read(&path).unwrap();
    let len = raw.len();
    raw[64] ^= 0xFF;
    std::fs::write(&path, &raw).unwrap();
    assert_eq!(std::fs::read(&path).unwrap().len(), len);

    let frames: Vec<_> = handle.reader().unwrap().collect();
    assert_eq!(frames.len(), 1, "the corrupt frame ends the stream");
    assert!(
        matches!(frames[0], Err(SpillError::ChecksumMismatch { .. })),
        "expected ChecksumMismatch, got {:?}",
        frames[0]
    );
}

#[test]
fn per_query_and_global_budgets_are_enforced() {
    let dir = tempdir().unwrap();
    let manager = manager(&dir, 512);

    // Per-query cap (fed from the resource group's temporary_disk_bytes).
    let mut tiny_group = ResourceGroup::for_class(WorkloadClass::Oltp);
    tiny_group.temporary_disk_bytes = 128;
    let query_id = QueryId::new_random();
    let session = manager.begin_query_in_group(query_id, &tiny_group).unwrap();
    assert_eq!(session.cap(), 128);
    let mut writer = session.new_writer().unwrap();
    writer.append(&[0u8; 64]).unwrap(); // 12 + 81 = 93 of 128
    let error = writer.append(&[0u8; 64]).unwrap_err();
    assert!(
        matches!(
            error,
            SpillError::BudgetExceeded {
                query_id: id,
                query_remaining: 35,
                ..
            } if id == query_id
        ),
        "expected per-query BudgetExceeded, got {error:?}"
    );
    drop(writer);

    // Global cap binds before a generous per-query cap does.
    let mut fat_group = ResourceGroup::for_class(WorkloadClass::Analytics);
    fat_group.temporary_disk_bytes = 1 << 30;
    let first = manager
        .begin_query_in_group(QueryId::new_random(), &fat_group)
        .unwrap();
    let second = manager
        .begin_query_in_group(QueryId::new_random(), &fat_group)
        .unwrap();
    let mut w1 = first.new_writer().unwrap();
    let mut w2 = second.new_writer().unwrap();
    w1.append(&[0u8; 400]).unwrap(); // 12 + 12 + 417 = 441 of 512
    let error = w2.append(&[0u8; 400]).unwrap_err();
    assert!(
        matches!(
            error,
            SpillError::BudgetExceeded {
                global_remaining: 71,
                ..
            }
        ),
        "expected global BudgetExceeded, got {error:?}"
    );
}

#[test]
fn cleanup_on_drop_error_and_cancel_paths() {
    let dir = tempdir().unwrap();
    let manager = manager(&dir, 1 << 20);
    let query_id = QueryId::new_random();

    // Cancel: an unfinished writer dropped mid-stream deletes its file.
    let session = manager.begin_query(query_id, 1 << 20).unwrap();
    let mut writer = session.new_writer().unwrap();
    writer.append(b"partial").unwrap();
    let partial = only_file(&dir, query_id);
    drop(writer);
    assert!(!partial.exists(), "cancel must delete the partial file");
    assert_eq!(session.used(), 0);

    // Error: a failed append leaves the writer droppable with no leak.
    let capped = manager.begin_query(QueryId::new_random(), 64).unwrap();
    let capped_id = capped.query_id();
    let mut writer = capped.new_writer().unwrap();
    writer.append(&[0u8; 32]).unwrap();
    assert!(writer.append(&[0u8; 32]).is_err());
    let failed = only_file(&dir, capped_id);
    drop(writer);
    assert!(!failed.exists(), "error path must delete the file");
    assert_eq!(capped.used(), 0);
    drop(capped);
    assert!(!query_dir(&dir, capped_id).exists());

    // Success: the sealed file lives until its handle drops.
    let mut writer = session.new_writer().unwrap();
    writer.append(b"sealed").unwrap();
    let handle = writer.finish().unwrap();
    let sealed = only_file(&dir, query_id);
    assert!(sealed.exists());
    assert_eq!(manager.stats().files_live, 1);
    drop(handle);
    assert!(!sealed.exists(), "handle drop must delete the sealed file");
    assert_eq!(manager.stats().files_live, 0);
    assert_eq!(manager.stats().global_used, 0);

    // Session drop removes the whole per-query namespace.
    assert!(query_dir(&dir, query_id).exists());
    drop(session);
    assert!(!query_dir(&dir, query_id).exists());
    assert_eq!(manager.stats().global_used, 0);
}

#[test]
fn startup_sweep_removes_stale_files() {
    let dir = tempdir().unwrap();
    let query_id = QueryId::new_random();
    let stale;
    {
        let first = manager(&dir, 1 << 20);
        let session = first.begin_query(query_id, 1 << 20).unwrap();
        let mut writer = session.new_writer().unwrap();
        writer.append(b"left behind by a crash").unwrap();
        let handle = writer.finish().unwrap();
        stale = only_file(&dir, query_id);
        // Leak everything, as a crashed process would.
        std::mem::forget(session);
        std::mem::forget(handle);
        std::mem::forget(first);
    }
    assert!(stale.exists());

    // The next process's manager sweeps the stale namespace at open.
    let second = manager(&dir, 1 << 20);
    assert!(
        !stale.exists(),
        "startup sweep must remove stale spill files"
    );
    assert!(!query_dir(&dir, query_id).exists());
    assert_eq!(second.stats().global_used, 0);
    assert_eq!(second.stats().files_live, 0);

    // The swept tree works normally afterwards.
    let session = second.begin_query(QueryId::new_random(), 1 << 20).unwrap();
    let mut writer = session.new_writer().unwrap();
    writer.append(b"fresh").unwrap();
    let handle = writer.finish().unwrap();
    let frames: Vec<Vec<u8>> = handle.reader().unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(frames, vec![b"fresh".to_vec()]);
}

#[test]
fn governor_spill_trigger_issues_a_grant_and_the_operator_spills() {
    let dir = tempdir().unwrap();
    let manager = manager(&dir, 1 << 20);
    // 1000-byte node, no reserved floor: 900 bytes of query memory is exactly
    // the 0.90 spill threshold.
    let governor = MemoryGovernor::new(GovernorConfig::new(1000).with_reserved_floor(0)).unwrap();
    let mut reservation = governor
        .try_reserve(500, MemoryClass::QueryExecution)
        .unwrap();
    // Below the trigger there is nothing to spill.
    assert!(!governor.spill_trigger());
    assert_eq!(governor.request_spill_grant(&mut reservation, 100), None);

    // Pressure crosses step 3: the trigger fires and the grant lands.
    let _pressure = governor
        .try_reserve(400, MemoryClass::ResultBuffering)
        .unwrap();
    assert!(governor.spill_trigger());
    let grant = governor
        .request_spill_grant(&mut reservation, 300)
        .expect("step 3 must grant an eligible operator");
    assert_eq!(grant.class(), MemoryClass::QueryExecution);
    assert_eq!(grant.bytes(), 300);
    assert_eq!(reservation.bytes(), 200, "spilled memory is freed up front");

    // The operator moves exactly the granted bytes to its spill namespace.
    let group = ResourceGroup::for_class(WorkloadClass::InteractiveSql);
    let session = manager
        .begin_query_in_group(QueryId::new_random(), &group)
        .unwrap();
    let mut writer = session.new_writer().unwrap();
    let spilled: Vec<u8> = (0..grant.bytes() as usize)
        .map(|i| (i % 256) as u8)
        .collect();
    writer.append(&spilled).unwrap();
    let handle = writer.finish().unwrap();
    let frames: Vec<Vec<u8>> = handle.reader().unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(frames, vec![spilled]);

    // An ineligible class is refused even under the trigger.
    let mut cache_reservation = governor.try_reserve(50, MemoryClass::PageCache).unwrap();
    assert_eq!(
        governor.request_spill_grant(&mut cache_reservation, 10),
        None
    );
    assert!(governor.stats().spill_triggers >= 1);
}

#[cfg(feature = "encryption")]
mod encrypted {
    use super::*;
    use mongreldb_core::encryption::{meta_dek_for, Kek, SALT_LEN};

    /// The DEK setup mirrors the encrypted-catalog test in
    /// `recovery_multitable.rs`: a KEK derived from a passphrase over a fixed
    /// salt, then the DB-wide meta DEK.
    fn test_dek(passphrase: &str) -> [u8; mongreldb_core::encryption::DEK_LEN] {
        let salt = [3u8; SALT_LEN];
        let kek = Kek::derive(passphrase, &salt).unwrap();
        meta_dek_for(Some(&kek)).unwrap()
    }

    #[test]
    fn encrypted_round_trip_under_the_meta_dek() {
        let dir = tempdir().unwrap();
        let root = DurableRoot::open(dir.path()).unwrap();
        let manager = SpillManager::open(
            &root,
            mongreldb_core::spill::SpillConfig::new(1 << 20),
            Some(test_dek("spill-pw")),
        )
        .unwrap();
        let query_id = QueryId::new_random();
        let group = ResourceGroup::for_class(WorkloadClass::Analytics);
        let session = manager.begin_query_in_group(query_id, &group).unwrap();

        let marker = b"spill-plaintext-that-must-never-touch-disk";
        let mut writer = session.new_writer().unwrap();
        writer.append(marker).unwrap();
        writer.append(&vec![0xC3; 65_536]).unwrap();
        writer.append(b"").unwrap();
        let handle = writer.finish().unwrap();

        // The on-disk form is sealed: neither the marker nor the payload
        // pattern appears anywhere in the file.
        let raw = std::fs::read(only_file(&dir, query_id)).unwrap();
        assert!(!raw
            .windows(marker.len())
            .any(|window| window == marker.as_slice()));
        assert!(!raw.windows(32).any(|window| window == [0xC3; 32]));

        // Reads authenticate and decrypt transparently.
        let frames: Vec<Vec<u8>> = handle.reader().unwrap().collect::<Result<_, _>>().unwrap();
        assert_eq!(
            frames,
            vec![marker.to_vec(), vec![0xC3; 65_536], Vec::new()]
        );

        // Cleanup is identical in encrypted mode.
        let sealed = only_file(&dir, query_id);
        drop(handle);
        assert!(!sealed.exists());
        assert_eq!(manager.stats().files_live, 0);
    }
}
