//! Integration tests for the in-memory commit log (spec 9.4, FND-004).

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use mongreldb_log::{
    CommandEnvelope, CommitLog, DurabilityLevel, EnvelopeError, ExecutionControl,
    InMemoryCommitLog, LogError, LogPosition, LogSnapshot,
};
use mongreldb_types::hlc::HlcTimestamp;
use mongreldb_types::ids::TransactionId;

fn envelope(id: u8) -> CommandEnvelope {
    CommandEnvelope::new(1, [id; 16], vec![id; 16])
}

fn envelope_seq(seq: u64) -> CommandEnvelope {
    let mut command_id = [0u8; 16];
    command_id[..8].copy_from_slice(&seq.to_le_bytes());
    CommandEnvelope::new(1, command_id, seq.to_le_bytes().to_vec())
}

#[test]
fn propose_assigns_ordered_positions_and_reads_back() {
    let log = InMemoryCommitLog::new();
    assert_eq!(log.applied_position(), LogPosition::ZERO);

    let mut receipts = Vec::new();
    for id in 1..=5u8 {
        receipts.push(
            log.propose(envelope(id), &ExecutionControl::default())
                .unwrap(),
        );
    }
    for (i, receipt) in receipts.iter().enumerate() {
        let index = (i + 1) as u64;
        assert_eq!(receipt.log_position, LogPosition { term: 0, index });
        assert_eq!(receipt.durability, DurabilityLevel::GroupCommit);
        assert_eq!(
            receipt.transaction_id,
            TransactionId::from_bytes([(i + 1) as u8; 16])
        );
        assert!(receipt.commit_ts > HlcTimestamp::ZERO);
    }
    // In-memory applies on commit: applied position tracks the last proposal.
    assert_eq!(log.applied_position(), LogPosition { term: 0, index: 5 });

    let entries = log.read_committed(LogPosition::ZERO, 100).unwrap();
    assert_eq!(entries.len(), 5);
    for (i, entry) in entries.iter().enumerate() {
        let index = (i + 1) as u64;
        assert_eq!(entry.position, LogPosition { term: 0, index });
        assert_eq!(entry.envelope, envelope((i + 1) as u8));
    }

    // `after` is exclusive and `limit` caps the batch.
    let entries = log
        .read_committed(LogPosition { term: 0, index: 2 }, 2)
        .unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].position, LogPosition { term: 0, index: 3 });
    assert_eq!(entries[1].position, LogPosition { term: 0, index: 4 });

    assert!(log
        .read_committed(LogPosition { term: 0, index: 5 }, 100)
        .unwrap()
        .is_empty());
}

#[test]
fn invalid_envelopes_are_rejected() {
    let log = InMemoryCommitLog::new();

    let mut corrupted = envelope(1);
    corrupted.payload_sha256[0] ^= 0xFF;
    assert!(matches!(
        log.propose(corrupted, &ExecutionControl::default()),
        Err(LogError::Envelope(EnvelopeError::ChecksumMismatch))
    ));

    let mut wrong_version = CommandEnvelope::new(1, [2u8; 16], vec![]);
    wrong_version.format_version += 1;
    wrong_version.payload_sha256 =
        CommandEnvelope::checksum(wrong_version.format_version, 1, &wrong_version.payload);
    assert!(matches!(
        log.propose(wrong_version, &ExecutionControl::default()),
        Err(LogError::Envelope(EnvelopeError::UnsupportedVersion { .. }))
    ));

    // Nothing was committed.
    assert_eq!(log.applied_position(), LogPosition::ZERO);
    assert!(log
        .read_committed(LogPosition::ZERO, 10)
        .unwrap()
        .is_empty());
}

#[test]
fn cancellation_and_deadline_are_honored() {
    let log = InMemoryCommitLog::new();

    let flag = Arc::new(AtomicBool::new(false));
    let control = ExecutionControl {
        deadline: None,
        cancellation: Some(Arc::clone(&flag)),
    };
    log.propose(envelope(1), &control).unwrap();
    flag.store(true, Ordering::Relaxed);
    assert!(matches!(
        log.propose(envelope(2), &control),
        Err(LogError::Cancelled)
    ));

    let past = Instant::now()
        .checked_sub(Duration::from_secs(1))
        .unwrap_or_else(Instant::now);
    let control = ExecutionControl {
        deadline: Some(past),
        cancellation: None,
    };
    assert!(matches!(
        log.propose(envelope(3), &control),
        Err(LogError::DeadlineExceeded)
    ));

    // Only the first proposal committed.
    assert_eq!(log.applied_position(), LogPosition { term: 0, index: 1 });
}

#[test]
fn concurrent_proposers_get_unique_ordered_positions() {
    const THREADS: u64 = 8;
    const PER_THREAD: u64 = 64;

    let log = Arc::new(InMemoryCommitLog::new());
    let barrier = Arc::new(Barrier::new(THREADS as usize));
    let mut handles = Vec::new();
    for t in 0..THREADS {
        let log = Arc::clone(&log);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let mut receipts = Vec::new();
            for j in 0..PER_THREAD {
                receipts.push(
                    log.propose(
                        envelope_seq(t * PER_THREAD + j + 1),
                        &ExecutionControl::default(),
                    )
                    .unwrap(),
                );
            }
            receipts
        }));
    }
    let mut positions = BTreeSet::new();
    for handle in handles {
        for receipt in handle.join().unwrap() {
            assert!(positions.insert(receipt.log_position), "duplicate position");
        }
    }
    assert_eq!(positions.len(), (THREADS * PER_THREAD) as usize);
    // Positions are contiguous 1..=N in term zero.
    for (i, position) in positions.iter().enumerate() {
        assert_eq!(
            *position,
            LogPosition {
                term: 0,
                index: (i + 1) as u64
            }
        );
    }

    let entries = log
        .read_committed(LogPosition::ZERO, (THREADS * PER_THREAD) as usize + 1)
        .unwrap();
    assert_eq!(entries.len(), (THREADS * PER_THREAD) as usize);
    let mut command_ids = BTreeSet::new();
    for (i, entry) in entries.iter().enumerate() {
        assert_eq!(
            entry.position,
            LogPosition {
                term: 0,
                index: (i + 1) as u64
            }
        );
        assert!(command_ids.insert(entry.envelope.command_id));
    }
    assert_eq!(command_ids.len(), (THREADS * PER_THREAD) as usize);
    assert_eq!(
        log.applied_position(),
        LogPosition {
            term: 0,
            index: THREADS * PER_THREAD
        }
    );
}

#[test]
fn snapshot_install_round_trip() {
    let source = InMemoryCommitLog::new();
    for seq in 1..=10u64 {
        source
            .propose(envelope_seq(seq), &ExecutionControl::default())
            .unwrap();
    }
    let snapshot = source.create_snapshot().unwrap();
    assert_eq!(snapshot.position, LogPosition { term: 0, index: 10 });

    let target = InMemoryCommitLog::new();
    target.install_snapshot(snapshot.clone()).unwrap();
    assert_eq!(target.applied_position(), snapshot.position);

    let source_entries = source.read_committed(LogPosition::ZERO, 100).unwrap();
    let target_entries = target.read_committed(LogPosition::ZERO, 100).unwrap();
    assert_eq!(source_entries.len(), 10);
    assert_eq!(source_entries.len(), target_entries.len());
    for (a, b) in source_entries.iter().zip(&target_entries) {
        assert_eq!(a.position, b.position);
        assert_eq!(a.commit_ts, b.commit_ts);
        assert_eq!(a.envelope, b.envelope);
    }

    // Proposals on the installed log continue after the snapshot position.
    let receipt = target
        .propose(envelope_seq(11), &ExecutionControl::default())
        .unwrap();
    assert_eq!(receipt.log_position, LogPosition { term: 0, index: 11 });

    // Empty snapshots install cleanly too.
    let empty = InMemoryCommitLog::new().create_snapshot().unwrap();
    assert_eq!(empty.position, LogPosition::ZERO);
    let target = InMemoryCommitLog::new();
    target.install_snapshot(empty).unwrap();
    assert_eq!(target.applied_position(), LogPosition::ZERO);
}

#[test]
fn malformed_snapshot_is_rejected() {
    let log = InMemoryCommitLog::new();
    let snapshot = LogSnapshot {
        position: LogPosition { term: 0, index: 1 },
        commit_ts: HlcTimestamp::ZERO,
        data: b"not a snapshot".to_vec(),
    };
    assert!(matches!(
        log.install_snapshot(snapshot),
        Err(LogError::Internal(_))
    ));
    assert_eq!(log.applied_position(), LogPosition::ZERO);
}

#[test]
fn injected_timestamp_source_is_used() {
    let counter = Arc::new(AtomicU64::new(100));
    let log = InMemoryCommitLog::with_timestamp_source(Box::new(move || {
        let micros = counter.fetch_add(100, Ordering::Relaxed);
        HlcTimestamp {
            physical_micros: micros,
            logical: 0,
            node_tiebreaker: 0,
        }
    }));
    let first = log
        .propose(envelope(1), &ExecutionControl::default())
        .unwrap();
    let second = log
        .propose(envelope(2), &ExecutionControl::default())
        .unwrap();
    assert_eq!(first.commit_ts.physical_micros, 100);
    assert_eq!(second.commit_ts.physical_micros, 200);
    let snapshot = log.create_snapshot().unwrap();
    assert_eq!(snapshot.commit_ts.physical_micros, 200);
}
