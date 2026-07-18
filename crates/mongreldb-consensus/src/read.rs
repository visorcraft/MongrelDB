//! Read consistency levels and read barriers (spec section 11.4, Stage 2D).
//!
//! A read barrier answers: *up to which applied position may this node serve
//! the read?* [`ConsensusGroup::consistent_read`](crate::group::ConsensusGroup::consistent_read)
//! evaluates one [`ReadConsistency`] level and returns the applied
//! [`ReadWatermark`] on success; the caller then serves its read at or below
//! that watermark.
//!
//! Level semantics:
//!
//! - [`ReadConsistency::Linearizable`]: raft read-index — the leader confirms
//!   with a quorum, then waits until the read position is applied. Never
//!   served by an unconfirmed leader (spec section 11.4).
//! - [`ReadConsistency::ReadYourWrites`]: waits until the replica applied at
//!   least the session token's commit index.
//! - [`ReadConsistency::Snapshot`]: waits until the applied watermark's
//!   commit timestamp covers the requested timestamp. The watermark is
//!   monotonic in apply order — leaders stamp proposals above the commit
//!   floor and the state machine observes every applied/snapshot-installed
//!   commit timestamp into the group clock (spec section 8.2) — so a
//!   satisfiable barrier stays satisfiable across leader failover. The wait
//!   is caller-bounded: a timestamp no committed entry will ever cover
//!   (for example one from a truncated LeaderDisk write) waits until the
//!   caller's [`ExecutionControl`](mongreldb_log::commit_log::ExecutionControl)
//!   fires and surfaces [`ReadConsistencyError::DeadlineExceeded`] or
//!   [`ReadConsistencyError::Cancelled`]; always set a deadline.
//! - [`ReadConsistency::BoundedStaleness`]: serves if the replica is fresh.
//!   Freshness is *known missing data*, not wall-clock age: a replica that
//!   applied every entry its local log holds serves immediately, however
//!   long ago the last write landed (an idle cluster is not stale). A
//!   replica that has seen nothing at all (an empty log) fails closed as
//!   arbitrarily stale. Only while behind does the applied commit timestamp
//!   bound the lag against the node's HLC now by `max_lag_ms`; the caller
//!   picks another replica on [`ReadConsistencyError::StalenessExceeded`].
//!   Best-effort: a partitioned replica can hold a stale log and still read
//!   as fresh — use [`ReadConsistency::Linearizable`] or
//!   [`ReadConsistency::ReadYourWrites`] for hard guarantees.
//! - [`ReadConsistency::Eventual`]: serves the current local applied
//!   watermark immediately.

use mongreldb_log::commit_log::LogPosition;
use mongreldb_types::hlc::HlcTimestamp;

use crate::error::ConsensusError;
use crate::identity::RaftNodeId;

/// How strongly a read must track the committed log (spec section 11.4).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ReadConsistency {
    /// Leader read-index + wait applied; never served by an unconfirmed
    /// leader.
    Linearizable,
    /// Wait until the replica applied at least `token.commit_index`.
    ReadYourWrites {
        /// Proof of an earlier committed write by this session.
        token: SessionToken,
    },
    /// Serve at `timestamp` once the applied watermark covers it (waits,
    /// caller-bounded by the `ExecutionControl`; see the module docs).
    Snapshot {
        /// The requested snapshot timestamp.
        timestamp: HlcTimestamp,
    },
    /// Serve if the replica is fresh: caught up with every entry its local
    /// log holds, or — while behind — with the applied commit timestamp
    /// lagging the node's HLC now by at most `max_lag_ms` (see the module
    /// docs for the exact semantics).
    BoundedStaleness {
        /// Maximum tolerated lag in milliseconds.
        max_lag_ms: u64,
    },
    /// Serve the current local applied watermark immediately.
    Eventual,
}

/// Proof of a committed write, handed to the client by
/// `RaftCommitLog::session_token` and presented on later reads to get
/// read-your-writes from any replica (spec section 11.4).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SessionToken {
    /// The consensus group that committed the write (raft group text form).
    pub group_id: String,
    /// Committed log index of the write.
    pub commit_index: u64,
    /// Leader-assigned commit timestamp of the write.
    pub commit_ts: HlcTimestamp,
}

/// The applied position a read barrier authorizes serving at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadWatermark {
    /// Highest log position applied locally. Reads at or below it satisfy
    /// the requested consistency level.
    pub position: LogPosition,
    /// Commit timestamp of the last applied command (`None` before any
    /// command was applied).
    pub commit_ts: Option<HlcTimestamp>,
}

/// Bounded-staleness freshness (spec section 11.4, review m9; see the module
/// docs for the level's contract). A replica is fresh when it has applied
/// every entry its local log holds — it is then missing no known data,
/// however long ago the last write landed, so an idle cluster never reads
/// as stale. `last_known_index` of `None` (a completely empty log) proves
/// the replica never saw a leader and fails closed as arbitrarily stale.
/// While behind, the lag is the applied commit timestamp's age against the
/// node's HLC now; `now` is only consulted on that path so a caught-up
/// replica's read never fails on clock skew.
pub(crate) fn evaluate_bounded_staleness(
    watermark: &ReadWatermark,
    last_known_index: Option<u64>,
    now: impl FnOnce() -> Result<HlcTimestamp, ReadConsistencyError>,
    max_lag_ms: u64,
) -> Result<(), ReadConsistencyError> {
    if last_known_index.is_some_and(|last| watermark.position.index >= last) {
        return Ok(());
    }
    let Some(applied) = watermark.commit_ts else {
        return Err(ReadConsistencyError::StalenessExceeded {
            max_lag_ms,
            lag_ms: u64::MAX,
        });
    };
    let lag_ms = now()?
        .physical_micros
        .saturating_sub(applied.physical_micros)
        / 1_000;
    if lag_ms <= max_lag_ms {
        Ok(())
    } else {
        Err(ReadConsistencyError::StalenessExceeded { max_lag_ms, lag_ms })
    }
}

/// Errors produced by read barriers (spec sections 11.4 and 11.7).
#[derive(Debug, thiserror::Error)]
pub enum ReadConsistencyError {
    /// The replica is not the leader; `leader_hint` carries the current
    /// leader when the group knows one. Retry at the hinted leader (spec
    /// section 11.7); carries `ErrorCategory::NotLeader` semantics.
    #[error("not the leader (current leader: {leader_hint:?})")]
    NotLeader {
        /// The group's current leader hint, if any.
        leader_hint: Option<RaftNodeId>,
    },
    /// No leader is currently known for the group, or leadership could not
    /// be confirmed with a quorum. Retry after leader discovery (spec
    /// section 11.7); carries `ErrorCategory::LeaderUnknown` semantics.
    #[error("no confirmed leader for the consensus group")]
    LeaderUnknown,
    /// The replica's applied timestamp lags further than the requested
    /// bounded-staleness window.
    #[error("staleness {lag_ms} ms exceeds the bound {max_lag_ms} ms")]
    StalenessExceeded {
        /// The requested bound.
        max_lag_ms: u64,
        /// The measured lag (`u64::MAX` when nothing was applied yet).
        lag_ms: u64,
    },
    /// The session token does not belong to this group.
    #[error("invalid session token: {0}")]
    InvalidSessionToken(String),
    /// The operation was cancelled.
    #[error("operation cancelled")]
    Cancelled,
    /// The operation's deadline expired.
    #[error("deadline exceeded")]
    DeadlineExceeded,
    /// The group is shut down.
    #[error("consensus group is closed")]
    Closed,
    /// The node's HLC clock could not produce a timestamp.
    #[error("clock failure: {0}")]
    Clock(String),
    /// Any other read barrier failure.
    #[error("read barrier failure: {0}")]
    Internal(String),
}

impl From<ConsensusError> for ReadConsistencyError {
    fn from(err: ConsensusError) -> Self {
        match err {
            ConsensusError::NotLeader { leader: Some(id) } => Self::NotLeader {
                leader_hint: Some(id),
            },
            // A read barrier that cannot confirm leadership and knows no
            // leader to redirect to is "leader unknown" for routing (spec
            // section 11.7).
            ConsensusError::NotLeader { leader: None } => Self::LeaderUnknown,
            ConsensusError::Cancelled => Self::Cancelled,
            ConsensusError::DeadlineExceeded => Self::DeadlineExceeded,
            ConsensusError::Closed => Self::Closed,
            ConsensusError::Clock(message) => Self::Clock(message),
            other => Self::Internal(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_token_serde_round_trip() {
        let token = SessionToken {
            group_id: "shard-7".to_owned(),
            commit_index: 42,
            commit_ts: HlcTimestamp {
                physical_micros: 1_234,
                logical: 5,
                node_tiebreaker: 9,
            },
        };
        let bytes = bincode::serialize(&token).unwrap();
        assert_eq!(bincode::deserialize::<SessionToken>(&bytes).unwrap(), token);
        let json = serde_json::to_string(&token).unwrap();
        assert_eq!(serde_json::from_str::<SessionToken>(&json).unwrap(), token);
    }

    #[test]
    fn read_consistency_serde_round_trip() {
        let levels = [
            ReadConsistency::Linearizable,
            ReadConsistency::ReadYourWrites {
                token: SessionToken {
                    group_id: "g".to_owned(),
                    commit_index: 1,
                    commit_ts: HlcTimestamp::ZERO,
                },
            },
            ReadConsistency::Snapshot {
                timestamp: HlcTimestamp::ZERO,
            },
            ReadConsistency::BoundedStaleness { max_lag_ms: 250 },
            ReadConsistency::Eventual,
        ];
        for level in levels {
            let bytes = bincode::serialize(&level).unwrap();
            assert_eq!(
                bincode::deserialize::<ReadConsistency>(&bytes).unwrap(),
                level
            );
        }
    }

    #[test]
    fn consensus_error_maps_onto_read_consistency_error() {
        assert!(matches!(
            ReadConsistencyError::from(ConsensusError::NotLeader { leader: Some(3) }),
            ReadConsistencyError::NotLeader {
                leader_hint: Some(3)
            }
        ));
        assert!(matches!(
            ReadConsistencyError::from(ConsensusError::NotLeader { leader: None }),
            ReadConsistencyError::LeaderUnknown
        ));
        assert!(matches!(
            ReadConsistencyError::from(ConsensusError::DeadlineExceeded),
            ReadConsistencyError::DeadlineExceeded
        ));
        assert!(matches!(
            ReadConsistencyError::from(ConsensusError::Closed),
            ReadConsistencyError::Closed
        ));
    }

    fn watermark(index: u64, commit_micros: Option<u64>) -> ReadWatermark {
        ReadWatermark {
            position: LogPosition { term: 1, index },
            commit_ts: commit_micros.map(|physical_micros| HlcTimestamp {
                physical_micros,
                logical: 0,
                node_tiebreaker: 0,
            }),
        }
    }

    #[test]
    fn caught_up_replica_is_fresh_regardless_of_write_age() {
        // The last write landed an hour ago, but the replica applied every
        // entry its log holds: nothing known is missing (review m9). The
        // clock must not even be consulted on this path.
        let panic_clock = || panic!("caught-up reads never consult the clock");
        for max_lag_ms in [0, 1, 3_600_000] {
            assert!(
                evaluate_bounded_staleness(
                    &watermark(42, Some(1_000_000)),
                    Some(42),
                    panic_clock,
                    max_lag_ms
                )
                .is_ok(),
                "caught-up replica must be fresh for any bound"
            );
        }
    }

    #[test]
    fn empty_log_fails_closed_as_arbitrarily_stale() {
        let panic_clock = || panic!("an unapplied replica never consults the clock");
        let err =
            evaluate_bounded_staleness(&watermark(0, None), None, panic_clock, 60_000).unwrap_err();
        assert!(matches!(
            err,
            ReadConsistencyError::StalenessExceeded {
                lag_ms: u64::MAX,
                ..
            }
        ));
    }

    #[test]
    fn behind_replica_measures_applied_ts_age() {
        let now = || {
            Ok(HlcTimestamp {
                physical_micros: 1_000_000_000,
                logical: 0,
                node_tiebreaker: 0,
            })
        };
        // Behind (applied 10 < known 12), watermark 5 ms old, bound 10 ms.
        assert!(
            evaluate_bounded_staleness(&watermark(10, Some(999_995_000)), Some(12), now, 10)
                .is_ok()
        );
        // Same age, bound 1 ms: stale, with the measured lag reported.
        let err = evaluate_bounded_staleness(&watermark(10, Some(999_995_000)), Some(12), now, 1)
            .unwrap_err();
        match err {
            ReadConsistencyError::StalenessExceeded { max_lag_ms, lag_ms } => {
                assert_eq!(max_lag_ms, 1);
                assert_eq!(lag_ms, 5);
            }
            other => panic!("expected StalenessExceeded, got {other:?}"),
        }
        // Behind with nothing applied yet: arbitrarily stale.
        let err =
            evaluate_bounded_staleness(&watermark(0, None), Some(12), now, 60_000).unwrap_err();
        assert!(matches!(
            err,
            ReadConsistencyError::StalenessExceeded {
                lag_ms: u64::MAX,
                ..
            }
        ));
    }
}
