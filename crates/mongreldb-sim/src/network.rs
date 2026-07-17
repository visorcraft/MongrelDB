//! Virtual network links between addressed nodes (spec section 9.5,
//! FND-005).
//!
//! Links carry messages with a configurable latency range and per-mille
//! drop and duplication rates, all drawn from the caller's seeded
//! [`SimRng`]. Latency jitter naturally reorders messages; a send whose
//! delivery time undercuts a previously scheduled send on the same link
//! is counted in [`NetworkStats`] as a reorder. [`Network::partition`]
//! cuts connectivity between two node sets (both directions) until
//! [`Network::heal`] restores it; messages already in flight when a
//! partition starts still arrive.

use crate::clock::Micros;
use crate::rng::SimRng;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;

/// A simulated node address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u16);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "n{}", self.0)
    }
}

/// Why a message never reached its destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DropReason {
    /// The link was cut by a partition.
    Partition,
    /// The link's configured drop rate fired.
    LinkFault,
    /// The destination node was crashed at delivery time.
    NodeCrashed,
}

/// Per-link behavior. Rates are per-mille to keep every decision in the
/// integer domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinkConfig {
    /// Minimum one-way latency in virtual micros.
    pub min_latency: Micros,
    /// Maximum one-way latency in virtual micros (inclusive). A spread
    /// greater than zero reorders messages.
    pub max_latency: Micros,
    /// Per-mille chance a sent message is dropped.
    pub drop_per_mille: u32,
    /// Per-mille chance a sent message is duplicated.
    pub duplicate_per_mille: u32,
}

impl LinkConfig {
    /// A link with fixed behavior and no faults.
    pub fn new(min_latency: Micros, max_latency: Micros) -> Self {
        assert!(
            min_latency <= max_latency,
            "min_latency must be <= max_latency"
        );
        Self {
            min_latency,
            max_latency,
            drop_per_mille: 0,
            duplicate_per_mille: 0,
        }
    }

    /// Sets the drop rate.
    pub fn with_drop_per_mille(mut self, per_mille: u32) -> Self {
        self.drop_per_mille = per_mille;
        self
    }

    /// Sets the duplication rate.
    pub fn with_duplicate_per_mille(mut self, per_mille: u32) -> Self {
        self.duplicate_per_mille = per_mille;
        self
    }
}

impl Default for LinkConfig {
    fn default() -> Self {
        Self::new(1, 1)
    }
}

/// A message in flight or in an inbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// Sender.
    pub from: NodeId,
    /// Destination.
    pub to: NodeId,
    /// Globally unique sequence number (duplicates get their own).
    pub seq: u64,
    /// Opaque payload.
    pub payload: Vec<u8>,
    /// Virtual time the message was sent.
    pub sent_at: Micros,
    /// Virtual time the message becomes deliverable.
    pub deliver_at: Micros,
}

/// The result of [`Network::send`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendOutcome {
    /// The message (and possibly a duplicate) entered the flight queue.
    Scheduled {
        /// Sequence number of the original copy.
        seq: u64,
        /// Virtual delivery time of the original copy.
        deliver_at: Micros,
        /// Whether an extra copy was scheduled.
        duplicated: bool,
        /// Whether this send undercut an earlier send on the same link.
        reordered: bool,
    },
    /// The message was dropped at send time.
    Dropped {
        /// Sequence number the dropped message would have had.
        seq: u64,
        /// Why it was dropped.
        reason: DropReason,
    },
}

/// What happened to a message that reached its delivery time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryOutcome {
    /// Moved into the destination inbox.
    Delivered,
    /// Thrown away (destination crashed).
    Discarded(DropReason),
}

/// A delivery event returned by [`Network::deliver_due`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Delivery {
    /// Sender.
    pub from: NodeId,
    /// Destination.
    pub to: NodeId,
    /// Sequence number.
    pub seq: u64,
    /// What happened.
    pub outcome: DeliveryOutcome,
}

/// Cumulative delivery counters for test assertions.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NetworkStats {
    /// Messages passed to [`Network::send`].
    pub sent: u64,
    /// Messages moved into a destination inbox.
    pub delivered: u64,
    /// Messages dropped (partition, link fault, or crashed receiver).
    pub dropped: u64,
    /// Extra copies scheduled by duplication.
    pub duplicated: u64,
    /// Sends whose delivery time undercut an earlier send on the link.
    pub reordered: u64,
}

/// The virtual network: links, cuts, the flight queue, and per-node
/// inboxes.
#[derive(Debug)]
pub struct Network {
    default_link: LinkConfig,
    links: BTreeMap<(NodeId, NodeId), LinkConfig>,
    cuts: BTreeSet<(NodeId, NodeId)>,
    in_flight: BTreeMap<(Micros, u64), Message>,
    inboxes: BTreeMap<NodeId, VecDeque<Message>>,
    last_deliver_at: BTreeMap<(NodeId, NodeId), Micros>,
    next_seq: u64,
    stats: NetworkStats,
}

impl Network {
    /// A fully connected network using `default_link` for pairs without
    /// an explicit configuration.
    pub fn new(default_link: LinkConfig) -> Self {
        Self {
            default_link,
            links: BTreeMap::new(),
            cuts: BTreeSet::new(),
            in_flight: BTreeMap::new(),
            inboxes: BTreeMap::new(),
            last_deliver_at: BTreeMap::new(),
            next_seq: 1,
            stats: NetworkStats::default(),
        }
    }

    /// Overrides the configuration of one directed link.
    pub fn set_link_config(&mut self, from: NodeId, to: NodeId, config: LinkConfig) {
        assert!(
            config.min_latency <= config.max_latency,
            "min_latency must be <= max_latency"
        );
        assert!(
            config.drop_per_mille <= 1000 && config.duplicate_per_mille <= 1000,
            "per-mille rates must be <= 1000"
        );
        self.links.insert((from, to), config);
    }

    /// Cuts connectivity between two node sets, in both directions.
    /// Messages already in flight are unaffected.
    pub fn partition(
        &mut self,
        group_a: impl IntoIterator<Item = NodeId>,
        group_b: impl IntoIterator<Item = NodeId>,
    ) {
        let group_b: Vec<NodeId> = group_b.into_iter().collect();
        for a in group_a {
            for &b in &group_b {
                if a != b {
                    self.cuts.insert(normalize(a, b));
                }
            }
        }
    }

    /// Restores full connectivity.
    pub fn heal(&mut self) {
        self.cuts.clear();
    }

    /// Whether a send between the two nodes would go through.
    pub fn connected(&self, a: NodeId, b: NodeId) -> bool {
        a == b || !self.cuts.contains(&normalize(a, b))
    }

    /// Queues a message for delivery, applying the link's latency, drop,
    /// and duplication behavior. All randomness comes from `rng`.
    pub fn send(
        &mut self,
        from: NodeId,
        to: NodeId,
        payload: Vec<u8>,
        now: Micros,
        rng: &mut SimRng,
    ) -> SendOutcome {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.stats.sent += 1;

        if !self.connected(from, to) {
            self.stats.dropped += 1;
            return SendOutcome::Dropped {
                seq,
                reason: DropReason::Partition,
            };
        }

        let link = self.link_config(from, to);
        if rng.chance(link.drop_per_mille) {
            self.stats.dropped += 1;
            return SendOutcome::Dropped {
                seq,
                reason: DropReason::LinkFault,
            };
        }

        let latency = if link.max_latency > link.min_latency {
            rng.range(link.min_latency, link.max_latency + 1)
        } else {
            link.min_latency
        };
        let deliver_at = now + latency;

        let key = (from, to);
        let previous = self.last_deliver_at.get(&key).copied();
        let reordered = previous.is_some_and(|earliest_max| deliver_at < earliest_max);
        if reordered {
            self.stats.reordered += 1;
        }
        self.last_deliver_at
            .insert(key, previous.map_or(deliver_at, |p| p.max(deliver_at)));

        let message = Message {
            from,
            to,
            seq,
            payload,
            sent_at: now,
            deliver_at,
        };
        let mut duplicated = false;
        if rng.chance(link.duplicate_per_mille) {
            duplicated = true;
            self.stats.duplicated += 1;
            let dup_seq = self.next_seq;
            self.next_seq += 1;
            let copy = Message {
                seq: dup_seq,
                deliver_at: deliver_at + 1,
                ..message.clone()
            };
            self.in_flight.insert((deliver_at + 1, dup_seq), copy);
        }
        self.in_flight.insert((deliver_at, seq), message);

        SendOutcome::Scheduled {
            seq,
            deliver_at,
            duplicated,
            reordered,
        }
    }

    /// Moves every message due at or before `now` into destination
    /// inboxes. Messages addressed to crashed nodes are discarded.
    pub fn deliver_due(&mut self, now: Micros, crashed: &BTreeSet<NodeId>) -> Vec<Delivery> {
        let later = self.in_flight.split_off(&(now.saturating_add(1), 0));
        let due = std::mem::replace(&mut self.in_flight, later);
        let mut deliveries = Vec::new();
        for (_, message) in due {
            let delivery = if crashed.contains(&message.to) {
                self.stats.dropped += 1;
                Delivery {
                    from: message.from,
                    to: message.to,
                    seq: message.seq,
                    outcome: DeliveryOutcome::Discarded(DropReason::NodeCrashed),
                }
            } else {
                self.stats.delivered += 1;
                self.inboxes
                    .entry(message.to)
                    .or_default()
                    .push_back(message.clone());
                Delivery {
                    from: message.from,
                    to: message.to,
                    seq: message.seq,
                    outcome: DeliveryOutcome::Delivered,
                }
            };
            deliveries.push(delivery);
        }
        deliveries
    }

    /// The earliest pending delivery time, if any.
    pub fn next_delivery(&self) -> Option<Micros> {
        self.in_flight.first_key_value().map(|(&(at, _), _)| at)
    }

    /// Pops the oldest message from a node's inbox.
    pub fn try_recv(&mut self, node: NodeId) -> Option<Message> {
        self.inboxes.get_mut(&node).and_then(VecDeque::pop_front)
    }

    /// Number of messages waiting in a node's inbox.
    pub fn inbox_len(&self, node: NodeId) -> usize {
        self.inboxes.get(&node).map_or(0, VecDeque::len)
    }

    /// Drops every buffered message of a node (its volatile state).
    pub fn clear_inbox(&mut self, node: NodeId) {
        self.inboxes.remove(&node);
    }

    /// Cumulative delivery counters.
    pub fn stats(&self) -> NetworkStats {
        self.stats
    }

    fn link_config(&self, from: NodeId, to: NodeId) -> LinkConfig {
        self.links
            .get(&(from, to))
            .copied()
            .unwrap_or(self.default_link)
    }
}

fn normalize(a: NodeId, b: NodeId) -> (NodeId, NodeId) {
    if a <= b {
        (a, b)
    } else {
        (b, a)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rng::Seed;

    const A: NodeId = NodeId(1);
    const B: NodeId = NodeId(2);

    fn no_crash() -> BTreeSet<NodeId> {
        BTreeSet::new()
    }

    #[test]
    fn messages_deliver_in_latency_order() {
        let mut net = Network::new(LinkConfig::default());
        net.set_link_config(A, B, LinkConfig::new(10, 10));
        let mut rng = SimRng::from_seed(Seed::new(1));
        net.send(A, B, b"x".to_vec(), 0, &mut rng);
        net.send(A, B, b"y".to_vec(), 5, &mut rng);

        assert_eq!(net.next_delivery(), Some(10));
        assert!(net.deliver_due(9, &no_crash()).is_empty());
        let due = net.deliver_due(10, &no_crash());
        assert_eq!(due.len(), 1);
        assert_eq!(net.try_recv(B).unwrap().payload, b"x");
        assert!(net.try_recv(B).is_none());

        let due = net.deliver_due(15, &no_crash());
        assert_eq!(due.len(), 1);
        assert_eq!(net.try_recv(B).unwrap().payload, b"y");
    }

    #[test]
    fn full_drop_rate_drops_everything() {
        let mut net = Network::new(LinkConfig::default());
        net.set_link_config(A, B, LinkConfig::new(1, 1).with_drop_per_mille(1000));
        let mut rng = SimRng::from_seed(Seed::new(2));
        for i in 0..10u8 {
            let outcome = net.send(A, B, vec![i], 0, &mut rng);
            assert!(matches!(
                outcome,
                SendOutcome::Dropped {
                    reason: DropReason::LinkFault,
                    ..
                }
            ));
        }
        assert_eq!(net.stats().sent, 10);
        assert_eq!(net.stats().dropped, 10);
        assert_eq!(net.stats().delivered, 0);
    }

    #[test]
    fn full_duplicate_rate_doubles_delivery() {
        let mut net = Network::new(LinkConfig::default());
        net.set_link_config(A, B, LinkConfig::new(1, 1).with_duplicate_per_mille(1000));
        let mut rng = SimRng::from_seed(Seed::new(3));
        for i in 0..5u8 {
            let outcome = net.send(A, B, vec![i], 0, &mut rng);
            assert!(matches!(
                outcome,
                SendOutcome::Scheduled {
                    duplicated: true,
                    ..
                }
            ));
        }
        assert_eq!(net.deliver_due(100, &no_crash()).len(), 10);
        assert_eq!(net.stats().duplicated, 5);
        assert_eq!(net.stats().delivered, 10);
        let mut drained = 0;
        while net.try_recv(B).is_some() {
            drained += 1;
        }
        assert_eq!(drained, 10);
    }

    #[test]
    fn partition_cuts_both_directions_until_heal() {
        let mut net = Network::new(LinkConfig::default());
        let mut rng = SimRng::from_seed(Seed::new(4));
        net.send(A, B, b"before".to_vec(), 0, &mut rng);

        net.partition([A], [B]);
        assert!(!net.connected(A, B));
        assert!(!net.connected(B, A));
        for (from, to) in [(A, B), (B, A)] {
            let outcome = net.send(from, to, b"x".to_vec(), 1, &mut rng);
            assert!(matches!(
                outcome,
                SendOutcome::Dropped {
                    reason: DropReason::Partition,
                    ..
                }
            ));
        }
        // A message sent before the partition still arrives.
        assert_eq!(net.deliver_due(100, &no_crash()).len(), 1);
        assert_eq!(net.try_recv(B).unwrap().payload, b"before");

        net.heal();
        assert!(net.connected(A, B));
        assert!(matches!(
            net.send(A, B, b"after".to_vec(), 2, &mut rng),
            SendOutcome::Scheduled { .. }
        ));
    }

    #[test]
    fn delivery_to_crashed_node_is_discarded() {
        let mut net = Network::new(LinkConfig::default());
        let mut rng = SimRng::from_seed(Seed::new(5));
        net.send(A, B, b"x".to_vec(), 0, &mut rng);

        let crashed = BTreeSet::from([B]);
        let due = net.deliver_due(100, &crashed);
        assert_eq!(
            due,
            vec![Delivery {
                from: A,
                to: B,
                seq: 1,
                outcome: DeliveryOutcome::Discarded(DropReason::NodeCrashed),
            }]
        );
        assert_eq!(net.inbox_len(B), 0);
        assert_eq!(net.stats().dropped, 1);
        assert_eq!(net.stats().delivered, 0);
    }
}
