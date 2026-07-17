//! Seeded pseudo-random streams (spec section 9.5, FND-005).
//!
//! All nondeterminism in the simulator flows through [`SimRng`], a
//! splitmix64 generator. splitmix64 is chosen because it is tiny, has
//! no zero-state trap, and its counter structure makes stream splitting
//! trivial: [`SimRng::fork`] and [`SimRng::fork_label`] hand each
//! subsystem an independent stream, so drawing extra numbers in one
//! subsystem never perturbs another.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

const GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

/// A reproducible seed for a simulation run.
///
/// The seed is the single input that fixes every interleaving, fault,
/// and latency decision in a scenario. It is recorded in failure
/// artifacts so a failing run can be replayed exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Seed(u64);

impl Seed {
    /// Wraps a raw 64-bit seed value.
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw 64-bit seed value.
    pub fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Seed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for Seed {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<u64>().map(Self)
    }
}

/// A splitmix64 pseudo-random stream.
#[derive(Debug, Clone)]
pub struct SimRng {
    state: u64,
}

impl SimRng {
    /// Creates a stream from a seed. A zero state is fine for splitmix64.
    pub fn from_seed(seed: Seed) -> Self {
        Self { state: seed.get() }
    }

    /// Advances the stream and returns the next 64-bit value.
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(GAMMA);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform value in `0..n`, using the multiply-high trick to avoid
    /// modulo bias. Panics if `n` is zero.
    pub fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0, "SimRng::below requires n > 0");
        ((self.next_u64() as u128 * n as u128) >> 64) as u64
    }

    /// Uniform value in `min..max` (half-open). Panics on an empty range.
    pub fn range(&mut self, min: u64, max: u64) -> u64 {
        assert!(min < max, "SimRng::range requires min < max");
        min + self.below(max - min)
    }

    /// True with probability `per_mille / 1000`.
    pub fn chance(&mut self, per_mille: u32) -> bool {
        assert!(per_mille <= 1000, "per-mille probability out of range");
        self.below(1000) < u64::from(per_mille)
    }

    /// Splits off an independent stream for a subsystem.
    pub fn fork(&mut self) -> SimRng {
        SimRng::from_seed(Seed(self.next_u64()))
    }

    /// Derives a named independent stream without advancing this one.
    /// The label is mixed into the current state with FNV-1a.
    pub fn fork_label(&self, label: &str) -> SimRng {
        let mut hash = 0xcbf2_9ce4_8422_2325u64;
        for byte in label.bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        let mut mixer = SimRng {
            state: self.state ^ hash,
        };
        SimRng {
            state: mixer.next_u64(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_sequence() {
        let mut a = SimRng::from_seed(Seed::new(42));
        let mut b = SimRng::from_seed(Seed::new(42));
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seeds_diverge() {
        let mut a = SimRng::from_seed(Seed::new(1));
        let mut b = SimRng::from_seed(Seed::new(2));
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn fork_derives_child_from_one_parent_draw() {
        // fork() consumes exactly one parent draw to seed the child;
        // afterwards the two streams evolve independently.
        let mut reference = SimRng::from_seed(Seed::new(7));
        let draws: Vec<u64> = (0..10).map(|_| reference.next_u64()).collect();

        let mut parent = SimRng::from_seed(Seed::new(7));
        let mut observed = vec![parent.next_u64()];
        let mut child = parent.fork();
        observed.extend((0..8).map(|_| parent.next_u64()));

        let mut expected = vec![draws[0]];
        expected.extend_from_slice(&draws[2..]);
        assert_eq!(observed, expected);

        let mut seeded_child = SimRng::from_seed(Seed::new(draws[1]));
        assert_eq!(child.next_u64(), seeded_child.next_u64());
    }

    #[test]
    fn fork_label_is_stable_and_distinct() {
        let rng = SimRng::from_seed(Seed::new(9));
        assert_eq!(
            rng.fork_label("net").next_u64(),
            rng.fork_label("net").next_u64()
        );
        assert_ne!(
            rng.fork_label("net").next_u64(),
            rng.fork_label("disk").next_u64()
        );
    }

    #[test]
    fn ranges_and_chances_respect_bounds() {
        let mut rng = SimRng::from_seed(Seed::new(3));
        for _ in 0..1_000 {
            assert!((5..10).contains(&rng.range(5, 10)));
            assert!(rng.below(4) < 4);
        }
        assert!(rng.chance(1000));
        assert!(!rng.chance(0));
    }

    #[test]
    fn seed_serializes_for_repro_artifacts() {
        let seed = Seed::new(123_456);
        let json = serde_json::to_string(&seed).unwrap();
        assert_eq!("123456", json);
        let back: Seed = serde_json::from_str(&json).unwrap();
        assert_eq!(seed, back);
        assert_eq!("123456".parse::<Seed>().unwrap(), seed);
    }
}
