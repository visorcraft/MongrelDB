//! Versioned network message envelope (spec section 4.10, S1D-001).
//!
//! Every message on every MongrelDB wire protocol travels inside a
//! [`ProtocolEnvelope`], never as an unversioned payload. The canonical
//! encoding is:
//!
//! ```text
//! protocol_version : u32 LE
//! message_type     : u32 LE
//! payload_len      : u32 LE
//! payload          : [u8; payload_len]
//! payload_crc32    : u32 LE
//! ```
//!
//! The CRC-32 (IEEE, reflected) covers the protocol version, the message
//! type, the payload length, and the payload. This mirrors the fail-closed
//! shape of `mongreldb-log`'s `CommandEnvelope`: unknown protocol versions,
//! oversized payloads, truncated frames, trailing bytes, and checksum
//! mismatches are all decode errors (spec section 4.10: unknown required
//! fields or incompatible versions fail closed).
//!
//! Unlike `CommandEnvelope` the checksum is a CRC-32 rather than SHA-256:
//! this crate's dependency set is frozen (`serde`, `thiserror`,
//! `mongreldb-types`), and transport integrity plus peer authentication are
//! provided by TLS 1.3 (S1D-002), so the checksum's job here is only
//! framing sanity, not cryptographic authentication.
//!
//! Encoding is deterministic: [`ProtocolEnvelope::encode`] is a pure
//! function of the envelope fields, so equal envelopes produce
//! byte-identical frames and `decode(encode(e))` is the identity.
//!
//! Payload evolution rules (spec section 4.10):
//!
//! - `message_type` discriminants and payload field numbers are never
//!   reused; new message types are allocated fresh numbers.
//! - The envelope never interprets payload bytes; payload decoding is the
//!   adapter's job (Protobuf control frames, Arrow IPC result frames, per
//!   S1D-002 and ADR 0005).
//! - Unknown protocol versions fail closed with
//!   [`EnvelopeError::UnsupportedVersion`].

use core::fmt;

/// The protocol version this build writes.
pub const PROTOCOL_VERSION: u32 = 1;
/// The oldest protocol version this build accepts.
pub const MIN_SUPPORTED_PROTOCOL_VERSION: u32 = 1;
/// Upper bound on a single message payload.
pub const MAX_MESSAGE_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
/// Encoded length of the fixed header preceding the payload.
pub const HEADER_LEN: usize = 4 + 4 + 4;
/// Encoded length of the trailing checksum.
pub const CHECKSUM_LEN: usize = 4;

/// Errors produced while verifying or decoding a [`ProtocolEnvelope`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EnvelopeError {
    /// The protocol version is outside the supported range.
    #[error("unsupported protocol version {found} (supported {min}..={max})")]
    UnsupportedVersion {
        /// Version found in the envelope.
        found: u32,
        /// Oldest version this build accepts.
        min: u32,
        /// Newest version this build accepts.
        max: u32,
    },
    /// The stored checksum does not match the recomputed one.
    #[error("protocol-envelope checksum mismatch")]
    ChecksumMismatch,
    /// The byte slice ended before a complete envelope was read.
    #[error("protocol envelope truncated: expected at least {expected} bytes, got {actual}")]
    Truncated {
        /// Minimum number of bytes required.
        expected: usize,
        /// Number of bytes actually present.
        actual: usize,
    },
    /// Extra bytes followed an otherwise complete envelope.
    #[error("protocol envelope has {0} trailing bytes")]
    TrailingBytes(usize),
    /// The payload exceeds [`MAX_MESSAGE_PAYLOAD_BYTES`].
    #[error("message payload too large: {0} bytes")]
    PayloadTooLarge(usize),
}

/// IEEE CRC-32 (reflected, polynomial 0xEDB88320) lookup table, built at
/// compile time.
const fn build_crc32_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 {
                0xEDB8_8320 ^ (c >> 1)
            } else {
                c >> 1
            };
            k += 1;
        }
        table[i] = c;
        i += 1;
    }
    table
}

static CRC32_TABLE: [u32; 256] = build_crc32_table();

fn crc32_update(mut crc: u32, bytes: &[u8]) -> u32 {
    for &byte in bytes {
        crc = CRC32_TABLE[((crc ^ u32::from(byte)) & 0xff) as usize] ^ (crc >> 8);
    }
    crc
}

/// CRC-32 over the concatenation of `parts`.
fn crc32(parts: &[&[u8]]) -> u32 {
    let mut crc = 0xffff_ffffu32;
    for part in parts {
        crc = crc32_update(crc, part);
    }
    crc ^ 0xffff_ffff
}

/// The versioned, checksummed form of every protocol message.
#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProtocolEnvelope {
    /// Protocol version; see [`PROTOCOL_VERSION`].
    pub protocol_version: u32,
    /// Discriminant of the payload message; numbers are never reused.
    pub message_type: u32,
    /// Encoded message payload. The envelope never interprets these bytes.
    pub payload: Vec<u8>,
    /// CRC-32 over version, type, length, and payload.
    pub payload_crc32: u32,
}

impl ProtocolEnvelope {
    /// Builds an envelope at the current protocol version, computing the
    /// checksum.
    pub fn new(message_type: u32, payload: Vec<u8>) -> Self {
        let payload_crc32 = Self::checksum(PROTOCOL_VERSION, message_type, &payload);
        Self {
            protocol_version: PROTOCOL_VERSION,
            message_type,
            payload,
            payload_crc32,
        }
    }

    /// Computes the checksum covering version, type, length, and payload.
    pub fn checksum(protocol_version: u32, message_type: u32, payload: &[u8]) -> u32 {
        crc32(&[
            &protocol_version.to_le_bytes(),
            &message_type.to_le_bytes(),
            &(payload.len() as u64).to_le_bytes(),
            payload,
        ])
    }

    /// Fails closed unless the version is supported and the checksum matches.
    pub fn verify(&self) -> Result<(), EnvelopeError> {
        if !(MIN_SUPPORTED_PROTOCOL_VERSION..=PROTOCOL_VERSION).contains(&self.protocol_version) {
            return Err(EnvelopeError::UnsupportedVersion {
                found: self.protocol_version,
                min: MIN_SUPPORTED_PROTOCOL_VERSION,
                max: PROTOCOL_VERSION,
            });
        }
        if self.payload.len() > MAX_MESSAGE_PAYLOAD_BYTES {
            return Err(EnvelopeError::PayloadTooLarge(self.payload.len()));
        }
        let expected = Self::checksum(self.protocol_version, self.message_type, &self.payload);
        if expected != self.payload_crc32 {
            return Err(EnvelopeError::ChecksumMismatch);
        }
        Ok(())
    }

    /// Serializes to the canonical deterministic encoding: equal envelopes
    /// always produce byte-identical output.
    pub fn encode(&self) -> Vec<u8> {
        debug_assert!(
            self.payload.len() <= MAX_MESSAGE_PAYLOAD_BYTES,
            "payload exceeds MAX_MESSAGE_PAYLOAD_BYTES"
        );
        let mut out = Vec::with_capacity(HEADER_LEN + self.payload.len() + CHECKSUM_LEN);
        out.extend_from_slice(&self.protocol_version.to_le_bytes());
        out.extend_from_slice(&self.message_type.to_le_bytes());
        out.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.payload);
        out.extend_from_slice(&self.payload_crc32.to_le_bytes());
        out
    }

    /// Parses one envelope, verifying version and checksum (fails closed).
    pub fn decode(bytes: &[u8]) -> Result<Self, EnvelopeError> {
        if bytes.len() < HEADER_LEN + CHECKSUM_LEN {
            return Err(EnvelopeError::Truncated {
                expected: HEADER_LEN + CHECKSUM_LEN,
                actual: bytes.len(),
            });
        }
        let protocol_version = u32::from_le_bytes(bytes[0..4].try_into().expect("slice len"));
        let message_type = u32::from_le_bytes(bytes[4..8].try_into().expect("slice len"));
        let payload_len = u32::from_le_bytes(bytes[8..12].try_into().expect("slice len")) as usize;
        // Fail closed on an oversize length prefix before copying any payload
        // bytes, regardless of how long the supplied frame actually is.
        if payload_len > MAX_MESSAGE_PAYLOAD_BYTES {
            return Err(EnvelopeError::PayloadTooLarge(payload_len));
        }
        let expected_total = HEADER_LEN + payload_len + CHECKSUM_LEN;
        if bytes.len() < expected_total {
            return Err(EnvelopeError::Truncated {
                expected: expected_total,
                actual: bytes.len(),
            });
        }
        if bytes.len() > expected_total {
            return Err(EnvelopeError::TrailingBytes(bytes.len() - expected_total));
        }
        let payload = bytes[HEADER_LEN..HEADER_LEN + payload_len].to_vec();
        let payload_crc32 = u32::from_le_bytes(
            bytes[HEADER_LEN + payload_len..expected_total]
                .try_into()
                .expect("slice len"),
        );
        let envelope = Self {
            protocol_version,
            message_type,
            payload,
            payload_crc32,
        };
        envelope.verify()?;
        Ok(envelope)
    }
}

impl fmt::Debug for ProtocolEnvelope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProtocolEnvelope")
            .field("protocol_version", &self.protocol_version)
            .field("message_type", &self.message_type)
            .field("payload_len", &self.payload.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_matches_reference_vector() {
        // The canonical IEEE CRC-32 check value.
        assert_eq!(crc32(&[b"123456789"]), 0xCBF4_3926);
        assert_eq!(crc32(&[]), 0);
        // Multi-part input equals the one-shot form.
        assert_eq!(crc32(&[b"1234", b"56789"]), crc32(&[b"123456789"]));
    }

    #[test]
    fn round_trip() {
        let envelope = ProtocolEnvelope::new(7, b"hello".to_vec());
        let bytes = envelope.encode();
        assert_eq!(ProtocolEnvelope::decode(&bytes).unwrap(), envelope);
        // Encoding is deterministic.
        assert_eq!(envelope.encode(), bytes);
    }

    #[test]
    fn bit_flip_breaks_checksum() {
        let envelope = ProtocolEnvelope::new(1, vec![9u8; 64]);
        let mut bytes = envelope.encode();
        bytes[HEADER_LEN] ^= 0x01;
        assert_eq!(
            ProtocolEnvelope::decode(&bytes),
            Err(EnvelopeError::ChecksumMismatch)
        );
    }

    #[test]
    fn unknown_version_fails_closed() {
        let mut envelope = ProtocolEnvelope::new(1, vec![]);
        envelope.protocol_version = PROTOCOL_VERSION + 1;
        envelope.payload_crc32 =
            ProtocolEnvelope::checksum(envelope.protocol_version, 1, &envelope.payload);
        assert!(matches!(
            envelope.verify(),
            Err(EnvelopeError::UnsupportedVersion { .. })
        ));
        // The wire form fails closed too, not just the in-memory form.
        let bytes = envelope.encode();
        assert!(matches!(
            ProtocolEnvelope::decode(&bytes),
            Err(EnvelopeError::UnsupportedVersion { .. })
        ));
    }

    #[test]
    fn truncation_and_trailing_bytes_fail() {
        let envelope = ProtocolEnvelope::new(3, vec![1, 2, 3]);
        let bytes = envelope.encode();
        assert!(matches!(
            ProtocolEnvelope::decode(&bytes[..bytes.len() - 1]),
            Err(EnvelopeError::Truncated { .. })
        ));
        // A frame shorter than header + checksum is truncated.
        assert!(matches!(
            ProtocolEnvelope::decode(&bytes[..HEADER_LEN]),
            Err(EnvelopeError::Truncated { .. })
        ));
        let mut longer = bytes.clone();
        longer.push(0);
        assert_eq!(
            ProtocolEnvelope::decode(&longer),
            Err(EnvelopeError::TrailingBytes(1))
        );
    }

    #[test]
    fn oversize_length_prefix_fails_before_copying() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&PROTOCOL_VERSION.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&(MAX_MESSAGE_PAYLOAD_BYTES as u32 + 1).to_le_bytes());
        bytes.extend_from_slice(&[0u8; CHECKSUM_LEN]);
        assert!(matches!(
            ProtocolEnvelope::decode(&bytes),
            Err(EnvelopeError::PayloadTooLarge(_))
        ));
    }

    #[test]
    fn envelope_serde_round_trip() {
        crate::test_support::assert_serde_round_trip(&ProtocolEnvelope::new(42, vec![1, 2, 3]));
    }
}
