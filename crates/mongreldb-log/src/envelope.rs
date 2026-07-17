//! Versioned durable command envelope (spec section 9.3, FND-003).
//!
//! Every new cluster command is persisted inside a [`CommandEnvelope`], never
//! as an unversioned `bincode` enum. The canonical encoding is:
//!
//! ```text
//! format_version : u32 LE
//! command_id     : [u8; 16]
//! command_type   : u32 LE
//! payload_len    : u32 LE
//! payload        : [u8; payload_len]
//! payload_sha256 : [u8; 32]
//! ```
//!
//! The checksum covers the format version, the command type, the payload
//! length, and the payload. Decoding fails closed: unknown format versions,
//! oversized payloads, truncated frames, trailing bytes, and checksum
//! mismatches are all errors.
//!
//! Encoding is deterministic: [`CommandEnvelope::encode`] is a pure function
//! of the envelope fields, so equal envelopes produce byte-identical frames
//! and `decode(encode(e))` is the identity.
//!
//! Payload evolution rules (spec section 9.3):
//!
//! - `command_type` discriminants and payload field numbers are never reused.
//! - Unknown optional fields are preserved or ignored safely at the payload
//!   layer; the envelope itself never interprets payload bytes.
//! - Unknown required command versions fail closed with
//!   [`EnvelopeError::UnsupportedVersion`].
//! - Payloads use a schema-evolution-safe encoding (Protobuf per
//!   `docs/architecture/adr/0005`, network protocol and serialization).

use core::fmt;
use sha2::{Digest, Sha256};

/// The envelope format version this build writes.
pub const COMMAND_ENVELOPE_FORMAT_VERSION: u32 = 1;
/// The oldest envelope format version this build accepts.
pub const MIN_SUPPORTED_FORMAT_VERSION: u32 = 1;
/// Upper bound on a single command payload.
pub const MAX_COMMAND_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
/// Encoded length of the fixed header preceding the payload.
pub const HEADER_LEN: usize = 4 + 16 + 4 + 4;
/// Encoded length of the trailing checksum.
pub const CHECKSUM_LEN: usize = 32;

/// Errors produced while verifying or decoding a [`CommandEnvelope`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EnvelopeError {
    /// The format version is outside the supported range.
    #[error("unsupported command-envelope format version {found} (supported {min}..={max})")]
    UnsupportedVersion {
        /// Version found in the envelope.
        found: u32,
        /// Oldest version this build accepts.
        min: u32,
        /// Newest version this build accepts.
        max: u32,
    },
    /// The stored checksum does not match the recomputed one.
    #[error("command-envelope checksum mismatch")]
    ChecksumMismatch,
    /// The byte slice ended before a complete envelope was read.
    #[error("command envelope truncated: expected at least {expected} bytes, got {actual}")]
    Truncated {
        /// Minimum number of bytes required.
        expected: usize,
        /// Number of bytes actually present.
        actual: usize,
    },
    /// Extra bytes followed an otherwise complete envelope.
    #[error("command envelope has {0} trailing bytes")]
    TrailingBytes(usize),
    /// The payload exceeds [`MAX_COMMAND_PAYLOAD_BYTES`].
    #[error("command payload too large: {0} bytes")]
    PayloadTooLarge(usize),
}

/// The versioned, checksummed form of every persisted command.
#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CommandEnvelope {
    /// Envelope format version; see [`COMMAND_ENVELOPE_FORMAT_VERSION`].
    pub format_version: u32,
    /// Unique identifier of this command (for idempotent apply).
    pub command_id: [u8; 16],
    /// Discriminant of the payload command; numbers are never reused.
    pub command_type: u32,
    /// Schema-evolution-safe encoded command payload. The envelope never
    /// interprets these bytes; unknown optional payload fields are preserved
    /// or ignored safely by the payload decoder.
    pub payload: Vec<u8>,
    /// SHA-256 over version, type, length, and payload.
    pub payload_sha256: [u8; 32],
}

impl CommandEnvelope {
    /// Builds an envelope at the current format version, computing the checksum.
    pub fn new(command_type: u32, command_id: [u8; 16], payload: Vec<u8>) -> Self {
        let payload_sha256 =
            Self::checksum(COMMAND_ENVELOPE_FORMAT_VERSION, command_type, &payload);
        Self {
            format_version: COMMAND_ENVELOPE_FORMAT_VERSION,
            command_id,
            command_type,
            payload,
            payload_sha256,
        }
    }

    /// Computes the checksum covering type, version, length, and payload.
    pub fn checksum(format_version: u32, command_type: u32, payload: &[u8]) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(format_version.to_le_bytes());
        hasher.update(command_type.to_le_bytes());
        hasher.update((payload.len() as u64).to_le_bytes());
        hasher.update(payload);
        hasher.finalize().into()
    }

    /// Fails closed unless the version is supported and the checksum matches.
    pub fn verify(&self) -> Result<(), EnvelopeError> {
        if !(MIN_SUPPORTED_FORMAT_VERSION..=COMMAND_ENVELOPE_FORMAT_VERSION)
            .contains(&self.format_version)
        {
            return Err(EnvelopeError::UnsupportedVersion {
                found: self.format_version,
                min: MIN_SUPPORTED_FORMAT_VERSION,
                max: COMMAND_ENVELOPE_FORMAT_VERSION,
            });
        }
        if self.payload.len() > MAX_COMMAND_PAYLOAD_BYTES {
            return Err(EnvelopeError::PayloadTooLarge(self.payload.len()));
        }
        let expected = Self::checksum(self.format_version, self.command_type, &self.payload);
        if expected != self.payload_sha256 {
            return Err(EnvelopeError::ChecksumMismatch);
        }
        Ok(())
    }

    /// Serializes to the canonical deterministic encoding: equal envelopes
    /// always produce byte-identical output.
    pub fn encode(&self) -> Vec<u8> {
        debug_assert!(
            self.payload.len() <= MAX_COMMAND_PAYLOAD_BYTES,
            "payload exceeds MAX_COMMAND_PAYLOAD_BYTES"
        );
        let mut out = Vec::with_capacity(HEADER_LEN + self.payload.len() + CHECKSUM_LEN);
        out.extend_from_slice(&self.format_version.to_le_bytes());
        out.extend_from_slice(&self.command_id);
        out.extend_from_slice(&self.command_type.to_le_bytes());
        out.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.payload);
        out.extend_from_slice(&self.payload_sha256);
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
        let format_version = u32::from_le_bytes(bytes[0..4].try_into().expect("slice len"));
        let command_id: [u8; 16] = bytes[4..20].try_into().expect("slice len");
        let command_type = u32::from_le_bytes(bytes[20..24].try_into().expect("slice len"));
        let payload_len = u32::from_le_bytes(bytes[24..28].try_into().expect("slice len")) as usize;
        // Fail closed on an oversize length prefix before copying any payload
        // bytes, regardless of how long the supplied frame actually is.
        if payload_len > MAX_COMMAND_PAYLOAD_BYTES {
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
        let payload_sha256: [u8; 32] = bytes[HEADER_LEN + payload_len..expected_total]
            .try_into()
            .expect("slice len");
        let envelope = Self {
            format_version,
            command_id,
            command_type,
            payload,
            payload_sha256,
        };
        envelope.verify()?;
        Ok(envelope)
    }
}

impl fmt::Debug for CommandEnvelope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommandEnvelope")
            .field("format_version", &self.format_version)
            .field("command_id", &self.command_id)
            .field("command_type", &self.command_type)
            .field("payload_len", &self.payload.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let envelope = CommandEnvelope::new(7, [42u8; 16], b"hello".to_vec());
        let bytes = envelope.encode();
        assert_eq!(CommandEnvelope::decode(&bytes).unwrap(), envelope);
    }

    #[test]
    fn bit_flip_breaks_checksum() {
        let envelope = CommandEnvelope::new(1, [1u8; 16], vec![9u8; 64]);
        let mut bytes = envelope.encode();
        bytes[HEADER_LEN] ^= 0x01;
        assert_eq!(
            CommandEnvelope::decode(&bytes),
            Err(EnvelopeError::ChecksumMismatch)
        );
    }

    #[test]
    fn unknown_version_fails_closed() {
        let mut envelope = CommandEnvelope::new(1, [1u8; 16], vec![]);
        envelope.format_version = COMMAND_ENVELOPE_FORMAT_VERSION + 1;
        envelope.payload_sha256 =
            CommandEnvelope::checksum(envelope.format_version, 1, &envelope.payload);
        assert!(matches!(
            envelope.verify(),
            Err(EnvelopeError::UnsupportedVersion { .. })
        ));
    }

    #[test]
    fn truncation_and_trailing_bytes_fail() {
        let envelope = CommandEnvelope::new(3, [7u8; 16], vec![1, 2, 3]);
        let bytes = envelope.encode();
        assert!(matches!(
            CommandEnvelope::decode(&bytes[..bytes.len() - 1]),
            Err(EnvelopeError::Truncated { .. })
        ));
        let mut longer = bytes.clone();
        longer.push(0);
        assert_eq!(
            CommandEnvelope::decode(&longer),
            Err(EnvelopeError::TrailingBytes(1))
        );
    }
}
